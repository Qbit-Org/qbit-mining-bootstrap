#!/usr/bin/env python3
"""Operator readiness checks for the PRISM pool profile."""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


PUBLIC_QBIT_CHAINS = {"mainnet", "testnet", "testnet3", "testnet4", "signet"}
QBIT_CHAIN_FLAGS = {
    "mainnet": "-chain=main",
    "regtest": "-regtest",
    "testnet": "-testnet",
    "testnet3": "-testnet3",
    "testnet4": "-testnet4",
    "signet": "-signet",
}
QBIT_RPC_CHAIN_ALIASES = {"main": "mainnet"}
BOOL_TRUE = {"1", "true", "yes", "on"}
BOOL_FALSE = {"0", "false", "no", "off"}
LAUNCH_READINESS_FLAG = "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"
PRELAUNCH_TIP_AGE_NAME = "QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"
MAX_SIGNED_INT64 = 9223372036854775807
BITCOIN_CHAIN_FLAGS = {
    "mainnet": "-chain=main",
    "regtest": "-regtest",
    "testnet": "-testnet",
    "testnet3": "-testnet",
    "testnet4": "-testnet4",
    "signet": "-signet",
}


class MissingHighdiffNotificationError(RuntimeError):
    """Raised when Stratum never advertises an initial difficulty."""


@dataclass
class CheckRow:
    status: str
    name: str
    detail: str
    hint: str | None = None


class Reporter:
    def __init__(self) -> None:
        self.rows: list[CheckRow] = []

    def pass_(self, name: str, detail: str) -> None:
        self.rows.append(CheckRow("PASS", name, detail))

    def warn(self, name: str, detail: str, *, hint: str | None = None) -> None:
        self.rows.append(CheckRow("WARN", name, detail, hint))

    def fail(self, name: str, detail: str, *, hint: str | None = None) -> None:
        self.rows.append(CheckRow("FAIL", name, detail, hint))

    @property
    def failed(self) -> bool:
        return any(row.status == "FAIL" for row in self.rows)

    def emit(self) -> None:
        width = max((len(row.name) for row in self.rows), default=0)
        for row in self.rows:
            print(f"{row.status:<4} {row.name:<{width}} {row.detail}")
            if row.hint:
                print(f"     {'':<{width}} hint: {row.hint}")


def env_file_args() -> list[str]:
    upstream = ROOT_DIR / "config" / "upstream.env"
    if not upstream.exists():
        upstream = ROOT_DIR / "config" / "upstream.env.example"
    args = ["--env-file", str(upstream)]
    deploy_env = os.environ.get("DEPLOY_ENV_FILE", "").strip()
    if deploy_env:
        deploy_path = Path(deploy_env)
        if not deploy_path.is_absolute():
            deploy_path = ROOT_DIR / deploy_path
        args.extend(["--env-file", str(deploy_path)])
    else:
        local_env = ROOT_DIR / ".env"
        if local_env.exists():
            args.extend(["--env-file", str(local_env)])
    return args


def compose_base_command() -> list[str]:
    project_name = os.environ.get("COMPOSE_PROJECT_NAME", "qbit-mining-bootstrap")
    return [
        "docker",
        "compose",
        *env_file_args(),
        "-f",
        str(ROOT_DIR / "compose.yaml"),
        "--project-name",
        project_name,
        "--profile",
        "prism",
    ]


def run_command(command: list[str], *, timeout: int = 10) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT_DIR,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def compose_environment(reporter: Reporter) -> dict[str, str]:
    if shutil.which("docker") is None:
        reporter.fail("docker.cli", "docker is not installed", hint="Install Docker with the Compose plugin.")
        return {}
    command = [*compose_base_command(), "config", "--environment"]
    try:
        completed = run_command(command, timeout=30)
    except subprocess.TimeoutExpired:
        reporter.fail("compose.config", "docker compose config timed out")
        return {}
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        reporter.fail(
            "compose.config",
            "docker compose config failed",
            hint=detail[-1] if detail else "Run docker compose config for the full error.",
        )
        return {}
    reporter.pass_("compose.config", "PRISM compose environment resolved")
    resolved: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        resolved[key] = value
    return resolved


def env_value(env: dict[str, str], name: str, default: str = "") -> str:
    return env.get(name, default)


def is_true(value: str) -> bool:
    return value.strip().lower() in BOOL_TRUE


def optional_bool_env(env: dict[str, str], name: str) -> bool | None:
    value = env.get(name, "")
    if value == "":
        return None
    normalized = value.strip().lower()
    if normalized in BOOL_TRUE:
        return True
    if normalized in BOOL_FALSE:
        return False
    raise ValueError(f"{name} must be a true/false style value, got {value!r}")


def launch_readiness_checks_enabled(env: dict[str, str]) -> bool:
    enabled = optional_bool_env(env, LAUNCH_READINESS_FLAG)
    if enabled is None:
        return True
    chain = env_value(env, "QBIT_CHAIN", "regtest").strip().lower() or "regtest"
    if chain != "mainnet":
        raise ValueError(f"{LAUNCH_READINESS_FLAG} is valid only for QBIT_CHAIN=mainnet")
    return enabled


def authorized_mainnet_prelaunch(env: dict[str, str]) -> bool:
    try:
        return (
            env_value(env, "QBIT_CHAIN", "regtest").strip().lower() == "mainnet"
            and optional_bool_env(env, "QBIT_PRODUCTION") is True
            and optional_bool_env(env, "QBIT_TOOLS_PRODUCTION") is True
            and optional_bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE") is False
            and launch_readiness_checks_enabled(env) is False
        )
    except ValueError:
        return False


def launch_readiness_checks_disabled(env: dict[str, str]) -> bool:
    # static_checks reports malformed or incomplete authorization. Live checks
    # must remain strict instead of treating it as prelaunch authorization.
    return authorized_mainnet_prelaunch(env)


def report_launch_dependent_failure(
    env: dict[str, str],
    reporter: Reporter,
    name: str,
    detail: str,
    *,
    hint: str | None = None,
) -> None:
    if launch_readiness_checks_disabled(env):
        reporter.warn(
            name,
            f"{detail}; tolerated only because {LAUNCH_READINESS_FLAG}=0 (prelaunch)",
            hint=f"Set {LAUNCH_READINESS_FLAG}=1 at launch; this condition will then fail the self-check.",
        )
    else:
        reporter.fail(name, detail, hint=hint)


def normalize_qbit_chain_name(value: object) -> str:
    normalized = str(value).strip().lower()
    return QBIT_RPC_CHAIN_ALIASES.get(normalized, normalized)


def production_mode(env: dict[str, str]) -> bool:
    return is_true(env_value(env, "QBIT_PRODUCTION", "0")) or is_true(env_value(env, "QBIT_TOOLS_PRODUCTION", "0"))


def prelaunch_tip_age_seconds(env: dict[str, str]) -> int | None:
    if PRELAUNCH_TIP_AGE_NAME not in env:
        return None

    value = env[PRELAUNCH_TIP_AGE_NAME]
    if value == "":
        raise ValueError(f"{PRELAUNCH_TIP_AGE_NAME} must not be empty")
    if not value.isascii() or not value.isdigit():
        raise ValueError(f"{PRELAUNCH_TIP_AGE_NAME} must be a positive integer")
    normalized = value.lstrip("0") or "0"
    if normalized == "0":
        raise ValueError(f"{PRELAUNCH_TIP_AGE_NAME} must be greater than zero")
    maximum = str(MAX_SIGNED_INT64)
    if len(normalized) > len(maximum) or (
        len(normalized) == len(maximum) and normalized > maximum
    ):
        raise ValueError(
            f"{PRELAUNCH_TIP_AGE_NAME} exceeds qbitd's signed 64-bit integer range"
        )
    seconds = int(normalized)
    production_enabled = optional_bool_env(env, "QBIT_PRODUCTION")
    if production_enabled is not True:
        raise ValueError(f"{PRELAUNCH_TIP_AGE_NAME} is valid only with QBIT_PRODUCTION=1")
    if env_value(env, "QBIT_CHAIN", "regtest") != "mainnet":
        raise ValueError(f"{PRELAUNCH_TIP_AGE_NAME} is valid only with QBIT_CHAIN=mainnet")
    return seconds


def parse_decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{value!r} is not a decimal number") from exc
    if not parsed.is_finite():
        raise ValueError(f"{value!r} is not a finite decimal number")
    return parsed


def parse_host_port(value: str, *, default_host: str, default_port: int) -> tuple[str, int]:
    if not value:
        return default_host, default_port
    if value.isdigit():
        return default_host, int(value)
    host, separator, port = value.rpartition(":")
    if not separator or not port.isdigit():
        raise ValueError(f"expected HOST:PORT or PORT, got {value!r}")
    return host or default_host, int(port)


def static_checks(env: dict[str, str], reporter: Reporter) -> None:
    qbit_chain = env_value(env, "QBIT_CHAIN", "regtest")
    prod = qbit_chain == "mainnet" or production_mode(env)
    qbit_chain_flag = env_value(env, "QBIT_CHAIN_FLAG", "-regtest")
    try:
        launch_checks_enabled = launch_readiness_checks_enabled(env)
    except ValueError as exc:
        reporter.fail(f"env.{LAUNCH_READINESS_FLAG}", str(exc))
    else:
        configured_launch_flag = env_value(env, LAUNCH_READINESS_FLAG)
        if not configured_launch_flag:
            reporter.pass_(
                "launch.readiness",
                f"{LAUNCH_READINESS_FLAG} is unset; launch-dependent checks remain strict",
            )
        elif launch_checks_enabled:
            reporter.pass_("launch.readiness", f"{LAUNCH_READINESS_FLAG}=1; launch checks are strict")
        elif not authorized_mainnet_prelaunch(env):
            reporter.fail(
                "launch.readiness",
                f"{LAUNCH_READINESS_FLAG}=0 requires QBIT_CHAIN=mainnet, "
                "QBIT_PRODUCTION=1, QBIT_TOOLS_PRODUCTION=1, and "
                "CKPOOL_NON_TEST_READINESS_GATE=0",
            )
        else:
            reporter.warn(
                "launch.readiness",
                f"{LAUNCH_READINESS_FLAG}=0; only explicit launch-dependent conditions are relaxed",
                hint=f"Set {LAUNCH_READINESS_FLAG}=1 at launch.",
            )
    if prod and qbit_chain == "regtest":
        reporter.fail("qbit.chain", "production mode cannot use regtest", hint="Set QBIT_CHAIN and QBIT_CHAIN_FLAG for the live network.")
    else:
        reporter.pass_("qbit.chain", f"QBIT_CHAIN={qbit_chain} QBIT_CHAIN_FLAG={qbit_chain_flag}")
    expected_chain_flag = QBIT_CHAIN_FLAGS.get(qbit_chain)
    if expected_chain_flag is not None and qbit_chain_flag != expected_chain_flag:
        reporter.fail("qbit.chain_flag", f"QBIT_CHAIN={qbit_chain} requires QBIT_CHAIN_FLAG={expected_chain_flag}")
    elif qbit_chain not in QBIT_CHAIN_FLAGS:
        reporter.fail("qbit.chain_flag", f"unknown QBIT_CHAIN={qbit_chain}")
    else:
        reporter.pass_("qbit.chain_flag", "chain flag matches the configured qbit chain")

    try:
        tip_age_seconds = prelaunch_tip_age_seconds(env)
    except ValueError as exc:
        reporter.fail(f"env.{PRELAUNCH_TIP_AGE_NAME}", str(exc))
    else:
        if tip_age_seconds is None:
            reporter.pass_(
                f"env.{PRELAUNCH_TIP_AGE_NAME}",
                "not configured; qbitd uses its normal tip-age policy",
            )
        else:
            reporter.pass_(
                f"env.{PRELAUNCH_TIP_AGE_NAME}",
                f"validated reviewed duration of {tip_age_seconds} seconds",
            )

    bitcoin_chain = env_value(env, "BITCOIN_CHAIN", "regtest")
    bitcoin_chain_flag = env_value(env, "BITCOIN_CHAIN_FLAG", "-regtest")
    expected_bitcoin_chain_flag = BITCOIN_CHAIN_FLAGS.get(bitcoin_chain)
    if expected_bitcoin_chain_flag is None:
        reporter.fail("bitcoin.chain_flag", f"unknown BITCOIN_CHAIN={bitcoin_chain}")
    elif bitcoin_chain_flag != expected_bitcoin_chain_flag:
        reporter.fail(
            "bitcoin.chain_flag",
            f"BITCOIN_CHAIN={bitcoin_chain} requires BITCOIN_CHAIN_FLAG={expected_bitcoin_chain_flag}",
        )
    else:
        reporter.pass_("bitcoin.chain_flag", "chain flag matches the configured Bitcoin chain")

    expected_genesis_hash = env_value(env, "QBIT_EXPECTED_GENESIS_HASH").strip()
    if qbit_chain == "mainnet" and not expected_genesis_hash:
        reporter.fail(
            "qbit.genesis_config",
            "QBIT_CHAIN=mainnet requires QBIT_EXPECTED_GENESIS_HASH",
            hint="Pin the final release genesis hash before starting the pool.",
        )
    elif expected_genesis_hash and (
        len(expected_genesis_hash) != 64
        or any(character not in "0123456789abcdefABCDEF" for character in expected_genesis_hash)
    ):
        reporter.fail("qbit.genesis_config", "QBIT_EXPECTED_GENESIS_HASH must be 64 hex characters")
    elif expected_genesis_hash:
        reporter.pass_("qbit.genesis_config", expected_genesis_hash.lower())
    else:
        reporter.pass_("qbit.genesis_config", "not pinned for this test chain")

    if prod:
        qbit_git_commit = env_value(env, "QBIT_GIT_COMMIT").strip()
        if len(qbit_git_commit) != 40 or any(
            character not in "0123456789abcdefABCDEF" for character in qbit_git_commit
        ):
            reporter.fail(
                "qbit.source_pin",
                "production requires QBIT_GIT_COMMIT as exactly 40 hex characters",
            )
        else:
            reporter.pass_("qbit.source_pin", qbit_git_commit.lower())

    if prod:
        for name in ("CKPOOL_GIT_REF", "CPUMINER_GIT_REF"):
            source_ref = env_value(env, name).strip()
            if len(source_ref) != 40 or any(
                character not in "0123456789abcdefABCDEF" for character in source_ref
            ):
                reporter.fail(
                    f"source.{name}",
                    f"production requires {name} as exactly 40 hex characters",
                )
            else:
                reporter.pass_(f"source.{name}", source_ref.lower())

    required_keys = (
        "PRISM_MANIFEST_SIGNING_SEED_HEX",
        "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX",
        "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX",
    )
    for name in required_keys:
        if env_value(env, name):
            reporter.pass_(f"env.{name}", "configured")
        else:
            reporter.fail(
                f"env.{name}",
                "missing",
                hint="Set real PRISM signing material before accepting miners.",
            )

    writer_defaults = {
        "PRISM_LEDGER_WRITER_ID": "prism-coordinator",
        "PRISM_LEDGER_WRITER_EPOCH": "1",
    }
    for name, default in writer_defaults.items():
        value = env_value(env, name, default)
        if value:
            reporter.pass_(f"env.{name}", value)
        else:
            reporter.fail(f"env.{name}", "missing")

    forbidden_flags = (
        "PRISM_ALLOW_MEMORY_LEDGER",
        "PRISM_ALLOW_TEST_SIGNING_SEEDS",
        "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY",
        "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN",
    )
    for name in forbidden_flags:
        value = env_value(env, name, "0")
        if prod and is_true(value):
            reporter.fail(f"env.{name}", "forbidden in production", hint=f"Set {name}=0.")
        elif is_true(value):
            reporter.warn(f"env.{name}", "test-only bypass enabled", hint="Keep this disabled for deploys.")
        else:
            reporter.pass_(f"env.{name}", "disabled")

    default_database_url = (
        "postgresql://"
        f"{env_value(env, 'PRISM_POSTGRES_USER', 'qbit')}:"
        f"{env_value(env, 'PRISM_POSTGRES_PASSWORD', 'change-this')}"
        f"@prism-postgres:5432/{env_value(env, 'PRISM_POSTGRES_DB', 'qbit')}"
    )
    database_url = env_value(env, "PRISM_DATABASE_URL", default_database_url)
    psql_command = env_value(env, "PRISM_POSTGRES_PSQL_COMMAND")
    if database_url or psql_command:
        reporter.pass_("ledger.database", "Postgres ledger configuration is present")
    elif prod:
        reporter.fail("ledger.database", "production requires PRISM_DATABASE_URL or PRISM_POSTGRES_PSQL_COMMAND")
    else:
        reporter.warn("ledger.database", "no Postgres ledger configuration found")
    postgres_password = env_value(env, "PRISM_POSTGRES_PASSWORD", "change-this")
    if prod and postgres_password == "change-this":
        reporter.fail("ledger.database_auth", "production cannot use the default PRISM_POSTGRES_PASSWORD")
    elif postgres_password == "change-this":
        reporter.warn("ledger.database_auth", "using default PRISM_POSTGRES_PASSWORD", hint="Set a real password before deploy.")
    else:
        reporter.pass_("ledger.database_auth", "Postgres password is non-default")

    audit_dir = env_value(env, "PRISM_AUDIT_DIR", "/var/lib/qbit-prism/audit")
    evidence_path = env_value(env, "PRISM_EVIDENCE_PATH", "/var/lib/qbit-prism/audit/prism-live-evidence.json")
    if audit_dir and evidence_path:
        reporter.pass_("audit.paths", f"dir={audit_dir} evidence={evidence_path}")
    else:
        reporter.fail("audit.paths", "PRISM_AUDIT_DIR and PRISM_EVIDENCE_PATH are required")

    qbit_user = env_value(env, "QBIT_RPC_USER", "qbitrpc")
    qbit_password = env_value(env, "QBIT_RPC_PASSWORD", "change-this")
    if not qbit_user or not qbit_password:
        reporter.fail("qbit.rpc_auth", "QBIT_RPC_USER and QBIT_RPC_PASSWORD are required")
    elif prod and qbit_password == "change-this":
        reporter.fail("qbit.rpc_auth", "production cannot use the default qbit RPC password")
    elif qbit_password == "change-this":
        reporter.warn("qbit.rpc_auth", "using default qbit RPC password", hint="Set a real password before deploy.")
    else:
        reporter.pass_("qbit.rpc_auth", "RPC credentials configured")

    try:
        min_ready = int(env_value(env, "PRISM_MIN_READY_MINERS", "3"))
        if min_ready <= 0:
            raise ValueError
        reporter.pass_("mining.min_ready", f"{min_ready} miners")
    except ValueError:
        reporter.fail("mining.min_ready", "PRISM_MIN_READY_MINERS must be a positive integer")

    try:
        share_diff = parse_decimal(env_value(env, "PRISM_STRATUM_SHARE_DIFF", "0.000000001"))
        if share_diff <= 0:
            raise ValueError("share difficulty must be positive")
        reporter.pass_("mining.share_diff", f"{share_diff}")
    except ValueError as exc:
        reporter.fail("mining.share_diff", str(exc))

    stale_grace = env_value(env, "PRISM_STRATUM_STALE_GRACE_SECONDS", "3").strip()
    if qbit_chain == "mainnet" and stale_grace != "0":
        reporter.fail(
            "mining.stale_grace",
            "mainnet requires PRISM_STRATUM_STALE_GRACE_SECONDS=0",
            hint="Enable stale-credit grace only after proving verifier compatibility for the deployed release.",
        )
    else:
        try:
            stale_grace_value = int(stale_grace)
            if stale_grace_value < 0:
                raise ValueError
        except ValueError:
            reporter.fail("mining.stale_grace", "PRISM_STRATUM_STALE_GRACE_SECONDS must be a non-negative integer")
        else:
            reporter.pass_("mining.stale_grace", f"{stale_grace_value} seconds")

    if prod and bitcoin_chain != "regtest":
        qbit_miner_address = env_value(env, "QBIT_MINER_ADDRESS", "auto").strip()
        bitcoin_miner_address = env_value(env, "BITCOIN_MINER_ADDRESS", "auto").strip()
        if not qbit_miner_address or qbit_miner_address == "auto":
            reporter.fail("auxpow.qbit_payout", "production AuxPoW requires an explicit QBIT_MINER_ADDRESS")
        else:
            reporter.pass_("auxpow.qbit_payout", "explicit payout configured")
        if not bitcoin_miner_address or bitcoin_miner_address == "auto":
            reporter.fail(
                "auxpow.bitcoin_payout",
                "production AuxPoW requires an explicit BITCOIN_MINER_ADDRESS",
            )
        else:
            reporter.pass_("auxpow.bitcoin_payout", "explicit payout configured")

    if is_true(env_value(env, "PRISM_STRATUM_VARDIFF", "1")):
        try:
            min_diff = parse_decimal(env_value(env, "PRISM_STRATUM_VARDIFF_MIN_DIFF", "0.000000001"))
            max_diff = parse_decimal(env_value(env, "PRISM_STRATUM_VARDIFF_MAX_DIFF", "1024"))
            start_diff = parse_decimal(env_value(env, "PRISM_STRATUM_VARDIFF_START_DIFF", str(min_diff)))
            if min_diff <= 0 or max_diff <= 0 or start_diff <= 0:
                raise ValueError("vardiff difficulty values must be positive")
            if min_diff > max_diff:
                raise ValueError("PRISM_STRATUM_VARDIFF_MIN_DIFF exceeds PRISM_STRATUM_VARDIFF_MAX_DIFF")
            if start_diff > max_diff:
                raise ValueError("PRISM_STRATUM_VARDIFF_START_DIFF exceeds PRISM_STRATUM_VARDIFF_MAX_DIFF")
            reporter.pass_("mining.vardiff", f"enabled start={start_diff} range={min_diff}..{max_diff}")
        except ValueError as exc:
            reporter.fail("mining.vardiff", str(exc))
    else:
        reporter.warn("mining.vardiff", "vardiff is disabled")

    if prod:
        difficulty_names = (
            "PRISM_STRATUM_SHARE_DIFF",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF",
            "PRISM_STRATUM_VARDIFF_START_DIFF",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF",
        )
        try:
            production_difficulties: dict[str, Decimal] = {}
            for name in difficulty_names:
                raw_value = env_value(env, name).strip()
                if not raw_value:
                    raise ValueError(f"production requires an explicit {name}")
                value = parse_decimal(raw_value)
                if value <= 0:
                    raise ValueError(f"{name} must be positive")
                if value == Decimal("0.000000001"):
                    raise ValueError(f"{name} cannot use the lab-only 1e-9 difficulty")
                production_difficulties[name] = value
            if (
                production_difficulties["PRISM_STRATUM_VARDIFF_MIN_DIFF"]
                > production_difficulties["PRISM_STRATUM_VARDIFF_START_DIFF"]
            ):
                raise ValueError("production vardiff minimum exceeds its start difficulty")
            if (
                production_difficulties["PRISM_STRATUM_VARDIFF_START_DIFF"]
                > production_difficulties["PRISM_STRATUM_VARDIFF_MAX_DIFF"]
            ):
                raise ValueError("production vardiff start exceeds its maximum difficulty")
        except ValueError as exc:
            reporter.fail("mining.production_difficulty", str(exc))
        else:
            reporter.pass_(
                "mining.production_difficulty",
                "reviewed share and bounded vardiff values configured",
            )

    if env_value(env, "PRISM_STRATUM_HIGHDIFF_PORT"):
        try:
            highdiff_port = int(env_value(env, "PRISM_STRATUM_HIGHDIFF_PORT"))
            if not 0 < highdiff_port < 65536:
                raise ValueError("PRISM_STRATUM_HIGHDIFF_PORT must be a valid TCP port")
            highdiff_min = parse_decimal(env_value(env, "PRISM_STRATUM_HIGHDIFF_MIN_DIFF", "500000"))
            highdiff_start = parse_decimal(env_value(env, "PRISM_STRATUM_HIGHDIFF_START_DIFF", "500000"))
            highdiff_max = parse_decimal(env_value(env, "PRISM_STRATUM_HIGHDIFF_MAX_DIFF", "4294967296"))
            if highdiff_min <= 0 or highdiff_start <= 0 or highdiff_max <= 0:
                raise ValueError("high-diff difficulty values must be positive")
            if highdiff_min > highdiff_start:
                raise ValueError("PRISM_STRATUM_HIGHDIFF_MIN_DIFF exceeds PRISM_STRATUM_HIGHDIFF_START_DIFF")
            if highdiff_start > highdiff_max:
                raise ValueError("PRISM_STRATUM_HIGHDIFF_START_DIFF exceeds PRISM_STRATUM_HIGHDIFF_MAX_DIFF")
            # Match the coordinator: an unset OR empty fixed difficulty tracks
            # the start difficulty (compose resolves the default to "").
            highdiff_share_raw = env_value(env, "PRISM_STRATUM_HIGHDIFF_SHARE_DIFF", "").strip()
            highdiff_share = parse_decimal(highdiff_share_raw) if highdiff_share_raw else highdiff_start
            if highdiff_share < highdiff_min or highdiff_share > highdiff_max:
                raise ValueError("PRISM_STRATUM_HIGHDIFF_SHARE_DIFF is outside the min/max bounds")
            reporter.pass_(
                "mining.highdiff",
                f"enabled port={highdiff_port} start={highdiff_start} range={highdiff_min}..{highdiff_max}",
            )
        except ValueError as exc:
            reporter.fail("mining.highdiff", str(exc))
    else:
        reporter.pass_("mining.highdiff", "disabled (single stratum listener)")

    if is_true(env_value(env, "PRISM_POOL_FEE_ENABLED", "0")):
        fee_bps = env_value(env, "PRISM_POOL_FEE_BPS")
        fee_address = env_value(env, "PRISM_POOL_FEE_ADDRESS")
        fee_program = env_value(env, "PRISM_POOL_FEE_P2MR_PROGRAM_HEX")
        if not fee_bps:
            reporter.fail("pool.fee", "PRISM_POOL_FEE_ENABLED=1 requires PRISM_POOL_FEE_BPS")
        elif not fee_address and not fee_program:
            reporter.fail("pool.fee", "pool fee requires PRISM_POOL_FEE_ADDRESS or PRISM_POOL_FEE_P2MR_PROGRAM_HEX")
        else:
            reporter.pass_("pool.fee", f"enabled bps={fee_bps}")
    else:
        reporter.pass_("pool.fee", "disabled")

    if is_true(env_value(env, "PRISM_CTV_SETTLEMENT_ENABLED", "0")):
        market_rate = env_value(
            env,
            "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
        ).strip()
        premium = env_value(env, "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS", "12000").strip()
        try:
            premium_value = int(premium)
            if premium_value <= 0:
                raise ValueError
        except ValueError:
            reporter.fail("ctv.fee_premium", "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS must be a positive integer")
        else:
            reporter.pass_("ctv.fee_premium", f"{premium_value} bps")

        if market_rate:
            try:
                market_rate_value = int(market_rate)
                if market_rate_value <= 0:
                    raise ValueError
            except ValueError:
                reporter.fail(
                    "ctv.fee_source",
                    "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT must be a positive integer",
                )
            else:
                reporter.pass_("ctv.fee_source", f"explicit rate={market_rate_value} bits/1000 weight")
        else:
            if qbit_chain == "mainnet":
                reporter.fail(
                    "ctv.fee_source",
                    "mainnet requires PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
                )
            else:
                try:
                    estimate_target = int(env_value(env, "PRISM_CTV_FANOUT_FEE_ESTIMATE_TARGET_BLOCKS", "2"))
                    if estimate_target <= 0:
                        raise ValueError
                except ValueError:
                    reporter.fail(
                        "ctv.fee_source",
                        "PRISM_CTV_FANOUT_FEE_ESTIMATE_TARGET_BLOCKS must be a positive integer",
                    )
                else:
                    reporter.warn(
                        "ctv.fee_source",
                        f"estimatesmartfee target={estimate_target}; live preflight required",
                        hint=(
                            "Fresh chains and chains with empty blocks have no empirical fee history. "
                            "Configure a positive explicit fanout fee rate until estimatesmartfee succeeds."
                        ),
                    )
    else:
        reporter.pass_("ctv.fee_source", "CTV settlement disabled")


def highdiff_probe_target(env: dict[str, str]) -> tuple[str, int] | None:
    """Host/port for probing the published high-diff listener.

    Only the host publish mapping counts: probing the container listen port
    would pass even when miners cannot reach the published port. Returns None
    when nothing usable is published (unset, empty, or the ephemeral
    loopback-port-0 default used while the listener is disabled)."""
    value = env_value(env, "PRISM_STRATUM_HIGHDIFF_PORT_HOST", "").strip()
    if not value:
        return None
    host, port = parse_host_port(value, default_host="127.0.0.1", default_port=4334)
    if port <= 0:
        return None
    return host, port


def tcp_connect_check(name: str, host: str, port: int, reporter: Reporter) -> None:
    try:
        with socket.create_connection((host, port), timeout=3):
            pass
    except OSError as exc:
        reporter.fail(name, f"cannot connect to {host}:{port}: {exc}", hint="Start the PRISM profile or check host port mapping.")
    else:
        reporter.pass_(name, f"reachable at {host}:{port}")


def stratum_first_advertised_difficulty(
    host: str,
    port: int,
    *,
    username: str,
    password: str = "x",
    timeout: float = 10.0,
) -> Decimal:
    """Subscribe and authorize like a rig, returning the first
    mining.set_difficulty the pool advertises on this connection.

    This mirrors the marketplace verification handshake: NiceHash-style
    probes judge the first advertised difficulty, so TCP reachability alone
    is not evidence that a listener honors its floor."""
    deadline = time.monotonic() + timeout
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        for request in (
            {"id": 1, "method": "mining.subscribe", "params": ["prism-self-check/1"]},
            {"id": 2, "method": "mining.authorize", "params": [username, password]},
        ):
            sock.sendall((json.dumps(request) + "\n").encode())
        buffer = b""
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(4096)
            except socket.timeout as exc:
                raise MissingHighdiffNotificationError(
                    "timed out waiting for mining.set_difficulty"
                ) from exc
            if not chunk:
                raise MissingHighdiffNotificationError(
                    "connection closed before mining.set_difficulty"
                )
            buffer += chunk
            while b"\n" in buffer:
                line, _, buffer = buffer.partition(b"\n")
                if not line.strip():
                    continue
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(message, dict):
                    continue
                if message.get("id") == 2 and (message.get("error") or message.get("result") is False):
                    raise RuntimeError(f"mining.authorize rejected: {message.get('error')}")
                if message.get("method") == "mining.set_difficulty":
                    params = message.get("params")
                    if not isinstance(params, list) or not params:
                        raise RuntimeError("mining.set_difficulty carried no difficulty")
                    return parse_decimal(str(params[0]))
        raise MissingHighdiffNotificationError("timed out waiting for mining.set_difficulty")


def check_highdiff_advertised_floor(env: dict[str, str], host: str, port: int, reporter: Reporter) -> None:
    floor_raw = env_value(env, "PRISM_STRATUM_HIGHDIFF_MIN_DIFF", "").strip() or "500000"
    try:
        floor = parse_decimal(floor_raw)
    except ValueError as exc:
        reporter.fail("stratum.highdiff_floor", f"PRISM_STRATUM_HIGHDIFF_MIN_DIFF: {exc}")
        return
    username = env_value(env, "PRISM_USERNAME_FALLBACK_ADDRESS", "").strip() or "prism-self-check"
    try:
        advertised = stratum_first_advertised_difficulty(host, port, username=username)
    except MissingHighdiffNotificationError as exc:
        report_launch_dependent_failure(
            env,
            reporter,
            "stratum.highdiff_floor",
            f"stratum handshake with {host}:{port} did not advertise a difficulty: {exc}",
            hint="Wait for a usable block template/job and check coordinator logs.",
        )
        return
    except (OSError, RuntimeError, ValueError) as exc:
        reporter.fail(
            "stratum.highdiff_floor",
            f"stratum handshake with {host}:{port} failed: {exc}",
            hint="Marketplace verification needs subscribe/authorize to answer with mining.set_difficulty; check coordinator logs and PRISM_USERNAME_FALLBACK_ADDRESS.",
        )
        return
    if advertised < floor:
        reporter.fail(
            "stratum.highdiff_floor",
            f"first mining.set_difficulty advertises {advertised}, below the {floor} floor",
            hint="Rental marketplaces judge the first advertised difficulty; the high-diff listener must clamp stamped jobs to its floor.",
        )
    else:
        reporter.pass_(
            "stratum.highdiff_floor",
            f"first mining.set_difficulty advertises {advertised} (floor {floor})",
        )


def qbit_rpc_call(env: dict[str, str], method: str, params: list[object] | None = None) -> object:
    host, port = parse_host_port(env_value(env, "QBIT_RPC_PORT_HOST", "127.0.0.1:18452"), default_host="127.0.0.1", default_port=18452)
    user = env_value(env, "QBIT_RPC_USER", "qbitrpc")
    password = env_value(env, "QBIT_RPC_PASSWORD", "change-this")
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    request = urllib.request.Request(
        f"http://{host}:{port}/",
        data=json.dumps(
            {
                "jsonrpc": "1.0",
                "id": "prism-self-check",
                "method": method,
                "params": params or [],
            }
        ).encode(),
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.loads(response.read())
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return payload.get("result")


def qbit_live_checks(env: dict[str, str], reporter: Reporter) -> None:
    try:
        blockchain_info = qbit_rpc_call(env, "getblockchaininfo")
    except (OSError, urllib.error.URLError, RuntimeError, ValueError) as exc:
        reporter.fail("qbit.rpc", f"RPC probe failed: {exc}", hint="Check qbitd, QBIT_RPC_PORT_HOST, and RPC credentials.")
        return
    if not isinstance(blockchain_info, dict):
        reporter.fail("qbit.rpc", "getblockchaininfo returned a non-object response")
        return
    expected_chain = env_value(env, "QBIT_CHAIN", "regtest")
    actual_chain = str(blockchain_info.get("chain", ""))
    if normalize_qbit_chain_name(actual_chain) != normalize_qbit_chain_name(expected_chain):
        reporter.fail("qbit.rpc_chain", f"qbitd reports chain={actual_chain}, expected {expected_chain}")
    else:
        reporter.pass_("qbit.rpc_chain", f"qbitd reports {actual_chain}")
    public_chain = expected_chain in PUBLIC_QBIT_CHAINS
    if (
        blockchain_info.get("initialblockdownload") is not False
        if public_chain
        else bool(blockchain_info.get("initialblockdownload"))
    ):
        report_launch_dependent_failure(
            env,
            reporter,
            "qbit.ibd",
            "qbitd is still in initial block download or did not report readiness",
        )
    else:
        reporter.pass_("qbit.ibd", "qbitd is not in initial block download")
    if public_chain:
        try:
            blocks = int(blockchain_info["blocks"])
            headers = int(blockchain_info["headers"])
            if blocks < 0 or headers < 0:
                raise ValueError("negative height")
        except (KeyError, TypeError, ValueError) as exc:
            reporter.fail("qbit.headers", f"qbitd did not report valid blocks and headers: {exc}")
        else:
            if blocks != headers:
                reporter.fail("qbit.headers", f"qbitd is not caught up: blocks={blocks} headers={headers}")
            else:
                reporter.pass_("qbit.headers", f"blocks={blocks} headers={headers}")

        try:
            template_rules = ["segwit"]
            if expected_chain == "signet":
                template_rules.append("signet")
            template = qbit_rpc_call(env, "getblocktemplate", [{"rules": template_rules}])
            if not isinstance(template, dict) or not template.get("previousblockhash"):
                raise ValueError("missing previousblockhash")
            template_time = int(template["curtime"])
            max_age = int(env_value(env, "PRISM_TEMPLATE_MAX_AGE_SECONDS", "120"))
            if max_age < 0:
                raise ValueError("PRISM_TEMPLATE_MAX_AGE_SECONDS must be non-negative")
            template_age = int(time.time()) - template_time
            if template_age > max_age:
                raise ValueError(f"template age {template_age}s exceeds {max_age}s")
        except (OSError, urllib.error.URLError, RuntimeError, ValueError, KeyError) as exc:
            reporter.fail("qbit.template", f"public-chain template preflight failed: {exc}")
        else:
            reporter.pass_("qbit.template", f"fresh template age={template_age}s")

    expected_genesis_hash = env_value(env, "QBIT_EXPECTED_GENESIS_HASH").strip().lower()
    if expected_genesis_hash:
        try:
            actual_genesis_hash = str(qbit_rpc_call(env, "getblockhash", [0])).lower()
        except (OSError, urllib.error.URLError, RuntimeError, ValueError) as exc:
            reporter.fail("qbit.genesis", f"could not read genesis hash: {exc}")
        else:
            if actual_genesis_hash != expected_genesis_hash:
                reporter.fail(
                    "qbit.genesis",
                    f"qbitd reports genesis={actual_genesis_hash}, expected {expected_genesis_hash}",
                )
            else:
                reporter.pass_("qbit.genesis", actual_genesis_hash)

    if (
        is_true(env_value(env, "PRISM_CTV_SETTLEMENT_ENABLED", "0"))
        and not env_value(
            env,
            "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
        ).strip()
    ):
        try:
            estimate_target = int(env_value(env, "PRISM_CTV_FANOUT_FEE_ESTIMATE_TARGET_BLOCKS", "2"))
            estimate = qbit_rpc_call(env, "estimatesmartfee", [estimate_target])
            if not isinstance(estimate, dict):
                raise ValueError("returned a non-object response")
            if estimate.get("errors"):
                raise ValueError(f"returned errors: {estimate['errors']}")
            fee_rate = parse_decimal(str(estimate.get("feerate", "")))
            if not fee_rate.is_finite() or fee_rate <= 0:
                raise ValueError(f"returned invalid feerate: {estimate.get('feerate')!r}")
        except (OSError, urllib.error.URLError, RuntimeError, ValueError) as exc:
            reporter.fail(
                "ctv.fee_estimator",
                f"estimatesmartfee preflight failed: {exc}",
                hint=(
                    "Fresh chains and empty blocks do not build fee-estimation history. Set "
                    "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT to a positive "
                    "operator-reviewed rate."
                ),
            )
        else:
            reporter.pass_("ctv.fee_estimator", f"feerate={fee_rate} target={estimate_target}")

    configured_ctv_rate = env_value(
        env,
        "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
    ).strip()
    if is_true(env_value(env, "PRISM_CTV_SETTLEMENT_ENABLED", "0")) and configured_ctv_rate:
        try:
            mempool_info = qbit_rpc_call(env, "getmempoolinfo")
            if not isinstance(mempool_info, dict):
                raise ValueError("returned a non-object response")
            relay_rates = []
            for name in ("minrelaytxfee", "mempoolminfee"):
                if mempool_info.get(name) is None:
                    continue
                rate = parse_decimal(str(mempool_info[name]))
                if not rate.is_finite() or rate <= 0:
                    raise ValueError(f"{name} is not positive")
                relay_rates.append(
                    int((rate * Decimal(100_000_000)).to_integral_value(rounding=ROUND_CEILING))
                )
            if not relay_rates:
                raise ValueError("relay fee floor was not reported")
            configured = int(configured_ctv_rate)
            required = max(relay_rates)
            if configured < required:
                raise ValueError(
                    f"configured={configured} required={required} bits/1000 weight"
                )
        except (OSError, urllib.error.URLError, RuntimeError, ValueError) as exc:
            reporter.fail("ctv.relay_floor", f"CTV fee rate is below or cannot verify relay floor: {exc}")
        else:
            reporter.pass_("ctv.relay_floor", f"configured={configured} required={required}")

    try:
        network_info = qbit_rpc_call(env, "getnetworkinfo")
        peers = int(network_info.get("connections", 0)) if isinstance(network_info, dict) else 0
    except (OSError, urllib.error.URLError, RuntimeError, ValueError) as exc:
        if expected_chain in PUBLIC_QBIT_CHAINS:
            reporter.fail("qbit.peers", f"could not verify public-chain peer count: {exc}")
        else:
            reporter.warn("qbit.peers", f"could not read peer count: {exc}")
        return
    if expected_chain in PUBLIC_QBIT_CHAINS and peers <= 0:
        reporter.fail("qbit.peers", "public-chain qbitd has no peers")
    else:
        reporter.pass_("qbit.peers", f"{peers} peers")


def compose_exec(service: str, code: str, *, timeout: int = 10) -> subprocess.CompletedProcess[str]:
    return run_command([*compose_base_command(), "exec", "-T", service, "python3", "-c", code], timeout=timeout)


def check_ready_miner_threshold(payload: dict[str, object], env: dict[str, str], reporter: Reporter) -> None:
    try:
        ready_miner_count = int(payload.get("ready_miner_count", -1))
    except (TypeError, ValueError):
        reporter.fail("coordinator.ready_miners", f"invalid ready_miner_count in /healthz payload: {payload.get('ready_miner_count')!r}")
        return
    try:
        min_ready_miners = int(env_value(env, "PRISM_MIN_READY_MINERS", "3"))
    except ValueError:
        reporter.fail("coordinator.ready_miners", "PRISM_MIN_READY_MINERS must be an integer")
        return
    if ready_miner_count < min_ready_miners:
        report_launch_dependent_failure(
            env,
            reporter,
            "coordinator.ready_miners",
            f"{ready_miner_count}/{min_ready_miners} miners ready",
            hint="Connect miners and wait for accepted PRISM shares before treating the pool as block-ready.",
        )
    else:
        reporter.pass_("coordinator.ready_miners", f"{ready_miner_count}/{min_ready_miners} miners ready")


def coordinator_live_checks(env: dict[str, str], reporter: Reporter) -> None:
    try:
        stratum_host, stratum_port = parse_host_port(
            env_value(env, "PRISM_STRATUM_PORT_HOST", "3340"),
            default_host="127.0.0.1",
            default_port=3340,
        )
    except ValueError as exc:
        reporter.fail("stratum.port", str(exc))
    else:
        tcp_connect_check("stratum.tcp", stratum_host, stratum_port, reporter)

    if env_value(env, "PRISM_STRATUM_HIGHDIFF_PORT"):
        try:
            highdiff_target = highdiff_probe_target(env)
        except ValueError as exc:
            reporter.fail("stratum.highdiff_port", str(exc))
        else:
            if highdiff_target is None:
                reporter.fail(
                    "stratum.highdiff_port",
                    "PRISM_STRATUM_HIGHDIFF_PORT is set but PRISM_STRATUM_HIGHDIFF_PORT_HOST does not publish a host port",
                    hint="Set PRISM_STRATUM_HIGHDIFF_PORT_HOST (e.g. 4334) so miners can reach the high-diff listener.",
                )
            else:
                tcp_connect_check("stratum.highdiff_tcp", highdiff_target[0], highdiff_target[1], reporter)
                check_highdiff_advertised_floor(env, highdiff_target[0], highdiff_target[1], reporter)

    health_code = r"""
import json
import os
import urllib.request
host = os.environ.get("PRISM_AUDIT_BIND", "127.0.0.1") or "127.0.0.1"
if host in ("0.0.0.0", "::"):
    host = "127.0.0.1"
port = int(os.environ.get("PRISM_AUDIT_PORT", "0") or "0")
if port <= 0:
    raise SystemExit("PRISM_AUDIT_PORT is disabled")
with urllib.request.urlopen(f"http://{host}:{port}/healthz", timeout=3) as response:
    print(response.read().decode())
"""
    completed = compose_exec("prism-coordinator", health_code, timeout=10)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        reporter.fail(
            "coordinator.healthz",
            "could not read /healthz inside prism-coordinator",
            hint=detail[-1] if detail else "Start prism-coordinator.",
        )
    else:
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            reporter.fail("coordinator.healthz", f"/healthz returned invalid JSON: {exc}")
        else:
            ready = payload.get("ok") is True
            ready_miner_count = payload.get("ready_miner_count", "?")
            accepted_share_count = payload.get("accepted_share_count", "?")
            if ready:
                reporter.pass_(
                    "coordinator.healthz",
                    f"ok, ready_miner_count={ready_miner_count}, accepted_share_count={accepted_share_count}",
                )
                check_ready_miner_threshold(payload, env, reporter)
            else:
                reporter.fail("coordinator.healthz", f"unhealthy payload: {payload}")

    audit_code = r"""
import os
import pathlib
import tempfile
audit_dir = pathlib.Path(os.environ.get("PRISM_AUDIT_DIR", "/var/lib/qbit-prism/audit"))
audit_dir.mkdir(parents=True, exist_ok=True)
with tempfile.NamedTemporaryFile(prefix=".self-check-", dir=audit_dir, delete=True) as handle:
    handle.write(b"ok")
"""
    completed = compose_exec("prism-coordinator", audit_code, timeout=10)
    if completed.returncode == 0:
        reporter.pass_("audit.writable", "PRISM_AUDIT_DIR is writable inside prism-coordinator")
    else:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        reporter.fail("audit.writable", "PRISM_AUDIT_DIR is not writable", hint=detail[-1] if detail else None)


def postgres_live_check(env: dict[str, str], reporter: Reporter) -> None:
    user = env_value(env, "PRISM_POSTGRES_USER", "qbit")
    database = env_value(env, "PRISM_POSTGRES_DB", "qbit")
    completed = run_command(
        [*compose_base_command(), "exec", "-T", "prism-postgres", "pg_isready", "-U", user, "-d", database],
        timeout=10,
    )
    if completed.returncode == 0:
        reporter.pass_("postgres.ready", completed.stdout.strip() or "pg_isready passed")
    else:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        reporter.fail("postgres.ready", "Postgres is not ready", hint=detail[-1] if detail else "Start prism-postgres.")


def live_checks(env: dict[str, str], reporter: Reporter) -> None:
    qbit_live_checks(env, reporter)
    postgres_live_check(env, reporter)
    coordinator_live_checks(env, reporter)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-live",
        action="store_true",
        help="only check static PRISM configuration; skip qbitd, Postgres, Stratum, and coordinator probes",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    reporter = Reporter()
    env = compose_environment(reporter)
    if env:
        static_checks(env, reporter)
        if args.skip_live:
            reporter.warn("live.probes", "skipped by --skip-live")
        else:
            live_checks(env, reporter)
    reporter.emit()
    return 1 if reporter.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
