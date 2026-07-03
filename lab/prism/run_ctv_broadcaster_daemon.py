#!/usr/bin/env python3
"""Run the PRISM CTV fanout broadcaster daemon."""

from __future__ import annotations

import os
import shlex
import time
from pathlib import Path

import sys

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism.ctv_broadcaster import CtvFanoutBroadcaster
from lab.prism.ctv_broadcaster_daemon import CtvFanoutBroadcastDaemon, CtvFanoutDaemonResult
from lab.prism.share_ledger import PsqlShareLedger, SingleWriterShareLedger
from lab.prism.prism_coordinator import JsonRpc, env, env_bool, env_int, env_positive_float


def env_positive_int(name: str, default: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None:
        if default is None:
            raise SystemExit(f"{name} is required")
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise SystemExit(f"{name} must be an integer") from exc
    if value <= 0:
        raise SystemExit(f"{name} must be positive")
    return value


def env_nonnegative_int(name: str, default: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None:
        if default is None:
            raise SystemExit(f"{name} is required")
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise SystemExit(f"{name} must be an integer") from exc
    if value < 0:
        raise SystemExit(f"{name} must be non-negative")
    return value


def env_nonnegative_int_with_legacy(primary_name: str, legacy_name: str, default: int) -> int:
    if os.environ.get(primary_name, "") != "":
        return env_nonnegative_int(primary_name, default)
    return env_nonnegative_int(legacy_name, default)


def make_ledger_from_env() -> SingleWriterShareLedger | PsqlShareLedger:
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
    return PsqlShareLedger(
        psql_command=psql_command,
        writer_id=env("PRISM_LEDGER_WRITER_ID", "prism-ctv-broadcaster"),
        writer_epoch=env_int("PRISM_LEDGER_WRITER_EPOCH", 1),
        writer_session_token=os.environ.get("PRISM_LEDGER_WRITER_SESSION_TOKEN"),
        initialize_schema=env("PRISM_POSTGRES_INIT_SCHEMA", "0") in {"1", "true", "yes"},
        lease_ttl_seconds=env_positive_float("PRISM_LEDGER_LEASE_TTL_SECONDS", 60.0),
    )


def make_daemon_from_env() -> CtvFanoutBroadcastDaemon:
    rpc = JsonRpc(
        host=env("QBIT_RPC_HOST"),
        port=env_int("QBIT_RPC_PORT", 18452),
        user=env("QBIT_RPC_USER"),
        password=env("QBIT_RPC_PASSWORD"),
    )
    wallet = os.environ.get("PRISM_CTV_BROADCASTER_WALLET") or None
    fee_sats = env_nonnegative_int_with_legacy(
        "PRISM_CTV_BROADCASTER_FEE_BITS",
        "PRISM_CTV_BROADCASTER_FEE_SATS",
        0,
    )
    if fee_sats > 0 and wallet is None:
        raise SystemExit(
            "PRISM_CTV_BROADCASTER_WALLET is required when "
            "PRISM_CTV_BROADCASTER_FEE_BITS is positive"
        )
    broadcaster = CtvFanoutBroadcaster(rpc.call, funding_wallet=wallet)
    return CtvFanoutBroadcastDaemon(make_ledger_from_env(), broadcaster, fee_sats=fee_sats)


def print_result(result: CtvFanoutDaemonResult) -> None:
    print(
        "ctv broadcaster: "
        f"scanned={result.scanned_count} "
        f"submitted={result.submitted_count} "
        f"updated={result.updated_count} "
        f"failed={result.failed_count}",
        flush=True,
    )


def main() -> int:
    daemon = make_daemon_from_env()
    limit = env_positive_int("PRISM_CTV_BROADCASTER_LIMIT", 100)
    once = env_bool("PRISM_CTV_BROADCASTER_ONCE", "0")
    interval_seconds = env_positive_int("PRISM_CTV_BROADCASTER_INTERVAL_SECONDS", 30)
    while True:
        print_result(daemon.run_once(limit=limit))
        if once:
            return 0
        time.sleep(interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
