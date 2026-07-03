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
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
PUBLIC_QBIT_CHAINS = {"mainnet", "testnet", "testnet3", "testnet4", "signet"}
TEST_CHAIN_FLAGS = {"-regtest", "-testnet", "-testnet3", "-testnet4", "-signet"}
QBIT_CHAIN_FLAGS = {
    "regtest": "-regtest",
    "testnet": "-testnet",
    "testnet3": "-testnet3",
    "testnet4": "-testnet4",
    "signet": "-signet",
}


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
    return value.lower() in {"1", "true", "yes", "on"}


def production_mode(env: dict[str, str]) -> bool:
    return is_true(env_value(env, "QBIT_PRODUCTION", "0")) or is_true(env_value(env, "QBIT_TOOLS_PRODUCTION", "0"))


def parse_decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{value!r} is not a decimal number") from exc


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
    prod = production_mode(env)
    qbit_chain = env_value(env, "QBIT_CHAIN", "regtest")
    qbit_chain_flag = env_value(env, "QBIT_CHAIN_FLAG", "-regtest")
    if prod and qbit_chain == "regtest":
        reporter.fail("qbit.chain", "production mode cannot use regtest", hint="Set QBIT_CHAIN and QBIT_CHAIN_FLAG for the live network.")
    else:
        reporter.pass_("qbit.chain", f"QBIT_CHAIN={qbit_chain} QBIT_CHAIN_FLAG={qbit_chain_flag}")
    expected_chain_flag = QBIT_CHAIN_FLAGS.get(qbit_chain)
    if expected_chain_flag is not None and qbit_chain_flag != expected_chain_flag:
        reporter.fail("qbit.chain_flag", f"QBIT_CHAIN={qbit_chain} requires QBIT_CHAIN_FLAG={expected_chain_flag}")
    elif qbit_chain == "mainnet" and qbit_chain_flag in TEST_CHAIN_FLAGS:
        reporter.fail("qbit.chain_flag", f"QBIT_CHAIN=mainnet cannot use {qbit_chain_flag}")
    elif qbit_chain not in QBIT_CHAIN_FLAGS and qbit_chain != "mainnet":
        reporter.fail("qbit.chain_flag", f"unknown QBIT_CHAIN={qbit_chain}")
    else:
        reporter.pass_("qbit.chain_flag", "chain flag matches the configured qbit chain")

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

    if is_true(env_value(env, "PRISM_STRATUM_VARDIFF", "1")):
        try:
            min_diff = parse_decimal(env_value(env, "PRISM_STRATUM_VARDIFF_MIN_DIFF", "0.000000001"))
            max_diff = parse_decimal(env_value(env, "PRISM_STRATUM_VARDIFF_MAX_DIFF", "1024"))
            start_diff = parse_decimal(env_value(env, "PRISM_STRATUM_VARDIFF_START_DIFF", str(min_diff)))
            if min_diff <= 0 or max_diff <= 0 or start_diff <= 0:
                raise ValueError("vardiff difficulty values must be positive")
            if min_diff > max_diff:
                raise ValueError("PRISM_STRATUM_VARDIFF_MIN_DIFF exceeds PRISM_STRATUM_VARDIFF_MAX_DIFF")
            reporter.pass_("mining.vardiff", f"enabled start={start_diff} range={min_diff}..{max_diff}")
        except ValueError as exc:
            reporter.fail("mining.vardiff", str(exc))
    else:
        reporter.warn("mining.vardiff", "vardiff is disabled")

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


def tcp_connect_check(name: str, host: str, port: int, reporter: Reporter) -> None:
    try:
        with socket.create_connection((host, port), timeout=3):
            pass
    except OSError as exc:
        reporter.fail(name, f"cannot connect to {host}:{port}: {exc}", hint="Start the PRISM profile or check host port mapping.")
    else:
        reporter.pass_(name, f"reachable at {host}:{port}")


def qbit_rpc_call(env: dict[str, str], method: str) -> object:
    host, port = parse_host_port(env_value(env, "QBIT_RPC_PORT_HOST", "127.0.0.1:18452"), default_host="127.0.0.1", default_port=18452)
    user = env_value(env, "QBIT_RPC_USER", "qbitrpc")
    password = env_value(env, "QBIT_RPC_PASSWORD", "change-this")
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    request = urllib.request.Request(
        f"http://{host}:{port}/",
        data=json.dumps({"jsonrpc": "1.0", "id": "prism-self-check", "method": method, "params": []}).encode(),
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
    if actual_chain != expected_chain:
        reporter.fail("qbit.rpc_chain", f"qbitd reports chain={actual_chain}, expected {expected_chain}")
    else:
        reporter.pass_("qbit.rpc_chain", f"qbitd reports {actual_chain}")
    if blockchain_info.get("initialblockdownload"):
        reporter.fail("qbit.ibd", "qbitd is still in initial block download")
    else:
        reporter.pass_("qbit.ibd", "qbitd is not in initial block download")

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
        reporter.fail(
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
