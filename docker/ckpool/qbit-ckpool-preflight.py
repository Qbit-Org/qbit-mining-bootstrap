#!/usr/bin/env python3
"""Fail-closed ckpool startup checks for qbit operator deployments."""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Protocol
from urllib import error, request


READINESS_POLL_SECONDS = 2.0
PUBLIC_CHAINS = {"mainnet", "testnet", "testnet3", "testnet4", "signet"}
CHAIN_HRPS = {
    "mainnet": "qb",
    "testnet": "tq",
    "testnet3": "tq",
    "testnet4": "tq",
    "signet": "tq",
    "regtest": "qbrt",
}
CHAIN_RPC_NAMES = {
    "mainnet": {"main", "mainnet"},
    "testnet": {"test", "testnet"},
    "testnet3": {"test", "testnet3"},
    "testnet4": {"testnet4"},
    "signet": {"signet"},
    "regtest": {"regtest"},
}
BOOL_TRUE = {"1", "true", "yes", "on"}
BOOL_FALSE = {"0", "false", "no", "off"}
DIFF_POLICY_EXPLICIT = {"explicit", "require", "required"}
DIFF_POLICY_PERMISSIVE = {"permissive", "allow-defaults", "defaults"}
HEXISH_HRP_RE = re.compile(r"^[a-z0-9]+$")
HASH_RE = re.compile(r"^[0-9a-fA-F]{64}$")
SUPERVISOR_SIGNALS = (signal.SIGTERM, signal.SIGINT)
CHILD_EXIT_TIMEOUT_SECONDS = 10.0


class PreflightError(RuntimeError):
    """Raised when startup must fail closed."""


def rpc_failure(method: str, rpc_error: Any, *, http_status: int | None = None) -> PreflightError:
    if isinstance(rpc_error, dict):
        code = rpc_error.get("code")
        message = rpc_error.get("message")
        if code is not None and message:
            detail = f"RPC {code}: {message}"
        else:
            detail = f"RPC error: {rpc_error}"
    else:
        detail = f"RPC error: {rpc_error}"
    if http_status is not None:
        detail += f" (HTTP {http_status})"
    return PreflightError(f"{method} failed: {detail}")


class RpcClient(Protocol):
    def call(self, method: str, params: list[Any] | None = None) -> Any:
        ...


@dataclass(frozen=True)
class HttpRpcClient:
    host: str
    port: str
    user: str
    password: str
    timeout: float

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        payload = json.dumps(
            {
                "jsonrpc": "1.0",
                "id": "qbit-ckpool-preflight",
                "method": method,
                "params": params or [],
            }
        ).encode("utf-8")
        credentials = f"{self.user}:{self.password}".encode("utf-8")
        req = request.Request(
            f"http://{self.host}:{self.port}",
            data=payload,
            headers={
                "Authorization": f"Basic {base64.b64encode(credentials).decode('ascii')}",
                "Content-Type": "application/json",
            },
        )
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                body = json.load(resp)
        except error.HTTPError as exc:
            try:
                body = json.load(exc)
            except (OSError, ValueError) as parse_error:
                raise PreflightError(
                    f"{method} failed: HTTP {exc.code}: {exc.reason}"
                ) from parse_error
            if isinstance(body, dict) and body.get("error"):
                raise rpc_failure(method, body["error"], http_status=exc.code) from exc
            raise PreflightError(f"{method} failed: HTTP {exc.code}: {exc.reason}") from exc
        if body.get("error"):
            raise rpc_failure(method, body["error"])
        return body.get("result")


def bool_env(env: dict[str, str], name: str, default: bool) -> bool:
    value = env.get(name, "")
    if value == "":
        return default
    normalized = value.strip().lower()
    if normalized in BOOL_TRUE:
        return True
    if normalized in BOOL_FALSE:
        return False
    raise PreflightError(f"{name} must be true/false style value, got {value!r}")


def optional_bool_env(env: dict[str, str], name: str) -> bool | None:
    value = env.get(name, "")
    if value == "":
        return None
    normalized = value.strip().lower()
    if normalized in BOOL_TRUE:
        return True
    if normalized in BOOL_FALSE:
        return False
    raise PreflightError(f"{name} must be true/false style value, got {value!r}")


def int_env(env: dict[str, str], name: str, default: int) -> int:
    value = env.get(name, "")
    if value == "":
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise PreflightError(f"{name} must be an integer, got {value!r}") from exc
    return parsed


def float_env(env: dict[str, str], name: str, default: float) -> float:
    value = env.get(name, "")
    if value == "":
        return default
    try:
        parsed = float(value)
    except ValueError as exc:
        raise PreflightError(f"{name} must be numeric, got {value!r}") from exc
    if not math.isfinite(parsed):
        raise PreflightError(f"{name} must be finite, got {value!r}")
    return parsed


def chain_name(env: dict[str, str]) -> str:
    return env.get("QBIT_CHAIN", "regtest").strip().lower() or "regtest"


def is_public_chain(chain: str) -> bool:
    return chain in PUBLIC_CHAINS


def production_mode(env: dict[str, str]) -> bool:
    qbit_production = bool_env(env, "QBIT_PRODUCTION", False)
    qbit_tools_production = bool_env(env, "QBIT_TOOLS_PRODUCTION", False)
    return chain_name(env) == "mainnet" or qbit_production or qbit_tools_production


def both_production_flags_enabled(env: dict[str, str]) -> bool:
    return bool_env(env, "QBIT_PRODUCTION", False) and bool_env(
        env, "QBIT_TOOLS_PRODUCTION", False
    )


def mainnet_launch_readiness_checks(env: dict[str, str]) -> bool | None:
    enabled = optional_bool_env(env, "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED")
    chain = chain_name(env)
    if enabled is not None and chain != "mainnet":
        raise PreflightError(
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED is valid only for "
            "QBIT_CHAIN=mainnet"
        )
    return enabled


def authorized_mainnet_prelaunch(env: dict[str, str]) -> bool:
    return (
        chain_name(env) == "mainnet"
        and both_production_flags_enabled(env)
        and not bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE", True)
        and mainnet_launch_readiness_checks(env) is False
    )


def validate_mainnet_readiness_flags(env: dict[str, str]) -> None:
    chain = chain_name(env)
    launch_checks = mainnet_launch_readiness_checks(env)
    readiness_gate = bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE", True)

    if not readiness_gate and not authorized_mainnet_prelaunch(env):
        raise PreflightError(
            "CKPOOL_NON_TEST_READINESS_GATE=0 requires the explicitly authorized "
            "mainnet prelaunch combination: QBIT_PRODUCTION=1, "
            "QBIT_TOOLS_PRODUCTION=1, and "
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0"
        )
    if chain != "mainnet":
        return
    if launch_checks is False and readiness_gate:
        raise PreflightError(
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0 requires "
            "CKPOOL_NON_TEST_READINESS_GATE=0 and both production flags enabled"
        )


def gbt_rules(chain: str) -> list[str]:
    rules = ["segwit"]
    if chain == "signet":
        rules.append("signet")
    return rules


def normalize_rpc_chain(value: Any) -> str:
    if not isinstance(value, str):
        raise PreflightError("getblockchaininfo.chain was missing or not a string")
    normalized = value.strip().lower()
    if not normalized:
        raise PreflightError("getblockchaininfo.chain was empty")
    return normalized


def address_hrp(address: str) -> str | None:
    if "1" not in address:
        return None
    hrp = address.split("1", maxsplit=1)[0].lower()
    if not hrp or HEXISH_HRP_RE.fullmatch(hrp) is None:
        return None
    return hrp


def validate_difficulty_policy(env: dict[str, str]) -> list[str]:
    chain = chain_name(env)
    messages: list[str] = []
    policy = env.get("CKPOOL_PUBLIC_DIFF_POLICY", "explicit").strip().lower() or "explicit"
    if policy not in DIFF_POLICY_EXPLICIT | DIFF_POLICY_PERMISSIVE:
        raise PreflightError(
            "CKPOOL_PUBLIC_DIFF_POLICY must be explicit or permissive, "
            f"got {env.get('CKPOOL_PUBLIC_DIFF_POLICY')!r}"
        )

    mindiff = float_env(env, "CKPOOL_MINDIFF", 0.0)
    startdiff = float_env(env, "CKPOOL_STARTDIFF", 0.0)
    maxdiff_text = env.get("CKPOOL_MAXDIFF", "").strip()
    if mindiff <= 0:
        raise PreflightError(f"CKPOOL_MINDIFF must be positive, got {mindiff:g}")
    if startdiff <= 0:
        raise PreflightError(f"CKPOOL_STARTDIFF must be positive, got {startdiff:g}")
    if mindiff > startdiff:
        raise PreflightError("CKPOOL_MINDIFF must be less than or equal to CKPOOL_STARTDIFF")
    if maxdiff_text:
        maxdiff = float_env(env, "CKPOOL_MAXDIFF", 0.0)
        if maxdiff <= 0:
            raise PreflightError(f"CKPOOL_MAXDIFF must be positive, got {maxdiff:g}")
        if maxdiff < mindiff:
            raise PreflightError("CKPOOL_MAXDIFF must be greater than or equal to CKPOOL_MINDIFF")
        if maxdiff < startdiff:
            raise PreflightError("CKPOOL_MAXDIFF must be greater than or equal to CKPOOL_STARTDIFF")

    if is_public_chain(chain) and policy in DIFF_POLICY_EXPLICIT:
        if env.get("CKPOOL_MINDIFF_EXPLICIT") != "1":
            raise PreflightError(
                f"QBIT_CHAIN={chain} requires explicit CKPOOL_MINDIFF; "
                "do not rely on bootstrap defaults for public-chain mining"
            )
        if env.get("CKPOOL_STARTDIFF_EXPLICIT") != "1":
            raise PreflightError(
                f"QBIT_CHAIN={chain} requires explicit CKPOOL_STARTDIFF; "
                "do not rely on bootstrap defaults for public-chain mining"
            )
    messages.append(
        "difficulty policy: "
        f"chain={chain} mindiff={mindiff:g} startdiff={startdiff:g} "
        f"maxdiff={maxdiff_text or '-'} policy={policy}"
    )
    return messages


def validate_production_gate(env: dict[str, str]) -> list[str]:
    validate_mainnet_readiness_flags(env)
    if not production_mode(env):
        return []

    chain = chain_name(env)
    if chain == "regtest":
        raise PreflightError("QBIT_PRODUCTION=1 rejects regtest QBIT_CHAIN")

    readiness_gate = bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE", True)
    policy = env.get("CKPOOL_PUBLIC_DIFF_POLICY", "explicit").strip().lower() or "explicit"
    if policy in DIFF_POLICY_PERMISSIVE:
        raise PreflightError("QBIT_PRODUCTION=1 rejects CKPOOL_PUBLIC_DIFF_POLICY=permissive")
    if not readiness_gate and not authorized_mainnet_prelaunch(env):
        raise PreflightError(
            "production mode permits CKPOOL_NON_TEST_READINESS_GATE=0 only for "
            "explicitly authorized mainnet prelaunch"
        )
    if not bool_env(env, "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS", True):
        raise PreflightError("QBIT_PRODUCTION=1 rejects CKPOOL_VALIDATE_QBIT_ASSUMPTIONS=0")
    if is_public_chain(chain) and not bool_env(env, "CKPOOL_REQUIRE_P2MR_PAYOUT", True):
        raise PreflightError("QBIT_PRODUCTION=1 rejects public-chain CKPOOL_REQUIRE_P2MR_PAYOUT=0")
    payout_address = env.get("QBIT_MINER_ADDRESS", "").strip()
    if not payout_address or payout_address.lower() == "auto":
        raise PreflightError(
            "production mode requires an explicit QBIT_MINER_ADDRESS for CKPool"
        )
    if env.get("QBIT_RPC_PASSWORD", "") in {"", "change-this"}:
        raise PreflightError("production mode requires a non-default QBIT_RPC_PASSWORD")
    if not env.get("CKPOOL_STRATUM_PORT", ""):
        raise PreflightError("QBIT_PRODUCTION=1 requires explicit CKPOOL_STRATUM_PORT")

    readiness_mode = "mainnet-prelaunch" if not readiness_gate else "strict"
    return [f"production gate: chain={chain} ckpool={readiness_mode}"]


def validate_ckpool_knobs(env: dict[str, str]) -> list[str]:
    notify = env.get("CKPOOL_NOTIFY", "false").strip().lower()
    if notify not in BOOL_TRUE | BOOL_FALSE:
        raise PreflightError(f"CKPOOL_NOTIFY must be true/false style value, got {notify!r}")

    blockpoll = int_env(env, "CKPOOL_BLOCKPOLL", 2)
    nonce1 = int_env(env, "CKPOOL_NONCE1LENGTH", 4)
    nonce2 = int_env(env, "CKPOOL_NONCE2LENGTH", 8)
    update_interval = int_env(env, "CKPOOL_UPDATE_INTERVAL", 30)
    donation = float_env(env, "CKPOOL_DONATION", 0.0)
    if blockpoll <= 0:
        raise PreflightError("CKPOOL_BLOCKPOLL must be positive")
    if nonce1 < 2 or nonce1 > 8:
        raise PreflightError("CKPOOL_NONCE1LENGTH must be between 2 and 8")
    if nonce2 < 2 or nonce2 > 8:
        raise PreflightError("CKPOOL_NONCE2LENGTH must be between 2 and 8")
    if update_interval <= 0:
        raise PreflightError("CKPOOL_UPDATE_INTERVAL must be positive")
    if is_public_chain(chain_name(env)):
        template_max_age = int_env(env, "CKPOOL_TEMPLATE_MAX_AGE_SECONDS", 120)
        if template_max_age <= 0:
            raise PreflightError(
                "CKPOOL_TEMPLATE_MAX_AGE_SECONDS must be positive on public chains"
            )
        if update_interval >= template_max_age:
            raise PreflightError(
                "CKPOOL_UPDATE_INTERVAL must be less than "
                "CKPOOL_TEMPLATE_MAX_AGE_SECONDS on public chains"
            )
    if donation < 0:
        raise PreflightError("CKPOOL_DONATION must be non-negative")

    return [
        "ckpool knobs: "
        f"notify={notify} blockpoll={blockpoll} donation={donation:g} "
        f"nonce1length={nonce1} nonce2length={nonce2} update_interval={update_interval}"
    ]


def expected_genesis_hash(env: dict[str, str]) -> str | None:
    value = env.get("QBIT_EXPECTED_GENESIS_HASH", "").strip().lower()
    if not value:
        if chain_name(env) == "mainnet":
            raise PreflightError("QBIT_CHAIN=mainnet requires QBIT_EXPECTED_GENESIS_HASH")
        return None
    if HASH_RE.fullmatch(value) is None:
        raise PreflightError("QBIT_EXPECTED_GENESIS_HASH must be 64 lowercase hex characters")
    return value


def validate_chain_identity(
    env: dict[str, str], rpc: RpcClient
) -> tuple[dict[str, Any], str]:
    chain = chain_name(env)
    info = rpc.call("getblockchaininfo")
    if not isinstance(info, dict):
        raise PreflightError("getblockchaininfo result was not an object")
    rpc_chain = normalize_rpc_chain(info.get("chain"))
    expected_rpc_chains = CHAIN_RPC_NAMES.get(chain, {chain})
    if rpc_chain not in expected_rpc_chains:
        raise PreflightError(
            f"QBIT_CHAIN={chain} does not match getblockchaininfo.chain={rpc_chain}"
        )

    expected_genesis = expected_genesis_hash(env)
    if expected_genesis is not None:
        actual_genesis = rpc.call("getblockhash", [0])
        if not isinstance(actual_genesis, str) or actual_genesis.lower() != expected_genesis:
            raise PreflightError(
                "QBIT_EXPECTED_GENESIS_HASH does not match getblockhash(0): "
                f"expected {expected_genesis}, got {actual_genesis!r}"
            )

    genesis_status = expected_genesis or "not-configured"
    return info, f"chain identity: chain={chain} rpc_chain={rpc_chain} genesis={genesis_status}"


def validate_readiness(env: dict[str, str], rpc: RpcClient) -> list[str]:
    chain = chain_name(env)
    if not is_public_chain(chain):
        expected_genesis_hash(env)
        return [f"readiness gate: skipped for QBIT_CHAIN={chain}"]

    readiness_gate = bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE", True)
    validate_mainnet_readiness_flags(env)
    info, identity_message = validate_chain_identity(env, rpc)
    rpc_chain = normalize_rpc_chain(info.get("chain"))
    if not readiness_gate:
        mode = (
            "explicitly relaxed for mainnet prelaunch"
            if authorized_mainnet_prelaunch(env)
            else "disabled"
        )
        return [
            identity_message,
            f"readiness gate: {mode} for QBIT_CHAIN={chain} rpc_chain={rpc_chain}",
        ]
    initial_block_download = info.get("initialblockdownload")
    if not isinstance(initial_block_download, bool):
        raise PreflightError(
            "getblockchaininfo.initialblockdownload was missing or not a boolean"
        )
    if initial_block_download:
        raise PreflightError(f"QBIT_CHAIN={chain} is still in initial block download")

    min_peers = int_env(env, "CKPOOL_MIN_PEERS", 1)
    if min_peers < 1:
        raise PreflightError("CKPOOL_MIN_PEERS must be at least 1 for public-chain readiness")

    readiness_timeout = float_env(env, "CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS", 120.0)
    if readiness_timeout < 0:
        raise PreflightError("CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS must be non-negative")
    deadline = time.monotonic() + readiness_timeout
    connections = 0
    attempts = 0
    while True:
        attempts += 1
        network_info = rpc.call("getnetworkinfo")
        if not isinstance(network_info, dict):
            raise PreflightError("getnetworkinfo result was not an object")
        connections = network_info.get("connections")
        if not isinstance(connections, int) or isinstance(connections, bool):
            raise PreflightError("getnetworkinfo.connections was missing or not an integer")
        if connections >= min_peers:
            break

        now = time.monotonic()
        if now >= deadline:
            print(
                "qbit ckpool preflight: readiness wait timed out: "
                f"chain={chain} peers={connections} min_peers={min_peers} "
                f"timeout={readiness_timeout:g}s attempts={attempts}",
                file=sys.stderr,
            )
            raise PreflightError(
                f"QBIT_CHAIN={chain} has {connections} peer connection(s), "
                f"requires at least {min_peers} after waiting {readiness_timeout:g}s"
            )

        remaining = deadline - now
        sleep_for = min(READINESS_POLL_SECONDS, remaining)
        print(
            "qbit ckpool preflight: readiness wait: "
            f"chain={chain} peers={connections} min_peers={min_peers} "
            f"remaining={remaining:.1f}s",
            file=sys.stderr,
        )
        time.sleep(sleep_for)

    return [
        identity_message,
        f"readiness gate: chain={chain} rpc_chain={rpc_chain} "
        f"ibd=false peers={connections} min_peers={min_peers}",
    ]


def validate_static_qbit_assumptions(env: dict[str, str]) -> tuple[int, int, int]:
    if not bool_env(env, "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS", True):
        return (0, 0, 0)

    expected_weight = int_env(env, "QBIT_EXPECTED_MAX_BLOCK_WEIGHT", 2_000_000)
    expected_witness_scale = int_env(env, "QBIT_EXPECTED_WITNESS_SCALE_FACTOR", 1)
    expected_maturity = int_env(env, "QBIT_EXPECTED_COINBASE_MATURITY", 1000)
    if expected_weight != 2_000_000:
        raise PreflightError(
            f"QBIT_EXPECTED_MAX_BLOCK_WEIGHT must be 2000000, got {expected_weight}"
        )
    if expected_witness_scale != 1:
        raise PreflightError(
            "QBIT_EXPECTED_WITNESS_SCALE_FACTOR must be 1, "
            f"got {expected_witness_scale}"
        )
    if expected_maturity != 1000:
        raise PreflightError(
            f"QBIT_EXPECTED_COINBASE_MATURITY must be 1000, got {expected_maturity}"
        )
    return expected_weight, expected_witness_scale, expected_maturity


def live_template(env: dict[str, str], rpc: RpcClient) -> dict[str, Any]:
    template = rpc.call("getblocktemplate", [{"rules": gbt_rules(chain_name(env))}])
    if not isinstance(template, dict):
        raise PreflightError("getblocktemplate result was not an object")
    return template


def validate_template_assumptions(env: dict[str, str], rpc: RpcClient) -> list[str]:
    if not bool_env(env, "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS", True):
        return ["qbit assumptions: validation disabled"]

    expected_weight, expected_witness_scale, expected_maturity = (
        validate_static_qbit_assumptions(env)
    )

    if authorized_mainnet_prelaunch(env):
        return [
            "qbit assumptions: "
            f"static weightlimit={expected_weight} witness_scale={expected_witness_scale} "
            f"coinbase_maturity={expected_maturity}; dynamic getblocktemplate validation "
            "deferred for explicitly authorized mainnet prelaunch"
        ]

    template = live_template(env, rpc)
    weightlimit = template.get("weightlimit")
    if weightlimit != expected_weight:
        raise PreflightError(
            f"getblocktemplate.weightlimit={weightlimit!r}, expected {expected_weight}"
        )

    return [
        "qbit assumptions: "
        f"weightlimit={expected_weight} witness_scale={expected_witness_scale} "
        f"coinbase_maturity={expected_maturity}"
    ]


def validate_payout_address(env: dict[str, str], rpc: RpcClient) -> list[str]:
    address = env.get("QBIT_MINER_ADDRESS", "").strip()
    if not address or address == "auto":
        raise PreflightError("QBIT_MINER_ADDRESS must be resolved before ckpool preflight")

    chain = chain_name(env)
    expected_hrp = env.get("QBIT_EXPECTED_ADDRESS_HRP", "").strip().lower() or CHAIN_HRPS.get(chain)
    if expected_hrp:
        actual_hrp = address_hrp(address)
        if actual_hrp != expected_hrp:
            raise PreflightError(
                f"QBIT_MINER_ADDRESS HRP is {actual_hrp or '<none>'}, expected {expected_hrp}"
            )

    validation = rpc.call("validateaddress", [address])
    if not isinstance(validation, dict):
        raise PreflightError("validateaddress result was not an object")
    if not validation.get("isvalid"):
        raise PreflightError("QBIT_MINER_ADDRESS is not valid for the configured qbit node")
    if validation.get("address") not in (None, address):
        raise PreflightError("validateaddress returned a different address than QBIT_MINER_ADDRESS")

    require_p2mr_default = is_public_chain(chain)
    if bool_env(env, "CKPOOL_REQUIRE_P2MR_PAYOUT", require_p2mr_default):
        is_witness = validation.get("iswitness")
        witness_version = validation.get("witness_version")
        if is_witness is not True:
            raise PreflightError("QBIT_MINER_ADDRESS must be a witness/P2MR address")
        if witness_version != 2:
            raise PreflightError(
                f"QBIT_MINER_ADDRESS witness_version={witness_version!r}, expected 2 for P2MR"
            )

    return [f"payout address: chain={chain} hrp={expected_hrp or '-'} address={address}"]


def build_rpc_client(env: dict[str, str]) -> HttpRpcClient:
    timeout = float_env(env, "CKPOOL_PREFLIGHT_RPC_TIMEOUT_SECONDS", 5.0)
    return HttpRpcClient(
        host=env.get("QBIT_RPC_HOST", "qbitd"),
        port=env.get("QBIT_RPC_PORT", "18452"),
        user=env["QBIT_RPC_USER"],
        password=env["QBIT_RPC_PASSWORD"],
        timeout=timeout,
    )


def run_static_preflight(env: dict[str, str]) -> list[str]:
    messages: list[str] = []
    messages.extend(validate_production_gate(env))
    messages.extend(validate_ckpool_knobs(env))
    messages.extend(validate_difficulty_policy(env))
    expected_genesis_hash(env)
    return messages


def run_preflight(env: dict[str, str], rpc: RpcClient) -> list[str]:
    messages = run_static_preflight(env)
    messages.extend(validate_readiness(env, rpc))
    messages.extend(validate_template_assumptions(env, rpc))
    messages.extend(validate_payout_address(env, rpc))
    return messages


def validate_strict_readiness_sample(
    env: dict[str, str], rpc: RpcClient, info: dict[str, Any]
) -> str:
    chain = chain_name(env)
    if not is_public_chain(chain):
        return f"readiness watchdog: skipped for QBIT_CHAIN={chain}"

    initial_block_download = info.get("initialblockdownload")
    if not isinstance(initial_block_download, bool):
        raise PreflightError(
            "getblockchaininfo.initialblockdownload was missing or not a boolean"
        )
    if initial_block_download:
        raise PreflightError(f"QBIT_CHAIN={chain} is still in initial block download")

    min_peers = int_env(env, "CKPOOL_MIN_PEERS", 1)
    if min_peers < 1:
        raise PreflightError("CKPOOL_MIN_PEERS must be at least 1 for public-chain readiness")
    network_info = rpc.call("getnetworkinfo")
    if not isinstance(network_info, dict):
        raise PreflightError("getnetworkinfo result was not an object")
    connections = network_info.get("connections")
    if not isinstance(connections, int) or isinstance(connections, bool):
        raise PreflightError("getnetworkinfo.connections was missing or not an integer")
    if connections < min_peers:
        raise PreflightError(
            f"QBIT_CHAIN={chain} has {connections} peer connection(s), "
            f"requires at least {min_peers}"
        )
    return f"readiness watchdog: ibd=false peers={connections} min_peers={min_peers}"


def validate_template_watchdog(
    env: dict[str, str], rpc: RpcClient, *, now: float | None = None
) -> str:
    template = live_template(env, rpc)
    if bool_env(env, "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS", True):
        expected_weight, _expected_scale, _expected_maturity = (
            validate_static_qbit_assumptions(env)
        )
        weightlimit = template.get("weightlimit")
        if weightlimit != expected_weight:
            raise PreflightError(
                f"getblocktemplate.weightlimit={weightlimit!r}, expected {expected_weight}"
            )

    max_age = float_env(env, "CKPOOL_TEMPLATE_MAX_AGE_SECONDS", 120.0)
    max_future = float_env(env, "CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS", 30.0)
    if max_age < 0:
        raise PreflightError("CKPOOL_TEMPLATE_MAX_AGE_SECONDS must be non-negative")
    if max_future < 0:
        raise PreflightError("CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS must be non-negative")
    curtime = template.get("curtime")
    if not isinstance(curtime, int) or isinstance(curtime, bool):
        raise PreflightError("getblocktemplate.curtime was missing or not an integer")
    current_time = time.time() if now is None else now
    age = current_time - curtime
    if age > max_age:
        raise PreflightError(
            f"getblocktemplate.curtime is {age:.1f}s old, maximum is {max_age:g}s"
        )
    if -age > max_future:
        raise PreflightError(
            f"getblocktemplate.curtime is {-age:.1f}s in the future, maximum is {max_future:g}s"
        )

    previous_hash = template.get("previousblockhash")
    active_tip = rpc.call("getbestblockhash")
    if not isinstance(previous_hash, str) or not isinstance(active_tip, str):
        raise PreflightError("template previousblockhash or active tip was missing")
    if previous_hash.lower() != active_tip.lower():
        raise PreflightError(
            "getblocktemplate does not build on the active tip: "
            f"previousblockhash={previous_hash} active_tip={active_tip}"
        )
    return (
        f"template watchdog: age={age:.1f}s max_age={max_age:g}s "
        f"max_future={max_future:g}s active_tip={active_tip}"
    )


def run_watchdog_check(env: dict[str, str], rpc: RpcClient) -> list[str]:
    messages = run_static_preflight(env)
    info, identity_message = validate_chain_identity(env, rpc)
    messages.append(identity_message)
    messages.extend(validate_payout_address(env, rpc))
    if authorized_mainnet_prelaunch(env):
        messages.append(
            "watchdog: launch-dependent readiness and live template checks deferred "
            "for explicitly authorized mainnet prelaunch"
        )
        return messages

    messages.append(validate_strict_readiness_sample(env, rpc, info))
    messages.append(validate_template_watchdog(env, rpc))
    return messages


def supervisor_settings(env: dict[str, str]) -> tuple[float, float]:
    poll_seconds = float_env(env, "CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS", 5.0)
    failure_exit_seconds = float_env(env, "CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS", 120.0)
    if poll_seconds <= 0:
        raise PreflightError("CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS must be positive")
    if failure_exit_seconds < 0:
        raise PreflightError("CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS must be non-negative")
    return poll_seconds, failure_exit_seconds


def print_messages(messages: list[str]) -> None:
    for message in messages:
        print(f"qbit ckpool preflight: {message}", file=sys.stderr)


def child_exit_status(returncode: int) -> int:
    return returncode if returncode >= 0 else 128 + abs(returncode)


def terminate_and_reap(child: subprocess.Popen[Any], signum: int) -> None:
    if child.poll() is None:
        child.send_signal(signum)
    try:
        child.wait(timeout=CHILD_EXIT_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()


def run_supervisor(env: dict[str, str], rpc: RpcClient, command: list[str]) -> int:
    if not command:
        raise PreflightError("--supervise requires a child command")
    poll_seconds, failure_exit_seconds = supervisor_settings(env)
    initial_messages = run_preflight(env, rpc)
    initial_messages.append(run_watchdog_check(env, rpc)[-1])
    print_messages(initial_messages)
    print("qbit ckpool preflight: initial supervisor checks: PASS", file=sys.stderr)

    child = subprocess.Popen(command)
    print(
        f"qbit ckpool preflight: supervisor: child started pid={child.pid}",
        file=sys.stderr,
    )
    received_signal: int | None = None
    signal_deadline: float | None = None
    old_handlers: dict[int, Any] = {}

    def forward_signal(signum: int, _frame: Any) -> None:
        nonlocal received_signal, signal_deadline
        if received_signal is None:
            received_signal = signum
            signal_deadline = time.monotonic() + CHILD_EXIT_TIMEOUT_SECONDS
        if child.poll() is None:
            try:
                child.send_signal(signum)
            except ProcessLookupError:
                pass

    for signum in SUPERVISOR_SIGNALS:
        old_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, forward_signal)

    failure_started: float | None = None
    failure_message: str | None = None
    prelaunch_notice_printed = False
    try:
        while True:
            returncode = child.poll()
            if returncode is not None:
                if received_signal is not None:
                    return 128 + received_signal
                return child_exit_status(returncode)

            now = time.monotonic()
            if received_signal is not None:
                if signal_deadline is not None and now >= signal_deadline:
                    child.kill()
                wait_for = max(0.01, min(poll_seconds, (signal_deadline or now) - now))
            else:
                try:
                    watchdog_messages = run_watchdog_check(env, rpc)
                except (KeyError, OSError, json.JSONDecodeError, PreflightError) as exc:
                    if failure_started is None:
                        failure_started = now
                        failure_message = str(exc)
                        print(
                            f"qbit ckpool preflight: watchdog failure: {exc}",
                            file=sys.stderr,
                        )
                    if now - failure_started >= failure_exit_seconds:
                        print(
                            "qbit ckpool preflight: FAIL: watchdog failure persisted "
                            f"for {now - failure_started:.1f}s: {failure_message}",
                            file=sys.stderr,
                        )
                        terminate_and_reap(child, signal.SIGTERM)
                        return 1
                else:
                    if failure_started is not None:
                        print("qbit ckpool preflight: watchdog recovered", file=sys.stderr)
                    failure_started = None
                    failure_message = None
                    if authorized_mainnet_prelaunch(env) and not prelaunch_notice_printed:
                        print_messages([watchdog_messages[-1]])
                        prelaunch_notice_printed = True
                wait_for = poll_seconds

            try:
                child.wait(timeout=wait_for)
            except subprocess.TimeoutExpired:
                pass
    finally:
        try:
            if child.poll() is None:
                terminate_and_reap(child, signal.SIGTERM)
        finally:
            for signum, handler in old_handlers.items():
                signal.signal(signum, handler)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed qbit checks and optional CKPool process supervision."
    )
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--production-gate-only",
        action="store_true",
        help="run static production, CKPool knob, and difficulty policy checks only",
    )
    modes.add_argument(
        "--supervise",
        nargs=argparse.REMAINDER,
        metavar="COMMAND",
        help="run full preflight, then supervise COMMAND and its arguments",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.supervise is not None and (
        not args.supervise or args.supervise[0].startswith("-")
    ):
        build_parser().error("--supervise requires a non-option child command")

    env = dict(os.environ)
    try:
        if args.production_gate_only:
            messages = run_static_preflight(env)
        elif args.supervise is not None:
            return run_supervisor(env, build_rpc_client(env), args.supervise)
        else:
            messages = run_preflight(env, build_rpc_client(env))
    except (KeyError, OSError, json.JSONDecodeError, PreflightError) as exc:
        print(f"qbit ckpool preflight: FAIL: {exc}", file=sys.stderr)
        return 1

    print_messages(messages)
    print("qbit ckpool preflight: PASS", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
