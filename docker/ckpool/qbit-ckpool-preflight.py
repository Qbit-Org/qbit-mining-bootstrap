#!/usr/bin/env python3
"""Fail-closed ckpool startup checks for qbit operator deployments."""

from __future__ import annotations

import base64
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Protocol
from urllib import request


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


class PreflightError(RuntimeError):
    """Raised when startup must fail closed."""


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
        with request.urlopen(req, timeout=self.timeout) as resp:
            body = json.load(resp)
        if body.get("error"):
            raise PreflightError(f"{method} failed: {body['error']}")
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
    return bool_env(env, "QBIT_PRODUCTION", False) or bool_env(env, "QBIT_TOOLS_PRODUCTION", False)


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
    if not production_mode(env):
        return []

    chain = chain_name(env)
    if chain == "regtest":
        raise PreflightError("QBIT_PRODUCTION=1 rejects regtest QBIT_CHAIN")

    policy = env.get("CKPOOL_PUBLIC_DIFF_POLICY", "explicit").strip().lower() or "explicit"
    if policy in DIFF_POLICY_PERMISSIVE:
        raise PreflightError("QBIT_PRODUCTION=1 rejects CKPOOL_PUBLIC_DIFF_POLICY=permissive")
    if not bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE", True):
        raise PreflightError("QBIT_PRODUCTION=1 rejects CKPOOL_NON_TEST_READINESS_GATE=0")
    if not bool_env(env, "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS", True):
        raise PreflightError("QBIT_PRODUCTION=1 rejects CKPOOL_VALIDATE_QBIT_ASSUMPTIONS=0")
    if is_public_chain(chain) and not bool_env(env, "CKPOOL_REQUIRE_P2MR_PAYOUT", True):
        raise PreflightError("QBIT_PRODUCTION=1 rejects public-chain CKPOOL_REQUIRE_P2MR_PAYOUT=0")
    if env.get("QBIT_RPC_PASSWORD", "") in {"", "change-this"}:
        raise PreflightError("QBIT_PRODUCTION=1 requires a non-default QBIT_RPC_PASSWORD")
    if not env.get("CKPOOL_STRATUM_PORT", ""):
        raise PreflightError("QBIT_PRODUCTION=1 requires explicit CKPOOL_STRATUM_PORT")

    return [f"production gate: chain={chain} ckpool=strict"]


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
    if donation < 0:
        raise PreflightError("CKPOOL_DONATION must be non-negative")

    return [
        "ckpool knobs: "
        f"notify={notify} blockpoll={blockpoll} donation={donation:g} "
        f"nonce1length={nonce1} nonce2length={nonce2} update_interval={update_interval}"
    ]


def validate_readiness(env: dict[str, str], rpc: RpcClient) -> list[str]:
    chain = chain_name(env)
    if not is_public_chain(chain):
        return [f"readiness gate: skipped for QBIT_CHAIN={chain}"]
    if not bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE", True):
        return [f"readiness gate: disabled for QBIT_CHAIN={chain}"]

    info = rpc.call("getblockchaininfo")
    if not isinstance(info, dict):
        raise PreflightError("getblockchaininfo result was not an object")
    rpc_chain = normalize_rpc_chain(info.get("chain"))
    expected_rpc_chains = CHAIN_RPC_NAMES.get(chain, {chain})
    if rpc_chain not in expected_rpc_chains:
        raise PreflightError(
            f"QBIT_CHAIN={chain} does not match getblockchaininfo.chain={rpc_chain}"
        )
    if bool(info.get("initialblockdownload")):
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
        if not isinstance(connections, int):
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
        f"readiness gate: chain={chain} rpc_chain={rpc_chain} "
        f"ibd=false peers={connections} min_peers={min_peers}"
    ]


def validate_template_assumptions(env: dict[str, str], rpc: RpcClient) -> list[str]:
    if not bool_env(env, "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS", True):
        return ["qbit assumptions: validation disabled"]

    chain = chain_name(env)
    expected_weight = int_env(env, "QBIT_EXPECTED_MAX_BLOCK_WEIGHT", 2_000_000)
    expected_witness_scale = int_env(env, "QBIT_EXPECTED_WITNESS_SCALE_FACTOR", 1)
    expected_maturity = int_env(env, "QBIT_EXPECTED_COINBASE_MATURITY", 1000)
    if expected_weight != 2_000_000:
        raise PreflightError(f"QBIT_EXPECTED_MAX_BLOCK_WEIGHT must be 2000000, got {expected_weight}")
    if expected_witness_scale != 1:
        raise PreflightError(f"QBIT_EXPECTED_WITNESS_SCALE_FACTOR must be 1, got {expected_witness_scale}")
    if expected_maturity != 1000:
        raise PreflightError(f"QBIT_EXPECTED_COINBASE_MATURITY must be 1000, got {expected_maturity}")

    template = rpc.call("getblocktemplate", [{"rules": gbt_rules(chain)}])
    if not isinstance(template, dict):
        raise PreflightError("getblocktemplate result was not an object")
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


def run_preflight(env: dict[str, str], rpc: RpcClient) -> list[str]:
    messages: list[str] = []
    messages.extend(validate_production_gate(env))
    messages.extend(validate_ckpool_knobs(env))
    messages.extend(validate_difficulty_policy(env))
    messages.extend(validate_readiness(env, rpc))
    messages.extend(validate_template_assumptions(env, rpc))
    messages.extend(validate_payout_address(env, rpc))
    return messages


def main() -> int:
    env = dict(os.environ)
    try:
        messages = run_preflight(env, build_rpc_client(env))
    except (KeyError, OSError, json.JSONDecodeError, PreflightError) as exc:
        print(f"qbit ckpool preflight: FAIL: {exc}", file=sys.stderr)
        return 1

    for message in messages:
        print(f"qbit ckpool preflight: {message}", file=sys.stderr)
    print("qbit ckpool preflight: PASS", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
