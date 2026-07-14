#!/usr/bin/env python3
"""Fail-closed ckpool startup and runtime checks for qbit deployments."""

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
from typing import Any, Callable, Protocol, Sequence
from urllib import request


READINESS_POLL_SECONDS = 2.0
MINING_STATE_SNAPSHOT_ATTEMPTS = 3
MINING_STATE_SNAPSHOT_RETRY_SECONDS = 1.0
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
HASH256_RE = re.compile(r"^[0-9a-f]{64}$")


class PreflightError(RuntimeError):
    """Raised when startup must fail closed."""


class TemplateValidationError(PreflightError):
    """Raised when a template response is known to be unsafe to mine."""


class MiningStateValidationError(TemplateValidationError):
    """Raised when the node or template is known not to be mining-ready."""


class RuntimeReadinessError(PreflightError):
    """Raised when a synchronized public node temporarily loses readiness."""


class RpcClient(Protocol):
    def call(self, method: str, params: list[Any] | None = None) -> Any:
        ...


@dataclass(frozen=True)
class TemplateStatus:
    age_seconds: int
    max_age_seconds: int
    max_future_seconds: int


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


def mainnet_launch_readiness_checks(env: dict[str, str]) -> bool | None:
    enabled = optional_bool_env(env, "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED")
    chain = chain_name(env)
    if enabled is not None and chain != "mainnet":
        raise PreflightError(
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED is valid only for "
            "QBIT_CHAIN=mainnet"
        )
    return enabled


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


def validated_expected_genesis_hash(env: dict[str, str], chain: str) -> str:
    expected_genesis = env.get("QBIT_EXPECTED_GENESIS_HASH", "").strip().lower()
    if chain == "mainnet" and not expected_genesis:
        raise PreflightError("QBIT_CHAIN=mainnet requires QBIT_EXPECTED_GENESIS_HASH")
    if expected_genesis and HASH256_RE.fullmatch(expected_genesis) is None:
        raise PreflightError("QBIT_EXPECTED_GENESIS_HASH must be 64 lowercase hex characters")
    return expected_genesis


def validate_production_gate(env: dict[str, str]) -> list[str]:
    if not production_mode(env):
        return []

    chain = chain_name(env)
    if chain == "regtest":
        raise PreflightError("production mode rejects regtest QBIT_CHAIN")
    validated_expected_genesis_hash(env, chain)

    launch_readiness_checks = mainnet_launch_readiness_checks(env)
    readiness_gate = bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE", True)
    policy = env.get("CKPOOL_PUBLIC_DIFF_POLICY", "explicit").strip().lower() or "explicit"
    if policy in DIFF_POLICY_PERMISSIVE:
        raise PreflightError("production mode rejects CKPOOL_PUBLIC_DIFF_POLICY=permissive")
    if not readiness_gate and not (chain == "mainnet" and launch_readiness_checks is False):
        raise PreflightError(
            "QBIT_PRODUCTION=1 permits CKPOOL_NON_TEST_READINESS_GATE=0 only when "
            "QBIT_CHAIN=mainnet and "
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0"
        )
    if not bool_env(env, "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS", True):
        raise PreflightError("production mode rejects CKPOOL_VALIDATE_QBIT_ASSUMPTIONS=0")
    if is_public_chain(chain) and not bool_env(env, "CKPOOL_REQUIRE_P2MR_PAYOUT", True):
        raise PreflightError("production mode rejects public-chain CKPOOL_REQUIRE_P2MR_PAYOUT=0")
    payout_address = env.get("QBIT_MINER_ADDRESS", "").strip()
    if not payout_address or payout_address.lower() == "auto":
        raise PreflightError(
            "production mode requires an explicit QBIT_MINER_ADDRESS for CKPool"
        )
    if env.get("QBIT_RPC_PASSWORD", "") in {"", "change-this"}:
        raise PreflightError("production mode requires a non-default QBIT_RPC_PASSWORD")
    if not env.get("CKPOOL_STRATUM_PORT", ""):
        raise PreflightError("production mode requires explicit CKPOOL_STRATUM_PORT")

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


def parse_blockchain_readiness(
    *, chain: str, info: Any
) -> tuple[str, bool, int, int]:
    if not isinstance(info, dict):
        raise PreflightError("getblockchaininfo result was not an object")

    rpc_chain = normalize_rpc_chain(info.get("chain"))
    expected_rpc_chains = CHAIN_RPC_NAMES.get(chain, {chain})
    if rpc_chain not in expected_rpc_chains:
        raise PreflightError(
            f"QBIT_CHAIN={chain} does not match getblockchaininfo.chain={rpc_chain}"
        )

    initial_block_download = info.get("initialblockdownload")
    if not isinstance(initial_block_download, bool):
        raise PreflightError(
            "getblockchaininfo initial block download flag was missing or not a boolean"
        )
    blocks = info.get("blocks")
    headers = info.get("headers")
    if (
        not isinstance(blocks, int)
        or isinstance(blocks, bool)
        or not isinstance(headers, int)
        or isinstance(headers, bool)
    ):
        raise PreflightError("getblockchaininfo blocks and headers must be integers")
    if blocks < 0 or headers < 0:
        raise PreflightError("getblockchaininfo blocks and headers must be non-negative")

    return rpc_chain, initial_block_download, blocks, headers


def parse_network_connections(network_info: Any) -> int:
    if not isinstance(network_info, dict):
        raise PreflightError("getnetworkinfo result was not an object")
    connections = network_info.get("connections")
    if not isinstance(connections, int) or isinstance(connections, bool):
        raise PreflightError("getnetworkinfo.connections was missing or not an integer")
    return connections


def fetch_public_mining_tip(env: dict[str, str], rpc: RpcClient) -> str:
    chain = chain_name(env)
    best_block_hash = rpc.call("getbestblockhash")
    if not isinstance(best_block_hash, str) or HASH256_RE.fullmatch(best_block_hash.lower()) is None:
        raise MiningStateValidationError(
            "getbestblockhash was missing or not a 64-character hex hash"
        )

    expected_genesis = validated_expected_genesis_hash(env, chain)
    if expected_genesis:
        actual_genesis = rpc.call("getblockhash", [0])
        if not isinstance(actual_genesis, str) or actual_genesis.lower() != expected_genesis:
            raise MiningStateValidationError(
                f"qbit genesis hash is {actual_genesis!r}, expected {expected_genesis}"
            )

    return best_block_hash.lower()


def validate_runtime_readiness(env: dict[str, str], rpc: RpcClient) -> None:
    chain = chain_name(env)
    if not is_public_chain(chain) or not bool_env(
        env,
        "CKPOOL_NON_TEST_READINESS_GATE",
        True,
    ):
        return

    info = rpc.call("getblockchaininfo")
    try:
        _rpc_chain, initial_block_download, blocks, headers = parse_blockchain_readiness(
            chain=chain,
            info=info,
        )
    except PreflightError as exc:
        raise MiningStateValidationError(f"invalid runtime chain state: {exc}") from exc

    min_peers = int_env(env, "CKPOOL_MIN_PEERS", 1)
    if min_peers < 1:
        raise PreflightError("CKPOOL_MIN_PEERS must be at least 1 for public-chain readiness")
    try:
        connections = parse_network_connections(rpc.call("getnetworkinfo"))
    except PreflightError as exc:
        raise MiningStateValidationError(f"invalid runtime network state: {exc}") from exc

    reasons: list[str] = []
    if initial_block_download:
        reasons.append("initial block download is active")
    if blocks != headers:
        reasons.append(f"not caught up (blocks={blocks}, headers={headers})")
    if connections < min_peers:
        reasons.append(f"{connections} peer connection(s), requires at least {min_peers}")
    if reasons:
        raise RuntimeReadinessError(
            f"QBIT_CHAIN={chain} is not mining-ready: {'; '.join(reasons)}"
        )


def validate_readiness(env: dict[str, str], rpc: RpcClient) -> list[str]:
    chain = chain_name(env)
    if not is_public_chain(chain):
        return [f"readiness gate: skipped for QBIT_CHAIN={chain}"]

    expected_genesis = validated_expected_genesis_hash(env, chain)
    readiness_gate = bool_env(env, "CKPOOL_NON_TEST_READINESS_GATE", True)
    launch_readiness_checks = mainnet_launch_readiness_checks(env)

    info = rpc.call("getblockchaininfo")
    rpc_chain, initial_block_download, blocks, headers = parse_blockchain_readiness(
        chain=chain, info=info
    )
    if expected_genesis:
        actual_genesis = rpc.call("getblockhash", [0])
        if not isinstance(actual_genesis, str) or actual_genesis.lower() != expected_genesis:
            raise PreflightError(
                f"qbit genesis hash is {actual_genesis!r}, expected {expected_genesis}"
            )

    if not readiness_gate:
        # Chain identity and the genesis pin are validated above even when the
        # gate is relaxed; only the IBD/height/peer waits are skipped.
        mode = (
            "explicitly relaxed for mainnet prelaunch"
            if chain == "mainnet" and launch_readiness_checks is False
            else "disabled"
        )
        return [f"readiness gate: {mode} for QBIT_CHAIN={chain} rpc_chain={rpc_chain}"]

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
        connections = parse_network_connections(rpc.call("getnetworkinfo"))
        if not initial_block_download and blocks == headers and connections >= min_peers:
            break

        now = time.monotonic()
        if now >= deadline:
            print(
                "qbit ckpool preflight: readiness wait timed out: "
                f"chain={chain} ibd={str(initial_block_download).lower()} "
                f"blocks={blocks} headers={headers} peers={connections} min_peers={min_peers} "
                f"timeout={readiness_timeout:g}s attempts={attempts}",
                file=sys.stderr,
            )
            reasons: list[str] = []
            if initial_block_download:
                reasons.append("initial block download is still active")
            if blocks != headers:
                reasons.append(f"not caught up (blocks={blocks}, headers={headers})")
            if connections < min_peers:
                reasons.append(
                    f"{connections} peer connection(s), requires at least {min_peers}"
                )
            raise PreflightError(
                f"QBIT_CHAIN={chain} readiness timed out after waiting "
                f"{readiness_timeout:g}s: {'; '.join(reasons)}"
            )

        remaining = deadline - now
        sleep_for = min(READINESS_POLL_SECONDS, remaining)
        print(
            "qbit ckpool preflight: readiness wait: "
            f"chain={chain} ibd={str(initial_block_download).lower()} "
            f"blocks={blocks} headers={headers} peers={connections} min_peers={min_peers} "
            f"remaining={remaining:.1f}s",
            file=sys.stderr,
        )
        time.sleep(sleep_for)
        info = rpc.call("getblockchaininfo")
        rpc_chain, initial_block_download, blocks, headers = parse_blockchain_readiness(
            chain=chain, info=info
        )

    return [
        f"readiness gate: chain={chain} rpc_chain={rpc_chain} "
        f"ibd=false blocks={blocks} headers={headers} peers={connections} min_peers={min_peers} "
        f"genesis={expected_genesis or '-'}"
    ]


def validate_template_response(
    env: dict[str, str],
    template: Any,
    *,
    now: int | None = None,
) -> TemplateStatus:
    chain = chain_name(env)
    expected_weight = int_env(env, "QBIT_EXPECTED_MAX_BLOCK_WEIGHT", 2_000_000)
    if not isinstance(template, dict):
        raise TemplateValidationError("getblocktemplate result was not an object")
    weightlimit = template.get("weightlimit")
    if weightlimit != expected_weight:
        raise TemplateValidationError(
            f"getblocktemplate.weightlimit={weightlimit!r}, expected {expected_weight}"
        )
    if not is_public_chain(chain):
        return TemplateStatus(age_seconds=0, max_age_seconds=0, max_future_seconds=0)

    previous_block_hash = template.get("previousblockhash")
    if not isinstance(previous_block_hash, str) or not previous_block_hash:
        raise TemplateValidationError("getblocktemplate.previousblockhash was missing")
    if HASH256_RE.fullmatch(previous_block_hash.lower()) is None:
        raise TemplateValidationError(
            "getblocktemplate.previousblockhash was not a 64-character hex hash"
        )
    try:
        template_time = int(template["curtime"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TemplateValidationError("getblocktemplate.curtime must be an integer") from exc
    max_age = int_env(env, "CKPOOL_TEMPLATE_MAX_AGE_SECONDS", 120)
    max_future = int_env(env, "CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS", 7200)
    # Template-policy misconfiguration must fail closed immediately in the
    # watchdog (TemplateValidationError); a plain PreflightError would ride
    # the transient-RPC grace window while CKPool keeps mining.
    if max_age <= 0:
        raise TemplateValidationError(
            "CKPOOL_TEMPLATE_MAX_AGE_SECONDS must be positive on public chains"
        )
    if max_future < 0:
        raise TemplateValidationError("CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS must be non-negative")
    template_age = (int(time.time()) if now is None else now) - template_time
    if template_age > max_age:
        raise TemplateValidationError(
            f"getblocktemplate is stale: age={template_age}s exceeds {max_age}s"
        )
    if template_age < -max_future:
        raise TemplateValidationError(
            "getblocktemplate is future-dated: "
            f"ahead={-template_age}s exceeds {max_future}s"
        )
    return TemplateStatus(
        age_seconds=template_age,
        max_age_seconds=max_age,
        max_future_seconds=max_future,
    )


def fetch_and_validate_template(
    env: dict[str, str],
    rpc: RpcClient,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> TemplateStatus:
    chain = chain_name(env)
    if not is_public_chain(chain):
        template = rpc.call("getblocktemplate", [{"rules": gbt_rules(chain)}])
        return validate_template_response(env, template)

    # A block arriving between getblocktemplate and getbestblockhash makes an
    # innocent snapshot look forked; only a mismatch that persists across
    # time-separated snapshots is a wedged or forked template builder.
    for attempt in range(MINING_STATE_SNAPSHOT_ATTEMPTS):
        try:
            template = rpc.call("getblocktemplate", [{"rules": gbt_rules(chain)}])
            status = validate_template_response(env, template)
            best_block_hash = fetch_public_mining_tip(env, rpc)
            previous_block_hash = template["previousblockhash"].lower()
            if previous_block_hash != best_block_hash:
                raise MiningStateValidationError(
                    "getblocktemplate.previousblockhash does not match the current qbit tip: "
                    f"template={previous_block_hash} tip={best_block_hash}"
                )
        except MiningStateValidationError:
            if attempt + 1 < MINING_STATE_SNAPSHOT_ATTEMPTS:
                sleep(MINING_STATE_SNAPSHOT_RETRY_SECONDS)
                continue
            raise
        return status

    raise AssertionError("mining-state validation retry loop exhausted")


def validate_template_assumptions(env: dict[str, str], rpc: RpcClient) -> list[str]:
    if not bool_env(env, "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS", True):
        return ["qbit assumptions: validation disabled"]

    expected_weight = int_env(env, "QBIT_EXPECTED_MAX_BLOCK_WEIGHT", 2_000_000)
    expected_witness_scale = int_env(env, "QBIT_EXPECTED_WITNESS_SCALE_FACTOR", 1)
    expected_maturity = int_env(env, "QBIT_EXPECTED_COINBASE_MATURITY", 1000)
    if expected_weight != 2_000_000:
        raise PreflightError(f"QBIT_EXPECTED_MAX_BLOCK_WEIGHT must be 2000000, got {expected_weight}")
    if expected_witness_scale != 1:
        raise PreflightError(f"QBIT_EXPECTED_WITNESS_SCALE_FACTOR must be 1, got {expected_witness_scale}")
    if expected_maturity != 1000:
        raise PreflightError(f"QBIT_EXPECTED_COINBASE_MATURITY must be 1000, got {expected_maturity}")

    status = fetch_and_validate_template(env, rpc)

    return [
        "qbit assumptions: "
        f"weightlimit={expected_weight} witness_scale={expected_witness_scale} "
        f"coinbase_maturity={expected_maturity} template_age={status.age_seconds}s"
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


def watchdog_required(env: dict[str, str]) -> bool:
    return is_public_chain(chain_name(env)) and bool_env(
        env,
        "CKPOOL_VALIDATE_QBIT_ASSUMPTIONS",
        True,
    )


def watchdog_settings(env: dict[str, str]) -> tuple[float, float]:
    poll_seconds = float_env(env, "CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS", 15.0)
    failure_exit_seconds = float_env(
        env,
        "CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS",
        120.0,
    )
    if poll_seconds < 1:
        raise PreflightError("CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS must be at least 1 second")
    if failure_exit_seconds <= 0:
        raise PreflightError("CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS must be positive")
    return poll_seconds, failure_exit_seconds


def normalized_child_status(returncode: int) -> int:
    return 128 + (-returncode) if returncode < 0 else returncode


def stop_child(child: subprocess.Popen[Any], *, timeout: float = 10.0) -> None:
    if child.poll() is not None:
        return
    child.terminate()
    try:
        child.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()


def supervise_ckpool(
    env: dict[str, str],
    rpc: RpcClient,
    command: Sequence[str],
    *,
    popen: Callable[..., subprocess.Popen[Any]] = subprocess.Popen,
    monotonic: Callable[[], float] = time.monotonic,
    startup_readiness: Callable[[dict[str, str], RpcClient], list[str]] = validate_readiness,
) -> int:
    """Run CKPool while independently enforcing the public-chain mining contract."""

    if not command:
        raise PreflightError("--supervise requires a CKPool command")
    validate_production_gate(env)
    poll_seconds, failure_exit_seconds = watchdog_settings(env)

    # The startup wrapper has just run the full preflight. Recheck readiness and
    # the template here so direct supervisor use also stays fail closed.
    startup_readiness(env, rpc)
    status = fetch_and_validate_template(env, rpc)
    checked_at = monotonic()
    last_success = checked_at
    valid_until = checked_at + max(0, status.max_age_seconds - status.age_seconds)
    child = popen(list(command))

    previous_handlers: dict[int, Any] = {}
    shutdown_signal: int | None = None
    shutdown_deadline: float | None = None

    def forward_signal(signum: int, _frame: Any) -> None:
        nonlocal shutdown_signal, shutdown_deadline
        shutdown_signal = signum
        shutdown_deadline = monotonic() + 10.0
        if child.poll() is None:
            child.send_signal(signum)

    try:
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, forward_signal)

        while True:
            now = monotonic()
            if shutdown_deadline is not None:
                wait_seconds = min(poll_seconds, max(1.0, shutdown_deadline - now))
            else:
                wait_seconds = min(poll_seconds, max(1.0, valid_until - now))
            try:
                returncode = child.wait(timeout=wait_seconds)
            except subprocess.TimeoutExpired:
                pass
            else:
                return normalized_child_status(returncode)

            returncode = child.poll()
            if returncode is not None:
                return normalized_child_status(returncode)

            if shutdown_deadline is not None:
                if monotonic() >= shutdown_deadline:
                    child.kill()
                    child.wait()
                    assert shutdown_signal is not None
                    return 128 + shutdown_signal
                continue

            try:
                status = fetch_and_validate_template(env, rpc)
                validate_runtime_readiness(env, rpc)
            except RuntimeReadinessError as exc:
                now = monotonic()
                failure_deadline = min(last_success + failure_exit_seconds, valid_until)
                remaining = failure_deadline - now
                if remaining <= 0:
                    print(
                        "qbit ckpool watchdog: FAIL: runtime readiness remained unsafe "
                        f"past the failure deadline: {exc}",
                        file=sys.stderr,
                    )
                    stop_child(child)
                    return 1
                print(
                    "qbit ckpool watchdog: runtime readiness is unsafe; "
                    f"failing closed in at most {remaining:.1f}s: {exc}",
                    file=sys.stderr,
                )
                valid_until = failure_deadline
                continue
            except TemplateValidationError as exc:
                print(f"qbit ckpool watchdog: FAIL: {exc}", file=sys.stderr)
                stop_child(child)
                return 1
            except (OSError, json.JSONDecodeError, PreflightError) as exc:
                now = monotonic()
                failure_deadline = min(last_success + failure_exit_seconds, valid_until)
                remaining = failure_deadline - now
                if remaining <= 0:
                    print(
                        "qbit ckpool watchdog: FAIL: mining-state validation unavailable "
                        f"past safe deadline: {exc}",
                        file=sys.stderr,
                    )
                    stop_child(child)
                    return 1
                print(
                    "qbit ckpool watchdog: mining-state validation unavailable; "
                    f"failing closed in at most {remaining:.1f}s: {exc}",
                    file=sys.stderr,
                )
                valid_until = failure_deadline
                continue

            checked_at = monotonic()
            last_success = checked_at
            valid_until = checked_at + max(0, status.max_age_seconds - status.age_seconds)
    finally:
        if child.poll() is None:
            stop_child(child)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def run_preflight(env: dict[str, str], rpc: RpcClient) -> list[str]:
    messages: list[str] = []
    messages.extend(validate_production_gate(env))
    messages.extend(validate_ckpool_knobs(env))
    messages.extend(validate_difficulty_policy(env))
    messages.extend(validate_readiness(env, rpc))
    messages.extend(validate_template_assumptions(env, rpc))
    messages.extend(validate_payout_address(env, rpc))
    return messages


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--production-gate-only",
        action="store_true",
        help="validate production-only settings without making RPC calls",
    )
    parser.add_argument(
        "--supervise",
        nargs=argparse.REMAINDER,
        metavar="COMMAND",
        help="run a CKPool command with the public-chain mining-state watchdog",
    )
    args = parser.parse_args(argv)
    env = dict(os.environ)
    try:
        if args.production_gate_only and args.supervise is not None:
            raise PreflightError("--production-gate-only and --supervise are mutually exclusive")
        if args.supervise is not None:
            command = args.supervise
            if command[:1] == ["--"]:
                command = command[1:]
            if not command:
                raise PreflightError("--supervise requires a CKPool command")
            validate_production_gate(env)
            if not watchdog_required(env):
                os.execvp(command[0], command)
            return supervise_ckpool(env, build_rpc_client(env), command)
        if args.production_gate_only:
            messages = validate_production_gate(env)
        else:
            messages = run_preflight(env, build_rpc_client(env))
    except (KeyError, OSError, json.JSONDecodeError, PreflightError) as exc:
        print(f"qbit ckpool preflight: FAIL: {exc}", file=sys.stderr)
        return 1

    for message in messages:
        print(f"qbit ckpool preflight: {message}", file=sys.stderr)
    if args.production_gate_only:
        print("qbit ckpool preflight: production gate-only PASS", file=sys.stderr)
    else:
        print("qbit ckpool preflight: PASS", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
