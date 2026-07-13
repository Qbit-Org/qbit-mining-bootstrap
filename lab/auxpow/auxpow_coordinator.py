#!/usr/bin/env python3
"""Run the AuxPoW operator lab against qbit and Bitcoin nodes."""

from __future__ import annotations

import base64
import copy
import io
import json
import os
import socket
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lab.auxpow import stratum_codec
from lab.auxpow import vardiff
from test_framework.auxpow import (
    AuxPowPayload,
    MERGED_MINING_HEADER,
    check_merkle_branch,
    get_expected_index,
)
from test_framework.blocktools import add_witness_commitment, create_block, create_coinbase
from test_framework.messages import CBlockHeader, CTransaction, hash256, ser_uint256, uint256_from_compact, uint256_from_str
from test_framework.script import CScript

getcontext().prec = 40


PUBLIC_CHAINS = {"mainnet", "testnet", "testnet3", "testnet4", "signet"}
QBIT_RPC_CHAIN_NAMES = {
    "mainnet": {"main", "mainnet"},
    "testnet": {"test", "testnet"},
    "testnet3": {"test", "testnet3"},
    "testnet4": {"testnet4"},
    "signet": {"signet"},
    "regtest": {"regtest"},
}
BITCOIN_RPC_CHAIN_NAMES = {
    "mainnet": {"main"},
    "testnet": {"test"},
    "testnet3": {"test"},
    "testnet4": {"testnet4"},
    "signet": {"signet"},
    "regtest": {"regtest"},
}
KNOWN_BITCOIN_GENESIS_HASHES = {
    "mainnet": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
}
AUTOMATIC_WALLET_READY_TIMEOUT_SECONDS = 180.0
AUTOMATIC_WALLET_RETRY_SECONDS = 1.0


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise SystemExit(f"{name} is required")
    return value


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_nonnegative_int(name: str, default: int) -> int:
    value = env_int(name, default)
    if value < 0:
        raise SystemExit(f"{name} must be >= 0")
    return value


def env_decimal(name: str, default: str) -> Decimal:
    try:
        value = Decimal(env(name, default))
    except InvalidOperation as exc:
        raise SystemExit(f"{name} is not a valid decimal") from exc
    if value <= 0:
        raise SystemExit(f"{name} must be > 0")
    return value


def env_optional_decimal(name: str) -> Decimal | None:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value == "":
        return None
    try:
        value = Decimal(raw_value)
    except InvalidOperation as exc:
        raise SystemExit(f"{name} is not a valid decimal") from exc
    if value <= 0:
        raise SystemExit(f"{name} must be > 0")
    return value


def env_decimal_default_on_empty(name: str, default: str) -> Decimal:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value == "":
        raw_value = default
    try:
        value = Decimal(raw_value)
    except InvalidOperation as exc:
        raise SystemExit(f"{name} is not a valid decimal") from exc
    if value <= 0:
        raise SystemExit(f"{name} must be > 0")
    return value


def env_nonnegative_decimal(name: str, default: str) -> Decimal:
    try:
        value = Decimal(env(name, default))
    except InvalidOperation as exc:
        raise SystemExit(f"{name} is not a valid decimal") from exc
    if value < 0:
        raise SystemExit(f"{name} must be >= 0")
    return value


def parse_mask_hex(value: object, *, field_name: str) -> int:
    try:
        return stratum_codec.parse_mask_hex(value, field_name=field_name)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an 8-character hex string") from exc


def env_mask(name: str, default: str) -> int:
    try:
        return parse_mask_hex(env(name, default), field_name=name)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def format_mask_hex(value: int) -> str:
    return stratum_codec.format_mask_hex(value)


def env_decimal_map(name: str) -> dict[str, Decimal]:
    raw_value = os.environ.get(name)
    if not raw_value:
        return {}
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{name} must be a JSON object mapping worker names to hashes/sec") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"{name} must be a JSON object mapping worker names to hashes/sec")
    result: dict[str, Decimal] = {}
    for worker, hash_rate in payload.items():
        try:
            parsed = Decimal(str(hash_rate))
        except InvalidOperation as exc:
            raise SystemExit(f"{name}.{worker} is not a valid decimal") from exc
        if parsed < 0:
            raise SystemExit(f"{name}.{worker} must be >= 0")
        result[str(worker)] = parsed
    return result


def load_vardiff_config() -> vardiff.VardiffConfig:
    target_interval = env_decimal("AUXPOW_STRATUM_VARDIFF_TARGET_SHARE_SECONDS", "5")
    target_shares_per_second = env_optional_decimal("AUXPOW_STRATUM_VARDIFF_TARGET_SHARES_PER_SECOND")
    if target_shares_per_second is not None:
        target_interval = Decimal(1) / target_shares_per_second
    try:
        return vardiff.VardiffConfig(
            enabled=env_bool("AUXPOW_STRATUM_VARDIFF_ENABLED", True),
            target_share_interval_seconds=target_interval,
            min_difficulty=env_decimal("AUXPOW_STRATUM_VARDIFF_MIN_DIFF", "1024"),
            max_difficulty=env_decimal("AUXPOW_STRATUM_VARDIFF_MAX_DIFF", "4294967296"),
            retarget_interval_seconds=env_decimal("AUXPOW_STRATUM_VARDIFF_RETARGET_SECONDS", "120"),
            max_step_factor=env_decimal("AUXPOW_STRATUM_VARDIFF_MAX_STEP_FACTOR", "4"),
            startup_difficulty=env_decimal_default_on_empty(
                "AUXPOW_STRATUM_VARDIFF_STARTUP_DIFF",
                "8192",
            ),
            max_step_down_factor=env_decimal("AUXPOW_STRATUM_VARDIFF_MAX_STEP_DOWN_FACTOR", "2"),
            ewma_alpha=env_decimal("AUXPOW_STRATUM_VARDIFF_EWMA_ALPHA", "0.4"),
            retarget_tolerance=env_nonnegative_decimal("AUXPOW_STRATUM_VARDIFF_RETARGET_TOLERANCE", "0.25"),
        )
    except ValueError as exc:
        raise SystemExit(f"AUXPOW_STRATUM_VARDIFF_* configuration is invalid: {exc}") from exc


class JsonRpc:
    def __init__(self, *, host: str, port: int, user: str, password: str):
        self.url = f"http://{host}:{port}"
        credentials = f"{user}:{password}".encode()
        self.auth = f"Basic {base64.b64encode(credentials).decode()}"

    def call(self, method: str, params: list[object] | None = None, *, wallet: str | None = None) -> object:
        body = json.dumps(
            {
                "jsonrpc": "1.0",
                "id": method,
                "method": method,
                "params": params or [],
            }
        ).encode()
        url = self.url
        if wallet:
            url += "/wallet/" + wallet
        request = urllib.request.Request(
            url,
            data=body,
            headers={
                "Accept": "application/json, */*",
                "Authorization": self.auth,
                "Content-Type": "application/json",
                "User-Agent": "qbit-mining-bootstrap/auxpow",
            },
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read())
        if payload["error"] is not None:
            raise RuntimeError(f"RPC {method} failed: {payload['error']}")
        return payload["result"]


def compact_target(compact_bits: int) -> int:
    return uint256_from_compact(compact_bits)


def difficulty_target(difficulty: Decimal) -> int:
    target = DIFF1_TARGET / difficulty
    return max(1, int(target))


def target_difficulty(target: int) -> Decimal:
    return Decimal(DIFF1_TARGET) / Decimal(target)


def double_sha256(data: bytes) -> bytes:
    return stratum_codec.double_sha256(data)


def flip_word_bytes(data: bytes) -> bytes:
    return stratum_codec.flip_word_bytes(data)


def assert_equal(actual, expected, message: str) -> None:
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


@dataclass(frozen=True)
class ResolvedAddress:
    address: str
    script_pubkey_hex: str


def ensure_wallet_loaded(
    rpc: JsonRpc,
    wallet_name: str,
    *,
    deadline: float | None = None,
) -> Exception | None:
    last_error: Exception | None = None
    for method in ("createwallet", "loadwallet"):
        if deadline is not None and time.monotonic() >= deadline:
            return last_error or TimeoutError("wallet readiness deadline elapsed")
        try:
            rpc.call(method, [wallet_name])
            return None
        except Exception as exc:
            last_error = exc
    return last_error


def get_new_address(
    rpc: JsonRpc,
    wallet_name: str,
    *,
    timeout_seconds: float = AUTOMATIC_WALLET_READY_TIMEOUT_SECONDS,
    retry_seconds: float = AUTOMATIC_WALLET_RETRY_SECONDS,
) -> str:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")
    if retry_seconds <= 0:
        raise ValueError("retry_seconds must be > 0")

    deadline = time.monotonic() + timeout_seconds
    attempts = 0
    last_error: Exception | None = None
    while True:
        if attempts > 0 and time.monotonic() >= deadline:
            break
        attempts += 1
        wallet_error = ensure_wallet_loaded(rpc, wallet_name, deadline=deadline)
        if wallet_error is not None:
            last_error = wallet_error

        for params in ([], ["", "p2mr"], ["", "bech32"]):
            if time.monotonic() >= deadline:
                break
            try:
                address = rpc.call("getnewaddress", params, wallet=wallet_name)
            except Exception as exc:
                last_error = exc
                continue
            if address:
                return str(address)
            last_error = RuntimeError("RPC getnewaddress returned an empty result")

        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            break
        time.sleep(min(retry_seconds, remaining_seconds))

    detail = f"; last RPC error: {last_error}" if last_error is not None else ""
    raise RuntimeError(
        f"{wallet_name} wallet did not return an address within "
        f"{timeout_seconds:g}s after {attempts} attempts{detail}"
    ) from last_error


def resolve_validated_address(rpc: JsonRpc, address: str, *, field_name: str) -> ResolvedAddress:
    validation = rpc.call("validateaddress", [address])
    if not validation.get("isvalid"):
        raise RuntimeError(f"{field_name} is not valid for the configured chain: {address}")
    script_pubkey_hex = validation.get("scriptPubKey")
    if not script_pubkey_hex:
        raise RuntimeError(f"{field_name} did not return scriptPubKey metadata: {address}")
    return ResolvedAddress(address=address, script_pubkey_hex=str(script_pubkey_hex))


def resolve_qbit_miner_address(qbit_rpc: JsonRpc) -> str:
    if QBIT_MINER_ADDRESS != "auto":
        return resolve_validated_address(qbit_rpc, QBIT_MINER_ADDRESS, field_name="QBIT_MINER_ADDRESS").address
    return get_new_address(qbit_rpc, QBIT_MINER_WALLET_NAME)


def resolve_bitcoin_miner_address(bitcoin_rpc: JsonRpc) -> ResolvedAddress:
    if BITCOIN_MINER_ADDRESS != "auto":
        return resolve_validated_address(bitcoin_rpc, BITCOIN_MINER_ADDRESS, field_name="BITCOIN_MINER_ADDRESS")
    address = get_new_address(bitcoin_rpc, BITCOIN_MINER_WALLET_NAME)
    return resolve_validated_address(bitcoin_rpc, address, field_name="BITCOIN_MINER_ADDRESS")


def auxpow_commitment_order(aux_template: dict[str, object]) -> str:
    raw_order = aux_template.get("commitmentorder")
    if raw_order is None:
        return "internal"
    order = str(raw_order).lower()
    if order not in {"display", "internal"}:
        raise RuntimeError(f"createauxblock returned unsupported commitmentorder={raw_order!r}")
    return order


def uint256_commitment_bytes(value: int, *, commitment_order: str) -> bytes:
    internal_bytes = ser_uint256(value)
    if commitment_order == "internal":
        return internal_bytes
    if commitment_order == "display":
        return internal_bytes[::-1]
    raise RuntimeError(f"unsupported AuxPoW commitment order {commitment_order!r}")


def build_chain_commitment(aux_template: dict[str, object], *, chain_nonce: int = 0) -> tuple[bytes, int]:
    chain_merkle_branch: list[int] = []
    chain_index = get_expected_index(
        nonce=chain_nonce,
        chain_id=aux_template["chainid"],
        merkle_height=len(chain_merkle_branch),
    )
    chain_root = check_merkle_branch(leaf=int(aux_template["hash"], 16), branch=chain_merkle_branch, index=chain_index)
    commitment = (
        MERGED_MINING_HEADER
        + uint256_commitment_bytes(chain_root, commitment_order=auxpow_commitment_order(aux_template))
        + (1 << len(chain_merkle_branch)).to_bytes(4, "little")
        + chain_nonce.to_bytes(4, "little")
    )
    return commitment, chain_index


def build_parent_coinbase(
    *,
    height: int,
    coinbase_value: int,
    script_pubkey_hex: str,
    commitment: bytes,
    extranonce_prefix: bytes = b"",
    extranonce_suffix: bytes = b"",
) -> CTransaction:
    coinbase = create_coinbase(height, script_pubkey=CScript(bytes.fromhex(script_pubkey_hex)))
    coinbase.vout[0].nValue = coinbase_value
    script_sig = bytes(coinbase.vin[0].scriptSig) + extranonce_prefix + extranonce_suffix + bytes(CScript([commitment]))
    coinbase.vin[0].scriptSig = CScript(script_sig)
    return coinbase


def build_coinbase_merkle_branch(block) -> list[int]:
    hashes = [tx.txid_int for tx in block.vtx]
    index = 0
    branch: list[int] = []
    while len(hashes) > 1:
        sibling_index = index ^ 1
        if sibling_index >= len(hashes):
            sibling_index = index
        branch.append(hashes[sibling_index])
        next_hashes: list[int] = []
        for offset in range(0, len(hashes), 2):
            left = hashes[offset]
            right = hashes[offset + 1] if offset + 1 < len(hashes) else hashes[offset]
            next_hashes.append(uint256_from_str(hash256(ser_uint256(left) + ser_uint256(right))))
        hashes = next_hashes
        index //= 2
    return branch


def compute_coinbase_merkle_root(coinbase_bytes: bytes, branch: list[int]) -> bytes:
    merkle = double_sha256(coinbase_bytes)
    for sibling_hash in branch:
        merkle = double_sha256(merkle + ser_uint256(sibling_hash))
    return merkle


def build_parent_block(
    *,
    aux_template: dict[str, object],
    btc_template: dict[str, object],
    bitcoin_script_pubkey_hex: str,
    extranonce_prefix: bytes = b"",
    extranonce_suffix: bytes = b"",
    chain_nonce: int = 0,
    parent_time: int | None = None,
    header_nonce: int = 0,
) -> tuple[AuxPowPayload, object]:
    commitment, chain_index = build_chain_commitment(aux_template, chain_nonce=chain_nonce)
    coinbase = build_parent_coinbase(
        height=int(btc_template["height"]),
        coinbase_value=int(btc_template["coinbasevalue"]),
        script_pubkey_hex=bitcoin_script_pubkey_hex,
        commitment=commitment,
        extranonce_prefix=extranonce_prefix,
        extranonce_suffix=extranonce_suffix,
    )
    ntime = int(btc_template["curtime"]) if parent_time is None else parent_time
    txlist = [str(tx["data"]) for tx in btc_template.get("transactions", [])]
    block = create_block(
        hashprev=int(btc_template["previousblockhash"], 16),
        coinbase=coinbase,
        ntime=ntime,
        version=int(btc_template["version"]),
        tmpl={
            "previousblockhash": btc_template["previousblockhash"],
            "bits": btc_template["bits"],
            "height": btc_template["height"],
            "curtime": ntime,
        },
        txlist=txlist,
    )
    if btc_template.get("default_witness_commitment"):
        add_witness_commitment(block)
    block.nBits = int(str(btc_template["bits"]), 16)
    block.nNonce = header_nonce
    coinbase_merkle_branch = build_coinbase_merkle_branch(block)
    payload = AuxPowPayload(
        coinbase_tx=block.vtx[0],
        coinbase_merkle_branch=coinbase_merkle_branch,
        coinbase_branch_index=0,
        chain_merkle_branch=[],
        chain_index=chain_index,
        parent_block=CBlockHeader(block),
    )
    return payload, block


def solve_block_for_targets(block, extra_compact_bits: list[int] | None = None) -> None:
    targets = [compact_target(block.nBits)]
    for bits in extra_compact_bits or []:
        targets.append(compact_target(bits))
    target = min(targets)
    block.nNonce = 0
    while block.hash_int > target:
        block.nNonce += 1


def solve_auxpow_parent(payload: AuxPowPayload, aux_compact_bits: int) -> None:
    parent_target = compact_target(payload.parent_block.nBits)
    aux_target = compact_target(aux_compact_bits)
    target = min(parent_target, aux_target)
    payload.parent_block.nNonce = 0
    while payload.parent_block.hash_int > target:
        payload.parent_block.nNonce += 1


def invalidate_auxpow_parent(payload: AuxPowPayload, aux_compact_bits: int) -> None:
    aux_target = compact_target(aux_compact_bits)
    while payload.parent_block.hash_int <= aux_target:
        payload.parent_block.nNonce += 1


def submit_bitcoin_block(bitcoin_rpc: JsonRpc, block) -> None:
    before = int(bitcoin_rpc.call("getblockcount"))
    result = bitcoin_rpc.call("submitblock", [block.serialize().hex()])
    after = int(bitcoin_rpc.call("getblockcount"))
    if result not in (None, "duplicate"):
        raise AssertionError(f"bitcoind rejected the parent block: {result}")
    if after != before + 1:
        raise AssertionError(f"bitcoind height did not advance after submitblock: {before} -> {after}")


def positive_auxpow_path(
    qbit_rpc: JsonRpc,
    bitcoin_rpc: JsonRpc,
    qbit_miner_address: str,
    bitcoin_miner_address: ResolvedAddress,
) -> None:
    print("auxpow: positive end-to-end path", flush=True)
    aux_template = qbit_rpc.call("createauxblock", [qbit_miner_address])
    btc_template = bitcoin_rpc.call("getblocktemplate", [{"rules": ["segwit"]}])
    qbit_before = int(qbit_rpc.call("getblockcount"))
    payload, parent_block = build_parent_block(
        aux_template=aux_template,
        btc_template=btc_template,
        bitcoin_script_pubkey_hex=bitcoin_miner_address.script_pubkey_hex,
    )
    solve_block_for_targets(parent_block, [int(str(aux_template["bits"]), 16)])
    payload.parent_block = CBlockHeader(parent_block)
    submit_bitcoin_block(bitcoin_rpc, parent_block)
    submit_result = qbit_rpc.call("submitauxblock", [aux_template["hash"], payload.to_hex()])
    qbit_after = int(qbit_rpc.call("getblockcount"))
    assert_equal(submit_result, None, "submitauxblock should accept a valid AuxPoW payload")
    if qbit_after != qbit_before + 1:
        raise AssertionError(f"qbit height did not advance after submitauxblock: {qbit_before} -> {qbit_after}")


def reject_invalid_parent_pow(
    qbit_rpc: JsonRpc,
    bitcoin_rpc: JsonRpc,
    qbit_miner_address: str,
    bitcoin_miner_address: ResolvedAddress,
) -> None:
    print("auxpow: reject parent headers that miss qbit's AuxPoW target", flush=True)
    aux_template = qbit_rpc.call("createauxblock", [qbit_miner_address])
    btc_template = bitcoin_rpc.call("getblocktemplate", [{"rules": ["segwit"]}])
    payload, _ = build_parent_block(
        aux_template=aux_template,
        btc_template=btc_template,
        bitcoin_script_pubkey_hex=bitcoin_miner_address.script_pubkey_hex,
    )
    invalidate_auxpow_parent(payload, int(str(aux_template["bits"]), 16))
    result = qbit_rpc.call("submitauxblock", [aux_template["hash"], payload.to_hex()])
    assert_equal(result, "bad-auxpow-parent-hash", "submitauxblock should reject invalid parent PoW")


def reject_invalid_commitment(
    qbit_rpc: JsonRpc,
    bitcoin_rpc: JsonRpc,
    qbit_miner_address: str,
    bitcoin_miner_address: ResolvedAddress,
) -> None:
    print("auxpow: reject malformed merged-mining commitments", flush=True)
    aux_template = qbit_rpc.call("createauxblock", [qbit_miner_address])
    btc_template = bitcoin_rpc.call("getblocktemplate", [{"rules": ["segwit"]}])
    payload, _ = build_parent_block(
        aux_template=aux_template,
        btc_template=btc_template,
        bitcoin_script_pubkey_hex=bitcoin_miner_address.script_pubkey_hex,
    )
    payload.coinbase_tx.vin[0].scriptSig = CScript([b"broken-commitment"])
    payload.update_parent_merkle_root()
    solve_auxpow_parent(payload, int(str(aux_template["bits"]), 16))
    result = qbit_rpc.call("submitauxblock", [aux_template["hash"], payload.to_hex()])
    assert_equal(result, "bad-auxpow-commitment", "submitauxblock should reject malformed commitments")


def reject_stale_template(
    qbit_rpc: JsonRpc,
    bitcoin_rpc: JsonRpc,
    qbit_miner_address: str,
    bitcoin_miner_address: ResolvedAddress,
) -> None:
    print("auxpow: reject stale cached templates after the qbit tip changes", flush=True)
    stale_template = qbit_rpc.call("createauxblock", [qbit_miner_address])
    btc_template = bitcoin_rpc.call("getblocktemplate", [{"rules": ["segwit"]}])
    stale_payload, _ = build_parent_block(
        aux_template=stale_template,
        btc_template=btc_template,
        bitcoin_script_pubkey_hex=bitcoin_miner_address.script_pubkey_hex,
    )
    positive_auxpow_path(qbit_rpc, bitcoin_rpc, qbit_miner_address, bitcoin_miner_address)
    result = qbit_rpc.call("submitauxblock", [stale_template["hash"], stale_payload.to_hex()])
    assert_equal(result, "stale-prevblk", "submitauxblock should reject stale cached aux templates")


def wait_for_rpc(rpc: JsonRpc) -> None:
    for _ in range(60):
        try:
            rpc.call("getblockcount")
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("RPC service never became ready")


def validate_node_readiness(
    rpc: JsonRpc,
    *,
    label: str,
    configured_chain: str,
    rpc_chain_names: dict[str, set[str]],
    expected_genesis_hash: str | None = None,
) -> None:
    expected_rpc_names = rpc_chain_names.get(configured_chain)
    if expected_rpc_names is None:
        raise RuntimeError(f"unsupported {label} chain {configured_chain!r}")

    blockchain_info = rpc.call("getblockchaininfo")
    if not isinstance(blockchain_info, dict):
        raise RuntimeError(f"{label} getblockchaininfo returned a non-object response")
    actual_chain = str(blockchain_info.get("chain", "")).lower()
    if actual_chain not in expected_rpc_names:
        expected = ", ".join(sorted(expected_rpc_names))
        raise RuntimeError(f"{label} RPC chain mismatch: expected {expected}, got {actual_chain or '<unset>'}")

    if configured_chain not in PUBLIC_CHAINS:
        return
    if blockchain_info.get("initialblockdownload") is not False:
        raise RuntimeError(f"{label} is still in initial block download")
    try:
        blocks = int(blockchain_info["blocks"])
        headers = int(blockchain_info["headers"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"{label} did not report numeric blocks and headers") from exc
    if blocks != headers:
        raise RuntimeError(f"{label} is not caught up: blocks={blocks}, headers={headers}")

    network_info = rpc.call("getnetworkinfo")
    if not isinstance(network_info, dict):
        raise RuntimeError(f"{label} getnetworkinfo returned a non-object response")
    try:
        connections = int(network_info["connections"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"{label} did not report a numeric connection count") from exc
    if connections < 1:
        raise RuntimeError(f"{label} has no peer connections")

    if expected_genesis_hash:
        actual_genesis = str(rpc.call("getblockhash", [0])).lower()
        if actual_genesis != expected_genesis_hash.lower():
            raise RuntimeError(
                f"{label} genesis mismatch: expected {expected_genesis_hash.lower()}, got {actual_genesis}"
            )


def validate_bitcoin_parent_template(
    parent_template: object,
    *,
    max_age_seconds: int,
    max_future_seconds: int = 7200,
) -> int:
    if not isinstance(parent_template, dict) or not parent_template.get("previousblockhash"):
        raise RuntimeError("Bitcoin getblocktemplate did not return a usable template")
    try:
        template_time = int(parent_template["curtime"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Bitcoin template did not report a numeric curtime") from exc
    template_age = int(time.time()) - template_time
    if template_age > max_age_seconds:
        raise RuntimeError(
            f"Bitcoin template is stale: age={template_age}s exceeds {max_age_seconds}s"
        )
    if template_age < -max_future_seconds:
        raise RuntimeError(
            "Bitcoin template is future-dated: "
            f"ahead={-template_age}s exceeds {max_future_seconds}s"
        )
    return template_age


def validate_auxpow_templates(
    qbit_rpc: JsonRpc,
    bitcoin_rpc: JsonRpc,
    *,
    qbit_miner_address: str,
    max_age_seconds: int,
    max_future_seconds: int = 7200,
) -> None:
    aux_template = qbit_rpc.call("createauxblock", [qbit_miner_address])
    if not isinstance(aux_template, dict) or not aux_template.get("hash"):
        raise RuntimeError("qbit createauxblock did not return a usable template")

    parent_template = bitcoin_rpc.call("getblocktemplate", [{"rules": ["segwit"]}])
    validate_bitcoin_parent_template(
        parent_template,
        max_age_seconds=max_age_seconds,
        max_future_seconds=max_future_seconds,
    )


def validate_auxpow_startup(qbit_rpc: JsonRpc, bitcoin_rpc: JsonRpc) -> None:
    if QBIT_CHAIN == "mainnet" and BITCOIN_CHAIN != "mainnet":
        raise RuntimeError("qbit mainnet AuxPoW requires BITCOIN_CHAIN=mainnet")
    if BITCOIN_CHAIN == "mainnet" and AUXPOW_MODE != "stratum":
        raise RuntimeError(
            "Bitcoin mainnet AuxPoW requires AUXPOW_MODE=stratum; bridge and once are lab-only"
        )
    if BITCOIN_CHAIN == "mainnet" and AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS <= 0:
        raise RuntimeError(
            "Bitcoin mainnet AuxPoW requires a positive "
            "AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS"
        )
    if BITCOIN_CHAIN == "mainnet":
        expected_bitcoin_genesis = KNOWN_BITCOIN_GENESIS_HASHES["mainnet"]
        if not BITCOIN_EXPECTED_GENESIS_HASH:
            raise RuntimeError(
                "BITCOIN_EXPECTED_GENESIS_HASH is required for Bitcoin mainnet AuxPoW"
            )
        if BITCOIN_EXPECTED_GENESIS_HASH.lower() != expected_bitcoin_genesis:
            raise RuntimeError(
                "BITCOIN_EXPECTED_GENESIS_HASH must equal the canonical Bitcoin mainnet genesis"
            )
        if AUXPOW_STRATUM_HEADER_VARIANT != "canonical":
            raise RuntimeError(
                "Bitcoin mainnet AuxPoW requires AUXPOW_STRATUM_HEADER_VARIANT=canonical"
            )
        if AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT:
            raise RuntimeError(
                "Bitcoin mainnet AuxPoW rejects AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT=1"
            )
    if QBIT_CHAIN == "mainnet":
        if not QBIT_EXPECTED_GENESIS_HASH:
            raise RuntimeError("QBIT_EXPECTED_GENESIS_HASH is required for mainnet AuxPoW")
        if len(QBIT_EXPECTED_GENESIS_HASH) != 64 or any(
            character not in "0123456789abcdefABCDEF" for character in QBIT_EXPECTED_GENESIS_HASH
        ):
            raise RuntimeError("QBIT_EXPECTED_GENESIS_HASH must be 64 hex characters")
        if QBIT_MINER_ADDRESS == "auto":
            raise RuntimeError("mainnet AuxPoW requires an explicit QBIT_MINER_ADDRESS")
    if BITCOIN_CHAIN == "mainnet" and BITCOIN_MINER_ADDRESS == "auto":
        raise RuntimeError("Bitcoin mainnet AuxPoW requires an explicit BITCOIN_MINER_ADDRESS")

    validate_node_readiness(
        qbit_rpc,
        label="qbit",
        configured_chain=QBIT_CHAIN,
        rpc_chain_names=QBIT_RPC_CHAIN_NAMES,
        expected_genesis_hash=QBIT_EXPECTED_GENESIS_HASH or None,
    )
    validate_node_readiness(
        bitcoin_rpc,
        label="Bitcoin",
        configured_chain=BITCOIN_CHAIN,
        rpc_chain_names=BITCOIN_RPC_CHAIN_NAMES,
        expected_genesis_hash=BITCOIN_EXPECTED_GENESIS_HASH or None,
    )


@dataclass
class AuxPowStratumJob:
    job_id: str
    aux_template: dict[str, object]
    btc_template: dict[str, object]
    bitcoin_script_pubkey_hex: str
    chain_nonce: int
    chain_index: int
    share_target: int
    qbit_target: int
    parent_target: int
    share_difficulty: Decimal
    coinbase_merkle_branch: list[int]
    prevhash: str
    coinb1: str
    coinb2: str
    version: str
    nbits: str
    ntime: str
    clean_jobs: bool = True
    created_at_monotonic: float = field(default_factory=time.monotonic)


@dataclass(eq=False)
class StratumClientState:
    sock: socket.socket
    address: tuple[str, int]
    extranonce1_hex: str
    connection_id: int
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    subscribed: bool = False
    authorized: bool = False
    username: str = ""
    version_mask: int = 0
    requested_version_mask: int | None = None
    version_min_bit_count: int | None = None
    agent: str = ""
    connected_at_monotonic: float = field(default_factory=time.monotonic)
    last_submit_monotonic: float | None = None
    share_difficulty: Decimal = Decimal("1")
    pending_share_difficulty: Decimal | None = None
    vardiff_window_started_monotonic: float = field(default_factory=time.monotonic)
    vardiff_window_accepted: int = 0
    vardiff_window_submitted: int = 0
    vardiff_window_work: Decimal = Decimal("0")
    vardiff_difficulty_estimate: Decimal | None = None
    last_accepted_share_monotonic: float | None = None
    active_job_ids: set[str] = field(default_factory=set)

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


@dataclass
class WorkerStats:
    first_seen_monotonic: float = field(default_factory=time.monotonic)
    last_submit_monotonic: float | None = None
    submitted: int = 0
    accepted: int = 0
    low_difficulty: int = 0
    stale: int = 0
    duplicate: int = 0
    qbit_candidates: int = 0
    qbit_accepted: int = 0
    parent_submitted: int = 0
    parent_accepted: int = 0


class StratumError(RuntimeError):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class AuxPowStratumServer:
    def __init__(
        self,
        *,
        qbit_rpc: JsonRpc,
        bitcoin_rpc: JsonRpc,
        qbit_miner_address: str,
        bitcoin_miner_address: ResolvedAddress,
    ):
        self.qbit_rpc = qbit_rpc
        self.bitcoin_rpc = bitcoin_rpc
        self.qbit_miner_address = qbit_miner_address
        self.bitcoin_miner_address = bitcoin_miner_address
        self.fixed_share_difficulty = AUXPOW_STRATUM_SHARE_DIFF
        self.vardiff_config = AUXPOW_STRATUM_VARDIFF
        self.clients: set[StratumClientState] = set()
        self.jobs: dict[str, AuxPowStratumJob] = {}
        self.current_job: AuxPowStratumJob | None = None
        self.tip_snapshot: tuple[str, str] | None = None
        self.lock = threading.RLock()
        self.clients_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.refresh_now = threading.Event()
        self.job_counter = 0
        self.extranonce_counter = 0
        self.connection_counter = 0
        self.header_variant = AUXPOW_STRATUM_HEADER_VARIANT
        self.version_mask = AUXPOW_STRATUM_VERSION_MASK
        self.worker_stats: dict[str, WorkerStats] = {}
        self.recent_share_keys: set[tuple[str, ...]] = set()
        self.last_stats_monotonic = time.monotonic()
        self.last_successful_refresh_monotonic: float | None = None
        self.refresh_fatal_error: str | None = None

    def next_connection_id(self) -> int:
        with self.lock:
            self.connection_counter += 1
            return self.connection_counter

    def next_job_id(self) -> str:
        with self.lock:
            self.job_counter += 1
            return f"{self.job_counter:016x}"

    def job_age_expired(self, job: AuxPowStratumJob | None, now: float) -> bool:
        return (
            job is not None
            and AUXPOW_STRATUM_JOB_MAX_AGE_SECONDS > 0
            and now - job.created_at_monotonic >= AUXPOW_STRATUM_JOB_MAX_AGE_SECONDS
        )

    def parent_template_age_expired(self, job: AuxPowStratumJob | None) -> bool:
        if job is None:
            return False
        try:
            validate_bitcoin_parent_template(
                job.btc_template,
                max_age_seconds=AUXPOW_TEMPLATE_MAX_AGE_SECONDS,
                max_future_seconds=AUXPOW_TEMPLATE_MAX_FUTURE_SECONDS,
            )
        except RuntimeError:
            return True
        return False

    def fresh_current_job(self) -> AuxPowStratumJob | None:
        with self.lock:
            current_job = self.current_job
            if not self.parent_template_age_expired(current_job):
                return current_job
            self.current_job = None
            self.jobs = {}
        return None

    def invalidate_expired_parent_work(self) -> bool:
        with self.lock:
            if not self.parent_template_age_expired(self.current_job):
                return False
            self.current_job = None
            self.jobs = {}
        return True

    def next_extranonce1_hex(self) -> str:
        with self.lock:
            self.extranonce_counter += 1
            value = self.extranonce_counter & 0xFFFFFFFF
        return value.to_bytes(4, "big").hex()

    def log_event(self, event: str, **fields: object) -> None:
        if not AUXPOW_STRATUM_DIAG_JSONL and not AUXPOW_STRATUM_DIAG_EVENTS:
            return
        payload = {"event": event, **fields}
        if AUXPOW_STRATUM_DIAG_JSONL:
            print(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")), flush=True)
            return
        details = " ".join(f"{key}={value}" for key, value in payload.items())
        print(f"auxpow stratum diag: {details}", flush=True)

    def worker_key(self, client: StratumClientState) -> str:
        return client.username or f"{client.address[0]}:{client.address[1]}"

    def stats_for_worker(self, worker: str) -> WorkerStats:
        with self.lock:
            stats = self.worker_stats.get(worker)
            if stats is None:
                stats = WorkerStats()
                self.worker_stats[worker] = stats
            return stats

    def record_stats(self, worker: str, field_name: str, amount: int = 1) -> None:
        stats = self.stats_for_worker(worker)
        with self.lock:
            setattr(stats, field_name, getattr(stats, field_name) + amount)
            if field_name == "submitted":
                stats.last_submit_monotonic = time.monotonic()

    def note_vardiff_accepted_share(
        self,
        client: StratumClientState,
        job: AuxPowStratumJob,
        worker: str,
    ) -> None:
        if not self.vardiff_config.enabled:
            return
        now = time.monotonic()
        with self.lock:
            client.last_accepted_share_monotonic = now
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
            worker=worker,
            reason="accepted_window",
        )

    def note_vardiff_submitted_share(self, client: StratumClientState) -> None:
        if not self.vardiff_config.enabled:
            return
        with self.lock:
            client.vardiff_window_submitted += 1

    def maybe_retarget_idle_clients(self) -> None:
        if not self.vardiff_config.enabled:
            return
        with self.clients_lock:
            clients = list(self.clients)
        now = time.monotonic()
        for client in clients:
            if not client.subscribed or not client.authorized:
                continue
            with self.lock:
                elapsed_seconds = Decimal(str(max(0.001, now - client.vardiff_window_started_monotonic)))
                if elapsed_seconds < self.vardiff_config.retarget_interval_seconds:
                    continue
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
                worker=self.worker_key(client),
                reason="idle_window",
            )

    def retarget_client(
        self,
        client: StratumClientState,
        *,
        current_difficulty: Decimal,
        accepted_shares: int,
        submitted_shares: int,
        accepted_difficulty: Decimal,
        elapsed_seconds: Decimal,
        worker: str,
        reason: str,
    ) -> None:
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
            client.pending_share_difficulty = next_difficulty
        if next_difficulty == previous_difficulty:
            return
        current_job = self.fresh_current_job()
        try:
            if AUXPOW_STRATUM_VARDIFF_APPLY_MODE == "clean_job" and current_job is not None:
                job = self.send_job_to_client(client, current_job, clean_jobs=True)
                advertised_difficulty = job.share_difficulty
            else:
                self.send_difficulty_value(client, next_difficulty)
                advertised_difficulty = next_difficulty
        except OSError:
            self.disconnect_client(client)
            return
        print(
            "auxpow stratum: vardiff retarget "
            f"user={worker} connection={client.connection_id} reason={reason} "
            f"accepted={accepted_shares} submitted={submitted_shares} elapsed={elapsed_seconds:.3f}s "
            f"old_diff={previous_difficulty.normalize()} desired_diff={next_difficulty.normalize()} "
            f"advertised_diff={advertised_difficulty.normalize()} apply_mode={AUXPOW_STRATUM_VARDIFF_APPLY_MODE}",
            flush=True,
        )
        self.log_event(
            "vardiff",
            user=worker,
            connection_id=client.connection_id,
            reason=reason,
            accepted=accepted_shares,
            submitted=submitted_shares,
            elapsed_seconds=elapsed_seconds,
            observed_difficulty=observed_difficulty,
            smoothed_difficulty=difficulty_estimate,
            old_difficulty=previous_difficulty,
            desired_difficulty=next_difficulty,
            advertised_difficulty=advertised_difficulty,
            apply_mode=AUXPOW_STRATUM_VARDIFF_APPLY_MODE,
        )

    def maybe_log_worker_stats(self) -> None:
        if AUXPOW_STRATUM_STATS_INTERVAL_SECONDS <= 0:
            return
        now = time.monotonic()
        with self.lock:
            if now - self.last_stats_monotonic < AUXPOW_STRATUM_STATS_INTERVAL_SECONDS:
                return
            self.last_stats_monotonic = now
            stats_snapshot = {worker: copy.copy(stats) for worker, stats in self.worker_stats.items()}
            current_job = self.current_job
        for worker, stats in stats_snapshot.items():
            elapsed = max(0.001, now - stats.first_seen_monotonic)
            accepted_per_second = Decimal(stats.accepted) / Decimal(str(elapsed))
            expected_hashrate = AUXPOW_STRATUM_EXPECTED_HASHRATES.get(worker)
            expected_shares_per_second: Decimal | None = None
            observed_expected_ratio: Decimal | None = None
            if expected_hashrate is not None and current_job is not None:
                expected_shares_per_second = expected_hashrate / Decimal(2**32) / current_job.share_difficulty
                if expected_shares_per_second > 0:
                    observed_expected_ratio = accepted_per_second / expected_shares_per_second
            print(
                "auxpow stratum: worker stats "
                f"user={worker} submitted={stats.submitted} accepted={stats.accepted} "
                f"low_diff={stats.low_difficulty} stale={stats.stale} duplicate={stats.duplicate} "
                f"qbit_candidates={stats.qbit_candidates} qbit_accepted={stats.qbit_accepted} "
                f"accepted_per_sec={accepted_per_second:.6f}"
                + (
                    f" expected_per_sec={expected_shares_per_second:.6f} observed_expected={observed_expected_ratio:.3f}"
                    if expected_shares_per_second is not None and observed_expected_ratio is not None
                    else ""
                ),
                flush=True,
            )

    def log_startup_contract(self) -> None:
        versionrollingmask = None
        versionrollingmask_error = None
        try:
            qbit_template = self.qbit_rpc.call("getblocktemplate", [{"rules": ["segwit"]}])
            if isinstance(qbit_template, dict):
                versionrollingmask = qbit_template.get("versionrollingmask")
        except Exception as exc:
            versionrollingmask_error = str(exc)
        print(
            "auxpow stratum: startup "
            f"configured_version_mask={format_mask_hex(self.version_mask)} "
            f"header_variant={self.header_variant} diag_variants={AUXPOW_STRATUM_DIAG_VARIANTS} "
            f"accept_diag_variant={AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT} "
            f"vardiff={'on' if self.vardiff_config.enabled else 'off'} "
            f"share_diff={self.fixed_share_difficulty.normalize()} "
            f"vardiff_target_seconds={self.vardiff_config.target_share_interval_seconds.normalize()} "
            f"vardiff_min={self.vardiff_config.min_difficulty.normalize()} "
            f"vardiff_max={self.vardiff_config.max_difficulty.normalize()} "
            f"vardiff_max_step_up={self.vardiff_config.max_step_factor.normalize()} "
            f"vardiff_max_step_down={self.vardiff_config.max_step_down_factor.normalize()} "
            f"vardiff_ewma_alpha={self.vardiff_config.ewma_alpha.normalize()} "
            f"vardiff_retarget_tolerance={self.vardiff_config.retarget_tolerance.normalize()} "
            f"vardiff_apply_mode={AUXPOW_STRATUM_VARDIFF_APPLY_MODE} "
            f"qbit_versionrollingmask={versionrollingmask if versionrollingmask is not None else '-'}",
            flush=True,
        )
        if versionrollingmask_error:
            print(
                "auxpow stratum: qbit getblocktemplate versionrollingmask probe unavailable "
                f"error={versionrollingmask_error}",
                flush=True,
            )

    def client_startup_difficulty(self) -> Decimal:
        if not self.vardiff_config.enabled:
            return self.fixed_share_difficulty
        return vardiff.clamp(
            self.vardiff_config.startup_difficulty,
            self.vardiff_config.min_difficulty,
            self.vardiff_config.max_difficulty,
        )

    def serve(self) -> int:
        initial_refresh_started = time.monotonic()
        while True:
            try:
                self.refresh_job(force=True)
                self.last_successful_refresh_monotonic = time.monotonic()
                break
            except Exception as exc:
                print(f"auxpow stratum: initial job refresh failed: {exc}", flush=True)
                if (
                    AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS > 0
                    and time.monotonic() - initial_refresh_started
                    >= AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS
                ):
                    raise RuntimeError(
                        "auxpow stratum: initial job refresh failure budget exhausted"
                    ) from exc
                self.stop_event.wait(AUXPOW_STRATUM_POLL_SECONDS)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((AUXPOW_STRATUM_BIND, AUXPOW_STRATUM_PORT))
        listener.listen()
        listener.settimeout(1)
        print(
            f"auxpow stratum: listening on stratum+tcp://{AUXPOW_STRATUM_BIND}:{AUXPOW_STRATUM_PORT}",
            flush=True,
        )
        print(
            "auxpow stratum: miners receive Bitcoin parent work; qbit payout is configured server-side",
            flush=True,
        )
        self.log_startup_contract()
        refresh_thread = threading.Thread(target=self.refresh_loop, daemon=True)
        refresh_thread.start()
        try:
            while not self.stop_event.is_set():
                try:
                    conn, address = listener.accept()
                except socket.timeout:
                    continue
                client = StratumClientState(
                    sock=conn,
                    address=address,
                    extranonce1_hex=self.next_extranonce1_hex(),
                    connection_id=self.next_connection_id(),
                    share_difficulty=self.client_startup_difficulty(),
                )
                with self.clients_lock:
                    self.clients.add(client)
                self.log_event(
                    "client_connected",
                    connection_id=client.connection_id,
                    address=f"{address[0]}:{address[1]}",
                    extranonce1=client.extranonce1_hex,
                )
                thread = threading.Thread(target=self.handle_client, args=(client,), daemon=True)
                thread.start()
        finally:
            listener.close()
            self.stop_event.set()
            refresh_thread.join(timeout=1)
            with self.clients_lock:
                clients = list(self.clients)
                self.clients.clear()
            for client in clients:
                client.close()
        return 1 if self.refresh_fatal_error is not None else 0

    def refresh_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                refreshed = self.refresh_job(force=self.refresh_now.is_set())
                self.last_successful_refresh_monotonic = time.monotonic()
                if refreshed:
                    self.refresh_now.clear()
            except Exception as exc:
                print(f"auxpow stratum: refresh failed: {exc}", flush=True)
                last_success = self.last_successful_refresh_monotonic
                if last_success is None:
                    last_success = time.monotonic()
                    self.last_successful_refresh_monotonic = last_success
                failure_age = time.monotonic() - last_success
                if (
                    AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS > 0
                    and failure_age >= AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS
                ):
                    self.refresh_fatal_error = (
                        "job refresh failed for "
                        f"{failure_age:.1f}s (budget="
                        f"{AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS}s)"
                    )
                    print(f"auxpow stratum: fatal: {self.refresh_fatal_error}", flush=True)
                    self.stop_event.set()
                    return
            self.maybe_log_worker_stats()
            self.maybe_retarget_idle_clients()
            self.stop_event.wait(AUXPOW_STRATUM_POLL_SECONDS)

    def refresh_job(self, *, force: bool) -> bool:
        parent_template_age_expired = self.invalidate_expired_parent_work()
        qbit_best = str(self.qbit_rpc.call("getbestblockhash"))
        bitcoin_best = str(self.bitcoin_rpc.call("getbestblockhash"))
        snapshot = (qbit_best, bitcoin_best)
        now = time.monotonic()
        with self.lock:
            current_job = self.current_job
            tip_snapshot = self.tip_snapshot
            job_age_expired = self.job_age_expired(current_job, now)
            if self.parent_template_age_expired(current_job):
                self.current_job = None
                self.jobs = {}
                current_job = None
                parent_template_age_expired = True
        if (
            not force
            and current_job is not None
            and snapshot == tip_snapshot
            and not job_age_expired
            and not parent_template_age_expired
        ):
            return False
        aux_template = self.qbit_rpc.call("createauxblock", [self.qbit_miner_address])
        commitment_order = auxpow_commitment_order(aux_template)
        btc_template = self.bitcoin_rpc.call("getblocktemplate", [{"rules": ["segwit"]}])
        validate_bitcoin_parent_template(
            btc_template,
            max_age_seconds=AUXPOW_TEMPLATE_MAX_AGE_SECONDS,
            max_future_seconds=AUXPOW_TEMPLATE_MAX_FUTURE_SECONDS,
        )
        job = self.make_job(
            job_id=self.next_job_id(),
            aux_template=aux_template,
            btc_template=btc_template,
            desired_share_difficulty=self.fixed_share_difficulty,
        )
        with self.lock:
            self.tip_snapshot = snapshot
            self.current_job = job
            if self.vardiff_config.enabled:
                self.jobs = {}
            else:
                self.jobs = {job.job_id: job}
        refresh_reason = self.refresh_reason(
            force,
            snapshot,
            tip_snapshot,
            job_age_expired,
            parent_template_age_expired,
        )
        print(
            "auxpow stratum: new job "
            f"{job.job_id} qbit_height={aux_template['height']} bitcoin_height={btc_template['height']} "
            f"share_diff={job.share_difficulty.normalize()} "
            f"commitment_order={commitment_order} "
            f"prevhash={job.prevhash} parent_prevhash={btc_template['previousblockhash']} "
            f"reason={refresh_reason}",
            flush=True,
        )
        self.log_event(
            "job",
            job_id=job.job_id,
            qbit_height=aux_template["height"],
            qbit_hash=aux_template["hash"],
            qbit_bits=aux_template["bits"],
            qbit_commitment_order=commitment_order,
            qbit_commitment_activation_height=aux_template.get("commitmentactivationheight"),
            bitcoin_height=btc_template["height"],
            bitcoin_prevhash=btc_template["previousblockhash"],
            advertised_prevhash=job.prevhash,
            version=job.version,
            nbits=job.nbits,
            ntime=job.ntime,
            share_target=f"{job.share_target:064x}",
            qbit_target=f"{job.qbit_target:064x}",
            parent_target=f"{job.parent_target:064x}",
            chain_nonce=job.chain_nonce,
            chain_index=job.chain_index,
            merkle_branch_length=len(job.coinbase_merkle_branch),
            coinb1_bytes=len(bytes.fromhex(job.coinb1)),
            coinb2_bytes=len(bytes.fromhex(job.coinb2)),
        )
        self.broadcast_job(job)
        return True

    def refresh_reason(
        self,
        force: bool,
        snapshot: tuple[str, str],
        tip_snapshot: tuple[str, str] | None,
        job_age_expired: bool,
        parent_template_age_expired: bool,
    ) -> str:
        if force:
            return "forced"
        if tip_snapshot is None:
            return "initial"
        if snapshot != tip_snapshot:
            return "tip"
        if parent_template_age_expired:
            return "parent-template-age"
        if job_age_expired:
            return "age"
        return "unknown"

    def minimum_advertised_difficulty(self) -> Decimal:
        minimum = AUXPOW_STRATUM_MIN_ADVERTISED_DIFF
        if self.vardiff_config.enabled:
            minimum = max(minimum, self.vardiff_config.min_difficulty)
        return minimum

    def effective_share_target(self, desired_share_difficulty: Decimal, qbit_target: int) -> int:
        effective_share_target = max(difficulty_target(desired_share_difficulty), qbit_target)
        minimum_advertised_difficulty = self.minimum_advertised_difficulty()
        if minimum_advertised_difficulty > 0:
            effective_share_target = min(
                effective_share_target,
                difficulty_target(minimum_advertised_difficulty),
            )
        return effective_share_target

    def make_job(
        self,
        *,
        job_id: str,
        aux_template: dict[str, object],
        btc_template: dict[str, object],
        desired_share_difficulty: Decimal,
    ) -> AuxPowStratumJob:
        placeholder = AUXPOW_PLACEHOLDER_BYTES
        qbit_target = compact_target(int(str(aux_template["bits"]), 16))
        effective_share_target = self.effective_share_target(desired_share_difficulty, qbit_target)
        share_difficulty = target_difficulty(effective_share_target)
        payload, parent_block = build_parent_block(
            aux_template=aux_template,
            btc_template=btc_template,
            bitcoin_script_pubkey_hex=self.bitcoin_miner_address.script_pubkey_hex,
            extranonce_prefix=placeholder,
            chain_nonce=AUXPOW_CHAIN_NONCE,
        )
        coinbase_bytes = parent_block.vtx[0].serialize_without_witness()
        marker_index = coinbase_bytes.find(placeholder)
        if marker_index == -1 or coinbase_bytes.find(placeholder, marker_index + 1) != -1:
            raise RuntimeError("failed to split coinbase into Stratum coinb1/coinb2")
        return AuxPowStratumJob(
            job_id=job_id,
            aux_template=aux_template,
            btc_template=btc_template,
            bitcoin_script_pubkey_hex=self.bitcoin_miner_address.script_pubkey_hex,
            chain_nonce=AUXPOW_CHAIN_NONCE,
            chain_index=payload.chain_index,
            share_target=effective_share_target,
            qbit_target=qbit_target,
            parent_target=compact_target(int(str(btc_template["bits"]), 16)),
            share_difficulty=share_difficulty,
            coinbase_merkle_branch=payload.coinbase_merkle_branch,
            prevhash=stratum_codec.stratum_prevhash_from_display_hash(str(btc_template["previousblockhash"])),
            coinb1=coinbase_bytes[:marker_index].hex(),
            coinb2=coinbase_bytes[marker_index + len(placeholder) :].hex(),
            version=f"{int(btc_template['version']) & 0xFFFFFFFF:08x}",
            nbits=str(btc_template["bits"]),
            ntime=f"{int(btc_template['curtime']) & 0xFFFFFFFF:08x}",
        )

    def broadcast_job(self, job: AuxPowStratumJob) -> None:
        with self.clients_lock:
            clients = list(self.clients)
        for client in clients:
            if client.subscribed and client.authorized:
                try:
                    self.send_job_to_client(client, job, clean_jobs=job.clean_jobs)
                except OSError:
                    self.disconnect_client(client)

    def job_for_client(
        self,
        client: StratumClientState,
        base_job: AuxPowStratumJob,
        *,
        clean_jobs: bool,
    ) -> AuxPowStratumJob:
        if not self.vardiff_config.enabled:
            return base_job
        desired_difficulty = client.pending_share_difficulty or client.share_difficulty
        share_target = self.effective_share_target(desired_difficulty, base_job.qbit_target)
        job = replace(
            base_job,
            job_id=self.next_job_id(),
            share_target=share_target,
            share_difficulty=target_difficulty(share_target),
            clean_jobs=clean_jobs,
        )
        with self.lock:
            self.jobs[job.job_id] = job
        return job

    def send_job_to_client(
        self,
        client: StratumClientState,
        base_job: AuxPowStratumJob,
        *,
        clean_jobs: bool,
    ) -> AuxPowStratumJob:
        job = self.job_for_client(client, base_job, clean_jobs=clean_jobs)
        with self.lock:
            pending_difficulty = client.pending_share_difficulty
            pending_was_applied = (
                pending_difficulty is not None
                and self.effective_share_target(pending_difficulty, base_job.qbit_target) == job.share_target
            )
            client.share_difficulty = job.share_difficulty
            if pending_was_applied:
                client.pending_share_difficulty = None
            if self.vardiff_config.enabled:
                if clean_jobs:
                    for job_id in client.active_job_ids:
                        self.jobs.pop(job_id, None)
                    client.active_job_ids.clear()
                client.active_job_ids.add(job.job_id)
        self.send_difficulty(client, job)
        self.send_job(client, job)
        return job

    def send_difficulty_value(self, client: StratumClientState, difficulty: Decimal) -> None:
        client.send(
            {
                "id": None,
                "method": "mining.set_difficulty",
                "params": [float(difficulty)],
            }
        )

    def send_difficulty(self, client: StratumClientState, job: AuxPowStratumJob) -> None:
        self.send_difficulty_value(client, job.share_difficulty)

    def send_job(self, client: StratumClientState, job: AuxPowStratumJob) -> None:
        client.send(
            {
                "id": None,
                "method": "mining.notify",
                "params": [
                    job.job_id,
                    job.prevhash,
                    job.coinb1,
                    job.coinb2,
                    [ser_uint256(item).hex() for item in job.coinbase_merkle_branch],
                    job.version,
                    job.nbits,
                    job.ntime,
                    job.clean_jobs,
                ],
            }
        )

    def handle_client(self, client: StratumClientState) -> None:
        print(f"auxpow stratum: client connected {client.address[0]}:{client.address[1]}", flush=True)
        reader = client.sock.makefile("r", encoding="utf-8", newline="\n")
        try:
            for line in reader:
                if self.stop_event.is_set():
                    break
                line = line.strip()
                if not line:
                    continue
                request: object | None = None
                request_id: object = None
                try:
                    request = json.loads(line)
                    if not isinstance(request, dict):
                        raise StratumError(20, "request must be an object")
                    request_id = request.get("id")
                    self.handle_request(client, request)
                except json.JSONDecodeError as exc:
                    self.send_error(client, None, 20, f"invalid JSON: {exc.msg}")
                except StratumError as exc:
                    self.send_error(client, request_id, exc.code, exc.message)
                except Exception as exc:
                    self.send_error(client, request_id, 20, str(exc))
        finally:
            reader.close()
            self.disconnect_client(client)

    def disconnect_client(self, client: StratumClientState) -> None:
        with self.clients_lock:
            self.clients.discard(client)
        if self.vardiff_config.enabled:
            with self.lock:
                for job_id in client.active_job_ids:
                    self.jobs.pop(job_id, None)
                client.active_job_ids.clear()
        client.close()

    def handle_request(self, client: StratumClientState, request: dict[str, object]) -> None:
        method = request.get("method")
        params = request.get("params", [])
        request_id = request.get("id")
        if not isinstance(method, str):
            raise StratumError(20, "missing method")
        if not isinstance(params, list):
            raise StratumError(20, "params must be an array")

        if method == "mining.configure":
            extensions = params[0] if params else []
            extension_params = params[1] if len(params) > 1 else {}
            result: dict[str, object] = {}
            if extension_params is None:
                extension_params = {}
            if not isinstance(extension_params, dict):
                raise StratumError(20, "extension parameters must be an object")
            if isinstance(extensions, list):
                for extension in extensions:
                    if extension == "version-rolling":
                        miner_mask = 0xFFFFFFFF
                        if "version-rolling.mask" in extension_params:
                            try:
                                miner_mask = parse_mask_hex(
                                    extension_params["version-rolling.mask"],
                                    field_name="version-rolling.mask",
                                )
                            except ValueError as exc:
                                raise StratumError(20, str(exc)) from exc
                        min_bit_count = extension_params.get("version-rolling.min-bit-count")
                        if min_bit_count is not None:
                            try:
                                client.version_min_bit_count = int(str(min_bit_count))
                            except ValueError as exc:
                                raise StratumError(20, "version-rolling.min-bit-count must be an integer") from exc
                        negotiated_mask = self.version_mask & miner_mask
                        client.requested_version_mask = miner_mask
                        client.version_mask = negotiated_mask
                        result["version-rolling"] = negotiated_mask != 0
                        result["version-rolling.mask"] = format_mask_hex(negotiated_mask)
                        print(
                            "auxpow stratum: version rolling negotiated "
                            f"connection={client.connection_id} requested_mask={format_mask_hex(miner_mask)} "
                            f"server_mask={format_mask_hex(self.version_mask)} "
                            f"negotiated_mask={format_mask_hex(negotiated_mask)} "
                            f"min_bit_count={client.version_min_bit_count if client.version_min_bit_count is not None else '-'}",
                            flush=True,
                        )
                        self.log_event(
                            "configure",
                            connection_id=client.connection_id,
                            requested_mask=format_mask_hex(miner_mask),
                            server_mask=format_mask_hex(self.version_mask),
                            negotiated_mask=format_mask_hex(negotiated_mask),
                            min_bit_count=client.version_min_bit_count,
                        )
                    else:
                        result[str(extension)] = False
            self.send_result(client, request_id, result)
            return

        if method == "mining.subscribe":
            client.subscribed = True
            client.agent = str(params[0]) if params else ""
            self.send_result(client, request_id, [[], client.extranonce1_hex, AUXPOW_STRATUM_EXTRANONCE2_SIZE])
            current_job = self.fresh_current_job()
            if client.authorized and current_job is not None:
                self.send_job_to_client(client, current_job, clean_jobs=current_job.clean_jobs)
            return

        if method == "mining.authorize":
            client.authorized = True
            client.username = str(params[0]) if params else ""
            self.send_result(client, request_id, True)
            current_job = self.fresh_current_job()
            if client.subscribed and current_job is not None:
                self.send_job_to_client(client, current_job, clean_jobs=current_job.clean_jobs)
            return

        if method == "mining.extranonce.subscribe":
            self.send_result(client, request_id, True)
            return

        if method == "mining.suggest_difficulty":
            self.send_result(client, request_id, True)
            return

        if method == "mining.submit":
            self.handle_submit(client, params)
            self.send_result(client, request_id, True)
            return

        raise StratumError(20, f"unsupported method {method}")

    def send_result(self, client: StratumClientState, request_id: object, result: object) -> None:
        client.send({"id": request_id, "result": result, "error": None})

    def send_error(self, client: StratumClientState, request_id: object, code: int, message: str) -> None:
        client.send({"id": request_id, "result": None, "error": [code, message, None]})

    def handle_submit(self, client: StratumClientState, params: list[object]) -> None:
        if len(params) < 5:
            raise StratumError(20, "submit params are incomplete")
        _, job_id, extranonce2_hex, ntime_hex, nonce_hex = [str(item) for item in params[:5]]
        version_bits_hex = str(params[5]) if len(params) > 5 else None
        if len(extranonce2_hex) != AUXPOW_STRATUM_EXTRANONCE2_SIZE * 2:
            raise StratumError(20, "unexpected extranonce2 size")
        if len(ntime_hex) != 8 or len(nonce_hex) != 8:
            raise StratumError(20, "ntime and nonce must be 4-byte hex strings")
        if version_bits_hex is not None and client.version_mask == 0:
            raise StratumError(20, "version_bits provided without version-rolling negotiation")
        if version_bits_hex is None and client.version_mask != 0:
            raise StratumError(20, "version_bits required after version-rolling negotiation")
        with self.lock:
            job = self.jobs.get(job_id)
            if job is not None and self.parent_template_age_expired(job):
                self.jobs.pop(job_id, None)
                if self.parent_template_age_expired(self.current_job):
                    self.current_job = None
                    self.jobs = {}
                job = None
            if self.vardiff_config.enabled and job is not None and job_id not in client.active_job_ids:
                job = None
        if job is None:
            worker = self.worker_key(client)
            self.record_stats(worker, "submitted")
            self.note_vardiff_submitted_share(client)
            self.record_stats(worker, "stale")
            raise StratumError(21, "stale job")
        worker = self.worker_key(client)
        client.last_submit_monotonic = time.monotonic()
        self.record_stats(worker, "submitted")
        self.note_vardiff_submitted_share(client)
        try:
            version_hex = stratum_codec.apply_version_bits(job.version, version_bits_hex, client.version_mask)
            coinbase_bytes, canonical_header_bytes = assemble_header(
                job,
                client.extranonce1_hex,
                extranonce2_hex,
                nonce_hex,
                ntime_hex=ntime_hex,
                version_hex=version_hex,
            )
        except ValueError as exc:
            raise StratumError(20, str(exc)) from exc
        if self.vardiff_config.enabled:
            share_key = ("header", worker, canonical_header_bytes.hex())
        else:
            share_key = (
                "job",
                worker,
                job_id,
                client.extranonce1_hex,
                extranonce2_hex,
                ntime_hex,
                nonce_hex,
                version_bits_hex or "",
            )
        with self.lock:
            if share_key in self.recent_share_keys:
                self.worker_stats[worker].duplicate += 1
                raise StratumError(22, "duplicate share")
            if len(self.recent_share_keys) > 50000:
                self.recent_share_keys.clear()
            self.recent_share_keys.add(share_key)
        self.log_event(
            "submit",
            connection_id=client.connection_id,
            user=worker,
            job_id=job.job_id,
            extranonce1=client.extranonce1_hex,
            extranonce2=extranonce2_hex,
            ntime=ntime_hex,
            nonce=nonce_hex,
            version_bits=version_bits_hex,
            applied_version=version_hex,
            negotiated_mask=format_mask_hex(client.version_mask),
        )
        canonical_candidate = ("canonical", canonical_header_bytes, header_hash_int(canonical_header_bytes))
        diagnostic_candidates: list[tuple[str, bytes, int]] = []
        if (
            AUXPOW_STRATUM_DIAG_VARIANTS
            or self.header_variant != "canonical"
            or AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT
        ):
            diagnostic_candidates = [
                (variant_name, candidate_header_bytes, header_hash_int(candidate_header_bytes))
                for variant_name, candidate_header_bytes in assemble_header_candidates(
                    job,
                    coinbase_bytes,
                    nonce_hex=nonce_hex,
                    ntime_hex=ntime_hex,
                    preferred_variant=None,
                    version_hex=version_hex,
                )
            ]
        if AUXPOW_STRATUM_DIAG_VARIANTS:
            all_diagnostic_candidates = [canonical_candidate, *diagnostic_candidates]
            for variant_name, candidate_header_bytes, candidate_hash in all_diagnostic_candidates:
                self.log_event(
                    "variant_result",
                    user=worker,
                    job_id=job.job_id,
                    variant=variant_name,
                    hash=f"{candidate_hash:064x}",
                    hash_display=stratum_codec.header_hash_hex(candidate_header_bytes),
                    share_pass=candidate_hash <= job.share_target,
                    qbit_pass=candidate_hash <= job.qbit_target,
                    parent_pass=candidate_hash <= job.parent_target,
                )
        share_candidates = [canonical_candidate]
        if self.header_variant != "canonical":
            override_candidates = [candidate for candidate in diagnostic_candidates if candidate[0] == self.header_variant]
            if not override_candidates:
                raise StratumError(20, f"unknown AUXPOW_STRATUM_HEADER_VARIANT {self.header_variant}")
            share_candidates = override_candidates
        accepted_share_candidates = [
            (variant_name, candidate_header_bytes, candidate_hash)
            for variant_name, candidate_header_bytes, candidate_hash in share_candidates
            if candidate_hash <= job.share_target
        ]
        block_candidates = [
            (variant_name, candidate_header_bytes, candidate_hash)
            for variant_name, candidate_header_bytes, candidate_hash in share_candidates
            if candidate_hash <= job.qbit_target
        ]
        if not accepted_share_candidates and not block_candidates:
            diagnostic_passes = [
                (variant_name, candidate_header_bytes, candidate_hash)
                for variant_name, candidate_header_bytes, candidate_hash in diagnostic_candidates
                if candidate_hash <= job.share_target or candidate_hash <= job.qbit_target
            ]
            if diagnostic_passes and AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT:
                share_candidates = diagnostic_passes
                accepted_share_candidates = [
                    (variant_name, candidate_header_bytes, candidate_hash)
                    for variant_name, candidate_header_bytes, candidate_hash in share_candidates
                    if candidate_hash <= job.share_target
                ]
                block_candidates = [
                    (variant_name, candidate_header_bytes, candidate_hash)
                    for variant_name, candidate_header_bytes, candidate_hash in share_candidates
                    if candidate_hash <= job.qbit_target
                ]
            else:
                if diagnostic_passes:
                    print(
                        "auxpow stratum: diagnostic variant would pass but canonical share failed "
                        f"user={worker} job={job.job_id} variants="
                        + ",".join(f"{name}:{candidate_hash:064x}" for name, _, candidate_hash in diagnostic_passes),
                        flush=True,
                    )
                self.record_stats(worker, "low_difficulty")
                raise StratumError(23, "low difficulty share")
        best_variant_name, _, best_hash = min(accepted_share_candidates or block_candidates, key=lambda item: item[2])
        variant_results: list[str] = []
        if accepted_share_candidates:
            self.record_stats(worker, "accepted")
            self.note_vardiff_accepted_share(client, job, worker)
            print(
                "auxpow stratum: accepted share "
                f"user={client.username or '-'} job={job.job_id} variant={best_variant_name} "
                f"hash={best_hash:064x}",
                flush=True,
            )
        else:
            print(
                "auxpow stratum: qbit block candidate met child target above pool share floor "
                f"user={client.username or '-'} job={job.job_id} variant={best_variant_name} "
                f"hash={best_hash:064x}",
                flush=True,
            )
        if not block_candidates:
            print(
                "auxpow stratum: share met pool target but not qbit target "
                f"user={client.username or '-'} job={job.job_id}",
                flush=True,
            )
            return
        self.record_stats(worker, "qbit_candidates", len(block_candidates))
        for variant_name, candidate_header_bytes, candidate_hash in block_candidates:
            payload, parent_block = self.build_submission(job, coinbase_bytes, candidate_header_bytes)
            qbit_result = self.qbit_rpc.call("submitauxblock", [job.aux_template["hash"], payload.to_hex()])
            if qbit_result == "stale-prevblk":
                self.refresh_now.set()
                self.record_stats(worker, "stale")
                raise StratumError(21, "stale job")
            if qbit_result is not None:
                variant_results.append(f"{variant_name}={qbit_result}")
                self.log_event(
                    "qbit_submit",
                    user=worker,
                    job_id=job.job_id,
                    variant=variant_name,
                    result=qbit_result,
                    auxpow_bytes=len(bytes.fromhex(payload.to_hex())),
                )
                continue
            self.record_stats(worker, "qbit_accepted")
            self.log_event(
                "qbit_submit",
                user=worker,
                job_id=job.job_id,
                variant=variant_name,
                result=None,
                auxpow_bytes=len(bytes.fromhex(payload.to_hex())),
            )
            print(f"auxpow stratum: qbit accepted AuxPoW block via {variant_name}", flush=True)
            self.refresh_now.set()
            if candidate_hash > job.parent_target:
                print(
                    "auxpow stratum: skipping parent submit because share missed Bitcoin target "
                    f"job={job.job_id} hash={candidate_hash:064x} target={job.parent_target:064x}",
                    flush=True,
                )
                return
            self.record_stats(worker, "parent_submitted")
            parent_result = self.bitcoin_rpc.call("submitblock", [parent_block.serialize().hex()])
            if parent_result in (None, "duplicate", "inconclusive"):
                self.record_stats(worker, "parent_accepted")
                self.log_event(
                    "parent_submit",
                    user=worker,
                    job_id=job.job_id,
                    variant=variant_name,
                    result=parent_result,
                )
                print(
                    "auxpow stratum: parent submit attempted after qbit acceptance "
                    f"job={job.job_id} result={parent_result!r}",
                    flush=True,
                )
                return
            self.log_event(
                "parent_submit",
                user=worker,
                job_id=job.job_id,
                variant=variant_name,
                result=parent_result,
            )
            print(
                "auxpow stratum: parent submit rejected after qbit acceptance "
                f"job={job.job_id} result={parent_result!r}",
                flush=True,
            )
            return
        if variant_results:
            print(
                "auxpow stratum: qbit rejected share variants "
                + ", ".join(variant_results),
                flush=True,
            )

    def build_submission(
        self,
        job: AuxPowStratumJob,
        coinbase_bytes: bytes,
        header_bytes: bytes,
    ) -> tuple[AuxPowPayload, object]:
        coinbase = CTransaction()
        coinbase.deserialize(io.BytesIO(coinbase_bytes))
        header = CBlockHeader()
        header.deserialize(io.BytesIO(header_bytes))
        payload, parent_block = build_parent_block(
            aux_template=job.aux_template,
            btc_template=job.btc_template,
            bitcoin_script_pubkey_hex=job.bitcoin_script_pubkey_hex,
            chain_nonce=job.chain_nonce,
            parent_time=header.nTime,
            header_nonce=header.nNonce,
        )
        coinbase.wit = copy.deepcopy(parent_block.vtx[0].wit)
        parent_block.vtx[0] = coinbase
        parent_block.nVersion = header.nVersion
        parent_block.hashPrevBlock = header.hashPrevBlock
        parent_block.hashMerkleRoot = header.hashMerkleRoot
        parent_block.nTime = header.nTime
        parent_block.nBits = header.nBits
        parent_block.nNonce = header.nNonce
        payload.coinbase_tx = coinbase
        payload.coinbase_merkle_branch = list(job.coinbase_merkle_branch)
        payload.chain_index = job.chain_index
        payload.parent_block = CBlockHeader(parent_block)
        return payload, parent_block


def assemble_header(
    job: AuxPowStratumJob,
    extranonce1_hex: str,
    extranonce2_hex: str,
    nonce_hex: str,
    *,
    ntime_hex: str | None = None,
    version_hex: str | None = None,
) -> tuple[bytes, bytes]:
    coinbase = stratum_codec.assemble_coinbase(job.coinb1, extranonce1_hex, extranonce2_hex, job.coinb2)
    merkle_root = compute_coinbase_merkle_root(coinbase, job.coinbase_merkle_branch)
    header = stratum_codec.serialize_header_from_stratum_fields(
        version_hex=version_hex or job.version,
        prevhash_hex=job.prevhash,
        merkle_root_serialized=merkle_root,
        ntime_hex=ntime_hex or job.ntime,
        nbits_hex=job.nbits,
        nonce_hex=nonce_hex,
    )
    return coinbase, header


def assemble_header_candidates(
    job: AuxPowStratumJob,
    coinbase_bytes: bytes,
    *,
    nonce_hex: str,
    ntime_hex: str,
    preferred_variant: str | None,
    version_hex: str | None = None,
) -> list[tuple[str, bytes]]:
    merkle_root = compute_coinbase_merkle_root(coinbase_bytes, job.coinbase_merkle_branch)
    candidate_map = {
        variant.name: variant.header
        for variant in stratum_codec.diagnostic_header_variants(
            version_hex=version_hex or job.version,
            prevhash_stratum_hex=job.prevhash,
            previousblockhash_display_hex=str(job.btc_template["previousblockhash"]),
            merkle_root_serialized=merkle_root,
            ntime_hex=ntime_hex,
            nbits_hex=job.nbits,
            nonce_hex=nonce_hex,
        )
    }
    if preferred_variant and preferred_variant in candidate_map:
        return [(preferred_variant, candidate_map[preferred_variant])]
    return list(candidate_map.items())


def header_hash_int(header_bytes: bytes) -> int:
    return stratum_codec.header_hash_int(header_bytes)


def apply_version_bits(job_version_hex: str, version_bits_hex: str | None, version_mask: int) -> str:
    return stratum_codec.apply_version_bits(job_version_hex, version_bits_hex, version_mask)


def bridge_mode(
    qbit_rpc: JsonRpc,
    bitcoin_rpc: JsonRpc,
    qbit_miner_address: str,
    bitcoin_miner_address: ResolvedAddress,
) -> int:
    print(
        f"auxpow bridge: starting long-running bridge with {AUXPOW_BRIDGE_INTERVAL_SECONDS}s interval",
        flush=True,
    )
    mined_blocks = 0
    while True:
        try:
            positive_auxpow_path(qbit_rpc, bitcoin_rpc, qbit_miner_address, bitcoin_miner_address)
            mined_blocks += 1
            print(f"auxpow bridge: mined qbit AuxPoW block #{mined_blocks}", flush=True)
        except Exception as exc:
            print(f"auxpow bridge: iteration failed: {exc}", flush=True)
        time.sleep(AUXPOW_BRIDGE_INTERVAL_SECONDS)


def main() -> int:
    qbit_rpc = JsonRpc(
        host=env("QBIT_RPC_HOST"),
        port=int(env("QBIT_RPC_PORT")),
        user=env("QBIT_RPC_USER"),
        password=env("QBIT_RPC_PASSWORD"),
    )
    bitcoin_rpc = JsonRpc(
        host=env("BITCOIN_RPC_HOST"),
        port=int(env("BITCOIN_RPC_PORT")),
        user=env("BITCOIN_RPC_USER"),
        password=env("BITCOIN_RPC_PASSWORD"),
    )
    for rpc in (qbit_rpc, bitcoin_rpc):
        wait_for_rpc(rpc)
    validate_auxpow_startup(qbit_rpc, bitcoin_rpc)
    qbit_miner_address = resolve_qbit_miner_address(qbit_rpc)
    bitcoin_miner_address = resolve_bitcoin_miner_address(bitcoin_rpc)
    validate_auxpow_templates(
        qbit_rpc,
        bitcoin_rpc,
        qbit_miner_address=qbit_miner_address,
        max_age_seconds=AUXPOW_TEMPLATE_MAX_AGE_SECONDS,
        max_future_seconds=AUXPOW_TEMPLATE_MAX_FUTURE_SECONDS,
    )
    print(f"auxpow: using qbit payout address {qbit_miner_address}", flush=True)
    print(f"auxpow: using Bitcoin payout address {bitcoin_miner_address.address}", flush=True)
    if AUXPOW_MODE == "bridge":
        return bridge_mode(qbit_rpc, bitcoin_rpc, qbit_miner_address, bitcoin_miner_address)
    if AUXPOW_MODE == "stratum":
        return AuxPowStratumServer(
            qbit_rpc=qbit_rpc,
            bitcoin_rpc=bitcoin_rpc,
            qbit_miner_address=qbit_miner_address,
            bitcoin_miner_address=bitcoin_miner_address,
        ).serve()
    positive_auxpow_path(qbit_rpc, bitcoin_rpc, qbit_miner_address, bitcoin_miner_address)
    reject_invalid_parent_pow(qbit_rpc, bitcoin_rpc, qbit_miner_address, bitcoin_miner_address)
    reject_invalid_commitment(qbit_rpc, bitcoin_rpc, qbit_miner_address, bitcoin_miner_address)
    reject_stale_template(qbit_rpc, bitcoin_rpc, qbit_miner_address, bitcoin_miner_address)
    print("auxpow lab passed: positive path plus stale / bad commitment / bad parent PoW checks", flush=True)
    return 0


DIFF1_TARGET = compact_target(0x1D00FFFF)
QBIT_CHAIN = env("QBIT_CHAIN", "regtest")
QBIT_EXPECTED_GENESIS_HASH = os.environ.get("QBIT_EXPECTED_GENESIS_HASH", "").strip()
QBIT_MINER_ADDRESS = env("QBIT_MINER_ADDRESS", "auto")
QBIT_MINER_WALLET_NAME = env("QBIT_MINER_WALLET_NAME", "auxpow")
BITCOIN_CHAIN = env("BITCOIN_CHAIN", "regtest")
BITCOIN_EXPECTED_GENESIS_HASH = os.environ.get("BITCOIN_EXPECTED_GENESIS_HASH", "").strip()
BITCOIN_MINER_ADDRESS = env("BITCOIN_MINER_ADDRESS", "auto")
BITCOIN_MINER_WALLET_NAME = env("BITCOIN_MINER_WALLET_NAME", "auxpow-parent")
AUXPOW_TEMPLATE_MAX_AGE_SECONDS = env_nonnegative_int("AUXPOW_TEMPLATE_MAX_AGE_SECONDS", 120)
AUXPOW_TEMPLATE_MAX_FUTURE_SECONDS = env_nonnegative_int("AUXPOW_TEMPLATE_MAX_FUTURE_SECONDS", 7200)
AUXPOW_MODE = env("AUXPOW_MODE", "once")
AUXPOW_BRIDGE_INTERVAL_SECONDS = env_int("AUXPOW_BRIDGE_INTERVAL_SECONDS", 15)
AUXPOW_STRATUM_BIND = env("AUXPOW_STRATUM_BIND", "0.0.0.0")
AUXPOW_STRATUM_PORT = env_int("AUXPOW_STRATUM_PORT", 3335)
AUXPOW_STRATUM_SHARE_DIFF = env_decimal("AUXPOW_STRATUM_SHARE_DIFF", "1")
AUXPOW_STRATUM_VARDIFF = load_vardiff_config()
AUXPOW_STRATUM_VARDIFF_APPLY_MODE = env("AUXPOW_STRATUM_VARDIFF_APPLY_MODE", "next_job")
if AUXPOW_STRATUM_VARDIFF_APPLY_MODE not in {"next_job", "clean_job"}:
    raise SystemExit("AUXPOW_STRATUM_VARDIFF_APPLY_MODE must be one of: next_job, clean_job")
AUXPOW_STRATUM_MIN_ADVERTISED_DIFF = env_nonnegative_decimal("AUXPOW_STRATUM_MIN_ADVERTISED_DIFF", "0")
AUXPOW_STRATUM_EXTRANONCE2_SIZE = env_int("AUXPOW_STRATUM_EXTRANONCE2_SIZE", 8)
AUXPOW_STRATUM_POLL_SECONDS = env_int("AUXPOW_STRATUM_POLL_SECONDS", 5)
AUXPOW_STRATUM_JOB_MAX_AGE_SECONDS = env_nonnegative_int("AUXPOW_STRATUM_JOB_MAX_AGE_SECONDS", 2700)
AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS = env_nonnegative_int(
    "AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS",
    120,
)
AUXPOW_STRATUM_VERSION_MASK = env_mask("AUXPOW_STRATUM_VERSION_MASK", "1fffe000")
AUXPOW_STRATUM_HEADER_VARIANT = env("AUXPOW_STRATUM_HEADER_VARIANT", "canonical")
AUXPOW_STRATUM_DIAG_JSONL = env_bool("AUXPOW_STRATUM_DIAG_JSONL")
AUXPOW_STRATUM_DIAG_EVENTS = env_bool("AUXPOW_STRATUM_DIAG_EVENTS")
AUXPOW_STRATUM_DIAG_VARIANTS = env_bool("AUXPOW_STRATUM_DIAG_VARIANTS")
AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT = env_bool("AUXPOW_STRATUM_ACCEPT_DIAGNOSTIC_VARIANT")
AUXPOW_STRATUM_STATS_INTERVAL_SECONDS = env_nonnegative_int("AUXPOW_STRATUM_STATS_INTERVAL_SECONDS", 60)
AUXPOW_STRATUM_EXPECTED_HASHRATES = env_decimal_map("AUXPOW_STRATUM_EXPECTED_HASHRATES")
AUXPOW_CHAIN_NONCE = env_int("AUXPOW_CHAIN_NONCE", 0)
AUXPOW_PLACEHOLDER_BYTES = (b"\x11" * 4) + (b"\x22" * AUXPOW_STRATUM_EXTRANONCE2_SIZE)


if __name__ == "__main__":
    raise SystemExit(main())
