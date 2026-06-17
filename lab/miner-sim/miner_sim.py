#!/usr/bin/env python3
"""Minimal Stratum v1 miner simulator for the permissionless lab."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise SystemExit(f"{name} is required")
    return value


def rpc_call(method: str, params: list[object] | None = None, *, wallet: str | None = None) -> object:
    body = json.dumps(
        {
            "jsonrpc": "1.0",
            "id": method,
            "method": method,
            "params": params or [],
        }
    ).encode()
    credentials = f"{QBIT_RPC_USER}:{QBIT_RPC_PASSWORD}".encode()
    url = f"http://{QBIT_RPC_HOST}:{QBIT_RPC_PORT}"
    if wallet:
        url += "/wallet/" + wallet
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Basic {base64.b64encode(credentials).decode()}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        payload = json.loads(response.read())
    if payload["error"] is not None:
        raise RuntimeError(f"qbit RPC {method} failed: {payload['error']}")
    return payload["result"]


def ensure_wallet_loaded(wallet_name: str) -> None:
    try:
        rpc_call("createwallet", [wallet_name])
    except Exception:
        pass
    try:
        rpc_call("loadwallet", [wallet_name])
    except Exception:
        pass


def resolve_miner_username() -> str:
    if MINER_USERNAME != "auto":
        return MINER_USERNAME

    ensure_wallet_loaded(MINER_WALLET_NAME)
    deadline = time.time() + MINER_USERNAME_TIMEOUT_SECONDS
    last_error = "wallet never returned an address"

    while time.time() < deadline:
        for params in ([], ["", "p2mr"]):
            try:
                address = rpc_call("getnewaddress", params, wallet=MINER_WALLET_NAME)
                if address:
                    return str(address)
            except Exception as exc:
                last_error = str(exc)
        time.sleep(1)

    raise SystemExit(f"timed out waiting for qbit to derive a miner username: {last_error}")


def double_sha256(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def flip_word_bytes(data: bytes) -> bytes:
    if len(data) % 4 != 0:
        raise ValueError("data length must be divisible by 4")
    return b"".join(data[offset : offset + 4][::-1] for offset in range(0, len(data), 4))


def target_from_compact(nbits_hex: str) -> int:
    compact = int(nbits_hex, 16)
    size = compact >> 24
    mantissa = compact & 0x007FFFFF
    if size <= 3:
        return mantissa >> (8 * (3 - size))
    return mantissa << (8 * (size - 3))


DIFF1_TARGET = int("00000000ffff0000000000000000000000000000000000000000000000000000", 16)


def target_from_difficulty(difficulty: float) -> int:
    if difficulty <= 0:
        return (1 << 256) - 1
    target = int(DIFF1_TARGET / difficulty)
    return max(0, min(target, (1 << 256) - 1))


@dataclass
class Job:
    job_id: str
    prevhash: str
    coinb1: str
    coinb2: str
    merkle_branch: list[str]
    version: str
    nbits: str
    ntime: str
    clean_jobs: bool


class RecoverableError(RuntimeError):
    pass


class StratumClient:
    def __init__(self, host: str, port: int, deadline: float):
        self.host = host
        self.port = port
        self.deadline = deadline
        self.socket = socket.create_connection((host, port), timeout=5)
        self.socket.settimeout(1)
        self.buffer = bytearray()
        self.request_id = 0

    def close(self) -> None:
        self.socket.close()

    def send(self, method: str, params: list[object]) -> int:
        self.request_id += 1
        payload = json.dumps({"id": self.request_id, "method": method, "params": params}).encode() + b"\n"
        self.socket.sendall(payload)
        return self.request_id

    def recv(self) -> dict[str, object]:
        while time.time() < self.deadline:
            newline_index = self.buffer.find(b"\n")
            if newline_index != -1:
                line = bytes(self.buffer[:newline_index])
                del self.buffer[: newline_index + 1]
                if not line.strip():
                    continue
                return json.loads(line.decode())
            try:
                chunk = self.socket.recv(65536)
            except socket.timeout:
                continue
            except OSError as exc:
                if "timed out" in str(exc).lower():
                    continue
                raise RecoverableError(f"stratum socket error: {exc}") from exc
            if not chunk:
                raise RecoverableError("stratum connection closed before a job was received")
            self.buffer.extend(chunk)
        raise RecoverableError("timed out waiting for a Stratum message")


def assemble_header(job: Job, extranonce1_hex: str, extranonce2_hex: str, nonce_hex: str) -> tuple[bytes, bytes]:
    coinbase = bytes.fromhex(job.coinb1 + extranonce1_hex + extranonce2_hex + job.coinb2)
    merkle = double_sha256(coinbase)
    for branch in job.merkle_branch:
        merkle = double_sha256(merkle + bytes.fromhex(branch))
    merkle_root = flip_word_bytes(merkle)
    data = bytes.fromhex(job.version + job.prevhash + merkle_root.hex() + job.ntime + job.nbits + nonce_hex)
    return coinbase, flip_word_bytes(data)


def solve_job(job: Job, extranonce1_hex: str, extranonce2_size: int, share_target: int, deadline: float) -> tuple[str, str]:
    target = min(target_from_compact(job.nbits), share_target)
    max_extranonce2 = 1 << (8 * extranonce2_size)
    extranonce2 = 0

    while extranonce2 < max_extranonce2 and time.time() < deadline:
        extranonce2_hex = f"{extranonce2:0{extranonce2_size * 2}x}"
        _, zero_nonce_header = assemble_header(job, extranonce1_hex, extranonce2_hex, "00000000")
        header = bytearray(zero_nonce_header)

        for nonce in range(0, 0xFFFFFFFF + 1):
            if nonce % 65536 == 0 and time.time() >= deadline:
                break
            header[76:80] = nonce.to_bytes(4, "little")
            header_hash = double_sha256(header)
            if int.from_bytes(header_hash, "little") <= target:
                return extranonce2_hex, f"{nonce:08x}"

        extranonce2 += 1

    raise RecoverableError("timed out before solving a share at the ckpool Stratum difficulty")


def wait_for_height(before: int, deadline: float) -> int | None:
    while time.time() < deadline:
        try:
            current = int(rpc_call("getblockcount"))
        except (RuntimeError, urllib.error.URLError):
            time.sleep(1)
            continue
        if current > before:
            return current
        time.sleep(1)
    return None


def receive_handshake_and_job(client: StratumClient, miner_username: str) -> tuple[str, int, Job, int, float]:
    subscribe_id = client.send("mining.subscribe", ["qbit-mining-bootstrap/0.1"])
    authorize_id = client.send("mining.authorize", [miner_username, MINER_PASSWORD])

    extranonce1 = None
    extranonce2_size = None
    authorized = False
    job = None
    share_target = None
    share_difficulty = None

    while time.time() < client.deadline:
        message = client.recv()
        if message.get("id") == subscribe_id:
            result = message.get("result")
            if isinstance(result, str):
                raise RecoverableError(f"pool is still initialising: {result}")
            if not isinstance(result, list) or len(result) < 3:
                raise RecoverableError(f"unexpected subscribe response: {message}")
            extranonce1 = result[1]
            extranonce2_size = int(result[2])
        elif message.get("id") == authorize_id:
            if message.get("result") is not True:
                raise RecoverableError(f"authorization failed: {message}")
            authorized = True
        elif message.get("method") == "mining.notify":
            params = message.get("params", [])
            if not isinstance(params, list) or len(params) < 9:
                raise RecoverableError(f"unexpected notify payload: {message}")
            job = Job(
                job_id=str(params[0]),
                prevhash=str(params[1]),
                coinb1=str(params[2]),
                coinb2=str(params[3]),
                merkle_branch=[str(item) for item in params[4]],
                version=str(params[5]),
                nbits=str(params[6]),
                ntime=str(params[7]),
                clean_jobs=bool(params[8]),
            )
        elif message.get("method") == "mining.set_difficulty":
            params = message.get("params", [])
            if isinstance(params, list) and params:
                share_difficulty = float(params[0])
                share_target = target_from_difficulty(share_difficulty)

        if extranonce1 and extranonce2_size is not None and authorized and job is not None and share_target is not None:
            return extranonce1, extranonce2_size, job, share_target, float(share_difficulty)

    raise RecoverableError("timed out waiting for subscribe/auth/job state")


def mine_once(miner_username: str, overall_deadline: float, before_height: int) -> bool:
    client = StratumClient(STRATUM_HOST, STRATUM_PORT, overall_deadline)
    try:
        extranonce1, extranonce2_size, job, share_target, share_difficulty = receive_handshake_and_job(
            client, miner_username
        )
        print(
            f"permissionless miner solving job {job.job_id}: bits={job.nbits} "
            f"share_difficulty={share_difficulty:.8g}",
            flush=True,
        )
        extranonce2, nonce = solve_job(job, extranonce1, extranonce2_size, share_target, overall_deadline)
        submit_id = client.send("mining.submit", [miner_username, job.job_id, extranonce2, job.ntime, nonce])
        submit_error = None

        while time.time() < overall_deadline:
            after_height = wait_for_height(before_height, time.time() + 1.2)
            if after_height is not None:
                print(f"permissionless lab mined a qbit block: height {before_height} -> {after_height}")
                return True

            try:
                message = client.recv()
            except RecoverableError:
                break

            if message.get("id") == submit_id and message.get("result") is not True:
                submit_error = message.get("error") or "share rejected"
        if submit_error is not None:
            raise RecoverableError(f"submitted a candidate share but qbit height never advanced: {submit_error}")
        return False
    finally:
        client.close()


def main() -> int:
    miner_username = resolve_miner_username()
    print(f"permissionless miner using username: {miner_username}", flush=True)
    before_height = int(rpc_call("getblockcount"))
    overall_deadline = time.time() + MINER_TIMEOUT_SECONDS
    last_error = None

    while time.time() < overall_deadline:
        try:
            if mine_once(miner_username, overall_deadline, before_height):
                return 0
        except RecoverableError as exc:
            last_error = str(exc)
            time.sleep(1)

    print(f"permissionless miner timed out before qbit accepted a block: {last_error}", file=sys.stderr)
    return 1


STRATUM_HOST = env("STRATUM_HOST")
STRATUM_PORT = int(env("STRATUM_PORT"))
MINER_USERNAME = env("MINER_USERNAME", "auto")
MINER_WALLET_NAME = env("MINER_WALLET_NAME", "permissionless-miner")
MINER_PASSWORD = env("MINER_PASSWORD", "x")
MINER_TIMEOUT_SECONDS = int(env("MINER_TIMEOUT_SECONDS", "120"))
MINER_USERNAME_TIMEOUT_SECONDS = int(env("MINER_USERNAME_TIMEOUT_SECONDS", "60"))
QBIT_RPC_HOST = env("QBIT_RPC_HOST")
QBIT_RPC_PORT = int(env("QBIT_RPC_PORT"))
QBIT_RPC_USER = env("QBIT_RPC_USER")
QBIT_RPC_PASSWORD = env("QBIT_RPC_PASSWORD")


if __name__ == "__main__":
    raise SystemExit(main())
