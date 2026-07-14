#!/usr/bin/env python3
"""Run a real Stratum-capable CPU miner against the permissionless lab."""

from __future__ import annotations

import base64
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from collections import deque
from pathlib import Path


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise SystemExit(f"{name} is required")
    return value


def rpc_call(method: str, params: list[object] | None = None) -> object:
    body = json.dumps(
        {
            "jsonrpc": "1.0",
            "id": method,
            "method": method,
            "params": params or [],
        }
    ).encode()
    credentials = f"{QBIT_RPC_USER}:{QBIT_RPC_PASSWORD}".encode()
    request = urllib.request.Request(
        f"http://{QBIT_RPC_HOST}:{QBIT_RPC_PORT}",
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


def stream_output(process: subprocess.Popen[str], tail: deque[str]) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        line = line.rstrip()
        if not line:
            continue
        tail.append(line)
        print(f"[cpuminer] {line}", flush=True)


def main() -> int:
    miner_username = resolve_miner_username()
    print(f"real miner using username: {miner_username}", flush=True)
    before_height = int(rpc_call("getblockcount"))
    deadline = time.time() + MINER_TIMEOUT_SECONDS
    tail: deque[str] = deque(maxlen=40)

    command = [
        CPUMINER_BIN,
        "-a",
        CPUMINER_ALGO,
        "-o",
        f"stratum+tcp://{STRATUM_HOST}:{STRATUM_PORT}",
        "-u",
        miner_username,
        "-p",
        MINER_PASSWORD,
        "-t",
        str(CPUMINER_THREADS),
    ]
    if CPUMINER_EXTRA_ARGS:
        command.extend(CPUMINER_EXTRA_ARGS.split())

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    reader = threading.Thread(target=stream_output, args=(process, tail), daemon=True)
    reader.start()

    success = False
    try:
        while time.time() < deadline:
            current = int(rpc_call("getblockcount"))
            if current > before_height:
                success = True
                print(f"real miner accepted a qbit block: height {before_height} -> {current}", flush=True)
                return 0
            if process.poll() is not None:
                break
            time.sleep(1)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    if success:
        return 0

    raise SystemExit(
        "real miner timed out before qbit accepted a block; "
        f"last miner lines: {list(tail)[-5:]}"
    )


def resolve_miner_username() -> str:
    if MINER_USERNAME != "auto":
        return MINER_USERNAME
    username_file = Path(MINER_USERNAME_FILE)
    deadline = time.time() + MINER_USERNAME_TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            username = username_file.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            username = ""
        if username:
            return username
        time.sleep(1)
    raise SystemExit(f"timed out waiting for miner username file {username_file}")


CPUMINER_BIN = env("CPUMINER_BIN", "/usr/local/bin/cpuminer")
CPUMINER_ALGO = env("CPUMINER_ALGO", "sha256d")
CPUMINER_THREADS = int(env("CPUMINER_THREADS", "1"))
CPUMINER_EXTRA_ARGS = os.environ.get("CPUMINER_EXTRA_ARGS", "")
STRATUM_HOST = env("STRATUM_HOST", "ckpool")
STRATUM_PORT = int(env("STRATUM_PORT", "3333"))
MINER_USERNAME = env("MINER_USERNAME")
MINER_USERNAME_FILE = env(
    "MINER_USERNAME_FILE",
    "/run/qbit-real-miner-smoke/miner-address.txt",
)
MINER_PASSWORD = env("MINER_PASSWORD", "x")
MINER_TIMEOUT_SECONDS = int(env("MINER_TIMEOUT_SECONDS", "120"))
MINER_USERNAME_TIMEOUT_SECONDS = int(env("MINER_USERNAME_TIMEOUT_SECONDS", "60"))
QBIT_RPC_HOST = env("QBIT_RPC_HOST", "qbitd")
QBIT_RPC_PORT = int(env("QBIT_RPC_PORT", "18452"))
QBIT_RPC_USER = env("QBIT_RPC_USER")
QBIT_RPC_PASSWORD = env("QBIT_RPC_PASSWORD")


if __name__ == "__main__":
    raise SystemExit(main())
