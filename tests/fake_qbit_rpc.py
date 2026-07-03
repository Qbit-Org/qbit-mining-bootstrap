#!/usr/bin/env python3
"""Tiny qbit-like JSON-RPC server for ckpool VarDiff probes.

This is intentionally not a consensus simulator. It serves enough RPC surface
for the patched ckpool container to build Stratum jobs against a controlled
network target, so VarDiff behavior can be observed without qbit regtest block
solves dominating the result.
"""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 28552
DEFAULT_BITS = "1d00ffff"
DEFAULT_TARGET = "00000000ffff0000000000000000000000000000000000000000000000000000"
DEFAULT_VERSION_ROLLING_MASK = "1fffe000"


class FakeQbitState:
    def __init__(
        self,
        *,
        bits: str,
        target: str,
        log_requests: int,
        chain: str = "regtest",
        initialblockdownload: bool = False,
        connections: int = 1,
        weightlimit: int = 2_000_000,
        versionrollingmask: str | None = DEFAULT_VERSION_ROLLING_MASK,
    ) -> None:
        self.bits = bits
        self.target = target
        self.log_requests = log_requests
        self.chain = chain
        self.initialblockdownload = initialblockdownload
        self.connections = connections
        self.weightlimit = weightlimit
        self.versionrollingmask = versionrollingmask
        self.height = 1
        self.requests = 0
        self.submits = 0

    def log(self, method: str) -> None:
        self.requests += 1
        if self.requests <= self.log_requests or method == "submitblock":
            print(f"fake qbit rpc {self.requests}: {method}", flush=True)

    def result_for(self, method: str, params: list[Any]) -> tuple[Any, dict[str, Any] | None]:
        if method == "validateaddress":
            address = str(params[0]) if params else ""
            is_witness = "1" in address
            return {
                "isvalid": True,
                "address": address,
                "isscript": False,
                "iswitness": is_witness,
                "witness_version": 2 if is_witness else None,
            }, None

        if method == "getblockchaininfo":
            return {
                "chain": self.chain,
                "blocks": self.height - 1,
                "headers": self.height - 1,
                "initialblockdownload": self.initialblockdownload,
            }, None

        if method == "getnetworkinfo":
            return {
                "version": 100000,
                "subversion": "/qbit-fake-rpc:0.1/",
                "connections": self.connections,
            }, None

        if method == "getblocktemplate":
            now = int(time.time())
            height = self.height
            template = {
                "capabilities": ["proposal"],
                "version": 0x20000000,
                "rules": ["csv", "segwit"],
                "vbavailable": {},
                "vbrequired": 0,
                "previousblockhash": f"{height - 1:064x}",
                "transactions": [],
                "coinbaseaux": {"flags": ""},
                "coinbasevalue": 5000000000,
                "longpollid": f"{height}:0",
                "target": self.target,
                "mintime": now - 1,
                "mutable": ["time", "transactions", "prevblock"],
                "noncerange": "00000000ffffffff",
                "sigoplimit": 80000,
                "sizelimit": self.weightlimit,
                "weightlimit": self.weightlimit,
                "curtime": now,
                "bits": self.bits,
                "height": height,
            }
            if self.versionrollingmask is not None:
                template["versionrollingmask"] = self.versionrollingmask
            return template, None

        if method == "submitblock":
            self.submits += 1
            self.height += 1
            return None, None

        if method == "getblockcount":
            return self.height - 1, None

        if method == "getblockhash":
            height = int(params[0]) if params else 0
            return f"{height:064x}", None

        if method == "getbestblockhash":
            return f"{self.height - 1:064x}", None

        return None, {"code": -32601, "message": f"unknown method {method}"}


def build_handler(state: FakeQbitState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format: str, *_args: object) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("content-length", "0"))
            try:
                request = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            except json.JSONDecodeError:
                request = {}

            method = str(request.get("method") or "")
            params = request.get("params")
            if not isinstance(params, list):
                params = []
            state.log(method)
            result, error = state.result_for(method, params)
            body = (
                json.dumps({"result": result, "error": error, "id": request.get("id")}, separators=(",", ":"))
                + "\n"
            ).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

    return Handler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve a minimal qbit-like RPC for ckpool VarDiff probes.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--bits", default=DEFAULT_BITS, help="compact target bits advertised in getblocktemplate")
    parser.add_argument("--target", default=DEFAULT_TARGET, help="full target advertised in getblocktemplate")
    parser.add_argument("--log-requests", type=int, default=20, help="number of initial RPC methods to print")
    parser.add_argument("--chain", default="regtest")
    parser.add_argument("--initialblockdownload", action="store_true")
    parser.add_argument("--connections", type=int, default=1)
    parser.add_argument("--weightlimit", type=int, default=2_000_000)
    parser.add_argument(
        "--versionrollingmask",
        default=DEFAULT_VERSION_ROLLING_MASK,
        help="versionrollingmask advertised in getblocktemplate; set to empty to omit",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    state = FakeQbitState(
        bits=args.bits,
        target=args.target,
        log_requests=args.log_requests,
        chain=args.chain,
        initialblockdownload=args.initialblockdownload,
        connections=args.connections,
        weightlimit=args.weightlimit,
        versionrollingmask=args.versionrollingmask or None,
    )
    server = ThreadingHTTPServer((args.host, args.port), build_handler(state))
    print(f"fake qbit RPC listening on {args.host}:{args.port}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
