"""Thread-local JSON-RPC client used by PRISM processes."""

from __future__ import annotations

import base64
import http.client
import json
import threading
import urllib.parse
from typing import Any


class JsonRpc:
    """Minimal qbit JSON-RPC client with one keep-alive connection per thread."""

    def __init__(self, *, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.url = f"http://{host}:{port}"
        credentials = f"{user}:{password}".encode()
        self.auth = f"Basic {base64.b64encode(credentials).decode()}"
        # Keep-alive connections, one per calling thread. qbitd is called on
        # the hot share/block paths (a fresh getaddrinfo + TCP connect per call
        # was ~seconds of overhead under load); reusing the connection removes
        # that. threading.local keeps each thread's HTTPConnection private, so
        # concurrent callers never share a non-thread-safe connection.
        self._connections = threading.local()

    def _acquire_connection(self, timeout: float) -> http.client.HTTPConnection:
        conn = getattr(self._connections, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
            self._connections.conn = conn
        else:
            # Reuse: refresh the deadline for this call on the live socket.
            conn.timeout = timeout
            if conn.sock is not None:
                conn.sock.settimeout(timeout)
        return conn

    def _drop_connection(self) -> None:
        conn = getattr(self._connections, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._connections.conn = None

    def call(
        self,
        method: str,
        params: list[object] | None = None,
        *,
        wallet: str | None = None,
        timeout: float = 10,
    ) -> Any:
        body = json.dumps(
            {
                "jsonrpc": "1.0",
                "id": method,
                "method": method,
                "params": params or [],
            }
        ).encode()
        path = "/"
        if wallet is not None:
            path = f"/wallet/{urllib.parse.quote(wallet, safe='')}"
        headers = {
            "Authorization": self.auth,
            "Content-Type": "application/json",
            "User-Agent": "qbit-prism-coordinator/0.1",
        }
        # One retry with a fresh connection on a transport error. The usual
        # cause is the server having closed an idle keep-alive connection, in
        # which case the request never reached qbitd, so retrying is safe; the
        # only state-changing RPC (submitblock) is idempotent (duplicate ->
        # "duplicate") regardless. A second failure raises to the caller, which
        # treats it as backend-rpc-unavailable (a rejected share/block, never a
        # lost or double-counted block).
        last_exc: Exception | None = None
        for attempt in range(2):
            conn = self._acquire_connection(timeout)
            try:
                conn.request("POST", path, body=body, headers=headers)
                response = conn.getresponse()
                data = response.read()  # drain so the connection can be reused
            except (http.client.HTTPException, OSError) as exc:
                last_exc = exc
                self._drop_connection()
                if attempt == 0:
                    continue
                raise
            if response.status != 200:
                # Non-200 bodies may hold a JSON-RPC error (qbitd returns the
                # error object with a 500 for some methods); surface it as the
                # same RuntimeError text callers already match on (e.g. the
                # "-32601 / Method not found" blockwait-unsupported probe).
                self._drop_connection()
                detail = data.decode("utf-8", "replace")
                try:
                    error = json.loads(detail).get("error")
                except Exception:
                    error = None
                if error is not None:
                    raise RuntimeError(f"qbit RPC {method} failed: {error}")
                raise RuntimeError(f"qbit RPC {method} HTTP {response.status}: {detail[:200]}")
            payload = json.loads(data)
            if payload["error"] is not None:
                raise RuntimeError(f"qbit RPC {method} failed: {payload['error']}")
            return payload["result"]
        raise last_exc if last_exc is not None else RuntimeError("qbit RPC call failed")
