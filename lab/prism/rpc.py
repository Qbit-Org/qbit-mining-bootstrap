"""Thread-local JSON-RPC client used by PRISM processes."""

from __future__ import annotations

import base64
import http.client
import json
import socket
import threading
import time
import urllib.parse
from typing import Any


# The default deadline for every JsonRpc.call without an explicit timeout,
# including the fence-guarded CTV broadcast RPCs. The ledger's own-write
# deferral margin is derived from the guarded deadlines, so route changes
# through this constant rather than the call signature's literal.
DEFAULT_QBIT_RPC_CALL_TIMEOUT_SECONDS = 10.0

_QBIT_RPC_NO_TRANSPORT_RETRY_METHODS = frozenset(
    {
        "getnewaddress",
        "sendrawtransaction",
        "signrawtransactionwithwallet",
        "submitblock",
        "submitpackage",
    }
)


class JsonRpc:
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
            # Never let http.client resurrect a severed connection: with
            # auto_open, request() silently reconnects when the deadline
            # watchdog's close() lands between the pre-send check and the
            # send, and that implicit fresh socket lives until the next
            # watchdog sweep — long enough for a short mutating POST to
            # reach qbitd after a caller released its ordering locks. With
            # auto_open off a cleared socket makes send() raise
            # NotConnected instead; call() connects explicitly under the
            # watchdog and rechecks the deadline before any byte goes out.
            conn.auto_open = 0
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
        timeout: float = DEFAULT_QBIT_RPC_CALL_TIMEOUT_SECONDS,
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
        # Read-only calls retry once with a fresh connection after a transport
        # error, normally an idle keep-alive that qbitd closed. Mutating calls
        # never retry inside this method: their writer-lease fence applies to
        # the outer call, so an invisible second POST could otherwise run after
        # that lease was lost. Durable block/CTV workflows retry later as a new
        # fully fenced operation and reconcile an uncertain first result.
        last_exc: Exception | None = None
        attempt_count = (
            1 if method in _QBIT_RPC_NO_TRANSPORT_RETRY_METHODS else 2
        )
        deadline = time.monotonic() + max(0.001, float(timeout))
        for attempt in range(attempt_count):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if last_exc is not None:
                    raise last_exc
                raise TimeoutError(f"qbit RPC {method} timed out")
            conn = self._acquire_connection(remaining)
            # The socket timeout bounds each individual socket operation,
            # not the call: a response body arriving one packet per
            # interval extends the call arbitrarily past its nominal
            # deadline. Lease authority margins are sized from this
            # timeout as a wall-clock bound on fence-guarded effects, so
            # enforce it end-to-end: past the deadline a watchdog severs
            # the connection's socket — repeatedly, because a stalled name
            # resolution has no socket yet and the one connect() creates
            # afterwards must not carry the RPC onward — until the attempt
            # observes the abort as a transport error. For mutating calls
            # that is the same uncertain outcome a socket timeout already
            # produces, reconciled by durable replay as a new fully fenced
            # operation.
            watchdog_fired = threading.Event()
            attempt_finished = threading.Event()

            # The allowance is the attempt's exact remaining wall clock, no
            # grace: callers like the submitblock hard-deadline wrapper
            # abandon the call and release ordering locks (the payout
            # landing fence) the instant their deadline passes, so a byte
            # transmitted in any grace window would race work those callers
            # already unblocked.
            def _enforce_deadline(
                doomed: http.client.HTTPConnection = conn,
                fired: threading.Event = watchdog_fired,
                finished: threading.Event = attempt_finished,
                allowance: float = remaining,
            ) -> None:
                if finished.wait(allowance):
                    return
                fired.set()
                while True:
                    sock = getattr(doomed, "sock", None)
                    if sock is not None:
                        try:
                            sock.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                    try:
                        doomed.close()
                    except Exception:
                        pass
                    if finished.wait(0.1):
                        return

            watchdog = threading.Thread(
                target=_enforce_deadline,
                name="qbit-rpc-deadline",
                daemon=True,
            )
            watchdog.start()
            try:
                # Establish the connection (name resolution + TCP) under
                # the watchdog and re-check the deadline before any request
                # byte is sent. Left to conn.request(), a name resolution
                # returning after the deadline resumes straight into
                # sendall(), and a short mutating POST can reach qbitd
                # between the watchdog's periodic sweeps; the explicit
                # check makes the late-connect case lose deterministically.
                # The check is against the wall clock, not just the
                # watchdog flag: the flag lags the deadline by thread
                # scheduling, and a caller that abandoned this call at its
                # deadline may already have released ordering locks that
                # a post-deadline send would race.
                if getattr(conn, "sock", None) is None:
                    conn_connect = getattr(conn, "connect", None)
                    if conn_connect is not None:
                        conn_connect()
                if watchdog_fired.is_set() or time.monotonic() >= deadline:
                    raise TimeoutError(f"qbit RPC {method} timed out")
                conn.request("POST", path, body=body, headers=headers)
                response = conn.getresponse()
                data = response.read()  # drain so the connection can be reused
            except (http.client.HTTPException, OSError) as exc:
                self._drop_connection()
                if watchdog_fired.is_set():
                    raise TimeoutError(f"qbit RPC {method} timed out") from exc
                last_exc = exc
                if attempt + 1 < attempt_count:
                    continue
                raise
            finally:
                attempt_finished.set()
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
