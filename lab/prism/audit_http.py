"""Audit/public HTTP routing and bounded server lifecycle for PRISM."""

from __future__ import annotations

from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from typing import Mapping, Protocol
import urllib.parse

from lab.prism import public_api


class AuditHttpPort(Protocol):
    """Purpose-specific application reads exposed to the HTTP facade."""

    def cached_health_payload(self) -> tuple[int, Mapping[str, object]]: ...

    def cached_metrics_payload(self) -> tuple[int, str]: ...

    def latest_evidence_payload(self) -> Mapping[str, object] | None: ...

    def owed_balances_payload(self) -> Mapping[str, object]: ...

    def carry_forward_integrity_payload(self) -> Mapping[str, object]: ...

    def miner_status_payload(self, recipient_id: str) -> Mapping[str, object]: ...

    def public_payload(
        self,
        path: str,
        query: Mapping[str, list[str]],
    ) -> tuple[int, object]: ...

    def ledger_backend(self) -> str: ...

    def audit_share_window(
        self,
        *,
        anchor_job_issued_at_ms: int,
        network_difficulty: int,
    ) -> list[dict[str, object]]: ...

    def audit_block_payouts(self, *, block_hash: str) -> list[dict[str, object]]: ...

    def audit_ctv_fanouts(self, *, block_hash: str) -> list[dict[str, object]]: ...

    def audit_ctv_fanout_manifest_set(
        self,
        *,
        block_hash: str,
    ) -> Mapping[str, object] | None: ...

    def ctv_fanout_status(
        self,
        *,
        fanout_txid: str,
    ) -> Mapping[str, object] | None: ...

    def pending_ctv_fanout_statuses(
        self,
        *,
        limit: int,
    ) -> list[dict[str, object]]: ...

    def audit_bundle(
        self,
        *,
        block_hash: str,
    ) -> Mapping[str, object] | None: ...

    def audit_bundle_by_commitment(
        self,
        *,
        commitment_leaf_hex: str,
    ) -> Mapping[str, object] | None: ...


@dataclass(frozen=True)
class AuditHttpConfig:
    bind: str
    port: int
    thread_name: str = "prism-audit-http"
    join_timeout_seconds: float = 1.0

    def __post_init__(self) -> None:
        if not self.bind:
            raise ValueError("audit HTTP bind must not be empty")
        if self.port < 0 or self.port > 65_535:
            raise ValueError("audit HTTP port must be between 0 and 65535")
        if not self.thread_name:
            raise ValueError("audit HTTP thread name must not be empty")
        if self.join_timeout_seconds < 0:
            raise ValueError("audit HTTP join timeout must be nonnegative")


@dataclass(frozen=True)
class AuditHttpState:
    lifecycle: str
    bound_address: tuple[str, int] | None
    thread_alive: bool


class _BoundedThreadingHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_type: type[BaseHTTPRequestHandler],
    ) -> None:
        self.ready = threading.Event()
        self._startup_lock = threading.Lock()
        self._startup_cancelled = False
        self._serve_committed = False
        super().__init__(server_address, handler_type)

    def service_actions(self) -> None:
        self.ready.set()

    def serve_unless_startup_cancelled(self, *, poll_interval: float) -> None:
        """Commit atomically to serving or honor startup cancellation."""

        with self._startup_lock:
            if self._startup_cancelled:
                return
            self._serve_committed = True
        self.serve_forever(poll_interval=poll_interval)

    def cancel_startup(self) -> bool:
        """Cancel an uncommitted start and report whether shutdown is safe."""

        with self._startup_lock:
            self._startup_cancelled = True
            return self._serve_committed


class AuditHttpFacade:
    """Own exact route behavior plus the listener socket and server thread."""

    def __init__(
        self,
        port: AuditHttpPort,
        config: AuditHttpConfig | None = None,
    ) -> None:
        self.port = port
        self.config = config
        self._public_response_cache = public_api.PublicResponseCache()
        self._lifecycle_lock = threading.Lock()
        self._server: _BoundedThreadingHttpServer | None = None
        self._thread: threading.Thread | None = None
        self._lifecycle = "new"
        self._handler_type = self._make_handler_type()

    def handler_type(self) -> type[BaseHTTPRequestHandler]:
        return self._handler_type

    def state(self) -> AuditHttpState:
        with self._lifecycle_lock:
            server = self._server
            thread = self._thread
            address = (
                None
                if server is None
                else (str(server.server_address[0]), int(server.server_address[1]))
            )
            return AuditHttpState(
                lifecycle=self._lifecycle,
                bound_address=address,
                thread_alive=bool(thread is not None and thread.is_alive()),
            )

    def start(self) -> AuditHttpState:
        config = self.config
        if config is None:
            raise RuntimeError("audit HTTP lifecycle requires configuration")
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return self._state_locked()
            server = _BoundedThreadingHttpServer(
                (config.bind, config.port),
                self._handler_type,
            )
            thread = threading.Thread(
                target=self._serve,
                args=(server,),
                name=config.thread_name,
                daemon=True,
            )
            self._server = server
            self._thread = thread
            self._lifecycle = "starting"
            try:
                thread.start()
            except Exception:
                server.server_close()
                self._server = None
                self._thread = None
                self._lifecycle = "stopped"
                raise
        try:
            if not server.ready.wait(1.0):
                raise RuntimeError("audit HTTP server did not enter its serve loop")
        except Exception:
            if server.cancel_startup():
                server.shutdown()
            server.server_close()
            thread.join(timeout=config.join_timeout_seconds)
            with self._lifecycle_lock:
                if self._server is server:
                    self._server = None
                    self._thread = None
                    self._lifecycle = (
                        "stopped" if not thread.is_alive() else "stop_timeout"
                    )
                elif (
                    self._server is None
                    and self._thread is None
                    and not thread.is_alive()
                ):
                    self._lifecycle = "stopped"
            raise
        with self._lifecycle_lock:
            if self._server is not server or self._thread is not thread:
                raise RuntimeError("audit HTTP server stopped during startup")
            if not thread.is_alive():
                self._lifecycle = "exited"
                raise RuntimeError("audit HTTP server exited during startup")
            self._lifecycle = "running"
            return self._state_locked()

    def _serve(self, server: _BoundedThreadingHttpServer) -> None:
        try:
            server.serve_unless_startup_cancelled(poll_interval=0.05)
        finally:
            try:
                server.server_close()
            finally:
                with self._lifecycle_lock:
                    if self._server is server:
                        self._server = None
                        self._thread = None
                        if self._lifecycle in {"starting", "running"}:
                            self._lifecycle = "exited"

    def stop(self) -> bool:
        with self._lifecycle_lock:
            server = self._server
            thread = self._thread
            if server is None or thread is None:
                self._lifecycle = "stopped"
                return True
            self._lifecycle = "stopping"
        if thread.is_alive():
            server.shutdown()
        server.server_close()
        join_timeout = self.config.join_timeout_seconds if self.config else 1.0
        thread.join(timeout=join_timeout)
        stopped = not thread.is_alive()
        with self._lifecycle_lock:
            if stopped and self._server is server:
                self._server = None
                self._thread = None
            self._lifecycle = "stopped" if stopped else "stop_timeout"
        return stopped

    def _state_locked(self) -> AuditHttpState:
        server = self._server
        thread = self._thread
        return AuditHttpState(
            lifecycle=self._lifecycle,
            bound_address=(
                None
                if server is None
                else (str(server.server_address[0]), int(server.server_address[1]))
            ),
            thread_alive=bool(thread is not None and thread.is_alive()),
        )

    def _make_handler_type(self) -> type[BaseHTTPRequestHandler]:
        facade = self

        class AuditHandler(BaseHTTPRequestHandler):
            server_version = "QbitPrismAudit/0.1"

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                query = urllib.parse.parse_qs(parsed.query)
                try:
                    if path == "/healthz":
                        status, payload = facade.port.cached_health_payload()
                        self.write_json(status, payload)
                        return
                    if path == "/metrics":
                        status, payload = facade.port.cached_metrics_payload()
                        self.write_text(
                            status,
                            payload,
                            "text/plain; version=0.0.4",
                        )
                        return
                    if path == "/public/v1" or path.startswith("/public/v1/"):
                        self.handle_public(path, query)
                        return
                    if path == "/audit/latest":
                        payload = facade.port.latest_evidence_payload()
                        if payload is None:
                            self.write_json(
                                404,
                                {"error": "no PRISM evidence has been produced"},
                            )
                        else:
                            self.write_json(200, payload)
                        return
                    if path in {"/owed", "/owed-balances"}:
                        self.write_json(200, facade.port.owed_balances_payload())
                        return
                    if path in {
                        "/audit/carry-forward-integrity",
                        "/audit/ledger-integrity",
                    }:
                        self.write_json(
                            200,
                            facade.port.carry_forward_integrity_payload(),
                        )
                        return
                    if path.startswith("/miners/") and path.endswith("/status"):
                        recipient_id = urllib.parse.unquote(
                            path.removeprefix("/miners/").removesuffix("/status")
                        )
                        self.write_json(
                            200,
                            facade.port.miner_status_payload(recipient_id),
                        )
                        return
                    if path.startswith("/payouts/") and path.endswith("/status"):
                        recipient_id = urllib.parse.unquote(
                            path.removeprefix("/payouts/").removesuffix("/status")
                        )
                        self.write_json(
                            200,
                            facade.port.miner_status_payload(recipient_id),
                        )
                        return
                    if path == "/audit/share-window":
                        self.handle_share_window(query)
                        return
                    if path.startswith("/audit/blocks/") and path.endswith(
                        "/payouts"
                    ):
                        block_hash = path.removeprefix(
                            "/audit/blocks/"
                        ).removesuffix("/payouts")
                        self.handle_block_payouts(block_hash)
                        return
                    if path.startswith("/audit/blocks/") and path.endswith(
                        "/ctv-fanouts"
                    ):
                        block_hash = path.removeprefix(
                            "/audit/blocks/"
                        ).removesuffix("/ctv-fanouts")
                        self.handle_block_ctv_fanouts(block_hash)
                        return
                    if path.startswith("/audit/blocks/") and path.endswith(
                        "/ctv-fanout-manifest-set"
                    ):
                        block_hash = path.removeprefix(
                            "/audit/blocks/"
                        ).removesuffix("/ctv-fanout-manifest-set")
                        self.handle_block_ctv_fanout_manifest_set(block_hash)
                        return
                    if path == "/audit/fanouts/pending":
                        self.handle_pending_ctv_fanouts(query)
                        return
                    if path.startswith("/audit/fanouts/") and path.endswith(
                        "/status"
                    ):
                        fanout_txid = path.removeprefix(
                            "/audit/fanouts/"
                        ).removesuffix("/status")
                        self.handle_ctv_fanout_status(fanout_txid)
                        return
                    if path.startswith("/audit/commitments/") and path.endswith(
                        "/bundle"
                    ):
                        commitment_leaf_hex = path.removeprefix(
                            "/audit/commitments/"
                        ).removesuffix("/bundle")
                        self.handle_commitment_bundle(commitment_leaf_hex)
                        return
                    if path.startswith("/audit/block/"):
                        block_hash = path.removeprefix("/audit/block/")
                        self.handle_block_payouts(block_hash)
                        return
                    if path.startswith("/audit/blocks/") and path.endswith(
                        "/bundle"
                    ):
                        block_hash = path.removeprefix(
                            "/audit/blocks/"
                        ).removesuffix("/bundle")
                        self.handle_block_bundle(block_hash)
                        return
                    self.write_json(404, {"error": "unknown endpoint"})
                except public_api.PublicApiError as exc:
                    self.write_json(
                        exc.status,
                        public_api.error_payload(exc.code, exc.message),
                        headers=public_api.public_error_headers(),
                    )
                except ValueError as exc:
                    if path == "/public/v1" or path.startswith("/public/v1/"):
                        self.write_json(
                            500,
                            public_api.error_payload(
                                "internal_error",
                                "internal server error",
                            ),
                            headers=public_api.public_error_headers(),
                        )
                    else:
                        self.write_json(400, {"error": str(exc)})
                except Exception as exc:
                    if path == "/public/v1" or path.startswith("/public/v1/"):
                        self.write_json(
                            500,
                            public_api.error_payload(
                                "internal_error",
                                "internal server error",
                            ),
                            headers=public_api.public_error_headers(),
                        )
                    else:
                        self.write_json(500, {"error": str(exc)})

            def handle_public(
                self,
                path: str,
                query: dict[str, list[str]],
            ) -> None:
                cache_policy = public_api.public_cache_policy(path)
                status, payload, cache_state, age_seconds = (
                    facade._public_response_cache.get_or_compute(
                        key=public_api.public_cache_key(path, query),
                        ttl_seconds=cache_policy.ttl_seconds,
                        compute=lambda: facade.port.public_payload(path, query),
                    )
                )
                self.write_json(
                    status,
                    payload,
                    headers=public_api.public_cache_headers(
                        cache_policy,
                        cache_state=cache_state,
                        age_seconds=age_seconds,
                    ),
                )

            def handle_share_window(self, query: dict[str, list[str]]) -> None:
                anchor_raw = self.first_query_value(
                    query,
                    "anchor_job_issued_at_ms",
                    "anchor",
                )
                difficulty_raw = self.first_query_value(
                    query,
                    "network_difficulty",
                )
                if anchor_raw is None or difficulty_raw is None:
                    raise ValueError(
                        "anchor_job_issued_at_ms and network_difficulty are required"
                    )
                rows = facade.port.audit_share_window(
                    anchor_job_issued_at_ms=int(anchor_raw),
                    network_difficulty=int(difficulty_raw),
                )
                self.write_json(
                    200,
                    {
                        "schema": "qbit.prism.audit-share-window.v1",
                        "ledger_backend": facade.port.ledger_backend(),
                        "rows": rows,
                    },
                )

            def handle_block_payouts(self, block_hash: str) -> None:
                block_hash = self.clean_hash(block_hash)
                rows = facade.port.audit_block_payouts(block_hash=block_hash)
                if not rows:
                    self.write_json(
                        404,
                        {
                            "error": "unknown PRISM block",
                            "block_hash": block_hash,
                        },
                    )
                    return
                self.write_json(
                    200,
                    {
                        "schema": "qbit.prism.audit-block-payouts.v1",
                        "ledger_backend": facade.port.ledger_backend(),
                        "block_hash": block_hash,
                        "rows": rows,
                    },
                )

            def handle_block_ctv_fanouts(self, block_hash: str) -> None:
                block_hash = self.clean_hash(block_hash, name="block hash")
                rows = facade.port.audit_ctv_fanouts(block_hash=block_hash)
                if not rows:
                    self.write_json(
                        404,
                        {
                            "error": "unknown CTV fanout block",
                            "block_hash": block_hash,
                        },
                    )
                    return
                self.write_json(
                    200,
                    {
                        "schema": "qbit.prism.audit-ctv-fanouts.v1",
                        "ledger_backend": facade.port.ledger_backend(),
                        "block_hash": block_hash,
                        "rows": rows,
                    },
                )

            def handle_block_ctv_fanout_manifest_set(self, block_hash: str) -> None:
                block_hash = self.clean_hash(block_hash, name="block hash")
                payload = facade.port.audit_ctv_fanout_manifest_set(
                    block_hash=block_hash
                )
                if payload is None:
                    self.write_json(
                        404,
                        {
                            "error": "unknown CTV fanout block",
                            "block_hash": block_hash,
                        },
                    )
                    return
                self.write_json(200, payload)

            def handle_ctv_fanout_status(self, fanout_txid: str) -> None:
                fanout_txid = self.clean_hash(fanout_txid, name="fanout txid")
                payload = facade.port.ctv_fanout_status(fanout_txid=fanout_txid)
                if payload is None:
                    self.write_json(
                        404,
                        {
                            "error": "unknown CTV fanout",
                            "fanout_txid": fanout_txid,
                        },
                    )
                    return
                self.write_json(200, payload)

            def handle_pending_ctv_fanouts(
                self,
                query: dict[str, list[str]],
            ) -> None:
                limit_raw = self.first_query_value(query, "limit")
                limit = int(limit_raw) if limit_raw is not None else 100
                rows = facade.port.pending_ctv_fanout_statuses(limit=limit)
                self.write_json(
                    200,
                    {
                        "schema": "qbit.prism.pending-ctv-fanouts.v1",
                        "ledger_backend": facade.port.ledger_backend(),
                        "count": len(rows),
                        "rows": rows,
                    },
                )

            def handle_block_bundle(self, block_hash: str) -> None:
                block_hash = self.clean_hash(block_hash, name="block hash")
                payload = facade.port.audit_bundle(block_hash=block_hash)
                if payload is None:
                    self.write_json(
                        404,
                        {
                            "error": "unknown PRISM block",
                            "block_hash": block_hash,
                        },
                    )
                    return
                self.write_json(200, payload)

            def handle_commitment_bundle(self, commitment_leaf_hex: str) -> None:
                commitment_leaf_hex = self.clean_hash(
                    commitment_leaf_hex,
                    name="audit commitment leaf",
                )
                payload = facade.port.audit_bundle_by_commitment(
                    commitment_leaf_hex=commitment_leaf_hex
                )
                if payload is None:
                    self.write_json(
                        404,
                        {
                            "error": "unknown PRISM audit commitment",
                            "audit_commitment_leaf_hex": commitment_leaf_hex,
                        },
                    )
                    return
                self.write_json(200, payload)

            def write_json(
                self,
                status: int,
                payload: object,
                headers: dict[str, str] | None = None,
            ) -> None:
                body = json.dumps(payload, sort_keys=True).encode() + b"\n"
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    for key, value in (headers or {}).items():
                        self.send_header(key, value)
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    return

            def write_text(
                self,
                status: int,
                payload: str,
                content_type: str,
            ) -> None:
                body = payload.encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    return

            def log_message(self, format: str, *args: object) -> None:
                return

            @staticmethod
            def first_query_value(
                query: dict[str, list[str]],
                *keys: str,
            ) -> str | None:
                for key in keys:
                    values = query.get(key)
                    if values:
                        return values[0]
                return None

            @staticmethod
            def clean_hash(value: str, *, name: str = "block hash") -> str:
                value = urllib.parse.unquote(value).strip()
                if len(value) != 64 or any(
                    char not in "0123456789abcdefABCDEF" for char in value
                ):
                    raise ValueError(f"{name} must be 64 hex characters")
                return value.lower()

        return AuditHandler


__all__ = [
    "AuditHttpConfig",
    "AuditHttpFacade",
    "AuditHttpPort",
    "AuditHttpState",
]
