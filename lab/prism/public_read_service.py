#!/usr/bin/env python3
"""Standalone process serving the PRISM public ``/public/v1`` read surface.

The coordinator used to serve this surface from its own audit/ops listener,
under the GIL that acknowledges shares and lands blocks, against the primary
Postgres the lease-holding writer commits through. Public read traffic scales
with public interest rather than with hashrate, so it is the one workload whose
volume the pool does not control -- and it sat on the money path. This process
takes it off that path.

What this is *not*: a rewrite. ``lab.prism.public_api`` reaches its host through
a deliberately small surface -- ``.ledger``, ``.rpc``, and an environment-backed
``getattr`` for the advertised Stratum endpoint -- so the extraction is a new
host object plus a small HTTP shell. Routing, payloads, cache policy and error
shapes stay in ``public_api``; ``tests/test_prism_public_read_service.py`` pins
this process's responses against ``public_api.dispatch`` directly.

Three properties this process guarantees that the coordinator's listener did
not need to:

Read-only in the strong sense
    The ledger is constructed without any ``PRISM_LEDGER_WRITER_*`` input and
    never acquires a writer lease: every public read model runs through
    ``PsqlShareLedger._run_read_json`` (a bounded read slot), never through the
    writer gate. The in-memory ledger is refused outright -- it lacks the
    ``dashboard_*`` read models, so ``public_api``'s duck-typed fallbacks would
    quietly reintroduce writer-lock reads (``current_owed_balances``,
    ``recipient_payout_history``) on the public surface, which is precisely the
    contention this extraction exists to remove.

Fail-closed configuration
    ``public_api.mining_configuration`` falls back to
    ``PRISM_STRATUM_PORT``/``127.0.0.1`` when its host exposes no ``port`` or
    ``bind``. This process deliberately exposes neither, so that fallback is
    live -- and left alone it would advertise ``127.0.0.1:3340`` to the world as
    the pool's Stratum endpoint. ``PRISM_PUBLIC_STRATUM_URL`` is therefore
    required at startup rather than defaulted.

An honest staleness contract
    Every extracted response states the age it is willing to serve
    (``X-Prism-Staleness-Budget-Seconds``) alongside the age it actually has
    (``Age``), and refuses with 503 rather than serve past it. The budgets live
    in ``lab.prism.endpoint_registry`` and are derived from the caches beneath
    each route; see that module's docstring. This follows
    ``lab/prism/observability.py``, where the cached ``/metrics`` endpoint
    already returns 503 rather than serve an unbounded-stale snapshot.

Entry point: ``python3 -m lab.prism.public_read_service``.
"""

from __future__ import annotations

import json
import os
import shlex
import signal
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lab.prism import endpoint_registry, public_api
from lab.prism.audit_artifacts import AuditArtifactConfig, AuditArtifactStore
from lab.prism.coordinator_config import (
    DEFAULT_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_COUNT,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS,
    DEFAULT_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS,
    env,
    env_int,
    env_nonnegative_float,
    env_positive_float,
    env_positive_int,
)
from lab.prism.rpc import JsonRpc
from lab.prism.share_ledger import PsqlShareLedger


DEFAULT_PUBLIC_API_BIND = "0.0.0.0"
DEFAULT_PUBLIC_API_PORT = 3342

# Emitted on every extracted response: the age this route is willing to serve.
# The observed age travels beside it in the standard Age header.
STALENESS_BUDGET_HEADER = "X-Prism-Staleness-Budget-Seconds"

# What that header says for /public/v1/artifacts/{sha256}, the one route with no
# budget. A number would be a lie in either direction; this names the contract.
UNBOUNDED_STALENESS_BUDGET = "unbounded"

# How often the background prober re-checks Postgres, and the age at which its
# answer stops counting. Same shape as observability.py's health staleness:
# three refresh intervals, floored, so ordinary jitter is not an outage.
DEFAULT_READINESS_PROBE_INTERVAL_SECONDS = 5.0
MINIMUM_READINESS_STALE_SECONDS = 15.0

HEALTH_SCHEMA = "qbit.prism.public-read-health.v1"


class PublicReadConfigurationError(RuntimeError):
    """Startup refused because the service would otherwise serve wrong facts."""


class PublicReadCoordinator:
    """The coordinator stand-in ``lab.prism.public_api`` reads through.

    ``public_api`` needs exactly ``.ledger`` (24 references) and ``.rpc``
    (getblockchaininfo / getblocktemplate / getnetworkinfo). It also probes
    ``getattr(coordinator, "port")`` and ``getattr(coordinator, "bind")`` in
    ``mining_configuration``, both of which already fall back to the
    environment.

    Those two attributes are deliberately absent here. On the coordinator they
    named the Stratum listener that process was itself running; this process
    runs no Stratum listener, so any value it invented would be a guess
    published to miners as connection instructions. Leaving them undefined
    routes the lookup to the environment, and ``require_public_stratum_url()``
    makes the operator state the answer rather than inherit ``127.0.0.1``.
    """

    def __init__(self, *, ledger: Any, rpc: Any) -> None:
        self.ledger = ledger
        self.rpc = rpc


class ServiceMetrics:
    """Counters for this process only, rendered as Prometheus text.

    Intentionally small: the coordinator keeps its own /metrics with the pool's
    real operational signal. What is worth knowing here is whether the tier is
    serving, whether its response cache is earning its keep, and whether the
    staleness contract is actually refusing anything.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests_total = 0
        self._responses_by_status: dict[int, int] = {}
        self._cache_by_state: dict[str, int] = {"hit": 0, "miss": 0, "bypass": 0}
        self._staleness_refusals = 0

    def record_request(self) -> None:
        with self._lock:
            self._requests_total += 1

    def record_response(self, status: int) -> None:
        with self._lock:
            self._responses_by_status[status] = (
                self._responses_by_status.get(status, 0) + 1
            )

    def record_cache_state(self, cache_state: str) -> None:
        key = cache_state.lower()
        with self._lock:
            if key in self._cache_by_state:
                self._cache_by_state[key] += 1

    def record_staleness_refusal(self) -> None:
        with self._lock:
            self._staleness_refusals += 1

    def render(self, *, ready: bool, readiness_age_seconds: float) -> str:
        with self._lock:
            requests_total = self._requests_total
            responses = dict(self._responses_by_status)
            cache_states = dict(self._cache_by_state)
            refusals = self._staleness_refusals
        lines = [
            "# HELP qbit_prism_public_requests_total Public read requests accepted.",
            "# TYPE qbit_prism_public_requests_total counter",
            f"qbit_prism_public_requests_total {requests_total}",
            "# HELP qbit_prism_public_responses_total Public read responses by status code.",
            "# TYPE qbit_prism_public_responses_total counter",
        ]
        lines.extend(
            f'qbit_prism_public_responses_total{{status="{status}"}} {count}'
            for status, count in sorted(responses.items())
        )
        lines.extend(
            [
                "# HELP qbit_prism_public_cache_total Response-cache outcomes by state.",
                "# TYPE qbit_prism_public_cache_total counter",
            ]
        )
        lines.extend(
            f'qbit_prism_public_cache_total{{state="{state}"}} {count}'
            for state, count in sorted(cache_states.items())
        )
        lines.extend(
            [
                "# HELP qbit_prism_public_staleness_refusals_total Responses refused for exceeding their staleness budget.",
                "# TYPE qbit_prism_public_staleness_refusals_total counter",
                f"qbit_prism_public_staleness_refusals_total {refusals}",
                "# HELP qbit_prism_public_ledger_ready Whether the last Postgres readiness probe succeeded.",
                "# TYPE qbit_prism_public_ledger_ready gauge",
                f"qbit_prism_public_ledger_ready {1 if ready else 0}",
                "# HELP qbit_prism_public_ledger_probe_age_seconds Age of the last readiness probe, or -1 before the first.",
                "# TYPE qbit_prism_public_ledger_probe_age_seconds gauge",
                f"qbit_prism_public_ledger_probe_age_seconds {readiness_age_seconds:.3f}",
            ]
        )
        return "\n".join(lines) + "\n"


class ReadinessProbe:
    """Background Postgres liveness, so /healthz never queries on the request thread.

    The compose healthcheck polls this forever. Probing inline would put a
    database round trip -- and a read slot -- on every poll, and a stalled
    database would hang the health request rather than answer it. Instead one
    background thread runs the constant-select probe on an interval and
    publishes the result; the request thread only reads two fields.
    """

    def __init__(
        self,
        probe: Callable[[], bool],
        *,
        interval_seconds: float = DEFAULT_READINESS_PROBE_INTERVAL_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._probe = probe
        self._interval_seconds = max(0.1, float(interval_seconds))
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._ok = False
        self._error: str | None = None
        self._checked_at: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def stale_after_seconds(self) -> float:
        return max(3 * self._interval_seconds, MINIMUM_READINESS_STALE_SECONDS)

    def check_once(self) -> None:
        try:
            ok = bool(self._probe())
            error = None if ok else "readiness probe returned false"
        except Exception as exc:  # noqa: BLE001 - any failure means "not ready"
            ok = False
            error = f"{type(exc).__name__}: {exc}"
        with self._lock:
            self._ok = ok
            self._error = error
            self._checked_at = self._monotonic()

    def start(self) -> None:
        # One synchronous probe first so a freshly started service reports its
        # real state immediately instead of a warm-up 503 the healthcheck would
        # have to wait out.
        self.check_once()
        self._thread = threading.Thread(
            target=self._run,
            name="prism-public-readiness",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            self.check_once()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5)

    def age_seconds(self) -> float:
        with self._lock:
            checked_at = self._checked_at
        if checked_at is None:
            return -1.0
        return max(0.0, self._monotonic() - checked_at)

    def snapshot(self) -> tuple[int, dict[str, object]]:
        with self._lock:
            ok = self._ok
            error = self._error
            checked_at = self._checked_at
        if checked_at is None:
            return 503, {
                "ok": False,
                "schema": HEALTH_SCHEMA,
                "state": "starting",
                "error": "readiness probe has not completed yet",
                "probe_age_seconds": -1.0,
            }
        age_seconds = max(0.0, self._monotonic() - checked_at)
        stale = age_seconds > self.stale_after_seconds
        payload: dict[str, object] = {
            "ok": bool(ok and not stale),
            "schema": HEALTH_SCHEMA,
            "state": "ready" if (ok and not stale) else "unready",
            "probe_age_seconds": round(age_seconds, 3),
            "probe_stale_after_seconds": self.stale_after_seconds,
        }
        if stale:
            payload["error"] = "readiness probe is stale"
        elif error is not None:
            payload["error"] = error
        return (200 if payload["ok"] else 503), payload


def staleness_budget_for_path(path: str) -> float | None:
    """The budget for one concrete request path, or None when it has none.

    None means "never refuse this for age", which today is only
    /public/v1/artifacts/{sha256} -- content-addressed and immutable, so a body
    that hashes to the requested sha256 is correct however old it is. An
    unclassified path also lands here, but dispatch() 404s it before any
    staleness question arises.
    """
    endpoint = endpoint_registry.endpoint_for_request_path(path)
    if endpoint is None or endpoint.immutable_content:
        return None
    return endpoint.max_staleness_seconds


def staleness_budget_header_value(path: str) -> str | None:
    """What X-Prism-Staleness-Budget-Seconds says for one path, or None to omit.

    Every classified route publishes a budget, including the immutable one:
    silence there would be indistinguishable from a route that simply forgot,
    and "unbounded" is the actual contract -- a content-addressed body is
    correct at any age. A path that is not a route at all publishes nothing,
    because there is no route whose budget it could be.
    """
    endpoint = endpoint_registry.endpoint_for_request_path(path)
    if endpoint is None:
        return None
    if endpoint.immutable_content:
        return UNBOUNDED_STALENESS_BUDGET
    if endpoint.max_staleness_seconds is None:
        return None
    return _format_budget(endpoint.max_staleness_seconds)


def staleness_error_payload(*, budget_seconds: float, age_seconds: int) -> dict[str, object]:
    """A refusal in the existing public error schema, naming both numbers.

    Uses public_api.error_payload so the body is prism.dashboard.error.v1 like
    every other public error; a bespoke schema here would make the one response
    a client most needs to parse the one it cannot.
    """
    return public_api.error_payload(
        "stale_read_model",
        (
            "cached response age "
            f"{age_seconds}s exceeds the staleness budget of "
            f"{budget_seconds:g}s for this endpoint"
        ),
    )


class PublicReadService:
    """One public read tier instance: what to read through, and what to remember.

    Holds the collaborators a handler needs -- the ``public_api`` host object,
    the shared response cache, this process's counters, and the background
    readiness probe -- so that binding a socket is a separate, trivial step.
    Tests construct one of these around a fake coordinator and get the real
    request path without a database, an RPC node, or a signal handler.
    """

    def __init__(
        self,
        coordinator: Any,
        *,
        response_cache: public_api.PublicResponseCache | None = None,
        metrics: ServiceMetrics | None = None,
        readiness: ReadinessProbe | None = None,
    ) -> None:
        self.coordinator = coordinator
        self.cache = (
            public_api.PublicResponseCache() if response_cache is None else response_cache
        )
        self.metrics = ServiceMetrics() if metrics is None else metrics
        self.readiness = readiness


def make_handler(service: PublicReadService) -> type[BaseHTTPRequestHandler]:
    """Build the request handler for one public read service instance.

    Deliberately not ``AuditHttpFacade``: that class's cancel-startup and
    bounded-serve-exit machinery encodes shutdown races specific to starting and
    stopping a listener inside the coordinator's long-lived process. This one
    binds, serves, and exits on a signal, so it needs none of it.
    """

    coordinator = service.coordinator
    cache = service.cache
    service_metrics = service.metrics
    readiness = service.readiness

    class PublicReadHandler(BaseHTTPRequestHandler):
        server_version = "PrismPublicRead/1"
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            parsed = urllib.parse.urlparse(self.path)
            # Same normalization the coordinator listener applied, so a
            # trailing slash resolves to the same route it always did.
            path = parsed.path.rstrip("/") or "/"
            query = urllib.parse.parse_qs(parsed.query)
            service_metrics.record_request()
            try:
                if path == "/healthz":
                    self.handle_healthz()
                    return
                if path == "/metrics":
                    self.handle_metrics()
                    return
                if path == "/public/v1" or path.startswith("/public/v1/"):
                    self.handle_public(path, query)
                    return
                self.write_json(404, {"error": "unknown endpoint"})
            except public_api.PublicApiError as exc:
                self.write_json(
                    exc.status,
                    public_api.error_payload(exc.code, exc.message),
                    headers=self.with_budget(path, public_api.public_error_headers()),
                )
            except Exception:  # noqa: BLE001 - never leak internals to the public
                # Matches the coordinator listener's old behaviour for
                # /public/v1: a generic 500 in the public error schema, with no
                # exception text. ValueError was folded in there too -- it took
                # the same branch -- so it needs no separate arm here.
                self.write_json(
                    500,
                    public_api.error_payload("internal_error", "internal server error"),
                    headers=self.with_budget(path, public_api.public_error_headers()),
                )

        def handle_public(self, path: str, query: dict[str, list[str]]) -> None:
            cache_policy = public_api.public_cache_policy(path)
            status, payload, cache_state, age_seconds = cache.get_or_compute(
                key=public_api.public_cache_key(path, query),
                ttl_seconds=cache_policy.ttl_seconds,
                compute=lambda: public_api.dispatch(coordinator, path, query),
            )
            service_metrics.record_cache_state(cache_state)
            budget_seconds = staleness_budget_for_path(path)
            if budget_seconds is not None and age_seconds > budget_seconds:
                # Refuse rather than serve past the budget. A silently stale
                # dashboard is worse than an honest one.
                service_metrics.record_staleness_refusal()
                headers = public_api.public_error_headers()
                headers[STALENESS_BUDGET_HEADER] = _format_budget(budget_seconds)
                headers["Age"] = str(max(0, age_seconds))
                self.write_json(
                    503,
                    staleness_error_payload(
                        budget_seconds=budget_seconds,
                        age_seconds=age_seconds,
                    ),
                    headers=headers,
                )
                return
            headers = public_api.public_cache_headers(
                cache_policy,
                cache_state=cache_state,
                age_seconds=age_seconds,
            )
            self.write_json(status, payload, headers=self.with_budget(path, headers))

        def with_budget(self, path: str, headers: dict[str, str]) -> dict[str, str]:
            """Attach the route's budget so callers can see it on every response."""

            budget = staleness_budget_header_value(path)
            if budget is not None:
                headers[STALENESS_BUDGET_HEADER] = budget
            return headers

        def handle_healthz(self) -> None:
            if readiness is None:
                self.write_json(200, {"ok": True, "schema": HEALTH_SCHEMA})
                return
            status, payload = readiness.snapshot()
            self.write_json(status, payload)

        def handle_metrics(self) -> None:
            ready = True
            age_seconds = -1.0
            if readiness is not None:
                status, _ = readiness.snapshot()
                ready = status == 200
                age_seconds = readiness.age_seconds()
            body = service_metrics.render(
                ready=ready,
                readiness_age_seconds=age_seconds,
            ).encode()
            service_metrics.record_response(200)
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                return

        def write_json(
            self,
            status: int,
            payload: object,
            headers: dict[str, str] | None = None,
        ) -> None:
            # Byte-identical framing to the coordinator listener's write_json:
            # sorted keys and a trailing newline, so the extracted bodies match
            # the pre-extraction ones exactly.
            body = json.dumps(payload, sort_keys=True).encode() + b"\n"
            service_metrics.record_response(status)
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # A client with a short timeout hung up before the response was
                # written; nothing to salvage.
                return

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            # BaseHTTPRequestHandler logs every request to stderr by default.
            # This tier's request volume is the whole reason it exists, so that
            # default would be the loudest thing in the container's logs.
            return

    return PublicReadHandler


def _format_budget(budget_seconds: float) -> str:
    """Render a budget without a trailing ``.0`` on whole-second values."""

    if float(budget_seconds).is_integer():
        return str(int(budget_seconds))
    return f"{budget_seconds:g}"


def require_public_stratum_url(environ: dict[str, str] | None = None) -> str:
    """Refuse to start unless the advertised Stratum endpoint is stated.

    public_api.mining_configuration composes the advertised endpoint from
    getattr(coordinator, "bind"/"port") with an environment fallback of
    127.0.0.1:3340. This process exposes neither attribute, on purpose, so
    without PRISM_PUBLIC_STRATUM_URL the public mining-configuration endpoint
    would confidently tell every miner to point its hashrate at the loopback
    address of whatever machine read the page.
    """
    source = os.environ if environ is None else environ
    value = (source.get("PRISM_PUBLIC_STRATUM_URL") or "").strip()
    if not value:
        raise PublicReadConfigurationError(
            "PRISM_PUBLIC_STRATUM_URL is required: the public read service runs "
            "no Stratum listener, so it cannot infer the pool's endpoint, and "
            "the fallback would advertise 127.0.0.1 to miners"
        )
    return value


def build_ledger_from_env(environ: dict[str, str] | None = None) -> PsqlShareLedger:
    """Construct the read-only Postgres ledger this service reads through.

    Two refusals rather than defaults:

    * The in-memory ledger is rejected. It implements none of the ``dashboard_*``
      read models, so public_api's duck-typed fallbacks would silently take the
      writer-lock paths (current_owed_balances, recipient_payout_history,
      ledger.all_shares) that this extraction exists to get off the public
      surface -- a memory-ledger public tier would look fine and reintroduce the
      exact contention it removed.
    * No PRISM_LEDGER_WRITER_* input is read at all. This process must never
      acquire a writer lease; leaving the writer identity at its default and
      touching only read models keeps that structural rather than conventional.
    """
    source = os.environ if environ is None else environ
    if _env_flag(source.get("PRISM_ALLOW_MEMORY_LEDGER")):
        raise PublicReadConfigurationError(
            "PRISM_ALLOW_MEMORY_LEDGER is not supported by the public read "
            "service: the in-memory ledger has no dashboard_* read models, so "
            "public_api would fall back to writer-lock reads "
            "(current_owed_balances, recipient_payout_history) on public routes"
        )
    psql_command = source.get("PRISM_POSTGRES_PSQL_COMMAND", "")
    database_url = source.get("PRISM_DATABASE_URL", "")
    if not psql_command and database_url:
        psql_command = f"psql {shlex.quote(database_url)}"
    if not psql_command:
        raise PublicReadConfigurationError(
            "PRISM_DATABASE_URL or PRISM_POSTGRES_PSQL_COMMAND is required: the "
            "public read service serves every route from Postgres"
        )
    return PsqlShareLedger(
        psql_command=psql_command,
        database_url=database_url or None,
        native_client_mode=env("PRISM_POSTGRES_NATIVE_CLIENT", "auto"),
        # Constructing an ordinary ledger claims the single-writer lease and
        # gives the object a real writer lock. read_only does neither: no lease
        # is acquired, and the writer gate refuses to be taken at all, so a
        # public route that reached a fenced read would fail loudly here rather
        # than quietly serialize dashboard polls against block landing.
        read_only=True,
        # Never initialize or repair schema from the read tier.
        initialize_schema=False,
        postgres_idle_in_transaction_timeout_seconds=env_positive_float(
            "PRISM_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS",
            DEFAULT_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS,
        ),
        postgres_tcp_keepalives_idle_seconds=env_positive_int(
            "PRISM_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS",
            DEFAULT_POSTGRES_TCP_KEEPALIVES_IDLE_SECONDS,
        ),
        postgres_tcp_keepalives_interval_seconds=env_positive_int(
            "PRISM_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS",
            DEFAULT_POSTGRES_TCP_KEEPALIVES_INTERVAL_SECONDS,
        ),
        postgres_tcp_keepalives_count=env_positive_int(
            "PRISM_POSTGRES_TCP_KEEPALIVES_COUNT",
            DEFAULT_POSTGRES_TCP_KEEPALIVES_COUNT,
        ),
        read_concurrency=env_positive_int("PRISM_POSTGRES_READ_CONCURRENCY", 4),
        # Read by the ledger itself, and the second cache the miner page's
        # staleness budget accounts for; both processes construct one.
        reward_window_cache_seconds=env_nonnegative_float(
            "PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS",
            30.0,
        ),
        audit_artifact_store=build_audit_artifact_store(source),
        pool_application_name="prism-public-read",
    )


def build_audit_artifact_store(
    environ: dict[str, str] | None = None,
) -> AuditArtifactStore | None:
    """Open the audit artifact root read-only, for artifact and settlement reads.

    /public/v1/artifacts/{sha256} and the direct-coinbase settlement read both
    resolve externalized bodies from body_uri on disk, sha256-verified. The
    compose service mounts that volume :ro precisely so this process cannot
    write to it, which also means it cannot create the publication lock file a
    normal store opens O_RDWR at construction -- hence read_only.
    """
    source = os.environ if environ is None else environ
    audit_dir = (source.get("PRISM_AUDIT_DIR") or "").strip()
    if not audit_dir:
        return None
    root = Path(audit_dir)
    return AuditArtifactStore(
        AuditArtifactConfig(
            root=root,
            evidence_path=root / "prism-live-stratum-evidence.json",
        ),
        read_only=True,
    )


def build_rpc_from_env(environ: dict[str, str] | None = None) -> JsonRpc:
    """This service's own qbit RPC client, built the way the coordinator builds its own.

    Read-only calls only: public_api reaches RPC for getblockchaininfo,
    getblocktemplate and getnetworkinfo, all of which describe the node rather
    than change it.
    """
    del environ  # env() reads os.environ directly, as every other PRISM entry point does.
    return JsonRpc(
        host=env("QBIT_RPC_HOST"),
        port=env_int("QBIT_RPC_PORT", 18452),
        user=env("QBIT_RPC_USER"),
        password=env("QBIT_RPC_PASSWORD"),
    )


def _env_flag(raw: str | None) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def build_service(environ: dict[str, str] | None = None) -> tuple[
    PublicReadCoordinator,
    ReadinessProbe,
    ServiceMetrics,
]:
    """Validate configuration and assemble the process's collaborators.

    Every refusal happens here, before a socket is bound, so a misconfigured
    service fails to start rather than starting and serving wrong facts.
    """
    require_public_stratum_url(environ)
    ledger = build_ledger_from_env(environ)
    rpc = build_rpc_from_env(environ)
    coordinator = PublicReadCoordinator(ledger=ledger, rpc=rpc)
    readiness = ReadinessProbe(
        ledger.dashboard_readiness_probe,
        interval_seconds=env_positive_float(
            "PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS",
            DEFAULT_READINESS_PROBE_INTERVAL_SECONDS,
        ),
    )
    return coordinator, readiness, ServiceMetrics()


def main(argv: list[str] | None = None) -> int:
    del argv
    bind = os.environ.get("PRISM_PUBLIC_API_BIND") or DEFAULT_PUBLIC_API_BIND
    port = env_int("PRISM_PUBLIC_API_PORT", DEFAULT_PUBLIC_API_PORT)
    try:
        coordinator, readiness, metrics = build_service()
    except PublicReadConfigurationError as exc:
        print(f"prism-public-read: {exc}", file=sys.stderr, flush=True)
        return 2

    readiness.start()
    handler = make_handler(
        PublicReadService(coordinator, metrics=metrics, readiness=readiness)
    )
    server = ThreadingHTTPServer((bind, port), handler)
    server.daemon_threads = True

    def request_shutdown(signum: int, _frame: object) -> None:
        # shutdown() blocks until serve_forever() returns and must not be called
        # from the serving thread, so hand it to a helper thread.
        del signum
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    print(
        f"prism-public-read: serving /public/v1 on {bind}:{port}",
        file=sys.stderr,
        flush=True,
    )
    try:
        server.serve_forever()
    finally:
        readiness.stop()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
