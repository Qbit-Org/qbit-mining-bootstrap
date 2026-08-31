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

    The same contract is what a database outage is answered with (#164). While
    the readiness probe says Postgres is unreachable, a database-backed route
    serves its warm cache entry if it has one inside that budget -- labelled as
    degraded, so the client is told the difference between "30s old because
    that is the poll interval" and "30s old because this is the last answer
    that exists" -- and refuses with 503 otherwise. It never calls the database
    to refill and never extends the entry it serves, so the budget stays the
    real bound: ordinary traffic cannot keep a stale dashboard alive past it.

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
from lab.prism.share_ledger import LedgerOperationTimeout, PsqlShareLedger


DEFAULT_PUBLIC_API_BIND = "0.0.0.0"
DEFAULT_PUBLIC_API_PORT = 3342

# Deadline for one origin computation's database work. Sized above the ~15s
# nginx proxy timeout in front of this tier: a statement that outlives the
# client's patience by a little may still land a cache entry the retry can
# hit, but one that runs for minutes after the client hung up only holds a
# read slot against every other public read.
DEFAULT_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS = 20

# Emitted on every extracted response: the age this route is willing to serve.
# The observed age travels beside it in the standard Age header.
STALENESS_BUDGET_HEADER = "X-Prism-Staleness-Budget-Seconds"

# What that header says for /public/v1/artifacts/{sha256}, the one route with no
# budget. A number would be a lie in either direction; this names the contract.
UNBOUNDED_STALENESS_BUDGET = "unbounded"

# Said on every response served, or refused, while the readiness probe reports
# the database unreachable. Age and STALENESS_BUDGET_HEADER already carry the
# numbers; these two carry the fact, which the numbers alone cannot. Without
# them an ordinary 30-second-old poll response and the last answer that still
# exists are the same response.
#
# Warning 110 is the registered "Response is Stale" warn-code (RFC 7234 5.5.1,
# retained by clients that still parse the header), with this service named as
# the agent that added it. The X- header is what an operator greps and what a
# dashboard frontend switches an outage banner on, because it is a single value
# rather than a warn-code line to parse.
DEGRADED_WARNING_HEADER = "Warning"
DEGRADED_WARNING = '110 qbit-prism "database unavailable; serving cached response"'
DATABASE_STATE_HEADER = "X-Prism-Database-State"
DATABASE_STATE_UNAVAILABLE = "unavailable"
ARTIFACT_CANONICAL_STATE_HEADER = "X-Prism-Artifact-Canonical-State"

# How often the background prober re-checks Postgres, and the age at which its
# answer stops counting. Same shape as observability.py's health staleness:
# three refresh intervals, floored, so ordinary jitter is not an outage.
DEFAULT_READINESS_PROBE_INTERVAL_SECONDS = 5.0
MINIMUM_READINESS_STALE_SECONDS = 15.0

# How long the standby's replication stream may be silent before its answers
# stop counting as current. Bounds the *stream*, not replay lag -- see
# ReplicaProbe.
DEFAULT_REPLICA_MAX_LAG_SECONDS = 60.0

# Observed replay lag, published on every replica-backed response. Advisory:
# the enforced bound is the heartbeat age, for the reason ReplicaProbe explains.
REPLICA_LAG_HEADER = "X-Prism-Replica-Lag-Seconds"

# require: refuse replica-backed routes unless the backing server is a standby
# whose replication stream is live within the bound. off: serve whatever the
# DSN names, which is the behaviour of the extraction before a standby existed.
# off is the default so that merging this does not 503 a deployment that has
# not provisioned one yet; the shipped compose sets require.
REPLICA_MODE_REQUIRE = "require"
REPLICA_MODE_OFF = "off"
REPLICA_MODES = (REPLICA_MODE_REQUIRE, REPLICA_MODE_OFF)

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
        # "stale" is a served response: an expired entry inside its
        # stale-while-revalidate window, answered immediately while one
        # background refresh recomputes it.
        self._cache_by_state: dict[str, int] = {
            "hit": 0,
            "miss": 0,
            "bypass": 0,
            "stale": 0,
        }
        self._staleness_refusals = 0
        self._replica_refusals = 0
        self._degraded_responses = 0
        self._database_outage_refusals = 0

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

    def record_replica_refusal(self) -> None:
        with self._lock:
            self._replica_refusals += 1

    def record_degraded_response(self) -> None:
        with self._lock:
            self._degraded_responses += 1

    def record_database_outage_refusal(self) -> None:
        with self._lock:
            self._database_outage_refusals += 1

    def render(
        self,
        *,
        ready: bool,
        readiness_age_seconds: float,
        replica: dict[str, object] | None = None,
    ) -> str:
        with self._lock:
            requests_total = self._requests_total
            responses = dict(self._responses_by_status)
            cache_states = dict(self._cache_by_state)
            refusals = self._staleness_refusals
            replica_refusals = self._replica_refusals
            degraded_responses = self._degraded_responses
            database_outage_refusals = self._database_outage_refusals
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
                "# HELP qbit_prism_public_replica_refusals_total Responses refused for failing the replica freshness contract.",
                "# TYPE qbit_prism_public_replica_refusals_total counter",
                f"qbit_prism_public_replica_refusals_total {replica_refusals}",
                "# HELP qbit_prism_public_degraded_responses_total Cached responses served while the database was unreachable.",
                "# TYPE qbit_prism_public_degraded_responses_total counter",
                f"qbit_prism_public_degraded_responses_total {degraded_responses}",
                "# HELP qbit_prism_public_database_outage_refusals_total Responses refused because the database was unreachable and no warm entry existed.",
                "# TYPE qbit_prism_public_database_outage_refusals_total counter",
                f"qbit_prism_public_database_outage_refusals_total {database_outage_refusals}",
            ]
        )
        if replica is not None:
            # -1 rather than omission for the unknowns: a missing series and a
            # series reading zero are the same alert, and neither is true.
            lines.extend(
                [
                    "# HELP qbit_prism_public_replica_in_recovery Whether the backing server is a standby.",
                    "# TYPE qbit_prism_public_replica_in_recovery gauge",
                    f"qbit_prism_public_replica_in_recovery {1 if replica.get('in_recovery') else 0}",
                    "# HELP qbit_prism_public_replica_heartbeat_age_seconds Age of the newest walreceiver message, or -1 when disconnected. This is the enforced bound.",
                    "# TYPE qbit_prism_public_replica_heartbeat_age_seconds gauge",
                    f"qbit_prism_public_replica_heartbeat_age_seconds {_metric_number(replica.get('receiver_heartbeat_age_seconds'))}",
                    "# HELP qbit_prism_public_replica_replay_lag_seconds Wall clock behind the newest replayed commit, or -1. Advisory: grows on an idle primary.",
                    "# TYPE qbit_prism_public_replica_replay_lag_seconds gauge",
                    f"qbit_prism_public_replica_replay_lag_seconds {_metric_number(replica.get('replay_lag_seconds'))}",
                    "# HELP qbit_prism_public_replica_apply_backlog_bytes WAL received but not yet replayed, or -1.",
                    "# TYPE qbit_prism_public_replica_apply_backlog_bytes gauge",
                    f"qbit_prism_public_replica_apply_backlog_bytes {_metric_number(replica.get('apply_backlog_bytes'))}",
                    "# HELP qbit_prism_public_replica_max_lag_seconds The configured replication-stream silence bound.",
                    "# TYPE qbit_prism_public_replica_max_lag_seconds gauge",
                    f"qbit_prism_public_replica_max_lag_seconds {_metric_number(replica.get('max_lag_seconds'))}",
                ]
            )
        return "\n".join(lines) + "\n"


class ReplicaProbe:
    """Is the standby this tier reads through still a standby, and still fed?

    Two refusals and one number, from one query per interval:

    * **Not in recovery.** The public tier exists to keep public read volume
      off the primary the coordinator lands blocks through. A DSN that
      resolves to a writable server silently undoes that, and the failure is
      invisible in the responses -- they are correct, just served from the
      wrong place. So it is refused rather than served.
    * **A silent replication stream.** The liveness proof is the walreceiver
      heartbeat age, *not* the transactional replay lag. Heartbeats flow every
      ``wal_receiver_status_interval`` (10s by default) whether or not the
      primary has anything to send, whereas replay lag grows with wall clock
      on an idle primary -- so bounding replay lag would 503 a perfectly
      healthy replica of a quiet pool every time the pool went quiet, which is
      exactly when the dashboard is least likely to be wrong.

    Replay lag and apply backlog are still measured and published (header,
    /healthz, /metrics); they are the number an operator wants, just not the
    number a gate can be built on.

    Freshness ages locally, which is what makes this fail *closed*. The bound
    is checked against the heartbeat age the last successful probe reported
    **plus how long ago that probe ran**, so a probe that starts failing --
    or a standby that stops answering entirely -- ages its own last good
    answer out of the bound instead of pinning it there forever.

    Runs as ReadinessProbe's probe callable rather than owning a thread:
    readiness and replica freshness are the same question, and one probe means
    one round trip per interval instead of two.
    """

    def __init__(
        self,
        status_probe: Callable[[], dict[str, object]],
        *,
        max_lag_seconds: float = DEFAULT_REPLICA_MAX_LAG_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._status_probe = status_probe
        self._max_lag_seconds = float(max_lag_seconds)
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._status: dict[str, object] | None = None
        self._probed_at: float | None = None
        self._last_error: str | None = None

    @property
    def max_lag_seconds(self) -> float:
        return self._max_lag_seconds

    def check(self) -> bool:
        """One probe. True when the backing server is a live standby.

        Failure is recorded and re-raised so ReadinessProbe reports the real
        error text rather than a bare "returned false"; the recorded snapshot
        is deliberately left in place, to age out through freshness_error()
        rather than vanish and read as warm-up.
        """
        try:
            status = self._status_probe()
        except Exception as exc:  # noqa: BLE001 - recorded, then re-raised
            with self._lock:
                self._last_error = f"{type(exc).__name__}: {exc}"
            raise
        with self._lock:
            self._status = dict(status)
            self._probed_at = self._monotonic()
            self._last_error = None
        return bool(status.get("in_recovery"))

    def view(self) -> dict[str, object]:
        """What /healthz and /metrics report, including the aged numbers."""

        with self._lock:
            status = dict(self._status) if self._status is not None else None
            probed_at = self._probed_at
            last_error = self._last_error
        payload: dict[str, object] = {
            "probed": status is not None,
            "max_lag_seconds": self._max_lag_seconds,
        }
        if last_error is not None:
            payload["last_error"] = last_error
        if status is None or probed_at is None:
            return payload
        snapshot_age = max(0.0, self._monotonic() - probed_at)
        payload["probe_age_seconds"] = round(snapshot_age, 3)
        payload["in_recovery"] = bool(status.get("in_recovery"))
        heartbeat = _coerce_float(status.get("receiver_heartbeat_age_seconds"))
        payload["receiver_heartbeat_age_seconds"] = (
            None if heartbeat is None else round(heartbeat + snapshot_age, 3)
        )
        replay_lag = _coerce_float(status.get("replay_lag_seconds"))
        payload["replay_lag_seconds"] = (
            None if replay_lag is None else round(replay_lag + snapshot_age, 3)
        )
        backlog = _coerce_float(status.get("apply_backlog_bytes"))
        payload["apply_backlog_bytes"] = None if backlog is None else int(backlog)
        return payload

    def replay_lag_seconds(self) -> float | None:
        """Observed replay lag for the response header, aged locally."""

        value = self.view().get("replay_lag_seconds")
        return None if value is None else float(value)

    def freshness_error(self) -> tuple[str, str] | None:
        """(code, message) for the 503 a replica-backed route owes, or None."""

        with self._lock:
            status = dict(self._status) if self._status is not None else None
            probed_at = self._probed_at
            last_error = self._last_error
        if status is None or probed_at is None:
            message = "public read service is warming up"
            if last_error is not None:
                message = f"{message}: replica probe failed: {last_error}"
            return "replica_unavailable", message
        if not status.get("in_recovery"):
            return (
                "replica_unavailable",
                "public read service refuses to serve from a server that is "
                "not in recovery; point PRISM_PUBLIC_DATABASE_URL at the read "
                "replica, or set PRISM_PUBLIC_REPLICA_MODE=off to serve from "
                "the primary deliberately",
            )
        heartbeat = _coerce_float(status.get("receiver_heartbeat_age_seconds"))
        if heartbeat is None:
            return (
                "replica_unavailable",
                "read replica replication stream is not connected; the "
                "walreceiver heartbeat is absent",
            )
        silent_for = heartbeat + max(0.0, self._monotonic() - probed_at)
        if silent_for > self._max_lag_seconds:
            detail = ""
            if last_error is not None:
                detail = f" (last probe failed: {last_error})"
            return (
                "replica_unavailable",
                f"read replica replication stream has been silent for "
                f"{silent_for:.1f}s, beyond the configured bound "
                f"{self._max_lag_seconds:.1f}s{detail}",
            )
        return None


def _coerce_float(value: object) -> float | None:
    """Postgres json numbers arrive as int/float/str depending on the driver."""

    if value is None:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


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


def path_reads_database(path: str) -> bool:
    """Does this route's answer come out of the database?

    One predicate, two gates. The replica contract asks it to decide which
    routes a silent standby may not answer; the outage gate asks it to decide
    which routes a dead database may not answer. The exempt set is the same
    both times, and for the same reasons, so it is derived once here rather
    than stated twice.

    Derived from the endpoint registry rather than a hand-kept path list, so a
    route added there is classified once. Two kinds of route are exempt:

    * those that touch no read slot at all -- today only
      /public/v1/mining-configuration, assembled from environment. It has no
      database answer to lose;
    * content-addressed ones, which declare ``immutable_content``. A body that
      hashes to the requested sha256 is correct at any age, which is why the
      registry gives them no staleness budget either; refusing one for
      replication lag, or for an outage, would contradict the unbounded budget
      already published on it.

    An unclassified path is not database-backed here because dispatch() 404s it
    before it can read anything.
    """
    endpoint = endpoint_registry.endpoint_for_request_path(path)
    if endpoint is None or endpoint.immutable_content:
        return False
    return endpoint_registry.LedgerAccess.READ_SLOT in endpoint.access


def public_read_statement_timeout_seconds() -> int:
    """The per-request database deadline, in seconds. 0 disables the bound.

    Read per request rather than captured at startup, so the knob behaves
    like the cache TTL knobs beside it and a background cache refresh sees
    the same value a foreground request would.
    """
    return public_api.env_nonnegative_int(
        "PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS",
        DEFAULT_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS,
    )


def bounded_public_dispatch(
    coordinator: Any,
    path: str,
    query: dict[str, list[str]],
    *,
    reads_database: bool,
) -> tuple[int, object]:
    """One origin computation, deadline-bounded when it reads the database.

    Without a bound, a request the fronting proxy has already abandoned at
    its ~15s timeout keeps its Postgres statement running to completion,
    holding one of the bounded read slots (PRISM_POSTGRES_READ_CONCURRENCY)
    against every other public read and competing with the share writer's
    I/O for as long as the scan takes. The ledger's ``statement_timeout``
    scope arms a server-side ``SET LOCAL statement_timeout`` per statement
    plus a matching local admission bound, so the whole request stops costing
    anything shortly after its caller stops listening.

    Duck-typed exactly like the coordinator's block-submitter scope: a ledger
    without the scope runs unbounded, as it always has. On the ledger's
    psql-subprocess fallback the server-side statement deadline is not armed;
    admission waits and the subprocess itself still observe the bound, and
    no deadline at all is an acceptable degradation there. Only
    database-backed routes are wrapped -- the same classification the
    replica and outage gates use -- because a route that touches no read
    slot has nothing this deadline could bound.

    A ledger-reported deadline becomes the public ``read_timeout`` 503. Its
    error response is no-store and never cached, so one expensive miss
    cannot poison the response cache with a refusal.
    """
    if not reads_database:
        return public_api.dispatch(coordinator, path, query)
    timeout_seconds = public_read_statement_timeout_seconds()
    statement_timeout = getattr(coordinator.ledger, "statement_timeout", None)
    if timeout_seconds <= 0 or not callable(statement_timeout):
        return public_api.dispatch(coordinator, path, query)
    try:
        with statement_timeout(float(timeout_seconds)):
            return public_api.dispatch(coordinator, path, query)
    except LedgerOperationTimeout as exc:
        raise public_api.PublicApiError(
            503,
            "read_timeout",
            "the read timed out; try again shortly",
        ) from exc


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


# The refusal body, built once so the handler can recognise it by identity.
#
# Reported under ``stale_read_model`` because ``public_api`` owns the public
# error enum and that is the entry whose meaning this is: no answer for this
# route exists inside the age it publishes. It is the same documented wire code
# (``upstream_unavailable``) the replica refusal already resolves to -- from a
# client's side the conditions are one condition -- and inventing an
# undocumented code instead would silently ship a 503 that says "internal
# error", because error_payload() rewrites codes it does not know.
DATABASE_UNAVAILABLE_PAYLOAD = public_api.error_payload(
    "stale_read_model",
    "public read service cannot reach the database and has no cached response "
    "for this request inside its staleness budget",
)


def database_unavailable_response() -> tuple[int, dict[str, object]]:
    """The 503 a database-backed route owes while the database is unreachable.

    Shaped as a ``compute`` for ``PublicResponseCache.get_or_compute`` because
    that is how the warm/cold distinction gets made without a second cache API:
    the cache answers a warm key from its entry and never calls this, and on a
    cold, bypassed or expired key it calls this and stores nothing, because the
    cache only stores 2xx. So the same call expresses "serve what you have" and
    "refuse, and do not go and ask".
    """

    return 503, DATABASE_UNAVAILABLE_PAYLOAD


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
        replica: ReplicaProbe | None = None,
    ) -> None:
        self.coordinator = coordinator
        self.cache = (
            public_api.PublicResponseCache() if response_cache is None else response_cache
        )
        self.metrics = ServiceMetrics() if metrics is None else metrics
        self.readiness = readiness
        # None when PRISM_PUBLIC_REPLICA_MODE=off: no replica contract is
        # enforced and no replica facts are published, because there are none.
        self.replica = replica


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
    replica = service.replica

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
            reads_database = path_reads_database(path)
            # Before the cache, not after. A replica outage does not make
            # cached entries expire, so a post-cache check would keep answering
            # 200 from the last good snapshot for as long as traffic kept the
            # entry warm -- the precise failure this gate exists to prevent.
            if replica is not None and reads_database:
                refusal = replica.freshness_error()
                if refusal is not None:
                    code, message = refusal
                    service_metrics.record_replica_refusal()
                    headers = self.with_replica_lag(
                        public_api.public_error_headers()
                    )
                    self.write_json(
                        503,
                        public_api.error_payload(code, message),
                        headers=self.with_budget(path, headers),
                    )
                    return
            # Also before the cache, and before any origin computation: while
            # readiness says the database is unreachable, a database-backed
            # route may still answer from what it already has, but it may not
            # go and ask. Handing the cache a non-cacheable 503 instead of
            # dispatch() is exactly that instruction -- a warm key returns its
            # entry and never reaches this, a cold, bypassed or expired one
            # gets the refusal and stores nothing.
            degraded = (
                readiness is not None
                and reads_database
                and readiness.snapshot()[0] != 200
            )
            cache_policy = public_api.public_cache_policy(path)
            budget_seconds = staleness_budget_for_path(path)
            # The stale-while-revalidate window the policy advertises to CDNs
            # also lets the in-process cache serve an expired entry while one
            # background refresh recomputes it -- but the budget check below
            # is unconditional, so the window is clamped to what the route's
            # staleness budget leaves after the TTL. Without the clamp, a
            # route whose ttl+swr exceeds its budget (the 5s routes: 5+30
            # against a 15s budget) would turn ages the blocking path used to
            # recompute into 503 refusals. While the database is unreachable
            # the window is withdrawn entirely: the outage contract serves
            # only unexpired entries, and its refusal compute must not be
            # deferred to a background thread nobody answers.
            swr_seconds = cache_policy.stale_while_revalidate_seconds
            if budget_seconds is not None:
                swr_seconds = max(
                    0, min(swr_seconds, int(budget_seconds) - cache_policy.ttl_seconds)
                )
            if degraded:
                swr_seconds = 0
            status, payload, cache_state, age_seconds = cache.get_or_compute(
                key=public_api.public_cache_key(path, query),
                ttl_seconds=cache_policy.ttl_seconds,
                stale_while_revalidate_seconds=swr_seconds,
                compute=(
                    database_unavailable_response
                    if degraded
                    else lambda: bounded_public_dispatch(
                        coordinator, path, query, reads_database=reads_database
                    )
                ),
            )
            service_metrics.record_cache_state(cache_state)
            if payload is DATABASE_UNAVAILABLE_PAYLOAD:
                # Nothing warm to serve. Recognised by identity rather than
                # from this request's own `degraded`, because get_or_compute
                # coalesces: a request that arrived while another was computing
                # gets that one's result whatever its own view of readiness
                # was. Keying on the request would ship this 503 with ordinary
                # shared-cache headers to a waiter whose probe had recovered in
                # between, and a CDN would then hold the refusal.
                service_metrics.record_database_outage_refusal()
                headers = self.with_replica_lag(public_api.public_error_headers())
                headers[DATABASE_STATE_HEADER] = DATABASE_STATE_UNAVAILABLE
                self.write_json(
                    status, payload, headers=self.with_budget(path, headers)
                )
                return
            if isinstance(payload, public_api.UncacheableJsonBody):
                # Legacy audit rows can predate canonical artifact storage.
                # Their reconstructed JSON remains available for continuity,
                # but it must never be cached under an immutable content URL.
                headers = self.with_replica_lag(public_api.public_error_headers())
                headers[ARTIFACT_CANONICAL_STATE_HEADER] = payload.reason
                self.write_json(
                    status,
                    payload.payload,
                    headers=self.with_budget(path, headers),
                )
                return
            if budget_seconds is not None and age_seconds > budget_seconds:
                # Refuse rather than serve past the budget. A silently stale
                # dashboard is worse than an honest one.
                service_metrics.record_staleness_refusal()
                headers = public_api.public_error_headers()
                headers[STALENESS_BUDGET_HEADER] = _format_budget(budget_seconds)
                headers["Age"] = str(max(0, age_seconds))
                if degraded:
                    # Stated, but deliberately not counted as an outage
                    # refusal: the budget check is unconditional, so this entry
                    # would have been refused with a healthy database too. The
                    # outage is why no fresher one replaced it, which is worth
                    # saying and is not the same claim.
                    headers[DATABASE_STATE_HEADER] = DATABASE_STATE_UNAVAILABLE
                self.write_json(
                    503,
                    staleness_error_payload(
                        budget_seconds=budget_seconds,
                        age_seconds=age_seconds,
                    ),
                    headers=self.with_replica_lag(headers),
                )
                return
            headers = public_api.public_cache_headers(
                cache_policy,
                cache_state=cache_state,
                age_seconds=age_seconds,
            )
            headers = self.with_replica_lag(headers)
            if degraded and cache_state == "HIT":
                # A cached body, inside the budget the same response publishes,
                # served with the outage stated rather than implied. The
                # shared-cache headers are deliberately left as they were: the
                # CDN TTL is the route's own, which the registry derives at a
                # third of the budget or less, so a downstream cache cannot
                # carry this body past the bound stated on it.
                #
                # Conditioned on the hit, not on `degraded` alone: a coalescing
                # waiter can receive an origin result computed by a request
                # that still saw a healthy database, and labelling that fresh
                # body "serving cached response" would be untrue.
                service_metrics.record_degraded_response()
                headers[DEGRADED_WARNING_HEADER] = DEGRADED_WARNING
                headers[DATABASE_STATE_HEADER] = DATABASE_STATE_UNAVAILABLE
            self.write_json(status, payload, headers=self.with_budget(path, headers))

        def with_replica_lag(self, headers: dict[str, str]) -> dict[str, str]:
            """Publish observed replay lag beside the enforced heartbeat bound.

            Advisory, and deliberately so: this is the number that answers "how
            far behind is the data I just got", which the enforced bound does
            not. Omitted rather than guessed when nothing has replayed yet.
            """

            if replica is None:
                return headers
            lag_seconds = replica.replay_lag_seconds()
            if lag_seconds is not None:
                headers[REPLICA_LAG_HEADER] = f"{lag_seconds:.3f}"
            return headers

        def with_budget(self, path: str, headers: dict[str, str]) -> dict[str, str]:
            """Attach the route's budget so callers can see it on every response."""

            budget = staleness_budget_header_value(path)
            if budget is not None:
                headers[STALENESS_BUDGET_HEADER] = budget
            return headers

        def handle_healthz(self) -> None:
            if readiness is None:
                payload: dict[str, object] = {"ok": True, "schema": HEALTH_SCHEMA}
                status = 200
            else:
                status, payload = readiness.snapshot()
            if replica is not None:
                # The replication facts are why a 503 here says what it says,
                # so they travel with it rather than only in /metrics.
                payload["replica"] = replica.view()
                refusal = replica.freshness_error()
                if refusal is not None:
                    payload["ok"] = False
                    payload["state"] = "unready"
                    # Overwrite rather than defer to the readiness probe's
                    # text: in require mode the replica check *is* the
                    # readiness probe, so a failed contract shows up there as
                    # the useless "readiness probe returned false". The
                    # probe's own exception, when there was one, is in the
                    # replica block beside this.
                    payload["error"] = refusal[1]
                    status = 503
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
                replica=None if replica is None else replica.view(),
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
            if isinstance(payload, public_api.RawJsonBody):
                # Content-addressed bytes (#154): any re-serialization (key
                # order, separators, trailing newline) would break
                # sha256(response body) == the advertised artifact hash.
                # /public/v1/artifacts/{sha256} moved here, so this branch
                # moved with it.
                body = payload.body
            else:
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


def _metric_number(value: object) -> str:
    """Render a possibly-absent replica number as a gauge value.

    -1 stands in for "not known", matching how the readiness probe age already
    reports its own pre-first-probe state.
    """

    number = _coerce_float(value)
    return "-1" if number is None else f"{number:.3f}"


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

    /public/v1/artifacts/{sha256} reads immutable compressed canonical bundles
    first. A clean miss can use a labelled, non-cacheable legacy body; damage
    fails closed. The compose service mounts that volume :ro precisely so this
    process cannot write to it, which also means it cannot create the publication
    lock file a normal store opens O_RDWR at construction -- hence read_only.
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


def resolve_replica_mode(environ: dict[str, str] | None = None) -> str:
    """Read PRISM_PUBLIC_REPLICA_MODE, refusing anything it does not mean.

    An unrecognised value is a configuration error, not a reason to pick a
    default: the two modes differ in whether this process will serve from the
    coordinator's primary, so guessing gets that wrong silently in one
    direction or the other.
    """
    source = os.environ if environ is None else environ
    raw = (source.get("PRISM_PUBLIC_REPLICA_MODE") or "").strip().lower()
    if not raw:
        return REPLICA_MODE_OFF
    if raw not in REPLICA_MODES:
        raise PublicReadConfigurationError(
            "PRISM_PUBLIC_REPLICA_MODE must be one of "
            f"{', '.join(REPLICA_MODES)} (got {raw!r})"
        )
    return raw


def build_replica_probe(
    ledger: Any,
    environ: dict[str, str] | None = None,
) -> ReplicaProbe | None:
    """The replica contract, or None when this deployment has no standby yet."""

    if resolve_replica_mode(environ) != REPLICA_MODE_REQUIRE:
        return None
    return ReplicaProbe(
        ledger.read_replica_status,
        max_lag_seconds=env_positive_float(
            "PRISM_PUBLIC_REPLICA_MAX_LAG_SECONDS",
            DEFAULT_REPLICA_MAX_LAG_SECONDS,
        ),
    )


def build_service(environ: dict[str, str] | None = None) -> tuple[
    PublicReadCoordinator,
    ReadinessProbe,
    ServiceMetrics,
    ReplicaProbe | None,
]:
    """Validate configuration and assemble the process's collaborators.

    Every refusal happens here, before a socket is bound, so a misconfigured
    service fails to start rather than starting and serving wrong facts.
    """
    require_public_stratum_url(environ)
    ledger = build_ledger_from_env(environ)
    rpc = build_rpc_from_env(environ)
    coordinator = PublicReadCoordinator(ledger=ledger, rpc=rpc)
    replica = build_replica_probe(ledger, environ)
    readiness = ReadinessProbe(
        # In require mode the replication probe *is* the readiness probe: it
        # round-trips to the same database and additionally proves the server
        # is the standby this tier is supposed to be reading. Running both
        # would be two round trips to answer one question.
        ledger.dashboard_readiness_probe if replica is None else replica.check,
        interval_seconds=env_positive_float(
            "PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS",
            DEFAULT_READINESS_PROBE_INTERVAL_SECONDS,
        ),
    )
    return coordinator, readiness, ServiceMetrics(), replica


def main(argv: list[str] | None = None) -> int:
    del argv
    bind = os.environ.get("PRISM_PUBLIC_API_BIND") or DEFAULT_PUBLIC_API_BIND
    port = env_int("PRISM_PUBLIC_API_PORT", DEFAULT_PUBLIC_API_PORT)
    try:
        coordinator, readiness, metrics, replica = build_service()
    except PublicReadConfigurationError as exc:
        print(f"prism-public-read: {exc}", file=sys.stderr, flush=True)
        return 2

    readiness.start()
    handler = make_handler(
        PublicReadService(
            coordinator,
            metrics=metrics,
            readiness=readiness,
            replica=replica,
        )
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

    backing = (
        "any server the DSN names (replica mode off)"
        if replica is None
        else (
            "a standby with a replication stream no more than "
            f"{replica.max_lag_seconds:g}s silent"
        )
    )
    print(
        f"prism-public-read: serving /public/v1 on {bind}:{port}, reading from "
        f"{backing}",
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
