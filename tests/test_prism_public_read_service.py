#!/usr/bin/env python3
"""Pin the extracted PRISM public read tier against the surface it replaced.

The extraction step of issue #145 moved exactly the 13 original ``/public/v1``
routes out of the coordinator's audit/ops listener into
``lab/prism/public_read_service.py``; ``/public/v1/block-markers`` joined the
extracted surface later, bringing it to 14 routes. The issue's read-replica back-end is a
separate follow-up -- this service still reads the primary Postgres, through
bounded read slots rather than the writer path. An extraction is
only worth doing if it changes nothing a client can observe and everything the
coordinator can feel, so these tests assert both halves:

* **Contract equality** -- every extracted route's status, body and cache
  headers still come from ``public_api`` unchanged. ``ContractEqualityTests``
  drives the new handler and ``public_api.dispatch`` against the *same*
  coordinator and compares. The concrete pre-extraction values are pinned
  separately and unchanged by ``tests/test_prism_public_dashboard_api.py``,
  whose 70 assertions were written against the coordinator's listener and now
  run against this handler.
* **The point of the move** -- ``NoWriterLockReadTests`` drives all 14 routes
  against a ledger that raises if any writer-lock read is reached. Moving the
  GIL contention while leaving the lock contention would have been motion
  without progress.
* **The staleness contract** -- budgets published on every response, 503 rather
  than a silently stale body, and no staleness refusal for immutable
  content-addressed artifacts.
* **Fail-closed startup** and **coordinator subtraction**.
"""

from __future__ import annotations

import json
import os
import threading
import time
from contextlib import contextmanager
from threading import BoundedSemaphore, Lock
import unittest
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

from lab.prism import endpoint_registry, public_api, public_read_service
from lab.prism.audit_http import AuditHttpConfig, AuditHttpFacade
from lab.prism.share_ledger import PsqlShareLedger, ReadOnlyLedgerError

# The public fixtures these routes are exercised against already exist, written
# against the coordinator's listener before the extraction. Reusing them rather
# than restating them is deliberate: a second copy could drift, and then the
# contract test would be comparing the new service to a fixture that no longer
# describes the old behaviour.
from tests.test_prism_public_dashboard_api import (
    DirectCoinbasePublicLedger,
    FakeCoordinator,
    FakePublicLedger,
)


ROOT = Path(__file__).resolve().parents[1]
AUDIT_HTTP_PATH = ROOT / "lab" / "prism" / "audit_http.py"

FROZEN_NOW = "2026-01-01T00:00:00Z"

RECIPIENT_ID = "miner-a"
BLOCK_HASH = FakePublicLedger.block_hash
FANOUT_TXID = FakePublicLedger.fanout_txid
ARTIFACT_SHA256 = FakePublicLedger.audit_bundle_sha256

# One concrete request per extracted route. EXTRACTED_ROUTE_COUNT is asserted
# against the registry so a new public route cannot be added without landing
# here too.
EXTRACTED_ROUTES: tuple[tuple[str, str], ...] = (
    ("/public/v1/pool-summary", ""),
    ("/public/v1/blocks", ""),
    ("/public/v1/leaderboard", ""),
    ("/public/v1/hashrate-series", ""),
    ("/public/v1/block-markers", ""),
    ("/public/v1/mining-configuration", ""),
    (f"/public/v1/miners/{RECIPIENT_ID}", ""),
    (f"/public/v1/miners/{RECIPIENT_ID}/earnings", ""),
    (f"/public/v1/miners/{RECIPIENT_ID}/payouts", ""),
    (f"/public/v1/miners/{RECIPIENT_ID}/workers", ""),
    (f"/public/v1/blocks/{BLOCK_HASH}/settlement-artifacts", ""),
    ("/public/v1/fanouts/pending", ""),
    (f"/public/v1/fanouts/{FANOUT_TXID}", ""),
    (f"/public/v1/artifacts/{ARTIFACT_SHA256}", ""),
)

EXTRACTED_ROUTE_COUNT = 14

CACHE_HEADERS = ("Cache-Control", "CDN-Cache-Control", "Vercel-CDN-Cache-Control", "Age")

WRITER_LOCK_LEDGER_METHODS = (
    "current_owed_balances",
    "recipient_payout_history",
    "audit_bundle",
    "audit_bundle_by_commitment",
    "audit_block_payouts",
    "audit_share_window",
    "carry_forward_integrity_report",
    # public_api.worker_rows falls back to this full-table read when
    # dashboard_miner_worker_rows is absent; on Postgres it is a writer-lock
    # read like the rest, so the public tier must never reach it either.
    "all_shares",
)


class WriterLockReached(AssertionError):
    """Raised when a public route reaches a read that fences on the writer."""


class ServedResponse:
    """One HTTP response, reduced to what the contract actually covers."""

    def __init__(self, status: int, body: bytes, headers: dict[str, str]) -> None:
        self.status = status
        self.body = body
        self.headers = headers

    @property
    def payload(self) -> object:
        return json.loads(self.body)

    def cache_headers(self) -> dict[str, str | None]:
        return {name: self.headers.get(name) for name in CACHE_HEADERS}


class ServiceHarness:
    """A bound public read service over one coordinator, for the test's lifetime."""

    def __init__(
        self,
        coordinator: object,
        *,
        response_cache: object | None = None,
        readiness: object | None = None,
        metrics: object | None = None,
        replica: object | None = None,
    ) -> None:
        self.service = public_read_service.PublicReadService(
            coordinator,
            response_cache=response_cache,  # type: ignore[arg-type]
            metrics=metrics,  # type: ignore[arg-type]
            readiness=readiness,  # type: ignore[arg-type]
            replica=replica,  # type: ignore[arg-type]
        )
        handler = public_read_service.make_handler(self.service)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def get(self, path_and_query: str) -> ServedResponse:
        url = self.base_url + path_and_query
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                return ServedResponse(
                    response.status,
                    response.read(),
                    dict(response.headers.items()),
                )
        except urllib.error.HTTPError as error:
            with error:
                return ServedResponse(
                    error.code,
                    error.read(),
                    dict(error.headers.items()),
                )

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def request_path(route: tuple[str, str]) -> str:
    path, query = route
    return f"{path}?{query}" if query else path


class RegistryAgreementTests(unittest.TestCase):
    """The routes exercised here must be exactly the routes the registry extracts."""

    def test_every_extracted_registry_path_is_covered(self) -> None:
        covered = set()
        for path, _query in EXTRACTED_ROUTES:
            endpoint = endpoint_registry.endpoint_for_request_path(path)
            self.assertIsNotNone(endpoint, f"{path} resolves to no registry endpoint")
            assert endpoint is not None
            covered.add(endpoint.primary_path)
        self.assertEqual(set(endpoint_registry.extracted_paths()), covered)

    def test_the_extracted_surface_is_still_fourteen_routes(self) -> None:
        self.assertEqual(
            EXTRACTED_ROUTE_COUNT,
            len(endpoint_registry.extracted_paths()),
        )
        self.assertEqual(EXTRACTED_ROUTE_COUNT, len(EXTRACTED_ROUTES))


class ContractEqualityTests(unittest.TestCase):
    """Same status, same body, same cache headers as dispatch() produces.

    Time is frozen for both sides so ``generated_at`` cannot make two
    structurally identical payloads compare unequal.
    """

    def setUp(self) -> None:
        self.coordinator = FakeCoordinator(ledger=FullReadModelLedger())
        self.harness = ServiceHarness(self.coordinator)
        self.addCleanup(self.harness.close)
        patcher = patch.object(public_api, "utc_now_iso", return_value=FROZEN_NOW)
        patcher.start()
        self.addCleanup(patcher.stop)

    def expected_success(self, path: str, query: str) -> tuple[int, object, dict[str, str]]:
        parsed_query = urllib.parse.parse_qs(query)
        status, payload = public_api.dispatch(self.coordinator, path, parsed_query)
        policy = public_api.public_cache_policy(path)
        # A fresh service cache serves the first request as a MISS at age 0,
        # which is exactly what the coordinator's handle_public did.
        headers = public_api.public_cache_headers(
            policy,
            cache_state="MISS",
            age_seconds=0,
        )
        return status, payload, headers

    def test_every_extracted_route_matches_dispatch(self) -> None:
        for route in EXTRACTED_ROUTES:
            path, query = route
            with self.subTest(path=path):
                served = self.harness.get(request_path(route))
                status, payload, headers = self.expected_success(path, query)

                self.assertEqual(status, served.status)
                self.assertEqual(payload, served.payload)
                # Byte-for-byte, not just structurally: the framing (sorted
                # keys, trailing newline) is part of what clients receive.
                self.assertEqual(
                    json.dumps(payload, sort_keys=True).encode() + b"\n",
                    served.body,
                )
                for name in CACHE_HEADERS:
                    self.assertEqual(
                        headers.get(name),
                        served.headers.get(name),
                        f"{path}: {name} changed across the extraction",
                    )

    def test_mining_configuration_highdiff_endpoint_survives_the_extraction(self) -> None:
        # This process runs no Stratum listener, so the High-diff endpoint in
        # the rendered body comes only from PRISM_STRATUM_HIGHDIFF_PORT in this
        # process's own environment (compose passes it through; see
        # tests/test_prism_compose_profile.py). With the env set the served
        # body must still match dispatch and must carry the High-diff endpoint.
        route = ("/public/v1/mining-configuration", "")
        with patch.dict(
            os.environ,
            {
                "PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://public-pool.example:3335",
                "PRISM_STRATUM_HIGHDIFF_PORT": "4334",
            },
            clear=True,
        ):
            served = self.harness.get(request_path(route))
            status, payload, headers = self.expected_success(*route)

        self.assertEqual(status, served.status)
        self.assertEqual(payload, served.payload)
        for name in CACHE_HEADERS:
            self.assertEqual(headers.get(name), served.headers.get(name))

        endpoints = served.payload["configurations"][0]["stratum_endpoints"]
        self.assertEqual(
            ["Primary", "High-diff"],
            [endpoint["label"] for endpoint in endpoints],
        )
        self.assertEqual(
            "stratum+tcp://public-pool.example:4334",
            endpoints[1]["url"],
        )
        self.assertEqual(4334, endpoints[1]["default_port"])

    def test_content_type_is_unchanged(self) -> None:
        for route in EXTRACTED_ROUTES:
            with self.subTest(path=route[0]):
                served = self.harness.get(request_path(route))
                self.assertEqual("application/json", served.headers.get("Content-Type"))

    def test_not_found_error_matches_dispatch(self) -> None:
        # A block with no settlement rows: dispatch raises PublicApiError(404).
        unknown_block = "f" * 64
        path = f"/public/v1/blocks/{unknown_block}/settlement-artifacts"
        with self.assertRaises(public_api.PublicApiError) as raised:
            public_api.dispatch(self.coordinator, path, {})
        expected = raised.exception

        served = self.harness.get(path)

        self.assertEqual(404, expected.status)
        self.assertEqual(expected.status, served.status)
        self.assertEqual(
            public_api.error_payload(expected.code, expected.message),
            served.payload,
        )
        self.assertEqual("no-store", served.headers.get("Cache-Control"))
        # Errors are not cached, so they carry no shared-cache or Age headers.
        self.assertIsNone(served.headers.get("CDN-Cache-Control"))
        self.assertIsNone(served.headers.get("Age"))

    def test_public_api_error_matches_dispatch(self) -> None:
        # An explicit PublicApiError raised by dispatch's own validation.
        path, query = "/public/v1/leaderboard", "window=bogus"
        with self.assertRaises(public_api.PublicApiError) as raised:
            public_api.dispatch(self.coordinator, path, urllib.parse.parse_qs(query))
        expected = raised.exception

        served = self.harness.get(f"{path}?{query}")

        self.assertEqual(400, expected.status)
        self.assertEqual("invalid_window", expected.code)
        self.assertEqual(expected.status, served.status)
        self.assertEqual(
            public_api.error_payload(expected.code, expected.message),
            served.payload,
        )
        self.assertEqual("prism.dashboard.error.v1", served.payload["schema"])
        self.assertEqual("no-store", served.headers.get("Cache-Control"))

    def test_reward_filter_conflict_matches_dispatch(self) -> None:
        path, query = "/public/v1/leaderboard", "window=reward&search=a&recipient_id=miner-a"
        with self.assertRaises(public_api.PublicApiError) as raised:
            public_api.dispatch(self.coordinator, path, urllib.parse.parse_qs(query))

        served = self.harness.get(f"{path}?{query}")

        self.assertEqual(raised.exception.status, served.status)
        self.assertEqual(
            public_api.error_payload(raised.exception.code, raised.exception.message),
            served.payload,
        )

    def test_unknown_public_route_is_a_public_schema_404(self) -> None:
        served = self.harness.get("/public/v1/not-a-route")

        self.assertEqual(404, served.status)
        self.assertEqual("prism.dashboard.error.v1", served.payload["schema"])
        self.assertEqual("not_found", served.payload["error"]["code"])

    def test_a_second_request_is_served_from_cache_with_a_real_age(self) -> None:
        # The response cache moved with the routes; a HIT must still report Age.
        route = ("/public/v1/blocks", "")
        first = self.harness.get(request_path(route))
        second = self.harness.get(request_path(route))

        self.assertEqual(200, first.status)
        self.assertEqual(first.body, second.body)
        self.assertEqual("0", second.headers.get("Age"))


class FullReadModelLedger(FakePublicLedger):
    """A ledger implementing every ``dashboard_*`` read model, as Postgres does.

    ``FakePublicLedger`` deliberately leaves some read models out so the
    pre-extraction tests could exercise ``public_api``'s duck-typed fallbacks.
    The extracted service never runs those fallbacks in production --
    ``PsqlShareLedger`` implements every read model -- and two of the fallbacks
    derive hashrate from ``all_shares()`` relative to wall-clock now, which
    makes them unusable for an equality comparison. This subclass fills the
    gaps with fixed rows so a route's answer depends only on its inputs.
    """

    def dashboard_miner_share_summary(self, *, recipient_id: str) -> dict[str, object]:
        return {
            "hashrate_ths": {"h1": "1.5", "h3": "2.5", "h24": "3.5"},
            "accepted_3h": 3,
            "accepted_difficulty_3h": "600000",
            "last_share_at": "2026-06-26T20:44:53Z",
            "share_percent": "60",
        }

    def dashboard_miner_worker_rows(
        self,
        *,
        recipient_id: str,
        page: int,
        limit: int,
        search: str | None,
        hide_inactive: bool,
    ) -> dict[str, object]:
        rows = [
            {
                "worker_name": "rig-1",
                "status": "active",
                "last_share_at": "2026-06-26T20:44:53Z",
                "hashrate_ths_60s": "1.5",
                "hashrate_ths_3h": "2.5",
            }
        ]
        if search:
            rows = [row for row in rows if search.lower() in str(row["worker_name"]).lower()]
        offset = (page - 1) * limit
        return {
            "pagination": public_api.pagination(page, limit, len(rows)),
            "rows": rows[offset : offset + limit],
            "active_count": len(rows),
        }

    def dashboard_miner_owed_balance_bits(self, *, recipient_id: str) -> int:
        return sum(
            int(balance["balance_sats"])
            for balance in self.current_owed_balances()
            if balance["recipient_id"] == recipient_id
        )

    def dashboard_direct_coinbase_settlement(
        self,
        *,
        block_hash: str,
    ) -> dict[str, object] | None:
        return None

    # The Postgres implementations of these two are independent SQL. The base
    # fixture builds them from recipient_payout_history(), which is itself a
    # writer-lock read, so they are re-based here on the raw fixture rows.
    def dashboard_miner_payout_rows(
        self,
        *,
        recipient_id: str,
        page: int,
        limit: int,
    ) -> dict[str, object]:
        rows = [public_api.miner_payout_row(row) for row in self._payout_history()]
        offset = (page - 1) * limit
        return {
            "pagination": public_api.pagination(page, limit, len(rows)),
            "rows": rows[offset : offset + limit],
        }

    def dashboard_miner_earning_rows(
        self,
        *,
        recipient_id: str,
        page: int,
        limit: int,
    ) -> dict[str, object]:
        rows = [public_api.miner_earning_row(row) for row in self._payout_history()]
        offset = (page - 1) * limit
        return {
            "pagination": public_api.pagination(page, limit, len(rows)),
            "rows": rows[offset : offset + limit],
        }


class NoWriterLockLedger(FullReadModelLedger):
    """A public ledger whose writer-lock reads are all landmines.

    Every method in WRITER_LOCK_LEDGER_METHODS is one ``public_api`` would
    reach only by falling back off a ``dashboard_*`` read model. On the real
    Postgres ledger each of them runs under
    ``_operation_gate(self._lock, "writer lock")``, so reaching one from a
    public route serializes a dashboard poll against the block-landing writer.
    The extraction's whole purpose is that none of them are reachable.
    """

    def __init__(self) -> None:
        super().__init__()
        self.settlement_calls = 0
        self.owed_balance_calls = 0

    def dashboard_miner_owed_balance_bits(self, *, recipient_id: str) -> int:
        self.owed_balance_calls += 1
        return 1234

    def dashboard_direct_coinbase_settlement(
        self,
        *,
        block_hash: str,
    ) -> dict[str, object] | None:
        self.settlement_calls += 1
        return None

    def dashboard_miner_share_summary(self, *, recipient_id: str) -> dict[str, object]:
        # FakePublicLedger has no share-summary read model, so public_api would
        # fall back to all_shares(). PsqlShareLedger does have one, and it is a
        # read-slot query; supply it so the fallback is genuinely unreachable.
        return {
            "hashrate_ths": {"m1": "0", "m5": "0", "m10": "0", "h3": "2.5", "h24": "2.5"},
            "accepted_3h": 2,
            "accepted_difficulty_3h": "180",
            "last_share_at": "2026-06-26T20:44:53Z",
            "share_percent": "60",
        }

    def dashboard_miner_payout_rows(
        self,
        *,
        recipient_id: str,
        page: int,
        limit: int,
    ) -> dict[str, object]:
        # The inherited version routes through recipient_payout_history, which
        # is a landmine here. On PsqlShareLedger this read model is its own
        # read-slot query and touches no fenced path.
        rows = [public_api.miner_payout_row(row) for row in self._payout_history()]
        offset = (page - 1) * limit
        return {
            "pagination": public_api.pagination(page, limit, len(rows)),
            "rows": rows[offset : offset + limit],
        }

    def dashboard_miner_earning_rows(
        self,
        *,
        recipient_id: str,
        page: int,
        limit: int,
    ) -> dict[str, object]:
        rows = [public_api.miner_earning_row(row) for row in self._payout_history()]
        offset = (page - 1) * limit
        return {
            "pagination": public_api.pagination(page, limit, len(rows)),
            "rows": rows[offset : offset + limit],
        }


def _install_writer_lock_landmines(ledger: object) -> None:
    for name in WRITER_LOCK_LEDGER_METHODS:
        def explode(*_args: object, _name: str = name, **_kwargs: object) -> object:
            raise WriterLockReached(
                f"public route reached the writer-lock read {_name}()"
            )

        setattr(ledger, name, explode)


class NoWriterLockReadTests(unittest.TestCase):
    """No extracted route may reach a read that fences on the writer lock.

    This is the test that proves the extraction achieved its purpose. If it
    fails, the public tier still serializes against the lease-holding writer
    and the move bought only GIL separation.
    """

    def setUp(self) -> None:
        self.ledger = NoWriterLockLedger()
        _install_writer_lock_landmines(self.ledger)
        self.coordinator = FakeCoordinator(ledger=self.ledger)
        self.harness = ServiceHarness(self.coordinator)
        self.addCleanup(self.harness.close)

    def test_no_extracted_route_touches_a_writer_lock_read(self) -> None:
        for route in EXTRACTED_ROUTES:
            with self.subTest(path=route[0]):
                served = self.harness.get(request_path(route))
                # A landmine surfaces as a 500 in the public error schema,
                # because the handler converts every unexpected exception. Any
                # 5xx here means a writer-lock read was reached.
                self.assertLess(
                    served.status,
                    500,
                    f"{route[0]} reached a writer-lock read (status {served.status})",
                )

    def test_the_landmines_are_actually_armed(self) -> None:
        # Guard against the previous test passing vacuously because the stub
        # stopped raising.
        for name in WRITER_LOCK_LEDGER_METHODS:
            with self.subTest(method=name):
                with self.assertRaises(WriterLockReached):
                    getattr(self.ledger, name)()

    def test_the_miner_page_uses_the_owed_balance_read_model(self) -> None:
        # /public/v1/miners/{recipient_id} is the most-polled route on the
        # surface; it must reach the per-recipient read model, not the
        # every-recipient writer-lock dump.
        served = self.harness.get(f"/public/v1/miners/{RECIPIENT_ID}")

        self.assertEqual(200, served.status)
        self.assertEqual(1, self.ledger.owed_balance_calls)

    def test_direct_coinbase_settlement_uses_the_read_model(self) -> None:
        # A block the CTV manifest-set read does not know: this is the branch
        # that used to fall through to audit_bundle() under the writer lock.
        served = self.harness.get(
            f"/public/v1/blocks/{'0e' * 32}/settlement-artifacts"
        )

        self.assertEqual(404, served.status)
        self.assertEqual(1, self.ledger.settlement_calls)


class StubCache:
    """A response cache that answers with a chosen age, to drive staleness."""

    def __init__(self, age_seconds: int, cache_state: str = "HIT") -> None:
        self.age_seconds = age_seconds
        self.cache_state = cache_state
        self.stale_windows: list[int] = []

    def get_or_compute(
        self,
        *,
        key: object,
        ttl_seconds: int,
        compute,
        stale_while_revalidate_seconds: int = 0,
    ) -> tuple:
        del key, ttl_seconds
        self.stale_windows.append(stale_while_revalidate_seconds)
        status, payload = compute()
        return status, payload, self.cache_state, self.age_seconds


class StalenessContractTests(unittest.TestCase):
    """Publish the budget, refuse past it, and never refuse immutable content."""

    def harness_at_age(self, age_seconds: int) -> ServiceHarness:
        harness = ServiceHarness(
            FakeCoordinator(ledger=FullReadModelLedger()),
            response_cache=StubCache(age_seconds),
        )
        self.addCleanup(harness.close)
        return harness

    def test_every_extracted_response_publishes_its_budget(self) -> None:
        harness = ServiceHarness(FakeCoordinator(ledger=FullReadModelLedger()))
        self.addCleanup(harness.close)
        for route in EXTRACTED_ROUTES:
            path = route[0]
            with self.subTest(path=path):
                served = harness.get(request_path(route))
                budget = served.headers.get(
                    public_read_service.STALENESS_BUDGET_HEADER
                )
                self.assertIsNotNone(
                    budget,
                    f"{path} served no staleness budget",
                )
                endpoint = endpoint_registry.endpoint_for_request_path(path)
                assert endpoint is not None
                if endpoint.immutable_content:
                    self.assertEqual(
                        public_read_service.UNBOUNDED_STALENESS_BUDGET,
                        budget,
                    )
                else:
                    self.assertEqual(
                        str(int(endpoint.max_staleness_seconds or 0)),
                        budget,
                    )

    def test_a_response_past_its_budget_is_refused(self) -> None:
        # One second past the largest budget on the surface, so every
        # non-immutable route is over its own.
        budgets = [
            endpoint.max_staleness_seconds or 0
            for endpoint in endpoint_registry.ENDPOINTS
            if endpoint.disposition is endpoint_registry.Disposition.EXTRACT
        ]
        harness = self.harness_at_age(int(max(budgets)) + 1)

        for route in EXTRACTED_ROUTES:
            path = route[0]
            endpoint = endpoint_registry.endpoint_for_request_path(path)
            assert endpoint is not None
            if endpoint.immutable_content:
                continue
            with self.subTest(path=path):
                served = harness.get(request_path(route))

                self.assertEqual(503, served.status)
                self.assertEqual("no-store", served.headers.get("Cache-Control"))
                self.assertEqual(
                    "prism.dashboard.error.v1",
                    served.payload["schema"],
                )
                message = served.payload["error"]["message"]
                budget = endpoint.max_staleness_seconds or 0
                self.assertIn(f"{int(budget)}", message)
                self.assertIn(f"{int(max(budgets)) + 1}", message)

    def test_a_refusal_still_reports_the_budget_and_the_observed_age(self) -> None:
        harness = self.harness_at_age(10_000)
        served = harness.get("/public/v1/pool-summary")

        self.assertEqual(503, served.status)
        self.assertEqual(
            "90",
            served.headers.get(public_read_service.STALENESS_BUDGET_HEADER),
        )
        self.assertEqual("10000", served.headers.get("Age"))

    def test_a_response_inside_its_budget_is_served(self) -> None:
        # 60s is inside pool-summary's 90s budget and outside the 15s floor
        # routes, so this also pins that the check is per-route.
        harness = self.harness_at_age(60)

        self.assertEqual(200, harness.get("/public/v1/pool-summary").status)
        self.assertEqual(503, harness.get("/public/v1/blocks").status)

    def test_immutable_artifacts_never_refuse_for_staleness(self) -> None:
        # Content-addressed: a body that hashes to the requested sha256 is
        # correct however old it is, so age must not turn it into a 503.
        harness = self.harness_at_age(10_000_000)
        served = harness.get(f"/public/v1/artifacts/{ARTIFACT_SHA256}")

        self.assertEqual(200, served.status)
        self.assertEqual(
            public_read_service.UNBOUNDED_STALENESS_BUDGET,
            served.headers.get(public_read_service.STALENESS_BUDGET_HEADER),
        )

    def test_refusals_are_counted_in_metrics(self) -> None:
        harness = self.harness_at_age(10_000)
        harness.get("/public/v1/pool-summary")

        metrics = harness.get("/metrics")
        self.assertEqual(200, metrics.status)
        self.assertIn(
            "qbit_prism_public_staleness_refusals_total 1",
            metrics.body.decode(),
        )


class StatementTimeoutScopeLedger(FullReadModelLedger):
    """FullReadModelLedger plus PsqlShareLedger's operation_timeout scope shape."""

    def __init__(self) -> None:
        super().__init__()
        self.statement_timeout_budgets: list[float] = []

    @contextmanager
    def operation_timeout(self, timeout_seconds: float) -> Iterator[None]:
        self.statement_timeout_budgets.append(timeout_seconds)
        yield


class StatementTimeoutWiringTests(unittest.TestCase):
    """The per-request database deadline arms exactly where a read slot is held.

    The deadline's own semantics (budget value, env kill switch, the
    read_timeout 503, cache non-poisoning) are pinned in
    tests/test_prism_public_dashboard_api.py; what belongs to this service is
    the classification -- path_occupies_read_slot(), which is deliberately
    wider than the staleness gates' path_reads_database(): a cold immutable
    artifact lookup holds a Postgres read slot exactly like any other cold
    read even though its body, once found, is correct at any age.
    """

    def test_the_scope_wraps_every_read_slot_route_and_nothing_else(self) -> None:
        ledger = StatementTimeoutScopeLedger()
        harness = ServiceHarness(FakeCoordinator(ledger=ledger))
        self.addCleanup(harness.close)

        for route in EXTRACTED_ROUTES:
            path = route[0]
            with self.subTest(path=path):
                armed_before = len(ledger.statement_timeout_budgets)
                served = harness.get(request_path(route))
                self.assertEqual(200, served.status)
                armed = len(ledger.statement_timeout_budgets) > armed_before
                # mining-configuration touches no read slot; every other
                # extracted route -- the immutable artifact route included --
                # occupies one on a cold request and owes the deadline.
                self.assertEqual(
                    public_read_service.path_occupies_read_slot(path), armed
                )

    def test_artifacts_owe_the_deadline_but_not_the_staleness_gates(self) -> None:
        path = f"/public/v1/artifacts/{ARTIFACT_SHA256}"
        self.assertTrue(public_read_service.path_occupies_read_slot(path))
        # Deliberately split from path_reads_database: immutable content is
        # exempt from replica/outage refusals, never from the deadline.
        self.assertFalse(public_read_service.path_reads_database(path))

    def test_mining_configuration_arms_nothing(self) -> None:
        path = "/public/v1/mining-configuration"
        self.assertFalse(public_read_service.path_occupies_read_slot(path))
        self.assertFalse(public_read_service.path_reads_database(path))

    def test_one_budget_covers_a_multi_read_dispatch(self) -> None:
        """The scope is entered once around the whole dispatch, not per read.

        The miner detail route performs several sequential ledger reads; a
        per-statement scope would hand each a fresh budget and let the
        request run many multiples of the configured limit. Exactly one
        operation_timeout entry proves the deadline spans them all.
        """
        ledger = StatementTimeoutScopeLedger()
        harness = ServiceHarness(FakeCoordinator(ledger=ledger))
        self.addCleanup(harness.close)

        served = harness.get(f"/public/v1/miners/{RECIPIENT_ID}")

        self.assertEqual(200, served.status)
        self.assertEqual(1, len(ledger.statement_timeout_budgets))


class StaleServeTests(unittest.TestCase):
    """Read-service wiring for the in-process stale-while-revalidate window."""

    def setUp(self) -> None:
        self.cache = public_api.PublicResponseCache()
        self.metrics = public_read_service.ServiceMetrics()
        self.harness = ServiceHarness(
            FakeCoordinator(ledger=FullReadModelLedger()),
            response_cache=self.cache,
            metrics=self.metrics,
        )
        self.addCleanup(self.harness.close)

    def age_blocks_entry(self, age_seconds: float) -> None:
        """Age the warm /public/v1/blocks entry to a chosen served age.

        The route's default TTL is 5s, so any age beyond that is expired;
        the bounds are edited directly for the reason cached_entry() states.
        """
        entry = cached_entry(self.cache, "/public/v1/blocks")
        assert entry is not None
        entry.stored_at = time.monotonic() - age_seconds  # type: ignore[union-attr]
        entry.expires_at = entry.stored_at + 5.0  # type: ignore[union-attr]

    def test_a_stale_entry_inside_the_budget_is_served_with_its_age(self) -> None:
        warm = self.harness.get("/public/v1/blocks")
        self.age_blocks_entry(8.0)  # 3s past the 5s TTL, inside the 15s budget

        served = self.harness.get("/public/v1/blocks")

        self.assertEqual(200, served.status)
        self.assertEqual(warm.body, served.body)
        self.assertEqual("8", served.headers.get("Age"))

    def test_stale_serves_are_counted_in_cache_metrics(self) -> None:
        self.harness.get("/public/v1/blocks")
        self.age_blocks_entry(8.0)
        self.harness.get("/public/v1/blocks")

        body = self.harness.get("/metrics").body.decode()

        self.assertIn('qbit_prism_public_cache_total{state="stale"} 1', body)

    def test_the_window_is_clamped_to_the_route_staleness_budget(self) -> None:
        """A stale serve must never turn an age the blocking path handled into a 503.

        Age 20 is inside the raw 30s stale-while-revalidate window but past
        the 15s budget /public/v1/blocks publishes. Unclamped, the cache
        would serve it STALE and the unconditional budget check would refuse
        it; clamped to budget minus TTL, the request falls through to the
        blocking recompute exactly as it did before the window existed.
        """
        self.harness.get("/public/v1/blocks")
        self.age_blocks_entry(20.0)

        served = self.harness.get("/public/v1/blocks")

        self.assertEqual(200, served.status)
        self.assertEqual("0", served.headers.get("Age"))

    def test_an_outage_withdraws_the_window(self) -> None:
        """While degraded, only unexpired entries are served (the #164 contract).

        The stale-while-revalidate window must not widen what an outage may
        answer with, and its background refresh must not become a second
        origin path the outage gate does not see.
        """
        probe = ScriptedReadiness()
        readiness = public_read_service.ReadinessProbe(probe, interval_seconds=3600)
        readiness.check_once()
        cache = public_api.PublicResponseCache()
        harness = ServiceHarness(
            FakeCoordinator(ledger=FullReadModelLedger()),
            response_cache=cache,
            readiness=readiness,
        )
        self.addCleanup(harness.close)
        harness.get("/public/v1/blocks")
        entry = cached_entry(cache, "/public/v1/blocks")
        assert entry is not None
        entry.stored_at = time.monotonic() - 8.0  # type: ignore[union-attr]
        entry.expires_at = entry.stored_at + 5.0  # type: ignore[union-attr]
        probe.healthy = False
        readiness.check_once()

        served = harness.get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertEqual("upstream_unavailable", served.payload["error"]["code"])


class FakeClock:
    """Monotonic time the test moves by hand."""

    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ScriptedReplica:
    """A read_replica_status() whose answers -- and failures -- the test picks."""

    def __init__(self, *answers: object) -> None:
        self.answers = list(answers)
        self.calls = 0

    def __call__(self) -> dict[str, object]:
        self.calls += 1
        answer = self.answers[min(self.calls, len(self.answers)) - 1]
        if isinstance(answer, Exception):
            raise answer
        assert isinstance(answer, dict)
        return answer


def standby(heartbeat: float | None = 1.0, replay_lag: float | None = 2.0) -> dict:
    return {
        "in_recovery": True,
        "receiver_heartbeat_age_seconds": heartbeat,
        "replay_lag_seconds": replay_lag,
        "apply_backlog_bytes": 4096,
    }


PRIMARY_STATUS = {
    "in_recovery": False,
    "receiver_heartbeat_age_seconds": None,
    "replay_lag_seconds": None,
    "apply_backlog_bytes": None,
}


class ReplicaContractTests(unittest.TestCase):
    """Serve from a live standby, or refuse and say which part failed.

    The bound is on the walreceiver heartbeat rather than replay lag; see
    ReplicaProbe. These tests drive the probe directly with a fake clock, so
    they pin the freshness rules without a Postgres.
    """

    def probe(
        self,
        *answers: object,
        max_lag_seconds: float = 60.0,
    ) -> tuple[public_read_service.ReplicaProbe, FakeClock]:
        clock = FakeClock()
        probe = public_read_service.ReplicaProbe(
            ScriptedReplica(*answers),
            max_lag_seconds=max_lag_seconds,
            monotonic=clock,
        )
        return probe, clock

    def harness(self, probe: object, **kwargs: object) -> ServiceHarness:
        harness = ServiceHarness(
            FakeCoordinator(ledger=FullReadModelLedger()),
            replica=probe,
            **kwargs,  # type: ignore[arg-type]
        )
        self.addCleanup(harness.close)
        return harness

    def test_before_the_first_probe_replica_routes_are_warming_up(self) -> None:
        probe, _ = self.probe(standby())
        served = self.harness(probe).get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertIn("warming up", served.payload["error"]["message"])

    def test_a_live_standby_is_served(self) -> None:
        probe, _ = self.probe(standby())
        probe.check()
        served = self.harness(probe).get("/public/v1/blocks")

        self.assertEqual(200, served.status)

    def test_a_writable_primary_is_refused(self) -> None:
        probe, _ = self.probe(PRIMARY_STATUS)
        self.assertFalse(probe.check())
        served = self.harness(probe).get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertIn("not in recovery", served.payload["error"]["message"])

    def test_a_refusal_uses_the_documented_public_error_enum(self) -> None:
        # PUBLIC_ERROR_CODES silently rewrites an unknown code to
        # internal_error, so an undocumented one would ship a 503 that says
        # "internal error" and never fail a test that only reads the message.
        probe, _ = self.probe(PRIMARY_STATUS)
        probe.check()
        served = self.harness(probe).get("/public/v1/blocks")

        self.assertEqual("prism.dashboard.error.v1", served.payload["schema"])
        self.assertEqual("upstream_unavailable", served.payload["error"]["code"])

    def test_a_disconnected_walreceiver_is_refused(self) -> None:
        probe, _ = self.probe(standby(heartbeat=None))
        probe.check()
        served = self.harness(probe).get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertIn("not connected", served.payload["error"]["message"])

    def test_a_heartbeat_past_the_bound_is_refused(self) -> None:
        probe, _ = self.probe(standby(heartbeat=61.0), max_lag_seconds=60.0)
        probe.check()
        served = self.harness(probe).get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertIn("silent for", served.payload["error"]["message"])

    def test_a_failing_probe_ages_its_last_good_answer_out(self) -> None:
        """The gate fails closed: a dead probe must not pin a fresh verdict.

        This is the whole reason freshness is computed as "heartbeat age when
        last probed, plus how long ago that was". Without the second term a
        replica that stopped answering entirely keeps serving 200 forever off
        its last good snapshot.
        """
        probe, clock = self.probe(
            standby(heartbeat=1.0),
            RuntimeError("connection refused"),
            max_lag_seconds=60.0,
        )
        probe.check()
        harness = self.harness(probe)

        # Probe starts failing; the snapshot is still inside the bound.
        for _ in range(100):
            with self.assertRaises(RuntimeError):
                probe.check()
        clock.advance(30.0)
        self.assertEqual(200, harness.get("/public/v1/blocks").status)

        # Past the bound, the same unchanged snapshot must now refuse.
        clock.advance(30.0)
        served = harness.get("/public/v1/blocks")
        self.assertEqual(503, served.status)
        self.assertIn("silent for", served.payload["error"]["message"])
        self.assertIn("connection refused", served.payload["error"]["message"])

    def test_a_refusal_is_not_masked_by_a_warm_cache(self) -> None:
        """The gate runs before the cache, not after.

        A replica outage does not expire cache entries, so a check placed after
        the cache would keep answering 200 from the last good snapshot for as
        long as traffic kept it warm.
        """
        probe, clock = self.probe(standby(heartbeat=1.0), max_lag_seconds=60.0)
        probe.check()
        harness = self.harness(probe, response_cache=StubCache(0))

        self.assertEqual(200, harness.get("/public/v1/blocks").status)
        clock.advance(120.0)
        self.assertEqual(503, harness.get("/public/v1/blocks").status)

    def test_routes_that_read_no_replica_state_keep_serving(self) -> None:
        probe, _ = self.probe(PRIMARY_STATUS)
        probe.check()
        harness = self.harness(probe)

        self.assertEqual(503, harness.get("/public/v1/blocks").status)
        self.assertEqual(
            200, harness.get("/public/v1/mining-configuration").status
        )

    def test_immutable_artifacts_are_not_replica_gated(self) -> None:
        # Content-addressed: the body either hashes to the requested sha256 or
        # it does not, which replication lag cannot change. The registry gives
        # these no staleness budget for the same reason.
        probe, _ = self.probe(PRIMARY_STATUS)
        probe.check()
        harness = self.harness(probe)

        self.assertFalse(
            public_read_service.path_reads_database(
                f"/public/v1/artifacts/{ARTIFACT_SHA256}"
            )
        )
        self.assertEqual(
            200, harness.get(f"/public/v1/artifacts/{ARTIFACT_SHA256}").status
        )

    def test_every_replica_backed_route_is_gated(self) -> None:
        """Exhaustive, not a sample: every extracted route that reads a read
        slot must refuse, and the classification must come from the registry."""

        probe, _ = self.probe(PRIMARY_STATUS)
        probe.check()
        harness = self.harness(probe)

        gated = 0
        for route in EXTRACTED_ROUTES:
            path = route[0]
            endpoint = endpoint_registry.endpoint_for_request_path(path)
            assert endpoint is not None
            expected_gated = (
                not endpoint.immutable_content
                and endpoint_registry.LedgerAccess.READ_SLOT in endpoint.access
            )
            with self.subTest(path=path):
                self.assertEqual(
                    expected_gated,
                    public_read_service.path_reads_database(path),
                )
                served = harness.get(request_path(route))
                if expected_gated:
                    gated += 1
                    self.assertEqual(503, served.status)
                    self.assertIn(
                        "not in recovery", served.payload["error"]["message"]
                    )
                else:
                    self.assertNotEqual(503, served.status)
        self.assertGreater(gated, 0, "no route exercised the replica gate")

    def test_observed_replay_lag_travels_on_the_response(self) -> None:
        probe, clock = self.probe(standby(heartbeat=1.0, replay_lag=2.0))
        probe.check()
        clock.advance(3.0)
        served = self.harness(probe).get("/public/v1/blocks")

        self.assertEqual(200, served.status)
        self.assertEqual(
            "5.000",
            served.headers.get(public_read_service.REPLICA_LAG_HEADER),
        )

    def test_healthz_carries_the_replication_facts(self) -> None:
        probe, _ = self.probe(standby(heartbeat=1.0))
        probe.check()
        served = self.harness(probe).get("/healthz")

        self.assertEqual(200, served.status)
        self.assertEqual(True, served.payload["replica"]["in_recovery"])
        self.assertEqual(
            1.0, served.payload["replica"]["receiver_heartbeat_age_seconds"]
        )
        self.assertEqual(60.0, served.payload["replica"]["max_lag_seconds"])

    def test_healthz_goes_unready_when_the_contract_fails(self) -> None:
        probe, _ = self.probe(PRIMARY_STATUS)
        probe.check()
        served = self.harness(probe).get("/healthz")

        self.assertEqual(503, served.status)
        self.assertFalse(served.payload["ok"])
        self.assertIn("not in recovery", served.payload["error"])

    def test_metrics_publish_the_replica_gauges_and_refusals(self) -> None:
        probe, _ = self.probe(standby(heartbeat=1.0, replay_lag=2.0))
        probe.check()
        harness = self.harness(probe)
        harness.get("/public/v1/blocks")

        body = harness.get("/metrics").body.decode()
        self.assertIn("qbit_prism_public_replica_in_recovery 1", body)
        self.assertIn(
            "qbit_prism_public_replica_heartbeat_age_seconds 1.000", body
        )
        self.assertIn("qbit_prism_public_replica_replay_lag_seconds 2.000", body)
        self.assertIn("qbit_prism_public_replica_apply_backlog_bytes 4096.000", body)
        self.assertIn("qbit_prism_public_replica_max_lag_seconds 60.000", body)
        self.assertIn("qbit_prism_public_replica_refusals_total 0", body)

    def test_replica_refusals_are_counted(self) -> None:
        probe, _ = self.probe(PRIMARY_STATUS)
        probe.check()
        harness = self.harness(probe)
        harness.get("/public/v1/blocks")

        self.assertIn(
            "qbit_prism_public_replica_refusals_total 1",
            harness.get("/metrics").body.decode(),
        )

    def test_unknown_numbers_report_minus_one_rather_than_zero(self) -> None:
        probe, _ = self.probe(standby(heartbeat=None, replay_lag=None))
        probe.check()
        harness = self.harness(probe)

        body = harness.get("/metrics").body.decode()
        self.assertIn("qbit_prism_public_replica_heartbeat_age_seconds -1", body)
        self.assertIn("qbit_prism_public_replica_replay_lag_seconds -1", body)


class ReplicaModeTests(unittest.TestCase):
    """PRISM_PUBLIC_REPLICA_MODE decides whether the contract is enforced."""

    class Ledger:
        def read_replica_status(self) -> dict[str, object]:
            return standby()

    def test_absent_mode_enforces_nothing(self) -> None:
        # The default must not 503 a deployment that merged this before it
        # provisioned a standby; the shipped compose sets require explicitly.
        self.assertEqual(
            public_read_service.REPLICA_MODE_OFF,
            public_read_service.resolve_replica_mode({}),
        )
        self.assertIsNone(public_read_service.build_replica_probe(self.Ledger(), {}))

    def test_require_builds_the_probe(self) -> None:
        probe = public_read_service.build_replica_probe(
            self.Ledger(), {"PRISM_PUBLIC_REPLICA_MODE": "require"}
        )
        self.assertIsInstance(probe, public_read_service.ReplicaProbe)

    def test_an_unrecognised_mode_refuses_startup(self) -> None:
        # Neither default is safe to guess: the two modes disagree about
        # whether this process may read the coordinator's primary.
        with self.assertRaises(
            public_read_service.PublicReadConfigurationError
        ) as raised:
            public_read_service.resolve_replica_mode(
                {"PRISM_PUBLIC_REPLICA_MODE": "yes"}
            )
        self.assertIn("PRISM_PUBLIC_REPLICA_MODE", str(raised.exception))


class DatabaseReached(AssertionError):
    """Raised when a route consulted the database while readiness said it was down."""


class ScriptedReadiness:
    """A readiness probe callable whose answer the test flips.

    Raises rather than returning False, because that is what an unreachable
    Postgres actually does to ``dashboard_readiness_probe``, and the two take
    different arms inside ReadinessProbe.check_once().
    """

    def __init__(self, *, healthy: bool = True) -> None:
        self.healthy = healthy
        self.calls = 0

    def __call__(self) -> bool:
        self.calls += 1
        if not self.healthy:
            raise RuntimeError("connection refused")
        return True


def cached_entry(
    cache: public_api.PublicResponseCache,
    path: str,
) -> object | None:
    """The live cache entry for a path, or None.

    Reaches into PublicResponseCache's storage deliberately. Expiry there is
    keyed on time.monotonic(), which is not injectable, so the alternatives for
    driving the expired-entry case are sleeping out a real TTL or replacing the
    cache with a double -- and a double cannot prove the thing under test,
    which is that a degraded request leaves the *real* entry's bounds alone.
    """
    return cache._entries.get(public_api.public_cache_key(path, {}))


class CoalescingCache:
    """A cache that hands back a result some other request computed.

    This is PublicResponseCache's waiter path: a request that arrives while
    another is already computing the same key never calls compute() itself and
    receives the owner's result. Modelled here rather than raced, because the
    property under test is what the handler does with a result it did not
    produce, not the cache's own locking.
    """

    def __init__(self, result: tuple) -> None:
        self.result = result
        self.computes = 0

    def get_or_compute(
        self,
        *,
        key: object,
        ttl_seconds: int,
        compute,
        stale_while_revalidate_seconds: int = 0,
    ) -> tuple:
        # a waiter never runs compute()
        del key, ttl_seconds, compute, stale_while_revalidate_seconds
        return self.result


class DatabaseOutageTests(unittest.TestCase):
    """Bounded-stale availability while Postgres is unreachable (#164).

    The endpoint is public and the database is not: an outage that turns every
    dashboard into an error page is an outage the public sees in full. What the
    surface owes instead is its own already-published bound -- serve the warm
    entry if there is one inside the route's staleness budget, say plainly that
    it is doing so, and refuse otherwise.

    Two properties make that honest rather than a way to hide an outage, and
    each has a test below: a degraded request never calls the database (so the
    outage is not amplified by the retry traffic it causes), and it never
    extends the entry it serves (so ordinary polling cannot walk a stale
    dashboard past the bound, one hit at a time, for as long as the outage
    lasts).
    """

    def setUp(self) -> None:
        self.ledger = FullReadModelLedger()
        self.coordinator = FakeCoordinator(ledger=self.ledger)
        self.cache = public_api.PublicResponseCache()
        self.probe_source = ScriptedReadiness()
        self.readiness = public_read_service.ReadinessProbe(
            self.probe_source,
            interval_seconds=3600,
        )
        self.readiness.check_once()
        self.metrics = public_read_service.ServiceMetrics()
        self.reached: list[str] = []
        self.armed: list[str] = []
        self.harness = ServiceHarness(
            self.coordinator,
            response_cache=self.cache,
            readiness=self.readiness,
            metrics=self.metrics,
        )
        self.addCleanup(self.harness.close)

    def take_database_down(self, *, arm_landmines: bool = True) -> None:
        """Readiness starts failing, and every database read becomes a landmine.

        Everything public_api asks a ledger for is a database read, so arming
        all of them turns "the handler went to the origin anyway" into a
        recorded method name rather than a subtle difference in a body.
        """
        self.probe_source.healthy = False
        self.readiness.check_once()
        self.assertEqual(503, self.readiness.snapshot()[0])
        if not arm_landmines:
            return
        names = {name for name in dir(self.ledger) if name.startswith("dashboard_")}
        names.update(
            name for name in WRITER_LOCK_LEDGER_METHODS if hasattr(self.ledger, name)
        )
        for name in sorted(names):
            def explode(*_args: object, _name: str = name, **_kwargs: object) -> object:
                self.reached.append(_name)
                raise DatabaseReached(f"the origin reached {_name}() during an outage")

            setattr(self.ledger, name, explode)
            self.armed.append(name)

    def bring_database_up(self) -> None:
        for name in self.armed:
            delattr(self.ledger, name)
        self.armed.clear()
        self.probe_source.healthy = True
        self.readiness.check_once()
        self.assertEqual(200, self.readiness.snapshot()[0])

    def assert_degraded(self, served: ServedResponse) -> None:
        self.assertEqual(
            public_read_service.DEGRADED_WARNING,
            served.headers.get(public_read_service.DEGRADED_WARNING_HEADER),
        )
        self.assertEqual(
            public_read_service.DATABASE_STATE_UNAVAILABLE,
            served.headers.get(public_read_service.DATABASE_STATE_HEADER),
        )

    def test_a_warm_entry_is_served_and_says_so(self) -> None:
        warm = self.harness.get("/public/v1/blocks")
        self.assertEqual(200, warm.status)
        self.take_database_down()

        served = self.harness.get("/public/v1/blocks")

        self.assertEqual(200, served.status)
        self.assertEqual(warm.body, served.body)
        self.assertEqual([], self.reached)
        # Pinned literally, not through the constant: this string is the
        # contract a dashboard frontend parses.
        self.assertEqual(
            '110 qbit-prism "database unavailable; serving cached response"',
            served.headers.get("Warning"),
        )
        self.assertEqual(
            "unavailable", served.headers.get("X-Prism-Database-State")
        )
        # The outage fact and the numerical bound travel together: without the
        # second, "stale" has no size.
        self.assertEqual("0", served.headers.get("Age"))
        self.assertEqual(
            "15",
            served.headers.get(public_read_service.STALENESS_BUDGET_HEADER),
        )

    def test_a_cold_cache_refuses_without_calling_the_database(self) -> None:
        self.take_database_down()

        served = self.harness.get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertEqual([], self.reached)
        self.assertEqual("no-store", served.headers.get("Cache-Control"))
        self.assertEqual("prism.dashboard.error.v1", served.payload["schema"])
        # PUBLIC_ERROR_CODES rewrites an undocumented code to internal_error,
        # so a 503 that only reads right in the message is not enough.
        self.assertEqual("upstream_unavailable", served.payload["error"]["code"])
        self.assertIn(
            "cannot reach the database", served.payload["error"]["message"]
        )
        self.assertEqual(
            "unavailable", served.headers.get("X-Prism-Database-State")
        )
        # 110 is "Response is Stale". Nothing was served, stale or otherwise.
        self.assertIsNone(served.headers.get("Warning"))
        self.assertEqual(
            "15",
            served.headers.get(public_read_service.STALENESS_BUDGET_HEADER),
        )

    def test_a_refusal_stores_nothing(self) -> None:
        # Otherwise the first refusal would become a warm entry and the next
        # request would serve a 503 body as a cached 503 -- or worse, count as
        # the warm entry the outage path is allowed to serve.
        self.take_database_down()
        self.harness.get("/public/v1/blocks")

        self.assertIsNone(cached_entry(self.cache, "/public/v1/blocks"))

    def test_an_expired_entry_refuses_and_traffic_cannot_refill_it(self) -> None:
        self.harness.get("/public/v1/blocks")
        entry = cached_entry(self.cache, "/public/v1/blocks")
        self.assertIsNotNone(entry)
        self.take_database_down()
        # Expire it where the cache actually looks, rather than sleeping out a
        # real TTL.
        entry.expires_at = 0.0  # type: ignore[union-attr]

        for _ in range(3):
            self.assertEqual(503, self.harness.get("/public/v1/blocks").status)

        self.assertEqual([], self.reached)
        self.assertIsNone(cached_entry(self.cache, "/public/v1/blocks"))

    def test_an_entry_past_the_route_budget_refuses_though_it_is_warm(self) -> None:
        """TTL and budget are separate numbers, and the budget is the promise.

        Nothing stops an operator raising PRISM_PUBLIC_*_CACHE_TTL_SECONDS
        above the registry budget the same route publishes. When that happens
        the entry is unexpired and past the bound at once, and the bound wins.
        """
        self.harness.get("/public/v1/blocks")
        entry = cached_entry(self.cache, "/public/v1/blocks")
        entry.stored_at -= 3600.0  # type: ignore[union-attr]
        entry.expires_at += 3600.0  # type: ignore[union-attr]
        self.take_database_down()

        for _ in range(3):
            served = self.harness.get("/public/v1/blocks")
            self.assertEqual(503, served.status)

        self.assertEqual([], self.reached)
        self.assertIn(
            "exceeds the staleness budget", served.payload["error"]["message"]
        )
        self.assertEqual("no-store", served.headers.get("Cache-Control"))
        self.assertEqual("3600", served.headers.get("Age"))
        self.assertEqual(
            "15",
            served.headers.get(public_read_service.STALENESS_BUDGET_HEADER),
        )
        self.assertEqual(
            "unavailable", served.headers.get("X-Prism-Database-State")
        )
        self.assertIsNone(served.headers.get("Warning"))

    def test_degraded_traffic_never_extends_the_entry_it_serves(self) -> None:
        """The bound has to survive the traffic, or it is not a bound.

        A degraded hit that refreshed stored_at or expires_at would let
        ordinary polling walk one snapshot forward indefinitely, and the
        staleness budget on every one of those responses would be a number
        describing nothing.
        """
        self.harness.get("/public/v1/blocks")
        entry = cached_entry(self.cache, "/public/v1/blocks")
        assert entry is not None
        bounds = (entry.stored_at, entry.expires_at)
        self.take_database_down()

        for _ in range(5):
            self.assert_degraded(self.harness.get("/public/v1/blocks"))

        self.assertEqual(bounds, (entry.stored_at, entry.expires_at))
        # And when those bounds are reached, the outage cannot renew them.
        entry.expires_at = 0.0
        self.assertEqual(503, self.harness.get("/public/v1/blocks").status)

    def test_recovery_restores_the_origin_and_drops_the_degraded_headers(self) -> None:
        self.harness.get("/public/v1/blocks")
        self.take_database_down()
        self.assert_degraded(self.harness.get("/public/v1/blocks"))

        self.bring_database_up()
        # Expire the warm entry so the answer has to come from the origin.
        cached_entry(self.cache, "/public/v1/blocks").expires_at = 0.0  # type: ignore[union-attr]
        served = self.harness.get("/public/v1/blocks")

        self.assertEqual(200, served.status)
        self.assertIsNone(served.headers.get("Warning"))
        self.assertIsNone(served.headers.get("X-Prism-Database-State"))
        self.assertEqual("0", served.headers.get("Age"))
        # Refilled, which only the origin path can do.
        self.assertIsNotNone(cached_entry(self.cache, "/public/v1/blocks"))

    def test_every_database_backed_route_degrades_and_the_rest_do_not(self) -> None:
        """Exhaustive and registry-derived, so a new route is classified once."""

        for route in EXTRACTED_ROUTES:
            self.assertEqual(
                200, self.harness.get(request_path(route)).status, route[0]
            )
        self.take_database_down()

        degraded = 0
        for route in EXTRACTED_ROUTES:
            path = route[0]
            endpoint = endpoint_registry.endpoint_for_request_path(path)
            assert endpoint is not None
            expected = (
                not endpoint.immutable_content
                and endpoint_registry.LedgerAccess.READ_SLOT in endpoint.access
            )
            with self.subTest(path=path):
                self.assertEqual(
                    expected, public_read_service.path_reads_database(path)
                )
                served = self.harness.get(request_path(route))
                self.assertEqual(200, served.status)
                if expected:
                    degraded += 1
                    self.assert_degraded(served)
                    self.assertIsNotNone(served.headers.get("Age"))
                    self.assertIsNotNone(
                        served.headers.get(
                            public_read_service.STALENESS_BUDGET_HEADER
                        )
                    )
                else:
                    self.assertIsNone(served.headers.get("Warning"))
                    self.assertIsNone(
                        served.headers.get("X-Prism-Database-State")
                    )
        self.assertEqual([], self.reached)
        self.assertGreater(degraded, 0, "no route exercised the outage gate")

    def test_the_exempt_routes_stay_available_from_a_cold_cache(self) -> None:
        """Mining configuration and immutable artifacts owe answers regardless.

        The first is assembled from the environment and has no database answer
        to lose; the second is content-addressed, so its body either hashes to
        the requested sha256 or it does not, which an outage cannot change.
        Landmines stay disarmed here precisely because these two are expected
        to reach their origin.
        """
        self.take_database_down(arm_landmines=False)

        refused = 0
        for route in EXTRACTED_ROUTES:
            path = route[0]
            with self.subTest(path=path):
                served = self.harness.get(request_path(route))
                if public_read_service.path_reads_database(path):
                    refused += 1
                    self.assertEqual(503, served.status)
                    self.assertEqual(
                        "upstream_unavailable", served.payload["error"]["code"]
                    )
                    self.assertEqual(
                        "no-store", served.headers.get("Cache-Control")
                    )
                else:
                    self.assertEqual(200, served.status)
                    self.assertIsNone(served.headers.get("Warning"))
                    self.assertIsNone(
                        served.headers.get("X-Prism-Database-State")
                    )
        self.assertEqual(len(EXTRACTED_ROUTES) - 2, refused)

    def test_a_bypassed_cache_refuses_rather_than_calling_the_database(self) -> None:
        # With the response cache off there is no warm entry to serve and no
        # way to make one, so every request is the cold case.
        with patch.dict(os.environ, {"PRISM_PUBLIC_CACHE_ENABLED": "false"}):
            self.harness.get("/public/v1/blocks")
            self.take_database_down()
            served = self.harness.get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertEqual([], self.reached)
        self.assertEqual(
            "unavailable", served.headers.get("X-Prism-Database-State")
        )

    def test_a_route_that_reads_no_database_is_never_degraded(self) -> None:
        self.take_database_down()

        served = self.harness.get("/public/v1/mining-configuration")

        self.assertEqual(200, served.status)
        self.assertIsNone(served.headers.get("Warning"))
        self.assertIsNone(served.headers.get("X-Prism-Database-State"))

    def test_degraded_responses_and_refusals_are_counted(self) -> None:
        self.harness.get("/public/v1/blocks")
        self.take_database_down()
        self.harness.get("/public/v1/blocks")  # warm: degraded 200
        self.harness.get("/public/v1/leaderboard")  # cold: refused

        body = self.harness.get("/metrics").body.decode()

        self.assertIn("qbit_prism_public_degraded_responses_total 1", body)
        self.assertIn("qbit_prism_public_database_outage_refusals_total 1", body)

    def test_a_healthy_readiness_snapshot_changes_nothing(self) -> None:
        # The gate must be invisible while the database is up, including on the
        # first, uncached request.
        served = self.harness.get("/public/v1/blocks")

        self.assertEqual(200, served.status)
        self.assertIsNone(served.headers.get("Warning"))
        self.assertIsNone(served.headers.get("X-Prism-Database-State"))
        body = self.harness.get("/metrics").body.decode()
        self.assertIn("qbit_prism_public_degraded_responses_total 0", body)
        self.assertIn("qbit_prism_public_database_outage_refusals_total 0", body)

    def test_a_warming_up_probe_is_treated_as_unavailable(self) -> None:
        # Before the first probe completes there is no evidence the database is
        # reachable, and this gate fails closed on absent evidence like every
        # other freshness check in this service.
        readiness = public_read_service.ReadinessProbe(
            ScriptedReadiness(), interval_seconds=3600
        )
        harness = ServiceHarness(
            FakeCoordinator(ledger=FullReadModelLedger()), readiness=readiness
        )
        self.addCleanup(harness.close)

        self.assertEqual(503, harness.get("/public/v1/blocks").status)
        self.assertEqual(200, harness.get("/public/v1/mining-configuration").status)

    def test_a_coalesced_refusal_is_never_shared_cacheable(self) -> None:
        """A waiter must not turn another request's refusal into a CDN entry.

        get_or_compute() coalesces: a request arriving while another is
        computing the same key gets that one's result, whatever its own view of
        readiness was. So a request whose probe had already recovered can
        receive the outage refusal -- and if the handler decided "is this the
        refusal" from the request instead of from the response, it would send
        that 503 out under the ordinary shared-cache headers and a CDN would
        hold it for the route's TTL.
        """
        harness = ServiceHarness(
            self.coordinator,
            response_cache=CoalescingCache(
                (503, public_read_service.DATABASE_UNAVAILABLE_PAYLOAD, "MISS", 0)
            ),
            readiness=self.readiness,  # healthy: this request never degraded
        )
        self.addCleanup(harness.close)

        served = harness.get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertEqual("no-store", served.headers.get("Cache-Control"))
        self.assertIsNone(served.headers.get("CDN-Cache-Control"))
        self.assertEqual(
            "unavailable", served.headers.get("X-Prism-Database-State")
        )

    def test_a_coalesced_fresh_body_is_not_labelled_stale(self) -> None:
        """The mirror case: the waiter is the degraded one, the body is fresh.

        Warning 110 and X-Prism-Database-State are claims about the body being
        served. A response another request just computed from a database that
        answered it is not a cached response, whatever this request's probe
        said a moment later.
        """
        fresh = {"schema": "prism.dashboard.blocks.v1", "rows": []}
        self.take_database_down()
        harness = ServiceHarness(
            self.coordinator,
            response_cache=CoalescingCache((200, fresh, "MISS", 0)),
            readiness=self.readiness,
        )
        self.addCleanup(harness.close)

        served = harness.get("/public/v1/blocks")

        self.assertEqual(200, served.status)
        self.assertEqual(fresh, served.payload)
        self.assertIsNone(served.headers.get("Warning"))
        self.assertIsNone(served.headers.get("X-Prism-Database-State"))

    def test_replica_mode_require_keeps_its_stricter_refusal(self) -> None:
        """A warm entry does not soften the standby contract.

        In require mode the replica probe is the readiness probe, so both gates
        see the same failure. They do not owe the same answer: reading the
        coordinator's primary is refused because of *where* the answer would
        come from, which a cached body from that same wrong place does not fix.
        """
        clock = FakeClock()
        replica = public_read_service.ReplicaProbe(
            ScriptedReplica(standby(heartbeat=1.0), PRIMARY_STATUS),
            max_lag_seconds=60.0,
            monotonic=clock,
        )
        readiness = public_read_service.ReadinessProbe(
            replica.check, interval_seconds=3600, monotonic=clock
        )
        readiness.check_once()
        harness = ServiceHarness(
            FakeCoordinator(ledger=FullReadModelLedger()),
            readiness=readiness,
            replica=replica,
        )
        self.addCleanup(harness.close)
        self.assertEqual(200, harness.get("/public/v1/blocks").status)

        readiness.check_once()
        served = harness.get("/public/v1/blocks")

        self.assertEqual(503, served.status)
        self.assertIn("not in recovery", served.payload["error"]["message"])
        self.assertIsNone(served.headers.get("Warning"))


class FailClosedStartupTests(unittest.TestCase):
    """Refuse to start rather than serve wrong facts."""

    def test_missing_public_stratum_url_refuses_startup(self) -> None:
        for environ in ({}, {"PRISM_PUBLIC_STRATUM_URL": ""}, {"PRISM_PUBLIC_STRATUM_URL": "   "}):
            with self.subTest(environ=environ):
                with self.assertRaises(
                    public_read_service.PublicReadConfigurationError
                ) as raised:
                    public_read_service.require_public_stratum_url(environ)
                self.assertIn("PRISM_PUBLIC_STRATUM_URL", str(raised.exception))

    def test_a_configured_public_stratum_url_is_accepted(self) -> None:
        value = public_read_service.require_public_stratum_url(
            {"PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://pool.example:3340"}
        )
        self.assertEqual("stratum+tcp://pool.example:3340", value)

    def test_memory_ledger_is_refused(self) -> None:
        with self.assertRaises(
            public_read_service.PublicReadConfigurationError
        ) as raised:
            public_read_service.build_ledger_from_env(
                {
                    "PRISM_ALLOW_MEMORY_LEDGER": "1",
                    "PRISM_DATABASE_URL": "postgresql:///prism",
                }
            )
        message = str(raised.exception)
        self.assertIn("PRISM_ALLOW_MEMORY_LEDGER", message)
        # The reason matters as much as the refusal: the memory ledger would
        # reintroduce writer-lock reads through public_api's fallbacks.
        self.assertIn("dashboard_", message)

    def test_missing_database_configuration_refuses_startup(self) -> None:
        with self.assertRaises(
            public_read_service.PublicReadConfigurationError
        ) as raised:
            public_read_service.build_ledger_from_env({})
        self.assertIn("PRISM_DATABASE_URL", str(raised.exception))

    def test_build_service_refuses_before_touching_the_database(self) -> None:
        # The stratum-url check must come first, so a service missing both
        # reports the configuration error rather than a connection failure.
        with self.assertRaises(
            public_read_service.PublicReadConfigurationError
        ) as raised:
            public_read_service.build_service({})
        self.assertIn("PRISM_PUBLIC_STRATUM_URL", str(raised.exception))

    def test_the_stand_in_defines_no_stratum_endpoint_attributes(self) -> None:
        # public_api.mining_configuration reads getattr(coordinator, "port"/
        # "bind") with an environment fallback. The stand-in must not define
        # them, or the fallback -- and with it the required-env check -- would
        # be dead code.
        coordinator = public_read_service.PublicReadCoordinator(
            ledger=object(),
            rpc=object(),
        )
        self.assertFalse(hasattr(coordinator, "port"))
        self.assertFalse(hasattr(coordinator, "bind"))


class ReadinessAndMetricsTests(unittest.TestCase):
    """The service's own liveness and counters, which are new rather than moved."""

    def test_healthz_reports_ready_when_the_probe_succeeds(self) -> None:
        probe = public_read_service.ReadinessProbe(lambda: True, interval_seconds=60)
        probe.check_once()
        harness = ServiceHarness(FakeCoordinator(), readiness=probe)
        self.addCleanup(harness.close)

        served = harness.get("/healthz")

        self.assertEqual(200, served.status)
        self.assertTrue(served.payload["ok"])

    def test_healthz_reports_unready_when_postgres_is_unreachable(self) -> None:
        def failing_probe() -> bool:
            raise RuntimeError("connection refused")

        probe = public_read_service.ReadinessProbe(failing_probe, interval_seconds=60)
        probe.check_once()
        harness = ServiceHarness(FakeCoordinator(), readiness=probe)
        self.addCleanup(harness.close)

        served = harness.get("/healthz")

        self.assertEqual(503, served.status)
        self.assertFalse(served.payload["ok"])
        self.assertIn("connection refused", str(served.payload["error"]))

    def test_healthz_refuses_a_stale_probe(self) -> None:
        clock = [1000.0]
        probe = public_read_service.ReadinessProbe(
            lambda: True,
            interval_seconds=5,
            monotonic=lambda: clock[0],
        )
        probe.check_once()
        clock[0] += probe.stale_after_seconds + 1
        harness = ServiceHarness(FakeCoordinator(), readiness=probe)
        self.addCleanup(harness.close)

        served = harness.get("/healthz")

        self.assertEqual(503, served.status)
        self.assertIn("stale", str(served.payload["error"]))

    def test_healthz_does_not_query_on_the_request_thread(self) -> None:
        # The compose healthcheck polls this forever; a database round trip per
        # poll would make liveness checking a source of load.
        calls: list[int] = []
        probe = public_read_service.ReadinessProbe(
            lambda: (calls.append(1), True)[1],
            interval_seconds=3600,
        )
        probe.check_once()
        harness = ServiceHarness(FakeCoordinator(), readiness=probe)
        self.addCleanup(harness.close)

        for _ in range(5):
            self.assertEqual(200, harness.get("/healthz").status)

        self.assertEqual(1, len(calls))

    def test_metrics_are_prometheus_text_for_this_process(self) -> None:
        harness = ServiceHarness(FakeCoordinator())
        self.addCleanup(harness.close)
        harness.get("/public/v1/blocks")

        served = harness.get("/metrics")

        self.assertEqual(200, served.status)
        self.assertTrue(
            served.headers.get("Content-Type", "").startswith("text/plain")
        )
        body = served.body.decode()
        for metric in (
            "qbit_prism_public_requests_total",
            "qbit_prism_public_responses_total",
            "qbit_prism_public_cache_total",
            "qbit_prism_public_staleness_refusals_total",
        ):
            self.assertIn(metric, body)

    def test_cache_hits_and_misses_are_counted(self) -> None:
        harness = ServiceHarness(FakeCoordinator())
        self.addCleanup(harness.close)
        harness.get("/public/v1/blocks")
        harness.get("/public/v1/blocks")

        body = harness.get("/metrics").body.decode()

        self.assertIn('qbit_prism_public_cache_total{state="miss"} 1', body)
        self.assertIn('qbit_prism_public_cache_total{state="hit"} 1', body)

    def test_unknown_paths_outside_the_public_surface_are_plain_404s(self) -> None:
        harness = ServiceHarness(FakeCoordinator())
        self.addCleanup(harness.close)

        served = harness.get("/audit/latest")

        self.assertEqual(404, served.status)
        self.assertEqual({"error": "unknown endpoint"}, served.payload)


class _MinimalAuditPort:
    """Answers nothing: an unknown path never reaches a port method."""

    def ledger_backend(self) -> str:
        return "fixture"


class CoordinatorSubtractionTests(unittest.TestCase):
    """The coordinator's listener no longer serves the public surface."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.facade = AuditHttpFacade(
            _MinimalAuditPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0, join_timeout_seconds=1.0),
        )
        state = cls.facade.start()
        assert state.bound_address is not None
        cls.host, cls.port = state.bound_address

    @classmethod
    def tearDownClass(cls) -> None:
        cls.facade.stop()

    def get(self, path: str) -> tuple[int, object]:
        url = f"http://{self.host}:{self.port}{path}"
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            with error:
                return error.code, json.loads(error.read())

    def test_public_routes_are_unknown_endpoints_on_the_coordinator(self) -> None:
        for path in (
            "/public/v1/pool-summary",
            "/public/v1/blocks",
            f"/public/v1/miners/{RECIPIENT_ID}",
            "/public/v1",
        ):
            with self.subTest(path=path):
                status, body = self.get(path)
                self.assertEqual(404, status)
                self.assertEqual({"error": "unknown endpoint"}, body)

    def test_audit_http_no_longer_imports_public_api(self) -> None:
        # The public error schema and cache policy left with the routes. An
        # import here would mean some public handling stayed behind.
        source = AUDIT_HTTP_PATH.read_text(encoding="utf-8")
        self.assertNotIn("import public_api", source)
        self.assertNotIn("public_api.", source)

    def test_the_audit_http_port_no_longer_exposes_public_payload(self) -> None:
        from lab.prism import audit_http

        self.assertFalse(hasattr(audit_http.AuditHttpPort, "public_payload"))


class DirectCoinbaseSettlementContractTests(unittest.TestCase):
    """The severed writer-lock settlement read must answer identically."""

    def test_read_model_and_audit_bundle_fallback_agree(self) -> None:
        # The read model added in share_ledger returns exactly what
        # public_api.direct_coinbase_settlement_payload built from audit_bundle.
        fallback_ledger = DirectCoinbasePublicLedger()
        expected = public_api.direct_coinbase_settlement_payload(
            fallback_ledger,
            block_hash=DirectCoinbasePublicLedger.block_hash,
        )

        class ReadModelLedger(DirectCoinbasePublicLedger):
            def dashboard_direct_coinbase_settlement(
                self,
                *,
                block_hash: str,
            ) -> dict[str, object] | None:
                # Stand in for the Postgres read-slot implementation by
                # reusing the same source row the fenced path would have read.
                return expected

            def audit_bundle(self, *, block_hash: str) -> dict[str, object] | None:
                raise WriterLockReached("audit_bundle() is a writer-lock read")

        actual = public_api.direct_coinbase_settlement_payload(
            ReadModelLedger(),
            block_hash=DirectCoinbasePublicLedger.block_hash,
        )

        self.assertEqual(expected, actual)
        self.assertEqual(
            {
                "block_hash",
                "block_height",
                "settlement_mode",
                "audit_bundle_sha256",
                "payout_manifest_sha256",
                "artifacts",
            },
            set(actual or {}),
        )

    def test_owed_balance_read_model_matches_the_python_sum(self) -> None:
        balances = [
            {"recipient_id": "miner-a", "balance_sats": 700},
            {"recipient_id": "miner-b", "balance_sats": 11},
            {"recipient_id": "miner-a", "balance_sats": 300},
        ]

        class SummingLedger:
            def current_owed_balances(self) -> list[dict[str, object]]:
                return balances

        class ReadModelLedger:
            def dashboard_miner_owed_balance_bits(self, *, recipient_id: str) -> int:
                return sum(
                    int(balance["balance_sats"])
                    for balance in balances
                    if balance["recipient_id"] == recipient_id
                )

            def current_owed_balances(self) -> list[dict[str, object]]:
                raise WriterLockReached("current_owed_balances() is a writer-lock read")

        self.assertEqual(
            public_api.owed_balance_for_recipient(SummingLedger(), "miner-a"),
            public_api.owed_balance_for_recipient(ReadModelLedger(), "miner-a"),
        )
        self.assertEqual(
            1000,
            public_api.owed_balance_for_recipient(ReadModelLedger(), "miner-a"),
        )

    def test_owed_balance_falls_back_for_a_ledger_without_the_read_model(self) -> None:
        # The in-memory ledger has no dashboard_* read models; the fallback
        # must stay for it.
        class MemoryStyleLedger:
            def current_owed_balances(self) -> list[dict[str, object]]:
                return [{"recipient_id": "miner-a", "balance_sats": 42}]

        self.assertEqual(
            42,
            public_api.owed_balance_for_recipient(MemoryStyleLedger(), "miner-a"),
        )


class StubbedReadSlotLedger(PsqlShareLedger):
    """A PsqlShareLedger with a real writer lock and a canned SQL backend.

    Bypasses __init__ (as the other share-ledger tests do) so the gates under
    test are real threading primitives while no database is involved: the
    property being asserted is which gate a method takes, and that is decided
    entirely by the Python code around the statement.
    """

    def __init__(self, results: dict[str, object]) -> None:
        self._lock = threading.Lock()
        self._read_semaphore = threading.BoundedSemaphore(4)
        self._results = results
        self.statements: list[str] = []

    def _run_json(self, sql: str) -> object:
        self.statements.append(sql)
        for marker, result in self._results.items():
            if marker in sql:
                return result
        raise AssertionError(f"unexpected statement: {sql[:120]}")


OWED_BALANCE_SQL_RESULT = {"qbit_current_owed_balances": {"owed_balance_bits": "1000"}}
SETTLEMENT_SQL_RESULT = {
    "qbit_pool_audit_bundles": {
        "block_hash": "ab" * 32,
        "block_height": 23342,
        "payout_manifest_sha256": "8c" * 32,
        "audit_bundle_sha256": "92" * 32,
        "audit_bundle": {
            "schema": "qbit.prism.audit-bundle.v1",
            "found_block": {"block_height": 23342},
            "settlement_mode_decision": {"mode": "direct_coinbase"},
        },
        "body_uri": None,
    }
}


class ReadSlotNotWriterLockTests(unittest.TestCase):
    """The severed reads must take the read slot, structurally.

    Asserted by holding the writer lock from another thread for the whole
    call: a method that fences would block until the holder released, so a
    call that returns under a held lock cannot have taken it. The two
    control cases at the end show the same setup does block the reads these
    replaced, so a passing assertion here is not vacuous.
    """

    HOLD_SECONDS = 30.0
    RETURN_TIMEOUT = 5.0

    def call_with_writer_lock_held(self, ledger: PsqlShareLedger, call) -> object:
        """Run call() while another thread holds ledger's writer lock."""

        acquired = threading.Event()
        release = threading.Event()

        def hold() -> None:
            with ledger._lock:
                acquired.set()
                release.wait(self.HOLD_SECONDS)

        holder = threading.Thread(target=hold, name="writer-lock-holder", daemon=True)
        holder.start()
        self.assertTrue(acquired.wait(5.0), "writer lock holder never started")
        try:
            result: list[object] = []
            error: list[BaseException] = []

            def run() -> None:
                try:
                    result.append(call())
                except BaseException as exc:  # noqa: BLE001 - reported below
                    error.append(exc)

            worker = threading.Thread(target=run, name="read-under-lock", daemon=True)
            worker.start()
            worker.join(timeout=self.RETURN_TIMEOUT)
            returned = not worker.is_alive()
        finally:
            release.set()
            holder.join(timeout=5.0)
        if error:
            raise error[0]
        if not returned:
            raise AssertionError("call did not return while the writer lock was held")
        return result[0]

    def blocks_while_writer_lock_held(self, ledger: PsqlShareLedger, call) -> bool:
        try:
            self.call_with_writer_lock_held(ledger, call)
        except AssertionError as exc:
            return "did not return" in str(exc)
        return False

    def test_owed_balance_read_model_returns_under_a_held_writer_lock(self) -> None:
        ledger = StubbedReadSlotLedger(OWED_BALANCE_SQL_RESULT)

        value = self.call_with_writer_lock_held(
            ledger,
            lambda: ledger.dashboard_miner_owed_balance_bits(recipient_id="miner-a"),
        )

        self.assertEqual(1000, value)

    def test_settlement_read_model_returns_under_a_held_writer_lock(self) -> None:
        ledger = StubbedReadSlotLedger(SETTLEMENT_SQL_RESULT)

        payload = self.call_with_writer_lock_held(
            ledger,
            lambda: ledger.dashboard_direct_coinbase_settlement(block_hash="ab" * 32),
        )

        assert payload is not None
        self.assertEqual("direct_coinbase", payload["settlement_mode"])
        self.assertEqual(23342, payload["block_height"])

    def test_owed_balance_sql_filters_by_recipient_rather_than_in_python(self) -> None:
        ledger = StubbedReadSlotLedger(OWED_BALANCE_SQL_RESULT)

        ledger.dashboard_miner_owed_balance_bits(recipient_id="miner-a")

        statement = ledger.statements[0]
        self.assertIn("qbit_current_owed_balances()", statement)
        # The same two predicates the Python sum applied, now in SQL.
        self.assertIn("owed_balance_sats > 0", statement)
        self.assertIn("miner_id = 'miner-a'", statement)

    def test_a_quoted_recipient_id_cannot_escape_its_literal(self) -> None:
        ledger = StubbedReadSlotLedger(OWED_BALANCE_SQL_RESULT)

        ledger.dashboard_miner_owed_balance_bits(recipient_id="mi'ner")

        self.assertIn("miner_id = 'mi''ner'", ledger.statements[0])

    def test_an_empty_recipient_id_is_rejected(self) -> None:
        ledger = StubbedReadSlotLedger(OWED_BALANCE_SQL_RESULT)

        with self.assertRaises(ValueError):
            ledger.dashboard_miner_owed_balance_bits(recipient_id="")

    def test_settlement_read_model_reports_non_direct_coinbase_as_none(self) -> None:
        ledger = StubbedReadSlotLedger(
            {
                "qbit_pool_audit_bundles": {
                    **SETTLEMENT_SQL_RESULT["qbit_pool_audit_bundles"],
                    "audit_bundle": {
                        "settlement_mode_decision": {
                            "mode": "hybrid_coinbase_ctv_fanout"
                        }
                    },
                }
            }
        )

        self.assertIsNone(
            ledger.dashboard_direct_coinbase_settlement(block_hash="ab" * 32)
        )

    def test_settlement_read_model_reports_a_missing_bundle_as_none(self) -> None:
        ledger = StubbedReadSlotLedger({"qbit_pool_audit_bundles": None})

        self.assertIsNone(
            ledger.dashboard_direct_coinbase_settlement(block_hash="ab" * 32)
        )

    def test_an_unreadable_external_body_is_none_rather_than_an_error(self) -> None:
        # public_api.audit_bundle_body_read_failed() tolerated exactly these
        # three failures on the fenced path; the read model must keep doing so.
        ledger = StubbedReadSlotLedger(
            {
                "qbit_pool_audit_bundles": {
                    **SETTLEMENT_SQL_RESULT["qbit_pool_audit_bundles"],
                    "audit_bundle": None,
                    "body_uri": "/nonexistent/prism-audit-bundle-body.json",
                }
            }
        )

        self.assertIsNone(
            ledger.dashboard_direct_coinbase_settlement(block_hash="ab" * 32)
        )

    def test_the_replaced_reads_really_do_block_on_the_writer_lock(self) -> None:
        # The control: without this, the assertions above could pass because
        # nothing in the harness ever takes the writer lock at all.
        owed = StubbedReadSlotLedger(
            {"qbit_current_owed_balances": [], "count(*)": {"count": 0}}
        )
        self.assertTrue(
            self.blocks_while_writer_lock_held(owed, owed.current_owed_balances),
            "current_owed_balances() no longer fences, so the contrast is void",
        )

        bundle = StubbedReadSlotLedger(SETTLEMENT_SQL_RESULT)
        self.assertTrue(
            self.blocks_while_writer_lock_held(
                bundle,
                lambda: bundle.audit_bundle(block_hash="ab" * 32),
            ),
            "audit_bundle() no longer fences, so the contrast is void",
        )

    def test_a_read_only_ledger_cannot_take_the_writer_lock_at_all(self) -> None:
        ledger = PsqlShareLedger(
            psql_command="psql postgresql://example.invalid/qbit",
            native_client_mode="psql",
            read_only=True,
        )

        with self.assertRaises(ReadOnlyLedgerError):
            ledger.current_owed_balances()
        with self.assertRaises(ReadOnlyLedgerError):
            ledger.audit_bundle(block_hash="ab" * 32)

    def test_a_read_only_ledger_refuses_schema_initialization(self) -> None:
        with self.assertRaises(ValueError):
            PsqlShareLedger(
                psql_command="psql postgresql://example.invalid/qbit",
                native_client_mode="psql",
                read_only=True,
                initialize_schema=True,
            )


class _GatedLedger(PsqlShareLedger):
    """A PsqlShareLedger with real gates and a stubbed SQL layer.

    Built without __init__ (the pattern tests/test_prism_public_dashboard_api.py
    already uses) so the writer lock and the read semaphore are the genuine
    objects while no database is involved.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._read_semaphore = BoundedSemaphore(4)
        self.statements: list[str] = []
        self.row: object = None

    def _text_literal(self, value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _run_retry_safe_read_json(self, sql: str) -> object:
        self.statements.append(sql)
        return self.row


class WriterLockIsNotTakenTests(unittest.TestCase):
    """Structural proof: the new read models never touch the writer gate.

    Each call is driven with the writer lock already held by another thread. A
    method that fenced would block there until the test timed out; these must
    return, because they take the bounded read slot instead.
    """

    def hold_writer_lock(self, ledger: _GatedLedger) -> None:
        acquired = threading.Event()
        release = threading.Event()

        def holder() -> None:
            ledger._lock.acquire()
            acquired.set()
            release.wait(10)
            ledger._lock.release()

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        self.assertTrue(acquired.wait(5), "helper thread never took the writer lock")
        self.addCleanup(thread.join, 5)
        self.addCleanup(release.set)

    def call_with_timeout(self, call) -> object:
        result: list[object] = []
        error: list[BaseException] = []

        def run() -> None:
            try:
                result.append(call())
            except BaseException as exc:  # noqa: BLE001 - reported below
                error.append(exc)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(10)
        self.assertFalse(
            thread.is_alive(),
            "the call blocked on the writer lock instead of taking a read slot",
        )
        if error:
            raise error[0]
        return result[0]

    def test_owed_balance_read_model_does_not_fence(self) -> None:
        ledger = _GatedLedger()
        ledger.row = {"owed_balance_bits": "1000"}
        self.hold_writer_lock(ledger)

        balance = self.call_with_timeout(
            lambda: ledger.dashboard_miner_owed_balance_bits(recipient_id=RECIPIENT_ID)
        )

        self.assertEqual(1000, balance)
        self.assertEqual(1, len(ledger.statements))
        # Filtered in SQL, not in Python: the point is to stop reading every
        # recipient's balance to answer for one.
        self.assertIn("miner_id", ledger.statements[0])
        self.assertIn(RECIPIENT_ID, ledger.statements[0])
        self.assertIn("owed_balance_sats > 0", ledger.statements[0])

    def test_owed_balance_read_model_returns_zero_without_rows(self) -> None:
        ledger = _GatedLedger()
        ledger.row = {"owed_balance_bits": "0"}
        self.hold_writer_lock(ledger)

        self.assertEqual(
            0,
            self.call_with_timeout(
                lambda: ledger.dashboard_miner_owed_balance_bits(
                    recipient_id="nobody"
                )
            ),
        )

    def test_direct_coinbase_settlement_read_model_does_not_fence(self) -> None:
        ledger = _GatedLedger()
        ledger.row = {
            "block_hash": DirectCoinbasePublicLedger.block_hash,
            "block_height": DirectCoinbasePublicLedger.block_height,
            "payout_manifest_sha256": DirectCoinbasePublicLedger.payout_manifest_sha256,
            "audit_bundle_sha256": DirectCoinbasePublicLedger.audit_bundle_sha256,
            "audit_bundle": {
                "schema": "qbit.prism.audit-bundle.v1",
                "found_block": {"block_height": DirectCoinbasePublicLedger.block_height},
                "settlement_mode_decision": {"mode": "direct_coinbase"},
            },
            "body_uri": None,
        }
        self.hold_writer_lock(ledger)

        payload = self.call_with_timeout(
            lambda: ledger.dashboard_direct_coinbase_settlement(
                block_hash=DirectCoinbasePublicLedger.block_hash
            )
        )

        # Identical to what the audit_bundle() path builds for the same row.
        expected = public_api.direct_coinbase_settlement_payload(
            DirectCoinbasePublicLedger(),
            block_hash=DirectCoinbasePublicLedger.block_hash,
        )
        self.assertEqual(expected, payload)

    def test_direct_coinbase_settlement_ignores_other_settlement_modes(self) -> None:
        ledger = _GatedLedger()
        ledger.row = {
            "block_hash": DirectCoinbasePublicLedger.block_hash,
            "block_height": 1,
            "payout_manifest_sha256": None,
            "audit_bundle_sha256": None,
            "audit_bundle": {
                "settlement_mode_decision": {"mode": "ctv_fanout"},
            },
            "body_uri": None,
        }
        self.hold_writer_lock(ledger)

        self.assertIsNone(
            self.call_with_timeout(
                lambda: ledger.dashboard_direct_coinbase_settlement(
                    block_hash=DirectCoinbasePublicLedger.block_hash
                )
            )
        )

    def test_direct_coinbase_settlement_is_none_without_a_bundle(self) -> None:
        ledger = _GatedLedger()
        ledger.row = None
        self.hold_writer_lock(ledger)

        self.assertIsNone(
            self.call_with_timeout(
                lambda: ledger.dashboard_direct_coinbase_settlement(
                    block_hash=DirectCoinbasePublicLedger.block_hash
                )
            )
        )

    def test_an_unreadable_external_body_is_reported_as_none(self) -> None:
        # public_api.audit_bundle_body_read_failed() tolerates exactly these
        # three; the read model must keep that behaviour rather than 500.
        for message in (
            "audit bundle body is not retrievable",
            "audit bundle body hash mismatch",
            "audit bundle body is not valid JSON",
        ):
            with self.subTest(message=message):
                ledger = _GatedLedger()
                ledger.row = {
                    "block_hash": DirectCoinbasePublicLedger.block_hash,
                    "audit_bundle_sha256": DirectCoinbasePublicLedger.audit_bundle_sha256,
                    "audit_bundle": None,
                    "body_uri": "file:///missing",
                }

                def explode(_body_uri: object, *, expected_sha256: object = None) -> object:
                    raise RuntimeError(message)

                ledger._read_external_body = explode  # type: ignore[assignment]

                self.assertIsNone(
                    ledger.dashboard_direct_coinbase_settlement(
                        block_hash=DirectCoinbasePublicLedger.block_hash
                    )
                )

    def test_an_unexpected_body_error_still_raises(self) -> None:
        ledger = _GatedLedger()
        ledger.row = {
            "block_hash": DirectCoinbasePublicLedger.block_hash,
            "audit_bundle_sha256": DirectCoinbasePublicLedger.audit_bundle_sha256,
            "audit_bundle": None,
            "body_uri": "file:///missing",
        }

        def explode(_body_uri: object, *, expected_sha256: object = None) -> object:
            raise RuntimeError("postgres connection lost")

        ledger._read_external_body = explode  # type: ignore[assignment]

        with self.assertRaises(RuntimeError):
            ledger.dashboard_direct_coinbase_settlement(
                block_hash=DirectCoinbasePublicLedger.block_hash
            )

    def test_the_readiness_probe_does_not_fence(self) -> None:
        ledger = _GatedLedger()
        ledger.row = {"ok": True}
        self.hold_writer_lock(ledger)

        self.assertTrue(
            self.call_with_timeout(ledger.dashboard_readiness_probe)
        )


if __name__ == "__main__":
    unittest.main()
