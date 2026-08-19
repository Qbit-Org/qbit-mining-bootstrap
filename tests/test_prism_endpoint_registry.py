#!/usr/bin/env python3
"""Pin the endpoint classification registry against real HTTP dispatch.

The registry in ``lab/prism/endpoint_registry.py`` is the reviewable form of
the issue #145 public/operator split. It is only worth reviewing if it cannot
drift from the code, so these tests assert both directions: every classified
path is actually routed, and every routed path is classified.
"""

from __future__ import annotations

import ast
import json
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from lab.prism import endpoint_registry, public_api
from lab.prism.audit_http import AuditHttpConfig, AuditHttpFacade
from lab.prism.endpoint_registry import (
    Audience,
    Disposition,
    ENDPOINTS,
    LedgerAccess,
)


ROOT = Path(__file__).resolve().parents[1]
AUDIT_HTTP_PATH = ROOT / "lab" / "prism" / "audit_http.py"
PUBLIC_API_PATH = ROOT / "lab" / "prism" / "public_api.py"

HEX64 = "ab" * 32
PLACEHOLDERS = {
    "{block_hash}": HEX64,
    "{fanout_txid}": HEX64,
    "{commitment_leaf_hex}": HEX64,
    "{sha256}": HEX64,
    "{recipient_id}": "miner-1",
}

UNKNOWN_ENDPOINT_BODY = {"error": "unknown endpoint"}
UNKNOWN_PUBLIC_MESSAGE = "unknown public dashboard endpoint"


def concrete_path(path: str) -> str:
    for placeholder, value in PLACEHOLDERS.items():
        path = path.replace(placeholder, value)
    if "{" in path:
        raise AssertionError(f"unsubstituted placeholder in {path!r}")
    return path


class _Sentinel(Exception):
    """Raised by the fake coordinator once dispatch reaches a real handler."""


class _ExplodingLedger:
    backend_name = "fixture"

    def __getattr__(self, name: str):
        def _call(*args: object, **kwargs: object):
            raise _Sentinel(name)

        return _call


class _ExplodingRpc:
    def call(self, *args: object, **kwargs: object):
        raise _Sentinel("rpc")


class _FakeCoordinator:
    def __init__(self) -> None:
        self.ledger = _ExplodingLedger()
        self.rpc = _ExplodingRpc()


class _RoutingPort:
    """Answers every AuditHttpPort method so only routing is under test."""

    def cached_health_payload(self) -> tuple[int, dict[str, object]]:
        return 200, {"ok": True}

    def cached_metrics_payload(self) -> tuple[int, str]:
        return 200, "fixture 1\n"

    def latest_evidence_payload(self) -> dict[str, object] | None:
        return {"schema": "fixture"}

    def owed_balances_payload(self) -> dict[str, object]:
        return {"balances": []}

    def carry_forward_integrity_payload(self) -> dict[str, object]:
        return {"ok": True}

    def miner_status_payload(self, recipient_id: str) -> dict[str, object]:
        return {"recipient_id": recipient_id}

    def ledger_backend(self) -> str:
        return "fixture"

    def audit_share_window(self, **kwargs: object) -> list[dict[str, object]]:
        return [{"fixture": True}]

    def audit_block_payouts(self, **kwargs: object) -> list[dict[str, object]]:
        return [{"fixture": True}]

    def audit_ctv_fanouts(self, **kwargs: object) -> list[dict[str, object]]:
        return [{"fixture": True}]

    def audit_ctv_fanout_manifest_set(self, **kwargs: object) -> dict[str, object]:
        return {"fixture": True}

    def ctv_fanout_status(self, **kwargs: object) -> dict[str, object]:
        return {"fixture": True}

    def pending_ctv_fanout_statuses(self, **kwargs: object) -> list[dict[str, object]]:
        return [{"fixture": True}]

    def audit_bundle(self, **kwargs: object) -> dict[str, object]:
        return {"fixture": True}

    def audit_bundle_by_commitment(self, **kwargs: object) -> dict[str, object]:
        return {"fixture": True}


def string_literals(source_path: Path, function_name: str) -> set[str]:
    """Absolute path literals mentioned inside one function."""

    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return {
                child.value
                for child in ast.walk(node)
                if isinstance(child, ast.Constant)
                and isinstance(child.value, str)
                and child.value.startswith("/")
            }
    raise AssertionError(f"{function_name} not found in {source_path.name}")


class EndpointRegistryShapeTests(unittest.TestCase):
    def test_paths_are_unique_across_the_registry(self) -> None:
        seen: dict[str, str] = {}
        for endpoint in ENDPOINTS:
            for path in endpoint.paths:
                self.assertNotIn(
                    path,
                    seen,
                    f"{path} classified twice ({seen.get(path)} and {endpoint.primary_path})",
                )
                seen[path] = endpoint.primary_path

    def test_only_the_public_read_surface_is_extracted(self) -> None:
        for endpoint in ENDPOINTS:
            expected = (
                Disposition.EXTRACT
                if endpoint.audience is Audience.PUBLIC_READ
                else Disposition.RETAIN
            )
            self.assertIs(
                endpoint.disposition,
                expected,
                f"{endpoint.primary_path}: {endpoint.audience} must be {expected}",
            )

    def test_extracted_surface_is_exactly_public_v1(self) -> None:
        # PRISM.md: "operators can expose only /public/v1 through a reverse
        # proxy or dashboard frontend". The extraction must not widen that.
        for path in endpoint_registry.extracted_paths():
            self.assertTrue(
                path.startswith("/public/v1/"),
                f"{path} is not part of the documented public API surface",
            )

    def test_audit_routes_are_never_extracted(self) -> None:
        # docs/public-dashboard-api/README.md and
        # tests/test_public_dashboard_api_contract.py both forbid exposing
        # /audit/* as public API.
        for path in endpoint_registry.extracted_paths():
            self.assertFalse(path.startswith("/audit/"))
        for prefix in ("/audit/", "/healthz", "/metrics", "/owed"):
            self.assertTrue(
                any(path.startswith(prefix) for path in endpoint_registry.retained_paths()),
                f"expected retained paths under {prefix}",
            )

    def test_extracted_and_retained_partition_the_registry(self) -> None:
        extracted = set(endpoint_registry.extracted_paths())
        retained = set(endpoint_registry.retained_paths())
        self.assertEqual(set(), extracted & retained)
        every_path = {path for endpoint in ENDPOINTS for path in endpoint.paths}
        self.assertEqual(every_path, extracted | retained)

    def test_writer_lock_extractions_record_a_blocker(self) -> None:
        # The Endpoint constructor enforces this; assert the set is non-empty so
        # the rule cannot be satisfied vacuously by dropping the classification.
        blockers = dict(endpoint_registry.extraction_blockers())
        for endpoint in ENDPOINTS:
            if (
                LedgerAccess.WRITER_LOCK in endpoint.access
                and endpoint.disposition is Disposition.EXTRACT
            ):
                self.assertIn(endpoint.primary_path, blockers)
        self.assertTrue(blockers)

    def test_health_and_metrics_are_retained_operator_surfaces(self) -> None:
        retained = endpoint_registry.retained_paths()
        self.assertIn("/healthz", retained)
        self.assertIn("/metrics", retained)

    def test_drift_count_and_owed_dump_stay_on_the_coordinator(self) -> None:
        retained = endpoint_registry.retained_paths()
        for path in (
            "/audit/carry-forward-integrity",
            "/audit/ledger-integrity",
            "/owed",
            "/owed-balances",
            "/miners/{recipient_id}/status",
            "/payouts/{recipient_id}/status",
        ):
            self.assertIn(path, retained)

    def test_payout_affecting_surface_is_named(self) -> None:
        payout = {
            path
            for endpoint in endpoint_registry.endpoints_by_audience(
                Audience.PAYOUT_AFFECTING
            )
            for path in endpoint.paths
        }
        self.assertEqual(
            {
                "/audit/carry-forward-integrity",
                "/audit/ledger-integrity",
                "/owed",
                "/owed-balances",
                "/miners/{recipient_id}/status",
                "/payouts/{recipient_id}/status",
            },
            payout,
        )

    def test_no_extracted_route_takes_the_writer_lock_without_a_blocker(self) -> None:
        blockers = dict(endpoint_registry.extraction_blockers())
        extracted = set(endpoint_registry.extracted_paths())
        for path in endpoint_registry.writer_lock_paths():
            if path in extracted:
                self.assertIn(path, blockers)


class StalenessBudgetTests(unittest.TestCase):
    """The staleness contract must be derived and stated, never hand-picked."""

    def extracted_endpoints(self) -> list[endpoint_registry.Endpoint]:
        return [
            endpoint
            for endpoint in ENDPOINTS
            if endpoint.disposition is Disposition.EXTRACT
        ]

    def test_every_extracted_route_states_a_budget_or_declares_immutability(self) -> None:
        for endpoint in self.extracted_endpoints():
            with self.subTest(path=endpoint.primary_path):
                self.assertTrue(
                    endpoint.max_staleness_seconds is not None
                    or endpoint.immutable_content,
                    f"{endpoint.primary_path} states no staleness tolerance",
                )

    def test_retained_routes_carry_no_public_staleness_budget(self) -> None:
        # The contract covers the extracted public surface. A budget on a
        # retained route would imply the coordinator enforces one, which it
        # does not.
        for endpoint in ENDPOINTS:
            if endpoint.disposition is Disposition.EXTRACT:
                continue
            with self.subTest(path=endpoint.primary_path):
                self.assertIsNone(endpoint.max_staleness_seconds)
                self.assertFalse(endpoint.immutable_content)

    def test_budgets_match_the_documented_derivation(self) -> None:
        # Pin each budget to the formula rather than to a literal, so changing
        # a cache default without revisiting the budget fails here.
        aggregate = endpoint_registry.PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS
        plain = endpoint_registry.PUBLIC_CACHE_TTL_DEFAULT_SECONDS
        config = endpoint_registry.PUBLIC_CONFIG_CACHE_TTL_DEFAULT_SECONDS
        reward_window = endpoint_registry.POOL_REWARD_WINDOW_CACHE_DEFAULT_SECONDS
        derive = endpoint_registry.staleness_budget_seconds
        expected = {
            "/public/v1/pool-summary": derive(cache_ttl_seconds=aggregate),
            "/public/v1/blocks": derive(cache_ttl_seconds=plain),
            "/public/v1/leaderboard": derive(cache_ttl_seconds=plain),
            "/public/v1/hashrate-series": derive(cache_ttl_seconds=aggregate),
            "/public/v1/mining-configuration": derive(cache_ttl_seconds=config),
            # The one route with a second cache in series beneath it.
            "/public/v1/miners/{recipient_id}": derive(
                cache_ttl_seconds=plain,
                underlying_cache_seconds=reward_window,
            ),
            "/public/v1/miners/{recipient_id}/earnings": derive(cache_ttl_seconds=plain),
            "/public/v1/miners/{recipient_id}/payouts": derive(cache_ttl_seconds=plain),
            "/public/v1/miners/{recipient_id}/workers": derive(cache_ttl_seconds=aggregate),
            "/public/v1/blocks/{block_hash}/settlement-artifacts": derive(
                cache_ttl_seconds=plain
            ),
            "/public/v1/fanouts/pending": derive(cache_ttl_seconds=plain),
            "/public/v1/fanouts/{fanout_txid}": derive(cache_ttl_seconds=plain),
        }
        actual = {
            endpoint.primary_path: endpoint.max_staleness_seconds
            for endpoint in self.extracted_endpoints()
            if endpoint.max_staleness_seconds is not None
        }
        self.assertEqual(expected, actual)

    def test_the_aggregate_ttl_really_is_what_the_cache_policy_applies(self) -> None:
        # The derivation above assumes public_cache_policy classifies these
        # three as aggregates and everything else as a plain row read. Assert
        # that against the policy itself so the two cannot drift apart.
        for path, expected_ttl in (
            ("/public/v1/pool-summary", endpoint_registry.PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS),
            ("/public/v1/hashrate-series", endpoint_registry.PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS),
            ("/public/v1/miners/miner-1/workers", endpoint_registry.PUBLIC_AGGREGATE_CACHE_TTL_DEFAULT_SECONDS),
            ("/public/v1/blocks", endpoint_registry.PUBLIC_CACHE_TTL_DEFAULT_SECONDS),
            ("/public/v1/miners/miner-1/earnings", endpoint_registry.PUBLIC_CACHE_TTL_DEFAULT_SECONDS),
            ("/public/v1/mining-configuration", endpoint_registry.PUBLIC_CONFIG_CACHE_TTL_DEFAULT_SECONDS),
        ):
            with self.subTest(path=path):
                self.assertEqual(
                    expected_ttl,
                    public_api.public_cache_policy(path).ttl_seconds,
                )

    def test_no_budget_falls_below_the_documented_floor(self) -> None:
        for endpoint in self.extracted_endpoints():
            if endpoint.max_staleness_seconds is None:
                continue
            with self.subTest(path=endpoint.primary_path):
                self.assertGreaterEqual(
                    endpoint.max_staleness_seconds,
                    endpoint_registry.MINIMUM_PUBLIC_STALE_SECONDS,
                )

    def test_only_the_artifact_route_is_exempt_from_staleness(self) -> None:
        # Content-addressed and immutable: correct at any age. Stated in the
        # registry rather than left implicit, so the exemption is reviewable.
        exempt = [
            endpoint.primary_path
            for endpoint in ENDPOINTS
            if endpoint.immutable_content
        ]
        self.assertEqual(["/public/v1/artifacts/{sha256}"], exempt)

    def test_immutable_content_and_a_budget_are_mutually_exclusive(self) -> None:
        with self.assertRaises(ValueError):
            endpoint_registry.Endpoint(
                paths=("/public/v1/example",),
                audience=Audience.PUBLIC_READ,
                disposition=Disposition.EXTRACT,
                access=(LedgerAccess.READ_SLOT,),
                rationale="fixture",
                immutable_content=True,
                max_staleness_seconds=30,
            )

    def test_an_extracted_route_cannot_omit_its_staleness_tolerance(self) -> None:
        with self.assertRaises(ValueError):
            endpoint_registry.Endpoint(
                paths=("/public/v1/example",),
                audience=Audience.PUBLIC_READ,
                disposition=Disposition.EXTRACT,
                access=(LedgerAccess.READ_SLOT,),
                rationale="fixture",
            )

    def test_derivation_rejects_negative_cache_seconds(self) -> None:
        with self.assertRaises(ValueError):
            endpoint_registry.staleness_budget_seconds(cache_ttl_seconds=-1)
        with self.assertRaises(ValueError):
            endpoint_registry.staleness_budget_seconds(
                cache_ttl_seconds=5,
                underlying_cache_seconds=-1,
            )


class EndpointForRequestPathTests(unittest.TestCase):
    """Concrete request paths must resolve to the endpoint that serves them."""

    def test_literal_paths_win_over_templates(self) -> None:
        # /public/v1/fanouts/pending is a literal list route that would also
        # match /public/v1/fanouts/{fanout_txid}; dispatch() gives the literal
        # precedence, so the registry lookup must too.
        endpoint = endpoint_registry.endpoint_for_request_path(
            "/public/v1/fanouts/pending"
        )
        assert endpoint is not None
        self.assertEqual("/public/v1/fanouts/pending", endpoint.primary_path)

    def test_templates_match_concrete_segments(self) -> None:
        for path, expected in (
            (f"/public/v1/fanouts/{HEX64}", "/public/v1/fanouts/{fanout_txid}"),
            ("/public/v1/miners/miner-1", "/public/v1/miners/{recipient_id}"),
            (
                "/public/v1/miners/miner-1/workers",
                "/public/v1/miners/{recipient_id}/workers",
            ),
            (
                f"/public/v1/blocks/{HEX64}/settlement-artifacts",
                "/public/v1/blocks/{block_hash}/settlement-artifacts",
            ),
        ):
            with self.subTest(path=path):
                endpoint = endpoint_registry.endpoint_for_request_path(path)
                assert endpoint is not None
                self.assertEqual(expected, endpoint.primary_path)

    def test_unknown_paths_resolve_to_nothing(self) -> None:
        for path in ("/public/v1/not-a-route", "/public/v1", "/nope"):
            with self.subTest(path=path):
                self.assertIsNone(endpoint_registry.endpoint_for_request_path(path))

    def test_every_extracted_path_resolves(self) -> None:
        for path in endpoint_registry.extracted_paths():
            with self.subTest(path=path):
                self.assertIsNotNone(
                    endpoint_registry.endpoint_for_request_path(concrete_path(path))
                )


class EndpointRegistryRoutingTests(unittest.TestCase):
    """Every classified path must actually be routed today."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.facade = AuditHttpFacade(
            _RoutingPort(),  # type: ignore[arg-type]
            AuditHttpConfig("127.0.0.1", 0, join_timeout_seconds=1.0),
        )
        state = cls.facade.start()
        assert state.bound_address is not None
        cls.host, cls.port = state.bound_address

    @classmethod
    def tearDownClass(cls) -> None:
        cls.facade.stop()

    @staticmethod
    def decode(body: bytes) -> object:
        # /metrics answers Prometheus text rather than JSON; the routing
        # assertion only needs "not the unknown-endpoint body".
        try:
            return json.loads(body or b"null")
        except json.JSONDecodeError:
            return body.decode(errors="replace")

    def get(self, path: str) -> tuple[int, object]:
        url = f"http://{self.host}:{self.port}{path}"
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                return response.status, self.decode(response.read())
        except urllib.error.HTTPError as error:
            with error:
                return error.code, self.decode(error.read())

    def test_every_non_public_registry_path_is_routed(self) -> None:
        for endpoint in ENDPOINTS:
            for path in endpoint.paths:
                if path.startswith("/public/v1"):
                    continue
                with self.subTest(path=path):
                    status, body = self.get(concrete_path(path))
                    self.assertNotEqual(
                        body,
                        UNKNOWN_ENDPOINT_BODY,
                        f"{path} is classified but not routed",
                    )
                    self.assertNotEqual(status, 404, f"{path} returned 404")

    def test_metrics_is_served_as_prometheus_text(self) -> None:
        url = f"http://{self.host}:{self.port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            self.assertTrue(
                response.headers["Content-Type"].startswith("text/plain")
            )

    def test_unclassified_path_still_404s(self) -> None:
        status, body = self.get("/definitely-not-a-route")
        self.assertEqual(status, 404)
        self.assertEqual(body, UNKNOWN_ENDPOINT_BODY)


class PublicDispatchRoutingTests(unittest.TestCase):
    """Every /public/v1 path in the registry must be routed by dispatch()."""

    def dispatch(self, path: str) -> None:
        public_api.dispatch(_FakeCoordinator(), path, {})

    def test_every_public_registry_path_is_routed(self) -> None:
        for path in endpoint_registry.extracted_paths():
            if not path.startswith("/public/v1"):
                continue
            with self.subTest(path=path):
                try:
                    self.dispatch(concrete_path(path))
                except _Sentinel:
                    # Reached a real handler, which is what "routed" means.
                    continue
                except public_api.PublicApiError as error:
                    self.assertNotEqual(
                        error.message,
                        UNKNOWN_PUBLIC_MESSAGE,
                        f"{path} is classified but dispatch() does not route it",
                    )

    def test_unknown_public_path_is_rejected(self) -> None:
        with self.assertRaises(public_api.PublicApiError) as caught:
            self.dispatch("/public/v1/not-a-route")
        self.assertEqual(caught.exception.message, UNKNOWN_PUBLIC_MESSAGE)


class RegistryCoverageTests(unittest.TestCase):
    """The HTTP layer's route literals must match the registry exactly.

    The guard is a frozen snapshot of the string literals in each dispatch
    function, compared for exact set equality. A looser substring match was
    rejected on review: a new route that merely *extends* an existing literal
    (for example a new ``/public/v1/blocks/...`` sub-route) would pass
    unclassified, which is precisely the drift this guard exists to catch.
    """

    # Every "/"-prefixed string constant in audit_http.do_GET, including the
    # prefix fragments the dispatcher matches against ("/audit/blocks/",
    # "/status", ...). Adding a route without classifying it in
    # lab/prism/endpoint_registry.py fails the snapshot equality first.
    EXPECTED_DO_GET_LITERALS = frozenset(
        {
            "/",
            "/audit/block/",
            "/audit/blocks/",
            "/audit/carry-forward-integrity",
            "/audit/commitments/",
            "/audit/fanouts/",
            "/audit/fanouts/pending",
            "/audit/latest",
            "/audit/ledger-integrity",
            "/audit/share-window",
            "/bundle",
            "/ctv-fanout-manifest-set",
            "/ctv-fanouts",
            "/healthz",
            "/metrics",
            "/miners/",
            "/owed",
            "/owed-balances",
            "/payouts",
            "/payouts/",
            "/status",
        }
    )

    # Every "/"-prefixed string constant in public_api.dispatch.
    EXPECTED_DISPATCH_LITERALS = frozenset(
        {
            "/",
            "/public/v1/artifacts/",
            "/public/v1/blocks",
            "/public/v1/blocks/",
            "/public/v1/fanouts/",
            "/public/v1/fanouts/pending",
            "/public/v1/hashrate-series",
            "/public/v1/leaderboard",
            "/public/v1/miners/",
            "/public/v1/mining-configuration",
            "/public/v1/pool-summary",
            "/settlement-artifacts",
        }
    )

    def registry_paths(self) -> set[str]:
        return {path for endpoint in ENDPOINTS for path in endpoint.paths}

    def assert_snapshot(self, literals: set[str], expected: frozenset[str]) -> None:
        added = sorted(literals - expected)
        removed = sorted(expected - literals)
        self.assertEqual(
            (added, removed),
            ([], []),
            "route literals in the HTTP layer changed without a matching "
            "classification update in lab/prism/endpoint_registry.py "
            f"(new: {added}, gone: {removed})",
        )

    def test_audit_http_route_literals_match_snapshot(self) -> None:
        literals = string_literals(AUDIT_HTTP_PATH, "do_GET")
        self.assertIn("/healthz", literals)
        self.assert_snapshot(literals, self.EXPECTED_DO_GET_LITERALS)

    def test_public_dispatch_route_literals_match_snapshot(self) -> None:
        literals = string_literals(PUBLIC_API_PATH, "dispatch")
        self.assertIn("/public/v1/pool-summary", literals)
        self.assert_snapshot(literals, self.EXPECTED_DISPATCH_LITERALS)

    def test_every_registry_path_is_reachable_through_a_route_literal(self) -> None:
        # The snapshots pin the literals; this pins the other direction: every
        # classified path must actually be matched by a literal from the
        # dispatcher that owns it, so the registry cannot list a path nothing
        # serves.
        public = set(endpoint_registry.extracted_paths())
        for path in self.registry_paths():
            surface = (
                self.EXPECTED_DISPATCH_LITERALS
                if path in public
                else self.EXPECTED_DO_GET_LITERALS
            )
            with self.subTest(path=path):
                self.assertTrue(
                    any(
                        path == literal or path.startswith(literal)
                        for literal in surface
                        if literal != "/"
                    ),
                    f"{path} is classified but no route literal in its "
                    "dispatcher can match it",
                )

    def test_path_counts_are_pinned(self) -> None:
        # The extraction set and the total surface are contract numbers:
        # PR 2 moves exactly the 13 /public/v1 routes, and the registry
        # classifies all 31 request paths the HTTP surface serves.
        self.assertEqual(13, len(endpoint_registry.extracted_paths()))
        self.assertEqual(31, len(self.registry_paths()))

    def test_http_handler_is_get_only(self) -> None:
        # The registry -- and the snapshot guard above -- only inspect do_GET
        # and dispatch. If a do_POST ever appears, its routes would be
        # invisible to the coverage tests, so pin the assumption the registry
        # rests on: the HTTP surface answers GET only.
        source = AUDIT_HTTP_PATH.read_text(encoding="utf-8")
        handler_methods = sorted(
            node.name
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.FunctionDef)
            and node.name.startswith("do_")
            and node.name.isupper() is False
            and node.name != "do_GET"
        )
        self.assertEqual(
            [],
            handler_methods,
            "the endpoint registry only covers GET; new HTTP verb methods "
            "need registry coverage first",
        )


if __name__ == "__main__":
    unittest.main()
