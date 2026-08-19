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

    def public_payload(
        self,
        path: str,
        query: dict[str, list[str]],
    ) -> tuple[int, object]:
        return 200, {"path": path}

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
    """No route literal may exist in the HTTP layer without a classification."""

    def registry_paths(self) -> set[str]:
        return {path for endpoint in ENDPOINTS for path in endpoint.paths}

    def normalized_registry_fragments(self) -> set[str]:
        """Registry paths with placeholders removed, for substring matching."""

        fragments: set[str] = set()
        for path in self.registry_paths():
            fragments.add(path)
            for placeholder in PLACEHOLDERS:
                path = path.replace(placeholder, "\x00")
            # Trailing placeholders leave an empty tail part, and a bare "/"
            # part matches everything -- both would make this guard vacuous.
            fragments.update(
                part for part in path.split("\x00") if part not in {"", "/"}
            )
        return fragments

    def assert_literals_are_classified(self, literals: set[str]) -> None:
        fragments = self.normalized_registry_fragments()
        unclassified = sorted(
            literal
            for literal in literals
            if not any(
                literal == fragment
                or fragment.startswith(literal)
                or literal.startswith(fragment)
                for fragment in fragments
            )
        )
        self.assertEqual(
            [],
            unclassified,
            "route literals present in the HTTP layer but absent from "
            "lab/prism/endpoint_registry.py",
        )

    def test_audit_http_route_literals_are_classified(self) -> None:
        literals = string_literals(AUDIT_HTTP_PATH, "do_GET")
        self.assertIn("/healthz", literals)
        self.assert_literals_are_classified(literals)

    def test_public_dispatch_route_literals_are_classified(self) -> None:
        literals = string_literals(PUBLIC_API_PATH, "dispatch")
        self.assertIn("/public/v1/pool-summary", literals)
        self.assert_literals_are_classified(literals)


if __name__ == "__main__":
    unittest.main()
