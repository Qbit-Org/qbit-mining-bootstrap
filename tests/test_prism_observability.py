#!/usr/bin/env python3
"""Direct ownership and safety tests for PRISM health observability."""

from __future__ import annotations

from dataclasses import replace
import threading
import time
import unittest
from types import SimpleNamespace

from lab.prism.observability import (
    HEALTH_SCHEMA,
    METRICS_STALE_WARNING,
    METRICS_STATES,
    METRICS_STATE_FRESH,
    METRICS_STATE_HEADER,
    METRICS_STATE_STALE,
    METRICS_STATE_UNAVAILABLE,
    MiningDeliveryInputs,
    ObservabilityService,
)
from lab.prism.prism_coordinator import (
    ClientState,
    PrismCoordinator,
    WorkerIdentity,
)


def healthy_inputs() -> MiningDeliveryInputs:
    return MiningDeliveryInputs(
        active_connections=0,
        connection_capacity=64,
        peak_active_connections=0,
        subscribed_connections=0,
        authorized_connections=0,
        pending_initial_jobs=0,
        pending_initial_job_capacity=32,
        oldest_pending_initial_job_age_seconds=0.0,
        oldest_genuinely_pending_initial_job_age_seconds=0.0,
        clients_with_current_tip_jobs=0,
        clients_with_semantically_current_work=0,
        clients_with_no_active_job=0,
        last_initial_job_delivery_monotonic=None,
        initial_job_timeout_seconds=5.0,
        initial_job_queue_rejections=0,
        initial_job_timeout_disconnects=0,
        initial_job_cancelled_tasks=0,
        initial_job_coalesced_tasks=0,
        initial_job_queue_capacity_reclaimed=0,
        handler_threads=0,
        delivery_executor_queue_depth=0,
        delivery_executor_active_workers=0,
        started_monotonic=0.0,
        startup_grace_seconds=0.0,
        stale_unknown_rejections=0,
        submitted_shares=0,
        job_preparation_pending=False,
        current_observed_tip=None,
        prepared_bundle_current=False,
        prepared_bundle_tip=None,
        prepared_bundle_template_generation=None,
        prepared_bundle_payout_generation=None,
    )


class FakeObservabilityPort:
    def __init__(self) -> None:
        self.now = 100.0
        self.inputs = healthy_inputs()
        self.progress: dict[str, object] = {
            "ok": True,
            "reason": None,
            "reasons": [],
        }
        self.raise_on_stats = False
        self.stats_calls = 0
        self.metrics_payload = "qbit_prism_fixture 1\n"
        self.metrics_error: Exception | None = None
        self.metrics_render_count = 0
        self.startup_phases: list[str] = []
        self.log_messages: list[str] = []
        self.exception_count = 0

    def monotonic(self) -> float:
        return self.now

    def mining_delivery_inputs(self, now: float) -> MiningDeliveryInputs:
        self.assert_current_time(now)
        return self.inputs

    def assert_current_time(self, now: float) -> None:
        if now != self.now:
            raise AssertionError(f"unexpected monotonic time: {now}")

    def accepted_share_stats(self) -> tuple[int, int]:
        self.stats_calls += 1
        if self.raise_on_stats:
            raise RuntimeError("stats unavailable")
        return 3, 2

    def ledger_backend(self) -> str:
        return "memory"

    def block_counts(self) -> tuple[int, int]:
        return 1, 2

    def progress_health(self) -> dict[str, object]:
        return dict(self.progress)

    def health_refresh_seconds(self) -> float:
        return 1.0

    def render_metrics_payload(self) -> str:
        self.metrics_render_count += 1
        if self.metrics_error is not None:
            raise self.metrics_error
        return self.metrics_payload

    def metrics_refresh_seconds(self) -> float:
        return 1.0

    def record_startup_phase(self, phase: str) -> None:
        self.startup_phases.append(phase)

    def stop_requested(self) -> bool:
        return False

    def wait_for_stop(self, timeout: float) -> bool:
        if timeout != 1.0:
            raise AssertionError(f"unexpected wait: {timeout}")
        return True

    def log(self, message: str) -> None:
        self.log_messages.append(message)

    def log_exception(self) -> None:
        self.exception_count += 1


class ObservabilityServiceTests(unittest.TestCase):
    def test_cached_base_health_uses_fresh_progress_overlay(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        service.refresh_health_snapshot()
        self.assertNotIn("reason", service.state().health_snapshot or {})

        port.progress = {
            "ok": False,
            "reason": "tip_poll_stale",
            "reasons": ["tip_poll_stale"],
        }
        status, payload = service.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["reason"], "tip_poll_stale")

    def test_refresh_stamps_health_snapshot_warm_startup_phase(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)

        service.refresh_health_snapshot()

        self.assertEqual(port.startup_phases, ["health_snapshot_warm"])

    def test_fresh_progress_cannot_mask_cached_base_failure(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = replace(
            port.inputs,
            subscribed_connections=1,
            authorized_connections=1,
            oldest_genuinely_pending_initial_job_age_seconds=6.0,
        )
        service = ObservabilityService(port)
        service.refresh_health_snapshot()

        port.inputs = healthy_inputs()
        status, payload = service.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertFalse(payload["ok"])
        self.assertIn("initial-delivery-stalled", payload["unhealthy_reasons"])

    def test_running_loop_without_snapshot_reports_starting_state(self) -> None:
        # Upstream #120: while the background warm-up runs, the endpoint
        # must fail closed with an explicit starting state instead of
        # collecting ledger aggregates inline on the handler thread.
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        self.assertTrue(service.begin_refresh_loop())

        status, payload = service.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["state"], "starting")
        self.assertEqual(payload["schema"], HEALTH_SCHEMA)
        self.assertEqual(port.stats_calls, 0)

    def test_stale_snapshot_fails_closed_with_current_progress(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        service.refresh_health_snapshot()
        port.now += 16.0
        port.progress = {
            "ok": False,
            "reason": "refresh_pending_too_long",
            "reasons": ["refresh_pending_too_long"],
        }

        status, payload = service.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["schema"], HEALTH_SCHEMA)
        self.assertEqual(payload["error"], "health snapshot is stale")
        self.assertEqual(payload["reason"], "refresh_pending_too_long")

    def test_delivery_timers_recover_after_sustained_overload(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = replace(
            port.inputs,
            active_connections=1,
            subscribed_connections=1,
            authorized_connections=1,
            pending_initial_jobs=32,
        )
        service = ObservabilityService(port)

        self.assertTrue(service.mining_delivery_snapshot()["mining_ready"])
        port.now += 6.0
        stalled = service.mining_delivery_snapshot()
        self.assertFalse(stalled["mining_ready"])
        self.assertEqual(
            stalled["unhealthy_reasons"],
            [
                "initial-delivery-stalled",
                "pending-initial-jobs-saturated",
            ],
        )

        port.inputs = replace(
            port.inputs,
            pending_initial_jobs=0,
            clients_with_current_tip_jobs=1,
        )
        recovered = service.mining_delivery_snapshot()
        self.assertTrue(recovered["mining_ready"])
        state = service.state()
        self.assertIsNone(state.mining_overload_started_monotonic)
        self.assertIsNone(state.mining_delivery_failure_started_monotonic)

    def test_semantic_gauges_report_count_and_six_decimal_ratio(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = replace(
            port.inputs,
            active_connections=3,
            subscribed_connections=3,
            authorized_connections=3,
            clients_with_current_tip_jobs=3,
            clients_with_semantically_current_work=1,
        )
        service = ObservabilityService(port)

        snapshot = service.mining_delivery_snapshot()

        self.assertEqual(snapshot["clients_with_semantically_current_work"], 1)
        self.assertEqual(snapshot["semantic_current_work_ratio"], round(1 / 3, 6))

    def test_semantic_ratio_is_one_with_zero_authorized_clients(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)

        snapshot = service.mining_delivery_snapshot()

        self.assertEqual(snapshot["clients_with_semantically_current_work"], 0)
        self.assertEqual(snapshot["semantic_current_work_ratio"], 1.0)
        self.assertTrue(snapshot["mining_ready"])

    def test_strict_coverage_alone_drives_fail_closed_health(self) -> None:
        # Perfect semantic currency must not mask a strict-coverage gap: the
        # strict gauge alone feeds the coverage timers and unhealthy reasons.
        port = FakeObservabilityPort()
        port.inputs = replace(
            port.inputs,
            active_connections=4,
            subscribed_connections=4,
            authorized_connections=4,
            clients_with_current_tip_jobs=0,
            clients_with_semantically_current_work=4,
        )
        service = ObservabilityService(port)

        first = service.mining_delivery_snapshot()
        self.assertTrue(first["mining_ready"])
        port.now += 6.0
        stalled = service.mining_delivery_snapshot()

        self.assertFalse(stalled["mining_ready"])
        self.assertIn("initial-delivery-stalled", stalled["unhealthy_reasons"])
        self.assertEqual(stalled["clients_with_semantically_current_work"], 4)
        self.assertEqual(stalled["semantic_current_work_ratio"], 1.0)

    def test_delivery_timer_observations_are_applied_in_capture_order(self) -> None:
        first_captured = threading.Event()
        release_first = threading.Event()
        second_captured = threading.Event()

        class OrderedPort(FakeObservabilityPort):
            def mining_delivery_inputs(self, now: float) -> MiningDeliveryInputs:
                if now == 100.0:
                    first_captured.set()
                    if not release_first.wait(1.0):
                        raise AssertionError("first health capture was not released")
                    return replace(
                        self.inputs,
                        authorized_connections=1,
                        clients_with_current_tip_jobs=0,
                    )
                second_captured.set()
                return replace(
                    self.inputs,
                    authorized_connections=1,
                    clients_with_current_tip_jobs=1,
                )

        port = OrderedPort()
        service = ObservabilityService(port)
        snapshots: list[dict[str, object]] = []
        first = threading.Thread(
            target=lambda: snapshots.append(
                service.mining_delivery_snapshot(now=100.0)
            )
        )
        second = threading.Thread(
            target=lambda: snapshots.append(
                service.mining_delivery_snapshot(now=101.0)
            )
        )
        first.start()
        self.assertTrue(first_captured.wait(1.0))
        second.start()
        self.assertFalse(second_captured.wait(0.05))
        release_first.set()
        for thread in (first, second):
            thread.join(1.0)
            self.assertFalse(thread.is_alive())

        self.assertEqual(len(snapshots), 2)
        self.assertTrue(second_captured.is_set())
        state = service.state()
        self.assertIsNone(state.mining_overload_started_monotonic)
        self.assertIsNone(state.mining_delivery_failure_started_monotonic)

    def test_refresh_loop_counts_failure_and_keeps_one_shot_latch(self) -> None:
        port = FakeObservabilityPort()
        port.raise_on_stats = True
        service = ObservabilityService(port)
        self.assertTrue(service.begin_refresh_loop())
        self.assertFalse(service.begin_refresh_loop())

        service.health_snapshot_loop()

        state = service.state()
        # Upstream #120: the running flag is a one-shot latch. Loop exit must
        # not re-open the legacy inline collection path for late requests.
        self.assertTrue(state.health_refresh_loop_running)
        self.assertEqual(state.health_snapshot_refresh_failure_count, 1)
        self.assertEqual(
            port.log_messages,
            ["prism coordinator: health snapshot refresh failed"],
        )
        self.assertEqual(port.exception_count, 1)

    def test_metrics_snapshot_preserves_renderer_bytes_and_adds_diagnostics(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)

        rendered = service.refresh_metrics_snapshot()
        status, cached = service.cached_metrics_payload()

        self.assertEqual(rendered, port.metrics_payload)
        self.assertEqual(status, 200)
        self.assertTrue(cached.startswith(port.metrics_payload))
        self.assertEqual(port.metrics_render_count, 1)
        self.assertIn("qbit_prism_metrics_snapshot_available 1\n", cached)
        self.assertIn("qbit_prism_metrics_snapshot_stale 0\n", cached)
        self.assertIn("qbit_prism_metrics_snapshot_generation 1\n", cached)

    def test_metrics_failure_preserves_prior_complete_generation(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        service.refresh_metrics_snapshot()
        port.metrics_payload = "partial"

        with self.assertRaises(ValueError):
            service.refresh_metrics_snapshot()

        state = service.metrics_state()
        self.assertEqual(state.metrics_snapshot, "qbit_prism_fixture 1\n")
        self.assertEqual(state.metrics_collection_generation, 1)
        self.assertEqual(state.metrics_collection_failure_count, 1)
        self.assertEqual(state.metrics_failure_invalid_payload_count, 1)
        status, cached = service.cached_metrics_payload()
        self.assertEqual(status, 200)
        self.assertTrue(cached.startswith("qbit_prism_fixture 1\n"))

        port.now += 16.0
        response = service.cached_metrics_payload()

        # The failed refresh left the last complete generation in place and
        # #184 keeps serving it. Refusing here discarded that payload along
        # with the failure counters that explain it, because Prometheus drops
        # the body of a non-200 response.
        self.assertEqual(response.status, 200)
        self.assertEqual(response.state, METRICS_STATE_STALE)
        self.assertTrue(response.body.startswith("qbit_prism_fixture 1\n"))
        self.assertIn("qbit_prism_metrics_snapshot_stale 1\n", response.body)
        self.assertIn(
            'qbit_prism_metrics_collection_failures_total{class="invalid_payload"} 1',
            response.body,
        )

    def test_metrics_without_snapshot_fails_closed_without_collecting(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)

        response = service.cached_metrics_payload()

        # Warm-up is the one case that still refuses: there is no complete
        # payload to serve, so the body is diagnostics only and a scraper must
        # not store it as this process's metrics.
        self.assertEqual(response.status, 503)
        self.assertEqual(response.state, METRICS_STATE_UNAVAILABLE)
        self.assertIsNone(response.age_seconds)
        self.assertEqual(port.metrics_render_count, 0)
        self.assertIn("qbit_prism_metrics_snapshot_available 0\n", response.body)
        self.assertIn(
            "qbit_prism_metrics_snapshot_age_seconds -1.000\n",
            response.body,
        )
        self.assertNotIn("qbit_prism_fixture", response.body)

    def test_stale_complete_snapshot_is_served_as_200_with_truthful_age(
        self,
    ) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        service.refresh_metrics_snapshot()

        port.now += 47.5

        response = service.cached_metrics_payload()

        self.assertEqual(response.status, 200)
        self.assertEqual(response.state, METRICS_STATE_STALE)
        # Byte-for-byte the generation that was published, with the current
        # diagnostics appended -- nothing re-rendered on this thread.
        self.assertTrue(response.body.startswith(port.metrics_payload))
        self.assertEqual(port.metrics_render_count, 1)
        self.assertIn("qbit_prism_metrics_snapshot_available 1\n", response.body)
        self.assertIn("qbit_prism_metrics_snapshot_stale 1\n", response.body)
        self.assertIn(
            "qbit_prism_metrics_snapshot_age_seconds 47.500\n",
            response.body,
        )
        # Floored, not rounded: Age must never overstate freshness.
        self.assertEqual(response.age_seconds, 47)

    def test_blocked_refresh_past_budget_serves_prior_document_promptly(
        self,
    ) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        service.refresh_metrics_snapshot()
        entered = threading.Event()
        release = threading.Event()
        render_calls: list[str] = []

        def blocked_render() -> str:
            render_calls.append("render")
            entered.set()
            release.wait(5.0)
            return "qbit_prism_fixture 2\n"

        port.render_metrics_payload = blocked_render  # type: ignore[method-assign]
        collector = threading.Thread(target=service.refresh_metrics_snapshot)
        collector.start()
        try:
            self.assertTrue(entered.wait(2.0))
            # Far past the staleness budget, with the collector still wedged
            # inside the renderer holding the collection lock.
            port.now += 600.0

            started = time.monotonic()
            response = service.cached_metrics_payload()
            elapsed = time.monotonic() - started
        finally:
            release.set()
            collector.join(5.0)

        # Never waited on the stuck collector, and never re-entered the
        # renderer itself: the only render is the collector's own.
        self.assertLess(elapsed, 1.0)
        self.assertEqual(render_calls, ["render"])
        self.assertEqual(response.status, 200)
        self.assertEqual(response.state, METRICS_STATE_STALE)
        self.assertTrue(response.body.startswith("qbit_prism_fixture 1\n"))
        self.assertEqual(response.age_seconds, 600)
        self.assertFalse(collector.is_alive())

    def test_metrics_response_metadata_is_exact_and_bounded(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)

        unavailable = service.cached_metrics_payload()
        service.refresh_metrics_snapshot()
        port.now += 2.0
        fresh = service.cached_metrics_payload()
        port.now += 20.0
        stale = service.cached_metrics_payload()

        self.assertEqual(
            unavailable.response_headers(),
            {
                "Cache-Control": "no-store",
                METRICS_STATE_HEADER: "unavailable",
            },
        )
        self.assertEqual(
            fresh.response_headers(),
            {
                "Cache-Control": "no-store",
                METRICS_STATE_HEADER: "fresh",
                "Age": "2",
            },
        )
        self.assertEqual(
            stale.response_headers(),
            {
                "Cache-Control": "no-store",
                METRICS_STATE_HEADER: "stale",
                "Age": "22",
                "Warning": METRICS_STALE_WARNING,
            },
        )
        # Bounded vocabulary, and the warn-code line is the registered 110.
        self.assertEqual(METRICS_STATES, ("fresh", "stale", "unavailable"))
        for response in (unavailable, fresh, stale):
            self.assertIn(response.state, METRICS_STATES)
        self.assertEqual(
            METRICS_STALE_WARNING,
            '110 qbit-prism "metrics snapshot is stale; '
            'serving last complete payload"',
        )
        # Still destructures as the pair every pre-#184 caller unpacks.
        status, body = fresh
        self.assertEqual((status, body), (fresh.status, fresh.body))
        self.assertEqual(fresh.state, METRICS_STATE_FRESH)

    def test_metrics_exception_is_bounded_and_recovery_replaces_snapshot(
        self,
    ) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        service.refresh_metrics_snapshot()
        port.metrics_error = RuntimeError("dynamic backend detail")

        with self.assertRaises(RuntimeError):
            service.refresh_metrics_snapshot()

        state = service.metrics_state()
        self.assertEqual(state.metrics_failure_exception_count, 1)
        self.assertEqual(state.metrics_last_failure_class, "exception")
        _, failed = service.cached_metrics_payload()
        self.assertNotIn("dynamic backend detail", failed)

        port.metrics_error = None
        port.metrics_payload = "qbit_prism_fixture 2\n"
        service.refresh_metrics_snapshot()

        recovered = service.metrics_state()
        self.assertEqual(recovered.metrics_collection_generation, 2)
        self.assertEqual(recovered.metrics_collection_success_count, 2)
        self.assertEqual(recovered.metrics_collection_failure_count, 1)
        self.assertIsNone(recovered.metrics_last_failure_class)
        status, cached = service.cached_metrics_payload()
        self.assertEqual(status, 200)
        self.assertTrue(cached.startswith("qbit_prism_fixture 2\n"))
        self.assertIn(
            'qbit_prism_metrics_collection_failures_total{class="exception"} 1',
            cached,
        )

    def test_slow_metrics_collection_does_not_block_or_partially_replace_cache(
        self,
    ) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        service.refresh_metrics_snapshot()
        entered = threading.Event()
        release = threading.Event()

        def slow_render() -> str:
            entered.set()
            release.wait(2.0)
            return "qbit_prism_fixture 2\n"

        port.render_metrics_payload = slow_render  # type: ignore[method-assign]
        collector = threading.Thread(target=service.refresh_metrics_snapshot)
        collector.start()
        self.assertTrue(entered.wait(1.0))

        status, during = service.cached_metrics_payload()

        self.assertEqual(status, 200)
        self.assertTrue(during.startswith("qbit_prism_fixture 1\n"))
        release.set()
        collector.join(2.0)
        self.assertFalse(collector.is_alive())
        _, after = service.cached_metrics_payload()
        self.assertTrue(after.startswith("qbit_prism_fixture 2\n"))

    def test_metrics_and_health_loop_state_are_independent(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)

        self.assertTrue(service.begin_refresh_loop())
        self.assertTrue(service.begin_metrics_refresh_loop())
        self.assertTrue(service.state().health_refresh_loop_running)
        self.assertTrue(service.metrics_state().metrics_refresh_loop_running)

        service.metrics_snapshot_loop()

        self.assertTrue(service.state().health_refresh_loop_running)
        self.assertFalse(service.metrics_state().metrics_refresh_loop_running)


def bare_semantic_coordinator() -> PrismCoordinator:
    server = PrismCoordinator.__new__(PrismCoordinator)
    server.lock = threading.RLock()
    server.stop_event = threading.Event()
    server.clients = set()
    server.jobs = {}
    server.connection_counter = 0
    server.job_counter = 0
    server.stratum_max_connections = 8
    server.stratum_max_connections_per_username = 0
    server.stratum_max_pending_initial_jobs = 4
    server.stratum_initial_job_timeout_seconds = 30.0
    server.mining_health_startup_grace_seconds = 30.0
    server.tip_refresh_max_workers = 1
    server.tip_template_snapshot = None
    server.started_monotonic = time.monotonic()
    server.submitted_share_count = 0
    server.rejection_counts_by_reason = {}
    server._ensure_job_cache_state()
    server._ensure_tip_refresh_state()
    server._ensure_initial_job_state()
    return server


def semantic_client(server: PrismCoordinator, connection_id: int) -> ClientState:
    state = ClientState(
        sock=SimpleNamespace(),
        address=("127.0.0.1", 40_000 + connection_id),
        connection_id=connection_id,
        extranonce1_hex=f"{connection_id:08x}",
    )
    state.subscribed = True
    state.authorized = True
    state.authorization_generation = 1
    state.worker = WorkerIdentity(
        username=f"miner-{connection_id}",
        payout_address=f"miner-{connection_id}",
        worker_name=None,
        script_pubkey_hex="5220" + "11" * 32,
        p2mr_program_hex="11" * 32,
    )
    state.username = state.worker.username
    server.clients.add(state)
    return state


class SemanticCurrencyAdapterTests(unittest.TestCase):
    def test_fingerprint_and_payout_generation_alone_determine_the_count(
        self,
    ) -> None:
        # Upstream #107: the semantic gauge compares template fingerprint and
        # payout generation only. Template generation and template object
        # identity do not participate, and the strict fail-closed gauge stays
        # untouched by semantic matches.
        server = bare_semantic_coordinator()
        fingerprint = "ff" * 32
        server.tip_template_snapshot = SimpleNamespace(
            bestblockhash="aa" * 32,
            template_fingerprint=fingerprint,
            template_generation=7,
            template_artifacts=None,
        )
        matching = semantic_client(server, 1)
        matching.active_job = SimpleNamespace(
            template={"previousblockhash": "aa" * 32},
            template_fingerprint=fingerprint,
            # Deliberately different template generation and a distinct
            # template object: neither may disqualify semantic currency.
            template_generation=3,
            payout_state_generation=0,
        )
        mismatched = semantic_client(server, 2)
        mismatched.active_job = SimpleNamespace(
            template={"previousblockhash": "aa" * 32},
            template_fingerprint="ee" * 32,
            template_generation=7,
            payout_state_generation=0,
        )

        snapshot = server.mining_delivery_snapshot()

        self.assertEqual(snapshot["clients_with_semantically_current_work"], 1)
        self.assertEqual(snapshot["semantic_current_work_ratio"], 0.5)
        self.assertEqual(snapshot["clients_with_current_tip_jobs"], 0)
        self.assertEqual(snapshot["current_tip_job_coverage"], 0.0)
        self.assertTrue(snapshot["mining_ready"])

    def test_stale_payout_generation_disqualifies_semantic_currency(self) -> None:
        server = bare_semantic_coordinator()
        fingerprint = "ff" * 32
        server.tip_template_snapshot = SimpleNamespace(
            bestblockhash="aa" * 32,
            template_fingerprint=fingerprint,
            template_generation=7,
            template_artifacts=None,
        )
        stale = semantic_client(server, 1)
        stale.active_job = SimpleNamespace(
            template={"previousblockhash": "aa" * 32},
            template_fingerprint=fingerprint,
            template_generation=7,
            payout_state_generation=99,
        )

        snapshot = server.mining_delivery_snapshot()

        self.assertEqual(snapshot["clients_with_semantically_current_work"], 0)
        self.assertEqual(snapshot["semantic_current_work_ratio"], 0.0)


if __name__ == "__main__":
    unittest.main()
