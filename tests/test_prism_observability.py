#!/usr/bin/env python3
"""Direct ownership and safety tests for PRISM health observability."""

from __future__ import annotations

from dataclasses import replace
import json
import threading
import time
import unittest
from types import SimpleNamespace
import urllib.error
import urllib.request

from lab.prism.audit_http import AuditHttpConfig, AuditHttpFacade
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
    _CoordinatorAuditHttp,
)
from lab.prism.progress_health import (
    MINING_READINESS_REASONS,
    MINING_READINESS_SCHEMA,
    MiningReadinessConfig,
)
from tests.prism_coordinator_test_support import ObservedRLock, coordinator


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


def incident_inputs() -> MiningDeliveryInputs:
    """The 2026-08-31 fleet: 4 of 33 miners on the exact observed tip while
    all 33 held semantically current work and nothing was genuinely pending."""

    return replace(
        healthy_inputs(),
        active_connections=33,
        subscribed_connections=33,
        authorized_connections=33,
        clients_with_current_tip_jobs=4,
        clients_with_semantically_current_work=33,
        last_initial_job_delivery_monotonic=99.5,
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
        self.inputs_calls = 0
        self.progress_calls = 0
        self.readiness_config = MiningReadinessConfig()
        self.preview_timeouts = 0
        self.preview_timeout_reads = 0
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
        self.inputs_calls += 1
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
        self.progress_calls += 1
        return dict(self.progress)

    def health_refresh_seconds(self) -> float:
        return 1.0

    def mining_readiness_config(self) -> MiningReadinessConfig:
        return self.readiness_config

    def accepted_parent_preview_wait_timeouts(self) -> int:
        self.preview_timeout_reads += 1
        return self.preview_timeouts

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
            clients_with_semantically_current_work=1,
        )
        recovered = service.mining_delivery_snapshot()
        self.assertTrue(recovered["mining_ready"])
        state = service.state()
        self.assertIsNone(state.mining_overload_started_monotonic)
        self.assertIsNone(state.mining_delivery_failure_started_monotonic)
        self.assertIsNone(state.mining_semantic_work_gap_started_monotonic)

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


class SemanticCoverageDeliveryHealthTests(unittest.TestCase):
    """Issue #216: the delivery stall is timed on semantic coverage."""

    def test_rapid_tip_churn_with_complete_semantic_coverage_stays_ready(
        self,
    ) -> None:
        port = FakeObservabilityPort()
        port.inputs = incident_inputs()
        service = ObservabilityService(port)

        # Exact coverage churns below 95% for four times the 5s deadline
        # while every miner keeps semantically current work, nothing is
        # genuinely pending, and deliveries keep landing. The refresher's
        # cached /healthz answer must hold 200 on every sample, not flap.
        for step, exact in enumerate((4, 12, 1, 20, 7, 30)):
            port.now = 100.0 + 4.0 * step
            port.inputs = replace(
                port.inputs,
                clients_with_current_tip_jobs=exact,
                last_initial_job_delivery_monotonic=port.now - 0.5,
            )
            service.refresh_health_snapshot()
            status, payload = service.cached_health_payload()

            self.assertEqual(status, 200, payload)
            self.assertTrue(payload["ok"])
            self.assertTrue(payload["mining_ready"])
            self.assertFalse(payload["initial_delivery_stalled"])
            self.assertEqual(payload["unhealthy_reasons"], [])
            self.assertEqual(payload["pending_initial_jobs"], 0)
            self.assertEqual(
                payload["oldest_genuinely_pending_initial_job_age_seconds"],
                0.0,
            )
            self.assertLess(payload["current_tip_job_coverage"], 0.95)
            self.assertEqual(payload["semantic_current_work_ratio"], 1.0)
            # The exact gap keeps reporting the churn as telemetry.
            self.assertEqual(
                payload["current_tip_coverage_gap_age_seconds"],
                4.0 * step,
            )
            self.assertEqual(payload["semantic_current_work_gap_age_seconds"], 0.0)

        state = service.state()
        self.assertEqual(state.mining_delivery_failure_started_monotonic, 100.0)
        self.assertIsNone(state.mining_semantic_work_gap_started_monotonic)

    def test_genuine_first_job_starvation_fails_closed_after_deadline(
        self,
    ) -> None:
        port = FakeObservabilityPort()
        port.inputs = replace(
            healthy_inputs(),
            active_connections=3,
            subscribed_connections=3,
            authorized_connections=3,
            pending_initial_jobs=3,
            oldest_pending_initial_job_age_seconds=4.999,
            oldest_genuinely_pending_initial_job_age_seconds=4.999,
            clients_with_no_active_job=3,
        )
        service = ObservabilityService(port)

        self.assertTrue(service.mining_delivery_snapshot()["mining_ready"])

        # Only the genuine pending age crosses the deadline here: both
        # coverage gaps are a millisecond old, so starvation fires on its own.
        port.now += 0.001
        port.inputs = replace(
            port.inputs,
            oldest_pending_initial_job_age_seconds=5.0,
            oldest_genuinely_pending_initial_job_age_seconds=5.0,
        )
        starved = service.mining_delivery_snapshot()

        self.assertFalse(starved["mining_ready"])
        self.assertTrue(starved["initial_delivery_stalled"])
        self.assertIn("initial-delivery-stalled", starved["unhealthy_reasons"])
        self.assertEqual(starved["semantic_current_work_gap_age_seconds"], 0.001)

    def test_sustained_semantic_work_loss_fails_closed_after_deadline(
        self,
    ) -> None:
        # Every miner holds work, so nothing is genuinely pending, but none
        # of it matches the current template content and none is on the
        # observed tip: fanout has stopped. This must still fail closed.
        port = FakeObservabilityPort()
        port.inputs = replace(
            healthy_inputs(),
            active_connections=4,
            subscribed_connections=4,
            authorized_connections=4,
            clients_with_current_tip_jobs=0,
            clients_with_semantically_current_work=0,
        )
        service = ObservabilityService(port)

        self.assertTrue(service.mining_delivery_snapshot()["mining_ready"])
        port.now = 104.999
        grace = service.mining_delivery_snapshot()
        self.assertTrue(grace["mining_ready"])
        self.assertEqual(grace["semantic_current_work_gap_age_seconds"], 4.999)
        self.assertEqual(grace["current_tip_coverage_gap_age_seconds"], 4.999)

        port.now = 105.0
        stalled = service.mining_delivery_snapshot()
        self.assertFalse(stalled["mining_ready"])
        self.assertTrue(stalled["initial_delivery_stalled"])
        self.assertEqual(stalled["unhealthy_reasons"], ["initial-delivery-stalled"])
        self.assertEqual(
            stalled["oldest_genuinely_pending_initial_job_age_seconds"],
            0.0,
        )

        # Semantic recovery alone ends the stall and closes the semantic gap
        # while the strict gauge is still lagging the observed tip.
        port.now = 106.0
        port.inputs = replace(port.inputs, clients_with_semantically_current_work=4)
        recovered = service.mining_delivery_snapshot()
        self.assertTrue(recovered["mining_ready"])
        self.assertFalse(recovered["initial_delivery_stalled"])
        self.assertEqual(recovered["semantic_current_work_gap_age_seconds"], 0.0)
        self.assertEqual(recovered["current_tip_coverage_gap_age_seconds"], 6.0)
        state = service.state()
        self.assertIsNone(state.mining_semantic_work_gap_started_monotonic)
        self.assertEqual(state.mining_delivery_failure_started_monotonic, 100.0)

    def test_semantic_gap_must_be_sustained_not_sampled(self) -> None:
        # A long-running exact gap plus one poor semantic sample is not a
        # stall: the semantic gap has its own timer and must itself last a
        # full deadline before health fails.
        port = FakeObservabilityPort()
        port.inputs = incident_inputs()
        service = ObservabilityService(port)
        service.mining_delivery_snapshot()

        port.now = 120.0
        port.inputs = replace(port.inputs, clients_with_semantically_current_work=20)
        dip = service.mining_delivery_snapshot()
        self.assertTrue(dip["mining_ready"])
        self.assertEqual(dip["current_tip_coverage_gap_age_seconds"], 20.0)
        self.assertEqual(dip["semantic_current_work_gap_age_seconds"], 0.0)

        port.now = 124.0
        port.inputs = replace(port.inputs, clients_with_semantically_current_work=33)
        back = service.mining_delivery_snapshot()
        self.assertTrue(back["mining_ready"])
        self.assertIsNone(service.state().mining_semantic_work_gap_started_monotonic)

        port.now = 128.0
        port.inputs = replace(port.inputs, clients_with_semantically_current_work=20)
        self.assertTrue(service.mining_delivery_snapshot()["mining_ready"])
        port.now = 132.999
        self.assertTrue(service.mining_delivery_snapshot()["mining_ready"])
        port.now = 133.0
        stalled = service.mining_delivery_snapshot()
        self.assertFalse(stalled["mining_ready"])
        self.assertEqual(stalled["unhealthy_reasons"], ["initial-delivery-stalled"])
        self.assertEqual(stalled["semantic_current_work_gap_age_seconds"], 5.0)

    def test_exact_tip_work_without_semantic_evidence_keeps_health_ready(
        self,
    ) -> None:
        # An embedder that publishes only the observed tip (no template
        # snapshot) reads semantic coverage 0 for every client while the
        # strict gauge is complete. In production strict currency is checked
        # against the same fingerprint and payout generation, so this state
        # means "no semantic evidence", never "semantically stale", and the
        # strict gauge must still count as delivered work.
        port = FakeObservabilityPort()
        port.inputs = replace(
            healthy_inputs(),
            active_connections=4,
            subscribed_connections=4,
            authorized_connections=4,
            clients_with_current_tip_jobs=4,
            clients_with_semantically_current_work=0,
        )
        service = ObservabilityService(port)

        service.mining_delivery_snapshot()
        port.now += 10.0
        payload = service.mining_delivery_snapshot()

        self.assertTrue(payload["mining_ready"])
        self.assertFalse(payload["initial_delivery_stalled"])
        self.assertEqual(payload["semantic_current_work_gap_age_seconds"], 10.0)
        self.assertEqual(payload["current_tip_coverage_gap_age_seconds"], 0.0)

    def test_incident_payload_retains_exact_and_semantic_telemetry(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = incident_inputs()
        service = ObservabilityService(port)
        service.mining_delivery_snapshot()

        port.now += 6.0
        payload = service.mining_delivery_snapshot()

        self.assertEqual(payload["authorized_connections"], 33)
        self.assertEqual(payload["clients_with_current_tip_jobs"], 4)
        self.assertEqual(payload["current_tip_job_coverage"], 0.121212)
        self.assertEqual(payload["clients_with_semantically_current_work"], 33)
        self.assertEqual(payload["semantic_current_work_ratio"], 1.0)
        self.assertEqual(payload["pending_initial_jobs"], 0)
        self.assertEqual(
            payload["oldest_genuinely_pending_initial_job_age_seconds"],
            0.0,
        )
        self.assertEqual(payload["clients_with_no_active_job"], 0)
        self.assertEqual(payload["current_tip_coverage_gap_age_seconds"], 6.0)
        self.assertEqual(payload["semantic_current_work_gap_age_seconds"], 0.0)
        # Compatibility aliases keep their exact-tip meaning.
        self.assertEqual(payload["clients_with_current_tip_job"], 4)
        self.assertEqual(payload["clients_without_current_tip_job"], 29)
        self.assertEqual(payload["current_tip_job_coverage_ratio"], 4 / 33)
        self.assertEqual(payload["oldest_initial_job_pending_seconds"], 0.0)
        self.assertTrue(payload["mining_ready"])
        self.assertTrue(payload["mining_delivery_healthy"])
        self.assertFalse(payload["initial_delivery_stalled"])
        self.assertFalse(payload["overload"])
        self.assertEqual(payload["unhealthy_reasons"], [])

    def test_overload_now_keeps_using_exact_coverage(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = replace(
            incident_inputs(),
            active_connections=64,
            subscribed_connections=64,
            authorized_connections=64,
            clients_with_current_tip_jobs=8,
            clients_with_semantically_current_work=64,
        )
        service = ObservabilityService(port)

        strict_pressure = service.mining_delivery_snapshot()

        self.assertTrue(strict_pressure["connection_capacity_saturated"])
        self.assertTrue(strict_pressure["overload"])
        self.assertTrue(strict_pressure["mining_ready"])
        self.assertEqual(
            service.state().mining_overload_started_monotonic,
            100.0,
        )

        # Semantic loss with complete exact coverage is not overload.
        port.now += 1.0
        port.inputs = replace(
            port.inputs,
            clients_with_current_tip_jobs=64,
            clients_with_semantically_current_work=0,
        )
        semantic_only = service.mining_delivery_snapshot()

        self.assertTrue(semantic_only["connection_capacity_saturated"])
        self.assertFalse(semantic_only["overload"])
        self.assertEqual(semantic_only["overload_age_seconds"], 0.0)
        self.assertIsNone(service.state().mining_overload_started_monotonic)

    def test_reject_storm_keeps_using_exact_coverage(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = replace(
            incident_inputs(),
            submitted_shares=100,
            stale_unknown_rejections=95,
        )
        service = ObservabilityService(port)

        storm = service.mining_delivery_snapshot()

        self.assertTrue(storm["overload"])
        self.assertFalse(storm["mining_ready"])
        self.assertEqual(
            storm["unhealthy_reasons"],
            ["stale-unknown-rejection-storm"],
        )

        # The same rejection ratio with complete exact coverage and no
        # semantic coverage is not a storm: the semantic gauge does not
        # corroborate it.
        port.now += 1.0
        port.inputs = replace(
            port.inputs,
            clients_with_current_tip_jobs=33,
            clients_with_semantically_current_work=0,
        )
        quiet = service.mining_delivery_snapshot()

        self.assertFalse(quiet["overload"])
        self.assertTrue(quiet["mining_ready"])
        self.assertEqual(quiet["unhealthy_reasons"], [])

    def test_cap_saturated_reason_keeps_using_exact_coverage(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = replace(
            incident_inputs(),
            active_connections=64,
            subscribed_connections=64,
            authorized_connections=64,
            clients_with_current_tip_jobs=8,
            clients_with_semantically_current_work=64,
        )
        service = ObservabilityService(port)
        service.mining_delivery_snapshot()

        port.now += 5.0
        persistent = service.mining_delivery_snapshot()

        # Strict-tip pressure under a full cap is surfaced on its own; the
        # delivery stall does not ride along on the exact gap.
        self.assertFalse(persistent["mining_ready"])
        self.assertEqual(
            persistent["unhealthy_reasons"],
            ["connection-capacity-saturated"],
        )
        self.assertFalse(persistent["initial_delivery_stalled"])
        self.assertEqual(persistent["current_tip_coverage_gap_age_seconds"], 5.0)

        # Exact coverage restored under the same full cap clears the reason
        # even though semantic coverage is now gone.
        port.now += 5.0
        port.inputs = replace(
            port.inputs,
            clients_with_current_tip_jobs=64,
            clients_with_semantically_current_work=0,
        )
        cleared = service.mining_delivery_snapshot()

        self.assertTrue(cleared["connection_capacity_saturated"])
        self.assertTrue(cleared["mining_ready"])
        self.assertEqual(cleared["unhealthy_reasons"], [])


def tip_cycle_inputs(semantic: int, authorized: int = 12) -> MiningDeliveryInputs:
    return replace(
        healthy_inputs(),
        active_connections=authorized,
        subscribed_connections=authorized,
        authorized_connections=authorized,
        clients_with_current_tip_jobs=semantic,
        clients_with_semantically_current_work=semantic,
    )


class MiningReadinessCacheTests(unittest.TestCase):
    """Issue #186: the latched signal is sampled by the refresher only."""

    def assert_no_live_reads(self, port: FakeObservabilityPort) -> None:
        self.assertEqual(port.inputs_calls, 0)
        self.assertEqual(port.stats_calls, 0)
        self.assertEqual(port.progress_calls, 0)
        self.assertEqual(port.preview_timeout_reads, 0)

    def test_warm_up_fails_closed_without_sampling_on_the_request_thread(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)

        status, payload = service.cached_mining_readiness_payload()

        self.assertEqual(status, 503)
        self.assertEqual(payload["schema"], MINING_READINESS_SCHEMA)
        self.assertFalse(payload["ready"])
        self.assertEqual(payload["state"], "degraded")
        self.assertEqual(payload["reasons"], ["warming_up"])
        self.assertEqual(payload["state_age_seconds"], 0.0)
        self.assertIsNone(payload["semantic_current_work_ratio"])
        self.assertIsNone(payload["refresh_pending_age_seconds"])
        self.assertIsNone(payload["oldest_durable_candidate_age_seconds"])
        self.assertEqual(payload["accepted_parent_preview_timeout_rate_per_second"], 0.0)
        self.assertEqual(payload["entry_dwell_seconds"], 60.0)
        self.assertEqual(payload["recovery_window_seconds"], 240.0)
        self.assertTrue(payload["sample_stale"])
        self.assertIsNone(service.mining_readiness_snapshot())
        # Unlike /healthz, there is no inline fallback when no refresher
        # runs: warm-up stays a cache miss whether or not the loop is armed.
        self.assertTrue(service.begin_refresh_loop())
        self.assertEqual(service.cached_mining_readiness_payload()[0], 503)
        self.assert_no_live_reads(port)

    def test_refresh_publishes_one_snapshot_and_requests_only_copy_it(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = tip_cycle_inputs(12)
        port.progress = {
            "ok": True,
            "reason": None,
            "reasons": [],
            "pending_refresh": False,
            "pending_refresh_age_seconds": None,
            "eligible_clients_requiring_refresh": 0,
        }
        service = ObservabilityService(port)
        service.refresh_health_snapshot()
        reads = (port.inputs_calls, port.stats_calls, port.progress_calls, port.preview_timeout_reads)
        self.assertEqual(reads, (1, 1, 1, 1))

        status, payload = service.cached_mining_readiness_payload()

        self.assertEqual(status, 200)
        self.assertTrue(payload["ready"])
        self.assertEqual(payload["state"], "ready")
        self.assertEqual(payload["reasons"], [])
        self.assertEqual(payload["semantic_current_work_ratio"], 1.0)
        self.assertEqual(payload["transitions"], 0)
        self.assertEqual(payload["sample_age_seconds"], 0.0)
        self.assertFalse(payload["sample_stale"])
        self.assertEqual(payload["sample_stale_after_seconds"], 15.0)

        # The world changes underneath without a refresh: the request keeps
        # serving the latched copy, ages it, and reads nothing live.
        port.inputs = tip_cycle_inputs(0)
        port.progress = {
            "ok": False,
            "reason": "refresh_pending_too_long",
            "reasons": ["refresh_pending_too_long"],
        }
        port.now += 20.0
        status, later = service.cached_mining_readiness_payload()
        self.assertEqual(status, 200)
        self.assertEqual(later["semantic_current_work_ratio"], 1.0)
        self.assertEqual(later["state_age_seconds"], 20.0)
        self.assertEqual(later["sample_age_seconds"], 20.0)
        self.assertTrue(later["sample_stale"])
        self.assertEqual(
            (port.inputs_calls, port.stats_calls, port.progress_calls, port.preview_timeout_reads),
            reads,
        )
        # /healthz keeps its own semantics: the fresh progress overlay
        # fails it right now while readiness holds its latch.
        health_status, health = service.cached_health_payload()
        self.assertEqual(health_status, 503)
        self.assertEqual(health["reason"], "refresh_pending_too_long")

    def test_sustained_entry_condition_degrades_the_cached_answer_once(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = tip_cycle_inputs(12)
        port.progress = {"ok": True, "reason": None, "reasons": []}
        service = ObservabilityService(port)
        service.refresh_health_snapshot()

        port.inputs = tip_cycle_inputs(1)
        port.progress = {
            "ok": False,
            "reason": "refresh_pending_too_long",
            "reasons": ["refresh_pending_too_long", "current_generation_not_delivered"],
            "pending_refresh": True,
            "pending_refresh_age_seconds": 30.0,
            "eligible_clients_requiring_refresh": 11,
        }
        statuses: list[int] = []
        for step in range(1, 14):
            port.now = 100.0 + 5.0 * step
            service.refresh_health_snapshot()
            statuses.append(service.cached_mining_readiness_payload()[0])
        # The bad streak starts at 105s and is ready through 55s of dwell;
        # the sample at 165s is the first with a full 60s behind it.
        self.assertEqual(statuses, [200] * 12 + [503])

        status, payload = service.cached_mining_readiness_payload()
        self.assertEqual(status, 503)
        self.assertFalse(payload["ready"])
        self.assertEqual(payload["state"], "degraded")
        self.assertEqual(
            payload["reasons"],
            ["semantic_coverage_low", "refresh_pending_too_long"],
        )
        self.assertEqual(payload["transitions"], 1)
        self.assertEqual(payload["state_age_seconds"], 0.0)
        self.assertEqual(payload["semantic_current_work_ratio"], round(1 / 12, 6))
        self.assertEqual(payload["refresh_pending_age_seconds"], 30.0)
        self.assertTrue(payload["refresh_pending_too_long"])
        self.assertEqual(payload["eligible_clients_requiring_refresh"], 11)
        # Every reason served is drawn from the pinned vocabulary.
        for reason in payload["reasons"]:
            self.assertIn(reason, MINING_READINESS_REASONS)

    def test_2026_08_21_tip_cycles_through_the_refresher_stay_200(self) -> None:
        # The refresher sees each accepted tip as 0 -> 10/12 -> 11/12 -> 12/12
        # -> 0 with the refresh resolving inside the ~36.77s normal cycle.
        port = FakeObservabilityPort()
        service = ObservabilityService(port)
        steps = (
            (0.0, 0, True),
            (5.0, 0, True),
            (10.0, 0, True),
            (15.0, 0, True),
            (20.0, 10, True),
            (25.0, 10, True),
            (30.0, 11, True),
            (35.0, 11, True),
            (36.77, 12, False),
            (40.0, 12, False),
        )
        seen_ratios: set[float] = set()
        for cycle in range(30):
            start = 100.0 + 45.0 * cycle
            for offset, semantic, pending in steps:
                port.now = start + offset
                port.inputs = tip_cycle_inputs(semantic)
                port.progress = {
                    "ok": not (pending and offset > 15.0),
                    "reason": None,
                    "reasons": ["refresh_pending_too_long"] if pending and offset > 15.0 else [],
                    "pending_refresh": pending,
                    "pending_refresh_age_seconds": offset if pending else None,
                    "eligible_clients_requiring_refresh": 12 - semantic,
                }
                service.refresh_health_snapshot()
                status, payload = service.cached_mining_readiness_payload()
                seen_ratios.add(payload["semantic_current_work_ratio"])
                self.assertEqual(status, 200, payload)
                self.assertTrue(payload["ready"])
                self.assertEqual(payload["transitions"], 0)
                self.assertAlmostEqual(payload["state_age_seconds"], port.now - 100.0, places=3)
        self.assertEqual(seen_ratios, {0.0, round(10 / 12, 6), round(11 / 12, 6), 1.0})

    def test_candidate_age_is_handed_over_by_the_metrics_renderer(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = tip_cycle_inputs(12)
        service = ObservabilityService(port)
        # The gauge's -1 "unavailable" and a missing handoff both read None.
        service.refresh_health_snapshot()
        self.assertIsNone(service.cached_mining_readiness_payload()[1]["oldest_durable_candidate_age_seconds"])
        service.record_oldest_durable_candidate_age(-1.0)
        port.now += 5.0
        service.refresh_health_snapshot()
        self.assertIsNone(service.cached_mining_readiness_payload()[1]["oldest_durable_candidate_age_seconds"])

        service.record_oldest_durable_candidate_age(75.25)
        port.now += 5.0
        service.refresh_health_snapshot()
        _, ready = service.cached_mining_readiness_payload()
        self.assertEqual(ready["oldest_durable_candidate_age_seconds"], 75.25)
        # Annotates only a degraded snapshot; never a transition cause.
        self.assertEqual(ready["reasons"], [])
        self.assertTrue(ready["ready"])

    def test_health_refresh_failure_keeps_the_last_readiness_snapshot(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = tip_cycle_inputs(12)
        service = ObservabilityService(port)
        service.refresh_health_snapshot()
        before = service.mining_readiness_snapshot()

        port.raise_on_stats = True
        port.now += 5.0
        with self.assertRaises(RuntimeError):
            service.refresh_health_snapshot()

        self.assertIs(service.mining_readiness_snapshot(), before)
        self.assertEqual(service.cached_mining_readiness_payload()[0], 200)

    def test_refresh_gap_cannot_complete_recovery(self) -> None:
        port = FakeObservabilityPort()
        port.readiness_config = MiningReadinessConfig(
            entry_dwell_seconds=0.0,
            recovery_window_seconds=20.0,
        )
        port.inputs = tip_cycle_inputs(0)
        port.progress = {
            "ok": False,
            "reason": "refresh_pending_too_long",
            "reasons": ["refresh_pending_too_long"],
            "pending_refresh": True,
            "pending_refresh_age_seconds": 30.0,
            "eligible_clients_requiring_refresh": 12,
        }
        service = ObservabilityService(port)
        service.refresh_health_snapshot()
        self.assertEqual(service.cached_mining_readiness_payload()[0], 503)

        port.inputs = tip_cycle_inputs(12)
        port.progress = {
            "ok": True,
            "reason": None,
            "reasons": [],
            "pending_refresh": False,
            "pending_refresh_age_seconds": None,
            "eligible_clients_requiring_refresh": 0,
        }
        port.now = 105.0
        service.refresh_health_snapshot()

        # This failed refresh publishes no sample. The next success is more
        # than the 15-second cache-staleness budget after the prior success.
        port.raise_on_stats = True
        port.now = 120.0
        with self.assertRaises(RuntimeError):
            service.refresh_health_snapshot()
        port.raise_on_stats = False
        port.now = 125.0
        service.refresh_health_snapshot()
        status, payload = service.cached_mining_readiness_payload()
        self.assertEqual(status, 503)
        self.assertEqual(payload["recovery_streak_seconds"], 0.0)

        # Continuous 10-second samples can now satisfy the 20-second window.
        for now in (135.0, 145.0):
            port.now = now
            service.refresh_health_snapshot()
        self.assertEqual(service.cached_mining_readiness_payload()[0], 200)

    def test_overlapping_inline_refreshes_publish_in_collection_order(self) -> None:
        port = FakeObservabilityPort()
        port.inputs = tip_cycle_inputs(0)
        service = ObservabilityService(port)
        first_observed = threading.Event()
        release_first = threading.Event()
        second_started = threading.Event()
        second_finished = threading.Event()
        original_observe = service._observe_mining_readiness

        def pause_first_observation(
            base_health: dict[str, object],
            progress: dict[str, object],
        ) -> object:
            snapshot = original_observe(base_health, progress)
            if not first_observed.is_set():
                first_observed.set()
                self.assertTrue(release_first.wait(2.0))
            return snapshot

        service._observe_mining_readiness = pause_first_observation  # type: ignore[method-assign]

        first = threading.Thread(target=service.refresh_health_snapshot)
        first.start()
        self.assertTrue(first_observed.wait(2.0))

        port.now = 105.0
        port.inputs = tip_cycle_inputs(12)

        def refresh_second() -> None:
            second_started.set()
            service.refresh_health_snapshot()
            second_finished.set()

        second = threading.Thread(target=refresh_second)
        second.start()
        self.assertTrue(second_started.wait(2.0))
        # The second collection stays behind the first publication. Without
        # refresh-wide serialization it finishes here and the first caller
        # can subsequently overwrite its newer cached answer.
        self.assertFalse(second_finished.wait(0.1))
        release_first.set()
        first.join(2.0)
        second.join(2.0)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertTrue(second_finished.is_set())
        snapshot = service.mining_readiness_snapshot()
        assert snapshot is not None
        self.assertEqual(snapshot.sample_monotonic, 105.0)
        self.assertEqual(snapshot.semantic_current_work_ratio, 1.0)

    def test_request_path_takes_no_coordinator_lock_and_no_ledger_read(self) -> None:
        server, _ = coordinator()
        server._accepted_parent_preview_wait_timeouts = 3
        facade = AuditHttpFacade(
            _CoordinatorAuditHttp(server),
            AuditHttpConfig("127.0.0.1", 0, join_timeout_seconds=1.0),
        )
        state = facade.start()
        self.addCleanup(facade.stop)
        assert state.bound_address is not None
        base_url = f"http://127.0.0.1:{state.bound_address[1]}"

        def get(path: str) -> tuple[int, dict[str, object], str | None]:
            try:
                with urllib.request.urlopen(base_url + path, timeout=5) as response:
                    return (
                        response.status,
                        json.loads(response.read()),
                        response.headers.get("Cache-Control"),
                    )
            except urllib.error.HTTPError as error:
                with error:
                    return error.code, json.loads(error.read()), error.headers.get("Cache-Control")

        # Warm-up: fail closed, and nothing sampled on the handler thread.
        status, payload, cache_control = get("/readyz/mining")
        self.assertEqual(status, 503)
        self.assertEqual(payload["reasons"], ["warming_up"])
        self.assertEqual(cache_control, "no-store")

        # One background refresh publishes the sample; the route serves it.
        server.refresh_health_snapshot()
        status, payload, cache_control = get("/readyz/mining")
        self.assertEqual(status, 200)
        self.assertTrue(payload["ready"])
        self.assertEqual(payload["schema"], MINING_READINESS_SCHEMA)
        self.assertEqual(cache_control, "no-store")

        # Now make every live path explode: the coordinator lock records any
        # acquisition, the ledger raises on any attribute, and the delivery
        # census / progress snapshot / counter reads all assert.
        observed = ObservedRLock()
        observed.observe_acquires = True
        server.lock = observed  # type: ignore[assignment]

        class ExplodingLedger:
            def __getattr__(self, name: str) -> object:
                raise AssertionError(f"ledger.{name} reached from /readyz/mining")

        server.ledger = ExplodingLedger()  # type: ignore[assignment]
        for name in (
            "mining_delivery_snapshot",
            "progress_health_snapshot",
            "accepted_share_stats",
            "refresh_health_snapshot",
            "block_submitter_snapshot",
        ):
            setattr(
                server,
                name,
                lambda *args, _name=name, **kwargs: (_ for _ in ()).throw(
                    AssertionError(f"{_name} reached from /readyz/mining")
                ),
            )

        status, payload, cache_control = get("/readyz/mining")

        self.assertEqual(status, 200)
        self.assertTrue(payload["ready"])
        self.assertFalse(observed.acquire_attempted.is_set())
        self.assertEqual(cache_control, "no-store")


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
    def test_template_generation_and_identity_do_not_affect_the_count(
        self,
    ) -> None:
        # Upstream #107: template generation and template object identity do
        # not participate in semantic currency. Client-session generations do,
        # and the strict fail-closed gauge stays untouched by semantic matches.
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
            connection_id=matching.connection_id,
            authorization_generation=matching.authorization_generation,
            difficulty_generation=matching.difficulty_generation,
        )
        mismatched = semantic_client(server, 2)
        mismatched.active_job = SimpleNamespace(
            template={"previousblockhash": "aa" * 32},
            template_fingerprint="ee" * 32,
            template_generation=7,
            payout_state_generation=0,
            connection_id=mismatched.connection_id,
            authorization_generation=mismatched.authorization_generation,
            difficulty_generation=mismatched.difficulty_generation,
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
            connection_id=stale.connection_id,
            authorization_generation=stale.authorization_generation,
            difficulty_generation=stale.difficulty_generation,
        )

        snapshot = server.mining_delivery_snapshot()

        self.assertEqual(snapshot["clients_with_semantically_current_work"], 0)
        self.assertEqual(snapshot["semantic_current_work_ratio"], 0.0)

    def test_session_generation_changes_disqualify_semantic_currency(self) -> None:
        server = bare_semantic_coordinator()
        fingerprint = "ff" * 32
        server.tip_template_snapshot = SimpleNamespace(
            bestblockhash="aa" * 32,
            template_fingerprint=fingerprint,
            template_generation=7,
            template_artifacts=None,
        )
        reauthorized = semantic_client(server, 1)
        reauthorized.active_job = SimpleNamespace(
            template={"previousblockhash": "aa" * 32},
            template_fingerprint=fingerprint,
            payout_state_generation=0,
            connection_id=reauthorized.connection_id,
            authorization_generation=reauthorized.authorization_generation,
            difficulty_generation=reauthorized.difficulty_generation,
        )
        retargeted = semantic_client(server, 2)
        retargeted.active_job = SimpleNamespace(
            template={"previousblockhash": "aa" * 32},
            template_fingerprint=fingerprint,
            payout_state_generation=0,
            connection_id=retargeted.connection_id,
            authorization_generation=retargeted.authorization_generation,
            difficulty_generation=retargeted.difficulty_generation,
        )

        reauthorized.authorization_generation += 1
        retargeted.difficulty_generation += 1
        snapshot = server.mining_delivery_snapshot()

        self.assertEqual(snapshot["clients_with_semantically_current_work"], 0)
        self.assertEqual(snapshot["semantic_current_work_ratio"], 0.0)


if __name__ == "__main__":
    unittest.main()
