#!/usr/bin/env python3
"""Direct ownership and safety tests for PRISM health observability."""

from __future__ import annotations

from dataclasses import replace
import threading
import unittest

from lab.prism.observability import (
    HEALTH_SCHEMA,
    MiningDeliveryInputs,
    ObservabilityService,
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
        self.metrics_payload = "qbit_prism_fixture 1\n"
        self.metrics_error: Exception | None = None
        self.metrics_render_count = 0
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

    def test_refresh_loop_counts_failure_and_clears_running_state(self) -> None:
        port = FakeObservabilityPort()
        port.raise_on_stats = True
        service = ObservabilityService(port)
        self.assertTrue(service.begin_refresh_loop())
        self.assertFalse(service.begin_refresh_loop())

        service.health_snapshot_loop()

        state = service.state()
        self.assertFalse(state.health_refresh_loop_running)
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
        status, cached = service.cached_metrics_payload()
        self.assertEqual(status, 503)
        self.assertIn("qbit_prism_metrics_snapshot_stale 1\n", cached)

    def test_metrics_without_snapshot_fails_closed_without_collecting(self) -> None:
        port = FakeObservabilityPort()
        service = ObservabilityService(port)

        status, payload = service.cached_metrics_payload()

        self.assertEqual(status, 503)
        self.assertEqual(port.metrics_render_count, 0)
        self.assertIn("qbit_prism_metrics_snapshot_available 0\n", payload)
        self.assertIn("qbit_prism_metrics_snapshot_age_seconds -1.000\n", payload)

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


if __name__ == "__main__":
    unittest.main()
