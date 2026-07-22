#!/usr/bin/env python3
"""Deterministic monotonic progress-health coverage for PRISM."""

from __future__ import annotations

import unittest
from concurrent.futures import CancelledError
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism.prism_coordinator import QbitTipTemplateSnapshot
from lab.prism.progress_health import (
    DeliveryProof,
    EligibilitySnapshot,
    ProgressHealthConfig,
    ProgressHealthService,
    WorkGeneration,
)
from tests.prism_coordinator_test_support import client, coordinator


class FakeMonotonicClock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def work(
    *, generation: int, fingerprint: str, payout_generation: int = 0
) -> WorkGeneration:
    return WorkGeneration(generation, fingerprint, payout_generation)


def progress_service(
    *, bundle_build_deadline_seconds: float = 60.0
) -> tuple[ProgressHealthService, FakeMonotonicClock]:
    clock = FakeMonotonicClock()
    return (
        ProgressHealthService(
            ProgressHealthConfig(
                pending_refresh_deadline_seconds=15.0,
                tip_poll_deadline_seconds=15.0,
                bundle_build_deadline_seconds=bundle_build_deadline_seconds,
            ),
            started_monotonic=clock.now,
            monotonic=clock,
        ),
        clock,
    )


def proof(
    connection_id: int,
    delivered_work: WorkGeneration,
    delivered_monotonic: float,
    *,
    collection_only: bool = False,
) -> DeliveryProof:
    return DeliveryProof(
        connection_id=connection_id,
        delivered_work=delivered_work,
        collection_only=collection_only,
        delivered_monotonic=delivered_monotonic,
    )


def eligibility(
    *connection_ids: int,
    proofs: tuple[DeliveryProof, ...] = (),
    ready_mode_required: bool = False,
) -> EligibilitySnapshot:
    return EligibilitySnapshot(
        eligible_connection_ids=connection_ids,
        delivery_proofs=proofs,
        ready_mode_required=ready_mode_required,
    )


def service_publish(
    service: ProgressHealthService,
    current_work: WorkGeneration,
) -> None:
    service.observe_tip(current_work)
    service.publish_work(current_work)


def service_health(
    service: ProgressHealthService,
    clients: EligibilitySnapshot | None = None,
    *,
    payout_generation: int = 0,
) -> dict[str, object]:
    return service.snapshot(
        clients or eligibility(),
        payout_generation,
    ).as_mapping()


def snapshot(
    *, generation: int, fingerprint: str, tip: str = "11" * 32
) -> QbitTipTemplateSnapshot:
    return QbitTipTemplateSnapshot(
        bestblockhash=tip,
        previousblockhash=tip,
        template_fingerprint=fingerprint,
        template_generation=generation,
    )


def context_for(
    work: QbitTipTemplateSnapshot,
    payout_generation: int,
    *,
    collection_only: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        template={"previousblockhash": work.previousblockhash},
        template_fingerprint=work.template_fingerprint,
        template_generation=work.template_generation,
        payout_state_generation=payout_generation,
        collection_only=collection_only,
    )


def progress_coordinator() -> tuple[object, FakeMonotonicClock]:
    server, _ = coordinator()
    clock = FakeMonotonicClock()
    server.started_monotonic = clock.now
    server.health_pending_refresh_max_age_seconds = 15.0
    server.health_tip_poll_max_age_seconds = 15.0
    server.progress_health_service = ProgressHealthService(
        ProgressHealthConfig(
            pending_refresh_deadline_seconds=15.0,
            tip_poll_deadline_seconds=15.0,
            bundle_build_deadline_seconds=60.0,
        ),
        started_monotonic=clock.now,
        monotonic=clock,
    )
    server._health_snapshot = None
    server._health_snapshot_monotonic = None
    server._health_refresh_loop_running = False
    return server, clock


def publish(
    server: object,
    work: QbitTipTemplateSnapshot,
    payout_generation: int = 0,
) -> None:
    server._record_progress_tip_poll(work)
    server._record_progress_publication(work, payout_generation)


class ProgressHealthTests(unittest.TestCase):
    def test_publication_watchdog_fires_with_heartbeat_watchdog_disabled(self) -> None:
        server, _clock = progress_coordinator()
        server.watchdog_enabled = False
        server.watchdog_interval_seconds = 0.001
        server.publication_progress_failure_expired = lambda _now: True  # type: ignore[method-assign]

        with (
            patch(
                "lab.prism.prism_coordinator.os._exit",
                side_effect=SystemExit(1),
            ) as exit_process,
            patch("builtins.print"),
            self.assertRaises(SystemExit),
        ):
            server.watchdog_loop()

        exit_process.assert_called_once_with(1)

    def test_unchanged_tip_for_hours_with_valid_work_stays_healthy(self) -> None:
        service, clock = progress_service()
        original = work(generation=1, fingerprint="aa" * 32)
        service_publish(service, original)
        delivered = proof(1, original, clock.now)
        service.record_delivery(delivered, ready_mode_required=False)
        clients = eligibility(1, proofs=(delivered,))

        clock.advance(6 * 60 * 60)
        same_work = work(generation=2, fingerprint=original.template_fingerprint or "")
        service.observe_tip(same_work)
        health = service_health(service, clients)

        self.assertTrue(health["ok"])
        self.assertEqual(health["published_template_generation"], 2)
        self.assertGreater(health["last_valid_delivery_age_seconds"], 21_000)

    def test_repeated_successful_same_tip_polls_stay_healthy(self) -> None:
        service, clock = progress_service()
        fingerprint = "aa" * 32
        service_publish(service, work(generation=1, fingerprint=fingerprint))

        for generation in range(2, 20):
            clock.advance(10)
            service.observe_tip(
                work(generation=generation, fingerprint=fingerprint)
            )
            self.assertTrue(service_health(service)["ok"])

    def test_publication_resolves_pending_before_a_later_signal(self) -> None:
        server, clock = progress_coordinator()
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        clock.advance(60)

        server._progress_note_refresh_pending()
        health = server.progress_health_snapshot()

        self.assertEqual(health["pending_refresh_age_seconds"], 0.0)

    def test_new_tip_without_publication_exceeds_deadline_and_returns_503(self) -> None:
        server, clock = progress_coordinator()
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        changed = snapshot(generation=2, fingerprint="bb" * 32, tip="22" * 32)
        server._record_progress_tip_poll(changed)
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(
                generation=3,
                fingerprint=changed.template_fingerprint,
                tip=changed.bestblockhash,
            )
        )

        status, health = server.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertIn("refresh_pending_too_long", health["reasons"])
        self.assertIn("current_generation_not_published", health["reasons"])

    def test_payout_change_without_replacement_delivery_returns_503(self) -> None:
        server, clock = progress_coordinator()
        current_work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, current_work)
        miner = client(1)
        old_delivery = context_for(current_work, 0)
        miner.active_job = old_delivery
        server.clients.add(miner)
        server._record_progress_delivery(miner, old_delivery, clock.now)

        server._record_progress_payout_generation(1, clock.now)
        server._record_progress_publication(current_work, 1)
        clock.advance(16)
        same_work = snapshot(
            generation=2,
            fingerprint=current_work.template_fingerprint,
        )
        server._record_progress_tip_poll(same_work)

        status, health = server.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertIn("current_generation_not_delivered", health["reasons"])
        self.assertEqual(health["current_payout_generation"], 1)
        self.assertEqual(health["published_payout_generation"], 1)

    def test_current_generation_delivery_clears_failure_immediately(self) -> None:
        server, clock = progress_coordinator()
        current_work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, current_work)
        miner = client(1)
        miner.active_job = context_for(current_work, 0)
        server.clients.add(miner)
        server._record_progress_payout_generation(1, clock.now)
        server._record_progress_publication(current_work, 1)
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(
                generation=2,
                fingerprint=current_work.template_fingerprint,
            )
        )
        self.assertFalse(server.progress_health_snapshot()["ok"])

        current_delivery = context_for(current_work, 1)
        miner.active_job = current_delivery
        server._record_progress_delivery(miner, current_delivery, clock.now)

        status, health = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])
        self.assertFalse(health["pending_refresh"])

    def test_blocked_bundle_build_becomes_unhealthy(self) -> None:
        service, clock = progress_service(bundle_build_deadline_seconds=60.0)
        original = work(generation=1, fingerprint="aa" * 32)
        service_publish(service, original)
        token = service.start_bundle_build()
        clock.advance(16)
        service.observe_tip(
            work(generation=2, fingerprint=original.template_fingerprint or "")
        )

        within_build_timeout = service_health(service)

        self.assertTrue(within_build_timeout["ok"])
        self.assertNotIn("bundle_build_stuck", within_build_timeout["reasons"])

        clock.advance(45)
        service.observe_tip(
            work(generation=3, fingerprint=original.template_fingerprint or "")
        )
        health = service_health(service)

        self.assertFalse(health["ok"])
        self.assertIn("bundle_build_stuck", health["reasons"])
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 61.0)
        token.finish()

    def test_no_eligible_miners_need_no_socket_delivery_after_publication(self) -> None:
        service, clock = progress_service()
        current = work(
            generation=1,
            fingerprint="aa" * 32,
            payout_generation=1,
        )
        service.observe_tip(work(generation=1, fingerprint="aa" * 32))
        service.observe_payout_generation(1, clock.now)
        service.publish_work(current)

        health = service_health(service, payout_generation=1)

        self.assertTrue(health["ok"])
        self.assertEqual(health["eligible_client_count"], 0)
        self.assertIsNone(health["last_valid_delivery_age_seconds"])

    def test_eligible_miners_require_current_generation_delivery(self) -> None:
        service, clock = progress_service()
        original = work(generation=1, fingerprint="aa" * 32)
        service_publish(service, original)
        service_health(service, eligibility(1))
        clock.advance(16)
        service.observe_tip(
            work(generation=2, fingerprint=original.template_fingerprint or "")
        )

        health = service_health(service, eligibility(1))

        self.assertFalse(health["ok"])
        self.assertIn("current_generation_not_delivered", health["reasons"])
        self.assertEqual(health["eligible_client_count"], 1)
        self.assertEqual(health["eligible_clients_requiring_refresh"], 1)

    def test_partial_fanout_stays_pending_until_every_client_is_current(self) -> None:
        service, clock = progress_service()
        original = work(generation=1, fingerprint="aa" * 32)
        service_publish(service, original)
        delivered = proof(1, original, clock.now)
        service.record_delivery(delivered, ready_mode_required=False)
        clock.advance(16)
        service.observe_tip(
            work(generation=2, fingerprint=original.template_fingerprint or "")
        )

        clients = eligibility(1, 2, proofs=(delivered,))
        health = service_health(service, clients)

        self.assertFalse(health["ok"])
        self.assertTrue(health["pending_refresh"])
        self.assertEqual(health["eligible_clients_requiring_refresh"], 1)
        self.assertIn("current_generation_not_delivered", health["reasons"])

        missing = proof(2, original, clock.now)
        service.record_delivery(missing, ready_mode_required=False)
        self.assertTrue(
            service_health(
                service,
                eligibility(1, 2, proofs=(delivered, missing)),
            )["ok"]
        )

    def test_registered_job_is_not_delivery_proof_before_socket_send(self) -> None:
        server, clock = progress_coordinator()
        work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, work)
        miner = client(1)
        current_context = context_for(work, 0)
        miner.active_job = current_context
        server.clients.add(miner)
        server.progress_health_snapshot()
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(generation=2, fingerprint=work.template_fingerprint)
        )

        before_send = server.progress_health_snapshot()

        self.assertEqual(before_send["eligible_clients_requiring_refresh"], 1)
        self.assertIn("current_generation_not_delivered", before_send["reasons"])

        server._record_progress_delivery(miner, current_context, clock.now)
        after_send = server.progress_health_snapshot()
        self.assertTrue(after_send["ok"])
        self.assertEqual(after_send["eligible_clients_requiring_refresh"], 0)

    def test_readiness_promotion_requires_successful_ready_delivery(self) -> None:
        server, clock = progress_coordinator()
        work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, work)
        miner = client(1)
        collection_context = context_for(work, 0, collection_only=True)
        miner.active_job = collection_context
        server.clients.add(miner)
        server._record_progress_delivery(miner, collection_context, clock.now)
        self.assertTrue(server.progress_health_snapshot()["ok"])

        self.assertTrue(server.pool_readiness_latched())
        promoted = server.progress_health_snapshot()
        self.assertTrue(promoted["pending_refresh"])
        self.assertEqual(promoted["eligible_clients_requiring_refresh"], 1)
        self.assertTrue(server.client_needs_tip_template_refresh(miner, work))

        # Fanout registers the ready context before its socket write. A write
        # that stalls here must not replace the delivered collection proof.
        ready_context = context_for(work, 0, collection_only=False)
        miner.active_job = ready_context
        server._record_progress_publication(work, 0)
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(generation=2, fingerprint=work.template_fingerprint)
        )

        stalled = server.progress_health_snapshot()
        self.assertFalse(stalled["ok"])
        self.assertIn("current_generation_not_delivered", stalled["reasons"])
        self.assertEqual(stalled["eligible_clients_requiring_refresh"], 1)

        server._record_progress_delivery(miner, ready_context, clock.now)
        delivered = server.progress_health_snapshot()
        self.assertTrue(delivered["ok"])
        self.assertFalse(delivered["pending_refresh"])
        self.assertEqual(delivered["eligible_clients_requiring_refresh"], 0)

    def test_startup_is_unready_until_initial_work_is_published(self) -> None:
        server, clock = progress_coordinator()

        status, startup = server.cached_health_payload()
        self.assertEqual(status, 503)
        self.assertIn("current_generation_not_published", startup["reasons"])

        clock.advance(16)
        self.assertIn("tip_poll_stale", server.progress_health_snapshot()["reasons"])

        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        status, ready = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertTrue(ready["ok"])

    def test_stale_cached_ok_cannot_mask_a_progress_failure(self) -> None:
        server, clock = progress_coordinator()
        original = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, original)
        server.health_refresh_seconds = 60.0
        server.refresh_health_snapshot()
        server._health_refresh_loop_running = True

        changed = snapshot(generation=2, fingerprint="bb" * 32, tip="22" * 32)
        server._record_progress_tip_poll(changed)
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(
                generation=3,
                fingerprint=changed.template_fingerprint,
                tip=changed.bestblockhash,
            )
        )

        status, health = server.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertFalse(health["ok"])
        self.assertLess(health["snapshot_age_seconds"], server.health_refresh_seconds)

    def test_wall_clock_changes_do_not_affect_health_decisions(self) -> None:
        service, clock = progress_service()
        service_publish(service, work(generation=1, fingerprint="aa" * 32))

        with patch("lab.prism.prism_coordinator.time.time", return_value=-10**12):
            self.assertTrue(service_health(service)["ok"])
        with patch("lab.prism.prism_coordinator.time.time", return_value=10**12):
            self.assertTrue(service_health(service)["ok"])
        self.assertEqual(clock.now, 100.0)

    def test_tip_poll_freshness_has_an_independent_deadline(self) -> None:
        service, clock = progress_service()
        service_publish(service, work(generation=1, fingerprint="aa" * 32))
        clock.advance(16)

        health = service_health(service)

        self.assertFalse(health["ok"])
        self.assertEqual(health["reasons"], ["tip_poll_stale"])

    def test_older_poll_cannot_renew_current_generation_freshness(self) -> None:
        service, clock = progress_service()
        current = work(generation=2, fingerprint="bb" * 32)
        service_publish(service, current)
        clock.advance(16)

        service.observe_tip(work(generation=1, fingerprint="aa" * 32))
        health = service_health(service)

        self.assertFalse(health["ok"])
        self.assertEqual(health["current_template_generation"], 2)
        self.assertEqual(health["tip_poll_age_seconds"], 16.0)
        self.assertIn("tip_poll_stale", health["reasons"])

    def test_progressing_refresh_does_not_report_tip_poll_stale(self) -> None:
        service, clock = progress_service()
        current = work(generation=1, fingerprint="aa" * 32)
        service_publish(service, current)
        refresh = service.start_refresh()
        clock.advance(10)
        service.publish_work(current)
        clock.advance(10)

        health = service_health(service)

        self.assertTrue(health["ok"])
        self.assertTrue(health["tip_refresh_in_progress"])
        self.assertEqual(health["tip_poll_age_seconds"], 20.0)
        self.assertEqual(health["tip_refresh_progress_age_seconds"], 10.0)
        self.assertNotIn("tip_poll_stale", health["reasons"])
        refresh.finish()

    def test_stalled_active_refresh_still_reports_tip_poll_stale(self) -> None:
        service, clock = progress_service()
        service_publish(service, work(generation=1, fingerprint="aa" * 32))
        refresh = service.start_refresh()
        clock.advance(16)

        health = service_health(service)

        self.assertFalse(health["ok"])
        self.assertIn("tip_poll_stale", health["reasons"])
        refresh.finish()

    def test_refresh_token_finishes_on_exception(self) -> None:
        service, _ = progress_service()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            with service.start_refresh():
                raise RuntimeError("boom")

        self.assertFalse(service_health(service)["tip_refresh_in_progress"])

    def test_bundle_token_finishes_on_cancellation(self) -> None:
        service, clock = progress_service(bundle_build_deadline_seconds=1.0)

        with self.assertRaises(CancelledError):
            with service.start_bundle_build():
                raise CancelledError()
        clock.advance(2)

        self.assertEqual(
            service_health(service)["bundle_build_oldest_age_seconds"],
            0.0,
        )

    def test_token_finish_is_idempotent(self) -> None:
        service, _ = progress_service()
        refresh = service.start_refresh()
        build = service.start_bundle_build()

        refresh.finish()
        refresh.finish()
        build.finish()
        build.finish()

        health = service_health(service)
        self.assertFalse(health["tip_refresh_in_progress"])
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 0.0)

    def test_overlapping_refresh_tokens_finish_independently(self) -> None:
        service, clock = progress_service()
        first = service.start_refresh()
        clock.advance(5)
        second = service.start_refresh()

        second.finish()
        self.assertTrue(service_health(service)["tip_refresh_in_progress"])
        first.note_activity()
        first.finish()

        self.assertFalse(service_health(service)["tip_refresh_in_progress"])

    def test_oldest_overlapping_bundle_controls_health(self) -> None:
        service, clock = progress_service(bundle_build_deadline_seconds=60.0)
        current = work(generation=1, fingerprint="aa" * 32)
        service_publish(service, current)
        first = service.start_bundle_build()
        clock.advance(10)
        second = service.start_bundle_build()
        clock.advance(51)
        service.observe_tip(work(generation=2, fingerprint="aa" * 32))

        health = service_health(service)
        self.assertIn("bundle_build_stuck", health["reasons"])
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 61.0)

        first.finish()
        health = service_health(service)
        self.assertNotIn("bundle_build_stuck", health["reasons"])
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 51.0)
        second.finish()

    def test_multiple_failures_keep_the_fixed_reason_order(self) -> None:
        service, clock = progress_service(bundle_build_deadline_seconds=60.0)
        build = service.start_bundle_build()
        clock.advance(61)

        health = service_health(service)

        self.assertEqual(
            health["reasons"],
            [
                "tip_poll_stale",
                "bundle_build_stuck",
                "current_generation_not_published",
            ],
        )
        build.finish()

    def test_progress_health_cannot_mask_base_mining_failure(self) -> None:
        server, _ = progress_coordinator()
        progress = {
            "ok": True,
            "reason": None,
            "reasons": [],
        }

        payload = server._apply_progress_health({"ok": False}, progress)

        self.assertFalse(payload["ok"])

    def test_healthy_response_fields_remain_backward_compatible(self) -> None:
        server, _ = progress_coordinator()
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))

        payload = server.health_payload()

        for field in (
            "ok",
            "schema",
            "ledger_backend",
            "accepted_share_count",
            "ready_miner_count",
            "accepted_block",
            "accepted_block_count",
            "max_blocks",
        ):
            self.assertIn(field, payload)

    def test_progress_metrics_have_bounded_state_and_age_gauges(self) -> None:
        server, _ = progress_coordinator()
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))

        metrics = server.metrics_payload()

        for metric in (
            "qbit_prism_refresh_pending",
            "qbit_prism_refresh_pending_age_seconds",
            "qbit_prism_tip_poll_age_seconds",
            "qbit_prism_current_generation_delivery_age_seconds",
            "qbit_prism_bundle_build_oldest_age_seconds",
            'qbit_prism_health_state{reason="healthy"} 1',
        ):
            self.assertIn(metric, metrics)


if __name__ == "__main__":
    unittest.main()
