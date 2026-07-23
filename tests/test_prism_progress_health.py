#!/usr/bin/env python3
"""Deterministic monotonic progress-health coverage for PRISM."""

from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism.prism_coordinator import QbitTipTemplateSnapshot
from tests.test_prism_coordinator_job_cache import client, coordinator


class FakeMonotonicClock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


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
    server._progress_monotonic = clock
    server.started_monotonic = clock.now
    server.health_pending_refresh_max_age_seconds = 15.0
    server.health_tip_poll_max_age_seconds = 15.0
    with server._progress_health_lock:
        server._progress_current_template_generation = 0
        server._progress_current_template_fingerprint = None
        server._progress_current_payout_generation = 0
        server._progress_published_template_generation = 0
        server._progress_published_template_fingerprint = None
        server._progress_published_payout_generation = 0
        server._progress_has_published_work = False
        server._progress_last_tip_poll_monotonic = None
        server._progress_last_delivery_template_generation = 0
        server._progress_last_delivery_template_fingerprint = None
        server._progress_last_delivery_payout_generation = 0
        server._progress_last_delivery_monotonic = None
        server._progress_pending_since_monotonic = clock.now
        server._progress_publication_divergence_since_monotonic = clock.now
        server._progress_refresh_signal_pending = False
        server._progress_active_refresh_count = 0
        server._progress_last_refresh_activity_monotonic = None
        server._progress_bundle_build_counter = 0
        server._progress_bundle_builds.clear()
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
    def test_publication_progress_uses_template_failure_budget(self) -> None:
        server, clock = progress_coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        current = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, current)
        replacement = snapshot(
            generation=2,
            fingerprint="bb" * 32,
            tip="22" * 32,
        )
        server._record_progress_tip_poll(replacement)

        clock.advance(9.999)
        self.assertFalse(server.publication_progress_failure_expired(clock.now))
        clock.advance(0.001)
        self.assertTrue(server.publication_progress_failure_expired(clock.now))

        server._record_progress_publication(replacement, 0)
        self.assertFalse(server.publication_progress_failure_expired(clock.now))

    def test_publication_watchdog_does_not_inherit_client_delivery_age(self) -> None:
        server, clock = progress_coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        current = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, current)
        miner = client(1)
        server.clients.add(miner)
        server._progress_reconcile_pending(now=clock.now)

        clock.advance(20.0)
        self.assertEqual(server._progress_pending_since_monotonic, 100.0)
        self.assertIsNone(
            server._progress_publication_divergence_since_monotonic
        )
        self.assertFalse(server.publication_progress_failure_expired(clock.now))

        replacement = snapshot(
            generation=2,
            fingerprint="bb" * 32,
            tip="22" * 32,
        )
        with server.lock:
            server.latest_detected_tip = (replacement.bestblockhash, 1)
        server._progress_note_refresh_pending(clock.now)

        # A delayed delivery of the still-published tip can clear the broader
        # client health condition, but it must not clear or age the newer
        # publication-divergence deadline.
        server._record_progress_delivery(
            miner,
            context_for(current, 0),
            clock.now,
        )
        self.assertIsNone(server._progress_pending_since_monotonic)
        self.assertEqual(
            server._progress_publication_divergence_since_monotonic,
            clock.now,
        )

        server._record_progress_tip_poll(replacement, clock.now)
        self.assertFalse(server.publication_progress_failure_expired(clock.now))
        clock.advance(9.999)
        self.assertFalse(server.publication_progress_failure_expired(clock.now))
        clock.advance(0.001)
        self.assertTrue(server.publication_progress_failure_expired(clock.now))

    def test_publication_divergence_survives_old_tip_delivery(self) -> None:
        server, clock = progress_coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        current = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, current)
        miner = client(1)
        server.clients.add(miner)
        replacement = snapshot(
            generation=2,
            fingerprint="bb" * 32,
            tip="22" * 32,
        )
        with server.lock:
            server.latest_detected_tip = (replacement.bestblockhash, 1)
        server._progress_note_refresh_pending(clock.now)

        clock.advance(6.0)
        server._record_progress_delivery(
            miner,
            context_for(current, 0),
            clock.now,
        )

        self.assertEqual(
            server._progress_publication_divergence_since_monotonic,
            100.0,
        )
        clock.advance(3.999)
        self.assertFalse(server.publication_progress_failure_expired(clock.now))
        clock.advance(0.001)
        self.assertTrue(server.publication_progress_failure_expired(clock.now))

    def test_publication_divergence_churn_does_not_renew_deadline(self) -> None:
        server, clock = progress_coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        latest = None
        first_replacement = None

        for generation, marker in ((2, "bb"), (3, "cc"), (4, "dd")):
            latest = snapshot(
                generation=generation,
                fingerprint=marker * 32,
                tip=marker * 32,
            )
            if first_replacement is None:
                first_replacement = latest
            with server.lock:
                server.latest_detected_tip = (latest.bestblockhash, generation)
            server._progress_note_refresh_pending(clock.now)
            server._record_progress_tip_poll(latest, clock.now)
            self.assertEqual(
                server._progress_publication_divergence_since_monotonic,
                100.0,
            )
            clock.advance(3.0)

        self.assertIsNotNone(latest)
        self.assertFalse(server.publication_progress_failure_expired(clock.now))
        clock.advance(1.0)
        self.assertTrue(server.publication_progress_failure_expired(clock.now))

        assert first_replacement is not None
        server._record_progress_publication(first_replacement, 0)
        self.assertEqual(
            server._progress_publication_divergence_since_monotonic,
            100.0,
        )

        assert latest is not None
        server._record_progress_publication(latest, 0)
        self.assertIsNone(
            server._progress_publication_divergence_since_monotonic
        )
        self.assertFalse(server.publication_progress_failure_expired(clock.now))

    def test_publication_watchdog_fires_with_heartbeat_watchdog_disabled(self) -> None:
        server, clock = progress_coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        server.watchdog_enabled = False
        server.watchdog_interval_seconds = 0.001
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        server._record_progress_tip_poll(
            snapshot(
                generation=2,
                fingerprint="bb" * 32,
                tip="22" * 32,
            )
        )
        clock.advance(10.0)

        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                return_value=clock.now,
            ),
            patch(
                "lab.prism.prism_coordinator.os._exit",
                side_effect=SystemExit(1),
            ) as exit_process,
            patch("builtins.print"),
            self.assertRaises(SystemExit),
        ):
            server.watchdog_loop()

        exit_process.assert_called_once_with(1)

    def test_brief_coordination_block_owns_publication_deadline(self) -> None:
        server, clock = progress_coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        server.coordination_blocked_exit_seconds = 30.0
        server.watchdog_enabled = False
        server.watchdog_interval_seconds = 0.001
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        server._record_progress_tip_poll(
            snapshot(
                generation=2,
                fingerprint="bb" * 32,
                tip="22" * 32,
            )
        )
        server._record_coordination_blocked_refresh(clock.now)
        clock.advance(10.0)

        self.assertTrue(server.publication_progress_failure_expired(clock.now))
        self.assertFalse(server.coordination_blocked_streak_expired(clock.now))
        wait_results = iter((False, True))
        server.stop_event = SimpleNamespace(
            wait=lambda _seconds: next(wait_results)
        )
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                return_value=clock.now,
            ),
            patch("lab.prism.prism_coordinator.os._exit") as exit_process,
            patch("builtins.print"),
        ):
            server.watchdog_loop()

        exit_process.assert_not_called()

    def test_coordination_start_during_publication_check_wins_arbitration(
        self,
    ) -> None:
        server, clock = progress_coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        server.coordination_blocked_exit_seconds = 30.0
        server.watchdog_enabled = False
        server.watchdog_interval_seconds = 0.001
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        server._record_progress_tip_poll(
            snapshot(
                generation=2,
                fingerprint="bb" * 32,
                tip="22" * 32,
            )
        )
        clock.advance(10.0)

        publication_check_started = threading.Event()
        resume_publication_check = threading.Event()
        server.stop_event = threading.Event()
        original_publication_check = server.publication_progress_failure_expired
        thread_errors: list[BaseException] = []

        def delayed_publication_check(now: float) -> bool:
            publication_check_started.set()
            if not resume_publication_check.wait(5.0):
                raise AssertionError("publication watchdog interleave timed out")
            server.stop_event.set()
            return original_publication_check(now)

        def run_watchdog() -> None:
            try:
                server.watchdog_loop()
            except BaseException as exc:
                thread_errors.append(exc)

        with (
            patch.object(
                server,
                "publication_progress_failure_expired",
                side_effect=delayed_publication_check,
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                return_value=clock.now,
            ),
            patch("lab.prism.prism_coordinator.os._exit") as exit_process,
            patch("builtins.print"),
        ):
            watchdog = threading.Thread(target=run_watchdog)
            watchdog.start()
            self.assertTrue(publication_check_started.wait(5.0))
            server._record_coordination_blocked_refresh(clock.now)
            resume_publication_check.set()
            watchdog.join(5.0)

        self.assertFalse(watchdog.is_alive())
        self.assertEqual(thread_errors, [])
        exit_process.assert_not_called()

    def test_unchanged_tip_for_hours_with_valid_work_stays_healthy(self) -> None:
        server, clock = progress_coordinator()
        original = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, original)
        miner = client(1)
        delivered = context_for(original, 0)
        miner.active_job = delivered
        server.clients.add(miner)
        server._record_progress_delivery(miner, delivered, clock.now)

        clock.advance(6 * 60 * 60)
        same_work = snapshot(generation=2, fingerprint=original.template_fingerprint)
        server._record_progress_tip_poll(same_work)
        health = server.progress_health_snapshot()

        self.assertTrue(health["ok"])
        self.assertEqual(health["published_template_generation"], 2)
        self.assertGreater(health["last_valid_delivery_age_seconds"], 21_000)

    def test_repeated_successful_same_tip_polls_stay_healthy(self) -> None:
        server, clock = progress_coordinator()
        fingerprint = "aa" * 32
        publish(server, snapshot(generation=1, fingerprint=fingerprint))

        for generation in range(2, 20):
            clock.advance(10)
            server._record_progress_tip_poll(
                snapshot(generation=generation, fingerprint=fingerprint)
            )
            self.assertTrue(server.progress_health_snapshot()["ok"])

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
        work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, work)
        miner = client(1)
        old_delivery = context_for(work, 0)
        miner.active_job = old_delivery
        server.clients.add(miner)
        server._record_progress_delivery(miner, old_delivery, clock.now)

        server._record_progress_payout_generation(1, clock.now)
        server._record_progress_publication(work, 1)
        clock.advance(16)
        same_work = snapshot(generation=2, fingerprint=work.template_fingerprint)
        server._record_progress_tip_poll(same_work)

        status, health = server.cached_health_payload()

        self.assertEqual(status, 503)
        self.assertIn("current_generation_not_delivered", health["reasons"])
        self.assertEqual(health["current_payout_generation"], 1)
        self.assertEqual(health["published_payout_generation"], 1)

    def test_current_generation_delivery_clears_failure_immediately(self) -> None:
        server, clock = progress_coordinator()
        work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, work)
        miner = client(1)
        miner.active_job = context_for(work, 0)
        server.clients.add(miner)
        server._record_progress_payout_generation(1, clock.now)
        server._record_progress_publication(work, 1)
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(generation=2, fingerprint=work.template_fingerprint)
        )
        self.assertFalse(server.progress_health_snapshot()["ok"])

        current_delivery = context_for(work, 1)
        miner.active_job = current_delivery
        server._record_progress_delivery(miner, current_delivery, clock.now)

        status, health = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])
        self.assertFalse(health["pending_refresh"])

    def test_blocked_bundle_build_becomes_unhealthy(self) -> None:
        server, clock = progress_coordinator()
        work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, work)
        server.bundle_build_timeout_seconds = 60.0
        token = server._progress_bundle_build_started()
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(generation=2, fingerprint=work.template_fingerprint)
        )

        within_build_timeout = server.progress_health_snapshot()

        self.assertTrue(within_build_timeout["ok"])
        self.assertNotIn("bundle_build_stuck", within_build_timeout["reasons"])

        clock.advance(45)
        server._record_progress_tip_poll(
            snapshot(generation=3, fingerprint=work.template_fingerprint)
        )
        health = server.progress_health_snapshot()

        self.assertFalse(health["ok"])
        self.assertIn("bundle_build_stuck", health["reasons"])
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 61.0)
        server._progress_bundle_build_finished(token)

    def test_no_eligible_miners_need_no_socket_delivery_after_publication(self) -> None:
        server, clock = progress_coordinator()
        work = snapshot(generation=1, fingerprint="aa" * 32)
        server._record_progress_tip_poll(work)
        server._record_progress_payout_generation(1, clock.now)
        server._record_progress_publication(work, 1)

        health = server.progress_health_snapshot()

        self.assertTrue(health["ok"])
        self.assertEqual(health["eligible_client_count"], 0)
        self.assertIsNone(health["last_valid_delivery_age_seconds"])

    def test_eligible_miners_require_current_generation_delivery(self) -> None:
        server, clock = progress_coordinator()
        work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, work)
        miner = client(1)
        server.clients.add(miner)
        server.progress_health_snapshot()
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(generation=2, fingerprint=work.template_fingerprint)
        )

        health = server.progress_health_snapshot()

        self.assertFalse(health["ok"])
        self.assertIn("current_generation_not_delivered", health["reasons"])
        self.assertEqual(health["eligible_client_count"], 1)
        self.assertEqual(health["eligible_clients_requiring_refresh"], 1)

    def test_partial_fanout_stays_pending_until_every_client_is_current(self) -> None:
        server, clock = progress_coordinator()
        work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, work)
        delivered = client(1)
        missing = client(2)
        current_context = context_for(work, 0)
        delivered.active_job = current_context
        server.clients.update((delivered, missing))
        server._record_progress_delivery(delivered, current_context, clock.now)
        clock.advance(16)
        server._record_progress_tip_poll(
            snapshot(generation=2, fingerprint=work.template_fingerprint)
        )

        health = server.progress_health_snapshot()

        self.assertFalse(health["ok"])
        self.assertTrue(health["pending_refresh"])
        self.assertEqual(health["eligible_clients_requiring_refresh"], 1)
        self.assertIn("current_generation_not_delivered", health["reasons"])

        missing.active_job = current_context
        server._record_progress_delivery(missing, current_context, clock.now)
        self.assertTrue(server.progress_health_snapshot()["ok"])

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
        server, clock = progress_coordinator()
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))

        with patch("lab.prism.prism_coordinator.time.time", return_value=-10**12):
            self.assertTrue(server.progress_health_snapshot()["ok"])
        with patch("lab.prism.prism_coordinator.time.time", return_value=10**12):
            self.assertTrue(server.progress_health_snapshot()["ok"])
        self.assertEqual(clock.now, 100.0)

    def test_tip_poll_freshness_has_an_independent_deadline(self) -> None:
        server, clock = progress_coordinator()
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        clock.advance(16)

        health = server.progress_health_snapshot()

        self.assertFalse(health["ok"])
        self.assertEqual(health["reasons"], ["tip_poll_stale"])

    def test_older_poll_cannot_renew_current_generation_freshness(self) -> None:
        server, clock = progress_coordinator()
        current = snapshot(generation=2, fingerprint="bb" * 32, tip="22" * 32)
        publish(server, current)
        clock.advance(16)

        server._record_progress_tip_poll(
            snapshot(generation=1, fingerprint="aa" * 32)
        )
        health = server.progress_health_snapshot()

        self.assertFalse(health["ok"])
        self.assertEqual(health["current_template_generation"], 2)
        self.assertEqual(health["tip_poll_age_seconds"], 16.0)
        self.assertIn("tip_poll_stale", health["reasons"])

    def test_progressing_refresh_does_not_report_tip_poll_stale(self) -> None:
        server, clock = progress_coordinator()
        work = snapshot(generation=1, fingerprint="aa" * 32)
        publish(server, work)
        server._progress_refresh_started()
        clock.advance(10)
        server._record_progress_publication(work, 0)
        clock.advance(10)

        health = server.progress_health_snapshot()

        self.assertTrue(health["ok"])
        self.assertTrue(health["tip_refresh_in_progress"])
        self.assertEqual(health["tip_poll_age_seconds"], 20.0)
        self.assertEqual(health["tip_refresh_progress_age_seconds"], 10.0)
        self.assertNotIn("tip_poll_stale", health["reasons"])
        server._progress_refresh_finished()

    def test_stalled_active_refresh_still_reports_tip_poll_stale(self) -> None:
        server, clock = progress_coordinator()
        publish(server, snapshot(generation=1, fingerprint="aa" * 32))
        server._progress_refresh_started()
        clock.advance(16)

        health = server.progress_health_snapshot()

        self.assertFalse(health["ok"])
        self.assertIn("tip_poll_stale", health["reasons"])
        server._progress_refresh_finished()

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
        server._record_coordination_blocked_refresh(100.0)

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            return_value=105.0,
        ):
            metrics = server.metrics_payload()

        for metric in (
            "qbit_prism_refresh_pending",
            "qbit_prism_refresh_pending_age_seconds",
            "qbit_prism_tip_poll_age_seconds",
            "qbit_prism_current_generation_delivery_age_seconds",
            "qbit_prism_bundle_build_oldest_age_seconds",
            "# TYPE qbit_prism_template_refresh_coordination_blocked_age_seconds gauge",
            "qbit_prism_template_refresh_coordination_blocked_age_seconds 5.000000",
            'qbit_prism_health_state{reason="healthy"} 1',
        ):
            self.assertIn(metric, metrics)

        server._clear_coordination_blocked_streak()
        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            return_value=106.0,
        ):
            cleared_metrics = server.metrics_payload()
        self.assertIn(
            "qbit_prism_template_refresh_coordination_blocked_age_seconds 0.000000",
            cleared_metrics,
        )


if __name__ == "__main__":
    unittest.main()
