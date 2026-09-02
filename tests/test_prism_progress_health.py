#!/usr/bin/env python3
"""Deterministic monotonic progress-health coverage for PRISM."""

from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from concurrent.futures import CancelledError

from lab.prism.prism_coordinator import QbitTipTemplateSnapshot
from lab.prism.progress_health import (
    DEFAULT_PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS,
    DEFAULT_PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS,
    MINING_READINESS_ENTRY_COVERAGE_RATIO,
    MINING_READINESS_OLD_CANDIDATE_AGE_SECONDS,
    MINING_READINESS_REASONS,
    MINING_READINESS_RECOVERY_COVERAGE_RATIO,
    MINING_READINESS_SCHEMA,
    MINING_READINESS_STATES,
    MiningReadinessConfig,
    MiningReadinessSample,
    MiningReadinessSnapshot,
    MiningReadinessTracker,
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
    return server, clock


def publish(
    server: object,
    work: QbitTipTemplateSnapshot,
    payout_generation: int = 0,
) -> None:
    server._record_progress_tip_poll(work)
    server._record_progress_publication(work, payout_generation)


class ProgressHealthTests(unittest.TestCase):
    def test_health_warmup_is_async_and_reports_starting_state(self) -> None:
        import time as _time

        server, _clock = progress_coordinator()
        entered = threading.Event()
        release = threading.Event()

        def blocking_stats() -> tuple[int, int]:
            entered.set()
            release.wait(5)
            return (0, 0)

        server.accepted_share_stats = blocking_stats  # type: ignore[method-assign]
        try:
            started = _time.monotonic()
            server.start_health_snapshot_refresher()
            # The bind path never blocks on the cold accepted-share
            # aggregate; the warm-up runs in the background loop.
            self.assertLess(_time.monotonic() - started, 1.0)
            self.assertTrue(entered.wait(2))
            status, payload = server.cached_health_payload()
            self.assertEqual(status, 503)
            self.assertEqual(payload["state"], "starting")
            self.assertFalse(payload["ok"])
        finally:
            release.set()
            server.stop_event.set()

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
        server._ensure_observability_service().set_loop_running_for_test(True)

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


def owner_progress_service(
    *, bundle_build_deadline_seconds: float = 60.0
) -> tuple[ProgressHealthService, FakeMonotonicClock, ProgressHealthConfig, float]:
    clock = FakeMonotonicClock()
    service = ProgressHealthService(
        started_monotonic=clock.now,
        monotonic=clock,
    )
    config = ProgressHealthConfig(
        pending_refresh_deadline_seconds=15.0,
        tip_poll_deadline_seconds=15.0,
        bundle_build_deadline_seconds=bundle_build_deadline_seconds,
    )
    return service, clock, config, clock.now


def work_generation(
    *, generation: int, fingerprint: str, payout_generation: int = 0
) -> WorkGeneration:
    return WorkGeneration(
        template_generation=generation,
        template_fingerprint=fingerprint,
        payout_generation=payout_generation,
    )


def owner_service_health(
    service: ProgressHealthService,
    config: ProgressHealthConfig,
    started_monotonic: float,
    *,
    payout_generation: int = 0,
) -> dict[str, object]:
    return service.snapshot(
        lambda _fingerprint, _payout_generation: (0, 0),
        payout_generation,
        config,
        started_monotonic,
    ).as_mapping()


class ProgressTokenOwnershipTests(unittest.TestCase):
    """Direct R1/J1 token contracts on the extracted G1 owner service."""

    def test_refresh_token_finishes_on_exception(self) -> None:
        service, _clock, config, started = owner_progress_service()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            with service.start_refresh():
                raise RuntimeError("boom")

        health = owner_service_health(service, config, started)
        self.assertFalse(health["tip_refresh_in_progress"])

    def test_bundle_token_finishes_on_cancellation(self) -> None:
        service, clock, config, started = owner_progress_service(
            bundle_build_deadline_seconds=1.0
        )

        with self.assertRaises(CancelledError):
            with service.start_bundle_build():
                raise CancelledError()
        clock.advance(2)

        health = owner_service_health(service, config, started)
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 0.0)

    def test_token_finish_is_idempotent(self) -> None:
        service, _clock, config, started = owner_progress_service()
        refresh = service.start_refresh()
        build = service.start_bundle_build()

        refresh.finish()
        refresh.finish()
        build.finish()
        build.finish()

        health = owner_service_health(service, config, started)
        self.assertFalse(health["tip_refresh_in_progress"])
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 0.0)

    def test_overlapping_refresh_tokens_finish_independently(self) -> None:
        service, clock, config, started = owner_progress_service()
        first = service.start_refresh()
        clock.advance(5)
        second = service.start_refresh()

        second.finish()
        health = owner_service_health(service, config, started)
        self.assertTrue(health["tip_refresh_in_progress"])
        first.note_activity()
        first.finish()

        health = owner_service_health(service, config, started)
        self.assertFalse(health["tip_refresh_in_progress"])

    def test_oldest_overlapping_bundle_controls_health(self) -> None:
        service, clock, config, started = owner_progress_service(
            bundle_build_deadline_seconds=60.0
        )
        current = work_generation(generation=1, fingerprint="aa" * 32)
        service.observe_tip(current)
        self.assertTrue(service.publish_work(current))
        first = service.start_bundle_build()
        clock.advance(10)
        second = service.start_bundle_build()
        clock.advance(51)
        service.observe_tip(
            work_generation(generation=2, fingerprint="aa" * 32)
        )

        health = owner_service_health(service, config, started)
        self.assertIn("bundle_build_stuck", health["reasons"])
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 61.0)

        first.finish()
        health = owner_service_health(service, config, started)
        self.assertNotIn("bundle_build_stuck", health["reasons"])
        self.assertEqual(health["bundle_build_oldest_age_seconds"], 51.0)
        second.finish()

    def test_multiple_failures_keep_the_fixed_reason_order(self) -> None:
        service, clock, config, started = owner_progress_service(
            bundle_build_deadline_seconds=60.0
        )
        build = service.start_bundle_build()
        clock.advance(61)

        health = owner_service_health(service, config, started)

        self.assertEqual(
            health["reasons"],
            [
                "tip_poll_stale",
                "bundle_build_stuck",
                "current_generation_not_published",
            ],
        )
        build.finish()


# --- Router-facing mining readiness (issue #186) ----------------------------


def readiness_sample(
    now: float,
    *,
    ratio: float = 1.0,
    pending: bool = False,
    pending_age: float | None = None,
    too_long: bool = False,
    requiring: int = 0,
    candidate_age: float | None = None,
    timeouts: int = 0,
) -> MiningReadinessSample:
    return MiningReadinessSample(
        monotonic=now,
        semantic_current_work_ratio=ratio,
        refresh_pending=pending,
        refresh_pending_age_seconds=pending_age,
        refresh_pending_too_long=too_long,
        eligible_clients_requiring_refresh=requiring,
        oldest_durable_candidate_age_seconds=candidate_age,
        accepted_parent_preview_wait_timeouts=timeouts,
    )


def hms(clock: str) -> float:
    """Seconds after 20:00:00Z on the incident day, from an HH:MM:SS string."""

    hours, minutes, seconds = (int(part) for part in clock.split(":"))
    return float((hours - 20) * 3600 + minutes * 60 + seconds)


class Trace:
    """Feed a tracker samples in order and record every state change."""

    def __init__(self, config: MiningReadinessConfig | None = None) -> None:
        self.tracker = MiningReadinessTracker(config or MiningReadinessConfig())
        self.transitions: list[tuple[float, str]] = []
        self.snapshots: list[MiningReadinessSnapshot] = []
        self._last_state: str | None = None
        self._last_now: float | None = None

    def observe(self, sample: MiningReadinessSample) -> MiningReadinessSnapshot:
        if self._last_now is not None and sample.monotonic < self._last_now:
            raise AssertionError("trace samples must be monotonic")
        self._last_now = sample.monotonic
        snapshot = self.tracker.observe(sample)
        if snapshot.state != self._last_state:
            self.transitions.append((sample.monotonic, snapshot.state))
            self._last_state = snapshot.state
        self.snapshots.append(snapshot)
        return snapshot


class MiningReadinessPolicyTests(unittest.TestCase):
    """The hysteresis contract, pinned on a scripted clock."""

    def test_vocabulary_thresholds_and_defaults_are_pinned(self) -> None:
        # Fixed cardinality: these names are the only labels the reason
        # gauge carries and the only members ``reasons`` may list.
        self.assertEqual(
            MINING_READINESS_REASONS,
            (
                "warming_up",
                "semantic_coverage_low",
                "refresh_pending_too_long",
                "refresh_pending",
                "recovery_window_pending",
                "durable_candidate_old",
                "accepted_parent_preview_timeouts",
            ),
        )
        self.assertEqual(MINING_READINESS_STATES, ("ready", "degraded"))
        self.assertEqual(MINING_READINESS_SCHEMA, "qbit.prism.mining-readiness.v1")
        self.assertEqual(MINING_READINESS_ENTRY_COVERAGE_RATIO, 0.95)
        self.assertEqual(MINING_READINESS_RECOVERY_COVERAGE_RATIO, 0.99)
        self.assertEqual(MINING_READINESS_OLD_CANDIDATE_AGE_SECONDS, 60.0)
        self.assertEqual(DEFAULT_PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS, 60.0)
        self.assertEqual(
            DEFAULT_PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS,
            240.0,
        )
        config = MiningReadinessConfig()
        self.assertEqual(config.entry_dwell_seconds, 60.0)
        self.assertEqual(config.recovery_window_seconds, 240.0)

    def test_config_rejects_recovery_shorter_than_dwell_and_bad_numbers(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least entry_dwell_seconds"):
            MiningReadinessConfig(entry_dwell_seconds=60.0, recovery_window_seconds=59.999)
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=bad), self.assertRaisesRegex(ValueError, "finite"):
                MiningReadinessConfig(entry_dwell_seconds=bad)
            with self.subTest(value=bad), self.assertRaisesRegex(ValueError, "finite"):
                MiningReadinessConfig(recovery_window_seconds=bad)
        with self.assertRaisesRegex(ValueError, "nonnegative"):
            MiningReadinessConfig(entry_dwell_seconds=-1.0)
        with self.assertRaisesRegex(ValueError, "nonnegative"):
            MiningReadinessConfig(entry_dwell_seconds=0.0, recovery_window_seconds=-0.5)
        with self.assertRaisesRegex(ValueError, "must be a number"):
            MiningReadinessConfig(entry_dwell_seconds="60")  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "must be a number"):
            MiningReadinessConfig(entry_dwell_seconds=True)  # type: ignore[arg-type]
        # Equal windows and zero windows are legitimate, and ints are
        # normalized to floats so the response echoes one type.
        equal = MiningReadinessConfig(entry_dwell_seconds=30, recovery_window_seconds=30)
        self.assertEqual(
            (equal.entry_dwell_seconds, equal.recovery_window_seconds),
            (30.0, 30.0),
        )
        zero = MiningReadinessConfig(entry_dwell_seconds=0, recovery_window_seconds=0)
        self.assertEqual((zero.entry_dwell_seconds, zero.recovery_window_seconds), (0.0, 0.0))

    def test_first_sample_latches_ready_and_starts_the_clock(self) -> None:
        trace = Trace()
        snapshot = trace.observe(readiness_sample(100.0, ratio=0.0, too_long=True))

        # Ready is the initial latch even on a bad first sample: the dwell
        # must be sustained before the signal ever degrades.
        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.state, "ready")
        self.assertEqual(snapshot.transitions, 0)
        self.assertEqual(snapshot.state_since_monotonic, 100.0)
        self.assertEqual(snapshot.state_age_seconds(100.0), 0.0)
        self.assertEqual(snapshot.entry_streak_seconds, 0.0)
        self.assertEqual(snapshot.recovery_streak_seconds, 0.0)
        # The conditions being dwelled on are visible while still ready.
        self.assertEqual(
            snapshot.reasons,
            ("semantic_coverage_low", "refresh_pending_too_long"),
        )
        self.assertEqual(snapshot.accepted_parent_preview_timeout_rate_per_second, 0.0)

    def test_2026_08_20_incident_degrades_once_and_recovers_after_stable_window(
        self,
    ) -> None:
        # Samples every 5s on the health-refresh cadence, phased so the
        # apparent healthy point at 20:10:27Z is itself a sample.
        trace = Trace()
        clocks: list[tuple[float, MiningReadinessSample]] = []

        def cycle(start: str, end: str, make: object) -> None:
            now = hms(start)
            while now <= hms(end):
                clocks.append((now, make(now)))  # type: ignore[operator]
                now += 5.0

        # A calm minute before the incident.
        cycle("20:00:02", "20:00:57", lambda now: readiness_sample(now))
        # The incident: coverage collapses to 0.077 / 0.286 while the
        # refresh stays pending far past the progress-health deadline.
        incident_ratios = (0.077, 0.286)
        cycle(
            "20:01:02",
            "20:10:22",
            lambda now: readiness_sample(
                now,
                ratio=incident_ratios[int(now // 5) % 2],
                pending=True,
                pending_age=now - hms("20:01:02"),
                too_long=True,
                requiring=30,
            ),
        )
        # Apparent health at 20:10:27Z, then two regressions inside the
        # 213 seconds before real stability at 20:14:00Z.
        cycle("20:10:27", "20:11:57", lambda now: readiness_sample(now))
        cycle(
            "20:12:02",
            "20:12:02",
            lambda now: readiness_sample(now, ratio=0.5, pending=True, pending_age=3.0),
        )
        cycle("20:12:07", "20:13:27", lambda now: readiness_sample(now))
        cycle(
            "20:13:32",
            "20:13:52",
            lambda now: readiness_sample(
                now, ratio=0.286, pending=True, pending_age=now - hms("20:13:30"), too_long=True
            ),
        )
        # Stability from 20:14:00Z, held well past the recovery window.
        cycle("20:14:02", "20:20:02", lambda now: readiness_sample(now))

        for now, sample in clocks:
            snapshot = trace.observe(sample)
            self.assertEqual(snapshot.state_age_seconds(now), now - snapshot.state_since_monotonic)
            if hms("20:01:02") <= now < hms("20:02:02"):
                # Dwelling: still ready, streak counting, no transition yet.
                self.assertTrue(snapshot.ready, now)
                self.assertEqual(snapshot.entry_streak_seconds, now - hms("20:01:02"))
                self.assertIn("semantic_coverage_low", snapshot.reasons)
                self.assertIn("refresh_pending_too_long", snapshot.reasons)
            elif hms("20:02:02") <= now < hms("20:18:02"):
                self.assertFalse(snapshot.ready, now)
                self.assertEqual(snapshot.transitions, 1, now)
                self.assertEqual(snapshot.state_since_monotonic, hms("20:02:02"))
            if hms("20:10:27") <= now <= hms("20:11:57"):
                # Recovery counting from the apparent healthy point.
                self.assertEqual(snapshot.recovery_streak_seconds, now - hms("20:10:27"))
                self.assertEqual(snapshot.reasons, ("recovery_window_pending",))
            if now == hms("20:12:02"):
                # A contrary sample cancels the streak outright.
                self.assertEqual(snapshot.recovery_streak_seconds, 0.0)
                self.assertEqual(
                    snapshot.reasons,
                    ("semantic_coverage_low", "refresh_pending"),
                )
            if now == hms("20:13:52"):
                self.assertEqual(snapshot.recovery_streak_seconds, 0.0)
                self.assertEqual(
                    snapshot.reasons,
                    ("semantic_coverage_low", "refresh_pending_too_long"),
                )
            if now == hms("20:17:57"):
                self.assertFalse(snapshot.ready)
                self.assertEqual(snapshot.recovery_streak_seconds, 235.0)
            if now >= hms("20:18:02"):
                self.assertTrue(snapshot.ready, now)
                self.assertEqual(snapshot.transitions, 2)
                self.assertEqual(snapshot.state_since_monotonic, hms("20:18:02"))
                self.assertEqual(snapshot.reasons, ())

        # Exactly one degradation and one recovery across the whole trace:
        # the initial latch, the dwell landing at 20:02:02Z, and the stable
        # window closing 240s after 20:14:02Z. No intermediate flap.
        self.assertEqual(
            trace.transitions,
            [
                (hms("20:00:02"), "ready"),
                (hms("20:02:02"), "degraded"),
                (hms("20:18:02"), "ready"),
            ],
        )
        self.assertEqual(trace.tracker.transitions, 2)

        # Counterfactual pin for the 240s default: a window shorter than the
        # 213-second oscillation would have announced ready off the apparent
        # healthy point, 125 seconds before the fleet was actually stable.
        short = Trace(MiningReadinessConfig(entry_dwell_seconds=60.0, recovery_window_seconds=90.0))
        for _, sample in clocks:
            short.observe(sample)
        self.assertEqual(short.transitions[2], (hms("20:11:57"), "ready"))
        self.assertLess(short.transitions[2][0], hms("20:14:02"))

    def test_2026_08_21_accepted_tip_cycles_never_transition(self) -> None:
        # Every accepted tip sweeps semantic coverage 0 -> 0.83 -> 0.91 -> 1
        # -> 0 while the refresh resolves within the observed ~36.77s. The
        # progress-health deadline (15s) marks the tail of each cycle as
        # refresh_pending_too_long, so the entry condition is genuinely
        # true for most of every cycle -- and still never long enough.
        cycle_seconds = 45.0
        resolution_seconds = 36.77
        steps = (
            (0.0, 0.0, True),
            (5.0, 0.0, True),
            (10.0, 0.0, True),
            (15.0, 0.0, True),
            (20.0, 0.83, True),
            (25.0, 0.83, True),
            (30.0, 0.91, True),
            (35.0, 0.91, True),
            (resolution_seconds, 1.0, False),
            (40.0, 1.0, False),
        )
        samples: list[MiningReadinessSample] = []
        for cycle in range(40):
            start = 1_000.0 + cycle * cycle_seconds
            for offset, ratio, pending in steps:
                samples.append(
                    readiness_sample(
                        start + offset,
                        ratio=ratio,
                        pending=pending,
                        pending_age=offset if pending else None,
                        too_long=pending and offset > 15.0,
                        requiring=0 if ratio >= 1.0 else 12,
                    )
                )

        trace = Trace()
        longest_dwell = 0.0
        for sample in samples:
            snapshot = trace.observe(sample)
            self.assertTrue(snapshot.ready, sample.monotonic)
            self.assertEqual(snapshot.transitions, 0)
            longest_dwell = max(longest_dwell, snapshot.entry_streak_seconds)
        self.assertEqual(trace.transitions, [(1_000.0, "ready")])
        # The dwell really was exercised: the entry condition held for the
        # whole 35s bad stretch of each cycle and was reset by the 1.0 sample.
        self.assertEqual(longest_dwell, 35.0)
        self.assertLess(resolution_seconds, MiningReadinessConfig().entry_dwell_seconds)

        # Counterfactual pin for the 60s default: a dwell inside the normal
        # cycle would have degraded on the first tip, and the few seconds of
        # full coverage between tips could never satisfy any recovery window.
        short = Trace(MiningReadinessConfig(entry_dwell_seconds=30.0, recovery_window_seconds=30.0))
        for sample in samples:
            short.observe(sample)
        self.assertEqual(short.transitions, [(1_000.0, "ready"), (1_030.0, "degraded")])

    def test_timers_reset_on_contrary_samples_and_change_state_once(self) -> None:
        trace = Trace()
        now = 0.0

        def run(seconds: float, **sample_kwargs: object) -> MiningReadinessSnapshot:
            nonlocal now
            snapshot = trace.observe(readiness_sample(now, **sample_kwargs))  # type: ignore[arg-type]
            end = now + seconds
            while now + 5.0 <= end:
                now += 5.0
                snapshot = trace.observe(readiness_sample(now, **sample_kwargs))  # type: ignore[arg-type]
            now += 5.0
            return snapshot

        # 55s bad, one good sample, 55s bad: two separate dwells, no latch.
        self.assertTrue(run(55.0, ratio=0.5).ready)
        self.assertEqual(trace.tracker.transitions, 0)
        cleared = run(0.0)
        self.assertEqual(cleared.entry_streak_seconds, 0.0)
        self.assertTrue(run(55.0, ratio=0.5).ready)
        self.assertEqual(trace.tracker.transitions, 0)
        # Only a full 60s dwell latches, and only once for the episode.
        degraded = run(120.0, ratio=0.5)
        self.assertFalse(degraded.ready)
        self.assertEqual(trace.tracker.transitions, 1)
        self.assertEqual(degraded.entry_streak_seconds, 0.0)
        # 235s good, one contrary sample, 235s good: no recovery.
        self.assertFalse(run(235.0).ready)
        self.assertEqual(run(0.0, ratio=0.97).recovery_streak_seconds, 0.0)
        self.assertFalse(run(235.0).ready)
        self.assertEqual(trace.tracker.transitions, 1)
        # A full 240s stable window recovers exactly once.
        recovered = run(300.0)
        self.assertTrue(recovered.ready)
        self.assertEqual(trace.tracker.transitions, 2)
        self.assertEqual(len(trace.transitions), 3)

    def test_entry_and_recovery_windows_are_independent(self) -> None:
        immediate = Trace(MiningReadinessConfig(entry_dwell_seconds=0.0, recovery_window_seconds=0.0))
        self.assertFalse(immediate.observe(readiness_sample(1.0, ratio=0.0)).ready)
        self.assertTrue(immediate.observe(readiness_sample(2.0)).ready)
        self.assertEqual(immediate.tracker.transitions, 2)

        skewed = Trace(MiningReadinessConfig(entry_dwell_seconds=10.0, recovery_window_seconds=100.0))
        for now in (0.0, 5.0, 9.0):
            self.assertTrue(skewed.observe(readiness_sample(now, ratio=0.0)).ready)
        self.assertFalse(skewed.observe(readiness_sample(10.0, ratio=0.0)).ready)
        # Recovery counts from the first good sample at 20s, not from the
        # degradation at 10s: the windows do not share a clock.
        for now in (20.0, 60.0, 119.0):
            self.assertFalse(skewed.observe(readiness_sample(now)).ready)
        self.assertTrue(skewed.observe(readiness_sample(120.0)).ready)
        self.assertEqual(skewed.transitions[-1], (120.0, "ready"))

    def test_short_pending_refresh_neither_degrades_nor_counts_as_stable(self) -> None:
        trace = Trace()
        # Ready: an in-budget pending refresh with full coverage is ordinary
        # tip handling and must not start a dwell.
        for now in range(0, 600, 5):
            snapshot = trace.observe(
                readiness_sample(float(now), pending=True, pending_age=float(now % 40))
            )
            self.assertTrue(snapshot.ready)
            self.assertEqual(snapshot.entry_streak_seconds, 0.0)
            self.assertEqual(snapshot.reasons, ())
        self.assertEqual(trace.tracker.transitions, 0)

        # Degraded: the same pending refresh blocks recovery, as does an
        # eligible client still requiring refresh or coverage under 0.99.
        degraded = Trace(MiningReadinessConfig(entry_dwell_seconds=0.0, recovery_window_seconds=10.0))
        degraded.observe(readiness_sample(0.0, ratio=0.0))
        pending = degraded.observe(readiness_sample(5.0, pending=True, pending_age=2.0))
        self.assertFalse(pending.ready)
        self.assertEqual(pending.recovery_streak_seconds, 0.0)
        self.assertEqual(pending.reasons, ("refresh_pending",))
        requiring = degraded.observe(readiness_sample(10.0, requiring=1))
        self.assertEqual(requiring.reasons, ("refresh_pending",))
        self.assertEqual(requiring.recovery_streak_seconds, 0.0)
        partial = degraded.observe(readiness_sample(15.0, ratio=0.985))
        self.assertEqual(partial.reasons, ("semantic_coverage_low",))
        self.assertEqual(partial.recovery_streak_seconds, 0.0)
        stable = degraded.observe(readiness_sample(20.0, ratio=0.99))
        self.assertEqual(stable.reasons, ("recovery_window_pending",))
        self.assertFalse(stable.ready)
        self.assertTrue(degraded.observe(readiness_sample(30.0, ratio=0.99)).ready)

    def test_annotations_decorate_degraded_snapshots_but_never_latch(self) -> None:
        trace = Trace(MiningReadinessConfig(entry_dwell_seconds=10.0, recovery_window_seconds=20.0))
        # Ready, with an ancient candidate and a preview-timeout burst:
        # both are reported as fields, neither is a reason, nothing latches.
        first = trace.observe(readiness_sample(0.0, candidate_age=500.0, timeouts=0))
        burst = trace.observe(readiness_sample(5.0, candidate_age=505.0, timeouts=10))
        self.assertTrue(burst.ready)
        self.assertEqual(first.reasons, ())
        self.assertEqual(burst.reasons, ())
        self.assertEqual(burst.oldest_durable_candidate_age_seconds, 505.0)
        self.assertEqual(burst.accepted_parent_preview_timeout_rate_per_second, 2.0)
        for now in (100.0, 200.0, 300.0):
            self.assertTrue(trace.observe(readiness_sample(now, candidate_age=now, timeouts=10 + int(now))).ready)
        self.assertEqual(trace.tracker.transitions, 0)

        # Degraded: the same facts annotate the snapshot ...
        trace.observe(readiness_sample(400.0, ratio=0.0, candidate_age=59.999, timeouts=1000))
        degraded = trace.observe(readiness_sample(410.0, ratio=0.0, candidate_age=60.0, timeouts=1005))
        self.assertFalse(degraded.ready)
        self.assertEqual(
            degraded.reasons,
            (
                "semantic_coverage_low",
                "durable_candidate_old",
                "accepted_parent_preview_timeouts",
            ),
        )
        below = trace.observe(readiness_sample(415.0, ratio=0.0, candidate_age=59.999, timeouts=1005))
        self.assertEqual(below.reasons, ("semantic_coverage_low",))
        # ... and do not block recovery either.
        recovering = trace.observe(readiness_sample(420.0, candidate_age=900.0, timeouts=1100))
        self.assertEqual(
            recovering.reasons,
            (
                "recovery_window_pending",
                "durable_candidate_old",
                "accepted_parent_preview_timeouts",
            ),
        )
        self.assertTrue(trace.observe(readiness_sample(440.0, candidate_age=920.0, timeouts=1200)).ready)
        self.assertEqual(trace.tracker.transitions, 2)

    def test_preview_timeout_rate_is_a_bounded_counter_difference(self) -> None:
        tracker = MiningReadinessTracker(MiningReadinessConfig())
        # First sample: no prior counter, so a safe zero rather than a guess.
        self.assertEqual(
            tracker.observe(readiness_sample(0.0, timeouts=7)).accepted_parent_preview_timeout_rate_per_second,
            0.0,
        )
        self.assertEqual(
            tracker.observe(readiness_sample(5.0, timeouts=10)).accepted_parent_preview_timeout_rate_per_second,
            0.6,
        )
        self.assertEqual(
            tracker.observe(readiness_sample(15.0, timeouts=10)).accepted_parent_preview_timeout_rate_per_second,
            0.0,
        )
        # A counter that runs backwards reads as no activity, never negative.
        self.assertEqual(
            tracker.observe(readiness_sample(20.0, timeouts=3)).accepted_parent_preview_timeout_rate_per_second,
            0.0,
        )
        # Zero elapsed time cannot divide; it also reads as no activity.
        self.assertEqual(
            tracker.observe(readiness_sample(20.0, timeouts=9)).accepted_parent_preview_timeout_rate_per_second,
            0.0,
        )
        # Only the last pair is retained: no history grows with uptime.
        self.assertEqual(
            sorted(name for name in vars(tracker) if "timeout" in name),
            ["_last_timeout_count", "_last_timeout_monotonic"],
        )

    def test_state_age_is_monotonic_between_transitions(self) -> None:
        trace = Trace(MiningReadinessConfig(entry_dwell_seconds=10.0, recovery_window_seconds=10.0))
        ages: list[float] = []
        for now in range(0, 100, 5):
            snapshot = trace.observe(readiness_sample(float(now), ratio=0.0 if now < 50 else 1.0))
            ages.append(snapshot.state_age_seconds(float(now)))
        # ready 0..5 (age 0,5), degraded from 10 (age 0..35), ready from 60.
        self.assertEqual(
            ages,
            [0.0, 5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0],
        )
        self.assertEqual(
            trace.transitions,
            [(0.0, "ready"), (10.0, "degraded"), (60.0, "ready")],
        )
        # Age keeps growing between refreshes, from the same latched stamp.
        last = trace.snapshots[-1]
        self.assertEqual(last.state_age_seconds(1_000.0), 940.0)
        self.assertEqual(last.sample_age_seconds(1_000.0), 905.0)


if __name__ == "__main__":
    unittest.main()
