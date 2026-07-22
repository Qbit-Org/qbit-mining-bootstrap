#!/usr/bin/env python3
"""Direct payout-state service and port-contract tests for the tip pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace as dataclass_replace
import json
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import patch

from lab.prism.payout_state import (
    PayoutStateConfig,
    PayoutStatePorts,
    PayoutStateService,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
)
from lab.prism.coordinator_shutdown import ShutdownInProgress
from lab.prism.template_artifacts import QbitTipTemplateSnapshot
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    qbit_template_fingerprint,
)
from lab.prism.tip_refresh import (
    FanoutCancellation,
    RefreshClientTarget,
    RefreshResult,
    TipRefreshConfig,
    TipRefreshPorts,
    TipRefreshService,
)


@dataclass(frozen=True)
class _Share:
    sequence: int

    def to_prism_json(self) -> dict[str, object]:
        return {"sequence": self.sequence}


class _ServiceFixture:
    def __init__(self) -> None:
        self.now = 100.0
        self.accepted_count = 1
        self.invalidated: list[tuple[int, float]] = []
        self.published: list[tuple[int, float]] = []
        self.cache_invalidations = 0
        self.refresh_retries = 0
        self.phases: list[tuple[str, float]] = []
        ports = PayoutStatePorts(
            accepted_share_stats=lambda: (self.accepted_count, 1),
            snapshot_at_job_issue=lambda _anchor, _window: [_Share(1)],
            current_prior_balances=lambda: [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 5,
                    "metadata": {"durability": ["accepted", "fenced"]},
                }
            ],
            snapshot_anchor_ms=lambda value: value - 1,
            current_template_network_difficulty=lambda: None,
            pool_ready=lambda: False,
            record_build_phase=lambda phase, elapsed: self.phases.append(
                (phase, elapsed)
            ),
            invalidate_job_cache=self._invalidate_cache,
            clear_retained_collection_refresh=lambda: None,
            cancel_obsolete_job_builds=lambda _reason: None,
            cancel_obsolete_bundle_builds=lambda _generation: None,
            payout_invalidated=lambda generation, stamp: self.invalidated.append(
                (generation, stamp)
            ),
            payout_published=lambda generation, stamp: self.published.append(
                (generation, stamp)
            ),
            schedule_refresh_retry=self._schedule_retry,
            chain_block_hash=lambda _height: "aa" * 32,
            stop_requested=lambda: False,
        )
        self.service = PayoutStateService(
            ports,
            monotonic=lambda: self.now,
            wall_time_ms=lambda: 1_700_000_000_000,
            config=PayoutStateConfig(
                accepted_block_preview_wait_seconds=0.0,
                reconcile_supersession_retries=2,
            ),
        )

    def _invalidate_cache(self) -> None:
        self.cache_invalidations += 1

    def _schedule_retry(self) -> None:
        self.refresh_retries += 1


class PayoutStateServiceTests(unittest.TestCase):
    def test_invalidation_and_publication_emit_exact_generation_boundary(
        self,
    ) -> None:
        fixture = _ServiceFixture()
        service = fixture.service
        service.reserve_source(
            "payout_only",
            invalidated_monotonic=42.5,
        )
        service.block_publication(force=True)
        published = service.publish_candidate(service.current_candidate())

        self.assertEqual(published, 1)
        self.assertEqual(fixture.invalidated, [(1, 42.5)])
        self.assertEqual(fixture.published, [(1, 42.5)])
        snapshot = service.snapshot()
        self.assertEqual(snapshot.generation, 1)
        self.assertFalse(snapshot.publication_blocked)
        self.assertIsNotNone(snapshot.published.artifact)
        self.assertEqual(fixture.cache_invalidations, 2)

    def test_stale_source_candidate_cannot_publish(self) -> None:
        fixture = _ServiceFixture()
        service = fixture.service
        service.reserve_source("first", invalidated_monotonic=1.0)
        stale = service.current_candidate()
        service.reserve_source("second", invalidated_monotonic=2.0)

        self.assertIsNone(service.publish_candidate(stale))
        self.assertEqual(service.snapshot().generation, 0)
        self.assertEqual(
            service.metrics_snapshot()["discarded_candidates"],
            1,
        )

    def test_first_delivery_priority_survives_publication(self) -> None:
        fixture = _ServiceFixture()
        service = fixture.service
        service.reserve_source("payout", invalidated_monotonic=1.0)
        service.block_publication(force=True)
        self.assertEqual(service.publish_candidate(service.current_candidate()), 1)

        with service.delivery(
            1,
            cancelled=lambda: False,
            priority=False,
        ) as routine:
            self.assertFalse(routine)
        with service.delivery(
            1,
            cancelled=lambda: False,
            priority=True,
        ) as first:
            self.assertTrue(first)
            first.mark_delivered()
        with service.delivery(
            1,
            cancelled=lambda: False,
            priority=False,
        ) as routine_after_first:
            self.assertTrue(routine_after_first)

    def test_landed_preview_withdrawal_leaves_fail_closed_tombstone(self) -> None:
        fixture = _ServiceFixture()
        service = fixture.service
        block_hash = "aa" * 32
        service.begin_accepted_block_preview(block_hash, block_height=10)
        service.mark_accepted_block_landed(block_hash, block_height=10)
        service.clear_accepted_block_preview(
            block_hash,
            invalidate_published=True,
        )

        self.assertNotIn(block_hash, service.previews)
        self.assertEqual(service.invalidated_previews, {block_hash: 10})
        with self.assertRaisesRegex(TemplateRefreshBlocked, "was withdrawn"):
            service.prior_balances_for_parent(block_hash, parent_height=10)
        self.assertEqual(fixture.refresh_retries, 1)

    def test_ledger_artifact_uses_pending_share_anchor_port(self) -> None:
        fixture = _ServiceFixture()
        artifact = fixture.service.build_ledger_artifact(0, 0, 100)

        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, 1_699_999_999_999)
        self.assertEqual(artifact.accepted_share_count, 1)
        self.assertEqual(artifact.shares_json, ({"sequence": 1},))
        self.assertEqual(
            [phase for phase, _elapsed in fixture.phases],
            ["ledger_snapshot", "serialization_copy"],
        )

    def test_ledger_artifact_snapshot_rejects_deep_mutation(self) -> None:
        fixture = _ServiceFixture()
        service = fixture.service
        service.prepare_ledger_artifact(0, 100)

        first = service.snapshot().ledger_artifact
        assert first is not None
        with self.assertRaisesRegex(TypeError, "immutable"):
            first.shares_json[0]["sequence"] = 99
        with self.assertRaisesRegex(TypeError, "immutable"):
            first.prior_balances[0]["balance_sats"] = 99
        metadata = first.prior_balances[0]["metadata"]
        assert isinstance(metadata, dict)
        durability = metadata["durability"]
        assert isinstance(durability, list)
        with self.assertRaisesRegex(TypeError, "immutable"):
            durability.append("corrupt")

        second = service.snapshot().ledger_artifact
        assert second is not None
        self.assertEqual(second.shares_json[0]["sequence"], 1)
        self.assertEqual(second.prior_balances[0]["balance_sats"], 5)
        self.assertEqual(
            second.prior_balances[0]["metadata"],
            {"durability": ["accepted", "fenced"]},
        )
        self.assertEqual(
            json.loads(
                json.dumps(
                    {
                        "shares": second.shares_json,
                        "prior_balances": second.prior_balances,
                    }
                )
            )["prior_balances"][0]["metadata"],
            {"durability": ["accepted", "fenced"]},
        )


class _TipPayoutGate:
    @contextmanager
    def delivery_cancelable(
        self,
        _cancelled: object,
        *,
        generation: int,
        priority: bool = False,
    ) -> object:
        del generation, priority
        yield True


class _TipPayout:
    def __init__(self) -> None:
        self.generation = 0
        self.delivery_gate = _TipPayoutGate()
        self.reservations: list[tuple[str, str, float]] = []

    def snapshot(self) -> object:
        return SimpleNamespace(
            generation=self.generation,
            publication_blocked=False,
            published=SimpleNamespace(artifact=None),
        )

    def reserve_source_for_tip_change(
        self,
        tip_hash: str,
        *,
        cause: str,
        invalidated_monotonic: float,
    ) -> int:
        self.reservations.append((tip_hash, cause, invalidated_monotonic))
        return self.generation


class _TipJobs:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.ready = False
        self.readiness_checks = 0
        self.prepared_clears = 0

    def begin_priority_preparation(
        self,
        requested_monotonic: float | None = None,
    ) -> tuple[int, float]:
        return 1, 0.0 if requested_monotonic is None else requested_monotonic

    def finish_priority_preparation(self, _token: int) -> None:
        return None

    def ready_latched(self) -> bool:
        return self.ready

    def clear_prepared_ready(self) -> None:
        self.prepared_clears += 1

    def record_failure(self) -> None:
        return None

    def pool_readiness_latched(self) -> bool:
        self.events.append("readiness")
        self.readiness_checks += 1
        return self.ready

    def set_preparation_pending(self, _pending: bool) -> None:
        return None

    def set_prepared_ready(self, _snapshot: object, _bundle: object) -> None:
        return None


class _TipDelivery:
    def eligible_clients(self) -> tuple[object, ...]:
        return ()

    def client_can_receive_jobs(self, _client: object) -> bool:
        return False

    def client_needs_refresh(
        self,
        _client: object,
        _snapshot: QbitTipTemplateSnapshot,
    ) -> bool:
        return False

    def active_job(self, _client: object) -> object | None:
        return None

    def connection_id(self, _client: object) -> int:
        return 0

    def delivery_priority(
        self,
        _client: object,
        _snapshot: QbitTipTemplateSnapshot,
        _expected_active_job: object | None,
    ) -> int:
        return 0

    def select_targets(
        self,
        _snapshot: QbitTipTemplateSnapshot,
        *,
        refresh_all: bool,
    ) -> tuple[RefreshClientTarget, ...]:
        del refresh_all
        return ()

    def merge_poll_start_targets(
        self,
        targets: tuple[RefreshClientTarget, ...],
        _poll_start_clients: tuple[object, ...],
        _snapshot: QbitTipTemplateSnapshot,
        *,
        refresh_all: bool,
    ) -> tuple[RefreshClientTarget, ...]:
        del refresh_all
        return targets

    def revalidate_targets(
        self,
        targets: tuple[RefreshClientTarget, ...],
        _snapshot: QbitTipTemplateSnapshot,
    ) -> tuple[tuple[RefreshClientTarget, ...], tuple[str, ...]]:
        return targets, ()

    def deliver_collection(
        self,
        _client: object,
        _snapshot: QbitTipTemplateSnapshot,
        _observation_sequence: int,
    ) -> RefreshResult:
        return RefreshResult("skipped")

    def take_post_accept_refresh(self, _client: object) -> tuple[int, str] | None:
        return None


class _RefreshActivity:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def note_activity(self, _observed_monotonic: float | None = None) -> None:
        self.events.append("activity")

    def finish(self) -> None:
        self.events.append("finish")


class _TipRefreshFixture:
    TIP_A = "aa" * 32
    TIP_B = "bb" * 32
    TIP_C = "cc" * 32

    def __init__(self) -> None:
        self.now = 100.0
        self.tip = self.TIP_A
        self.events: list[str] = []
        self.removed_heartbeats: list[str] = []
        self.bundle_cancellations: list[tuple[str | None, int | None]] = []
        self.payout = _TipPayout()
        self.jobs = _TipJobs(self.events)
        self.delivery = _TipDelivery()
        self.snapshot = self.make_snapshot(self.tip, "11" * 32)
        self.fetch_error: Exception | None = None
        self.service = TipRefreshService(
            TipRefreshConfig(
                blockpoll_seconds=1.0,
                blockwait_timeout_seconds=5.0,
                failure_holdoff_seconds=0.0,
                max_workers=2,
                submit_tip_max_age_seconds=10.0,
                failure_exit_seconds=30.0,
                watchdog_timeout_seconds=120.0,
            ),
            TipRefreshPorts(
                rpc_call=self.rpc_call,
                rpc_call_with_timeout=lambda method, params, timeout: self.rpc_call(
                    method, params
                ),
                payout_state=lambda: self.payout,
                job_bundles=lambda: self.jobs,
                delivery=self.delivery,
                mark_progress_pending=lambda _stamp: self.events.append("pending"),
                observe_progress_tip_poll=lambda _snapshot: self.events.append(
                    "coherent"
                ),
                publish_progress_work=lambda _snapshot, _generation: self.events.append(
                    "publish"
                ),
                start_progress_refresh=self.start_refresh,
                cancel_obsolete_bundle_builds=self.cancel_bundles,
                cancel_obsolete_job_builds=lambda _reason: self.events.append(
                    "cancel-jobs"
                ),
                prune_evicted_jobs=lambda _now, _force: None,
                delivery_queue_limit=lambda: 4,
                stop_requested=lambda: False,
                heartbeat=lambda _name: None,
                remove_heartbeat=self.removed_heartbeats.append,
                chain_view_untrusted=lambda: False,
                ensure_reorg_current=lambda _tip: True,
                observe_job_build_elapsed=lambda _elapsed, _phases: None,
                fetch_snapshot=self.fetch_snapshot,
                ensure_reorg_tip=lambda _tip: True,
                wait_for_execution_permit=lambda _timeout: True,
                wait_for_stop=lambda _seconds: False,
                hard_exit=lambda code: (_ for _ in ()).throw(SystemExit(code)),
            ),
            monotonic=lambda: self.now,
        )

    @staticmethod
    def make_snapshot(tip: str, fingerprint: str) -> QbitTipTemplateSnapshot:
        return QbitTipTemplateSnapshot(
            bestblockhash=tip,
            previousblockhash=tip,
            template_fingerprint=fingerprint,
        )

    def rpc_call(self, method: str, _params: list[object] | None) -> object:
        if method == "getbestblockhash":
            return self.tip
        if method == "getblock":
            return {"previousblockhash": "00" * 32}
        raise AssertionError(method)

    def fetch_snapshot(self) -> QbitTipTemplateSnapshot:
        if self.fetch_error is not None:
            raise self.fetch_error
        return self.snapshot

    def start_refresh(self) -> _RefreshActivity:
        self.events.append("start")
        return _RefreshActivity(self.events)

    def cancel_bundles(
        self,
        tip_hash: str | None,
        payout_generation: int | None,
    ) -> None:
        self.bundle_cancellations.append((tip_hash, payout_generation))
        self.events.append("cancel-bundles")


class TipRefreshServiceTests(unittest.TestCase):
    @staticmethod
    def _scheduler_trigger(
        fixture: _TipRefreshFixture,
        *,
        sequence: int,
        tip: str,
        payout_generation: int = 0,
        ready_required: bool = False,
        reason: str = "blockpoll",
        pending_token: int | None = None,
    ) -> object:
        return fixture.service._new_trigger(
            observation_sequence=sequence,
            tip_hash=tip,
            payout_state_generation=payout_generation,
            ready_required=ready_required,
            reasons=(reason,),
            pending_signal_token=pending_token,
        )

    def test_observation_effects_cannot_reorder_behind_a_newer_tip(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(service.publish_tip(fixture.TIP_A, observation_sequence=1))

        older_effect_started = threading.Event()
        release_older_effect = threading.Event()
        newer_call_started = threading.Event()
        newer_call_finished = threading.Event()
        original_reserve = fixture.payout.reserve_source_for_tip_change

        def pause_older_reservation(
            tip_hash: str,
            *,
            cause: str,
            invalidated_monotonic: float,
        ) -> int:
            if tip_hash == fixture.TIP_B:
                older_effect_started.set()
                self.assertTrue(release_older_effect.wait(2.0))
            return original_reserve(
                tip_hash,
                cause=cause,
                invalidated_monotonic=invalidated_monotonic,
            )

        fixture.payout.reserve_source_for_tip_change = pause_older_reservation  # type: ignore[method-assign]
        results: list[tuple[str, bool]] = []
        errors: list[BaseException] = []

        def observe(tip_hash: str, sequence: int) -> None:
            try:
                if tip_hash == fixture.TIP_C:
                    newer_call_started.set()
                results.append(
                    (tip_hash, service.observe_tip(tip_hash, observation_sequence=sequence))
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                if tip_hash == fixture.TIP_C:
                    newer_call_finished.set()

        older = threading.Thread(target=observe, args=(fixture.TIP_B, 2))
        newer = threading.Thread(target=observe, args=(fixture.TIP_C, 3))
        older.start()
        self.assertTrue(older_effect_started.wait(2.0))
        newer.start()
        self.assertTrue(newer_call_started.wait(2.0))
        self.assertFalse(newer_call_finished.wait(0.05))
        release_older_effect.set()
        older.join(2.0)
        newer.join(2.0)

        self.assertFalse(older.is_alive())
        self.assertFalse(newer.is_alive())
        self.assertEqual(errors, [])
        self.assertCountEqual(
            results,
            [(fixture.TIP_B, True), (fixture.TIP_C, True)],
        )
        self.assertEqual(
            [tip_hash for tip_hash, _cause, _stamp in fixture.payout.reservations],
            [fixture.TIP_B, fixture.TIP_C],
        )
        self.assertEqual(
            fixture.bundle_cancellations,
            [(fixture.TIP_B, None), (fixture.TIP_C, None)],
        )
        self.assertEqual(
            service.snapshot().latest_detected_tip,
            (fixture.TIP_C, 3),
        )

    def test_monotonic_observation_rejects_reordered_tip(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service

        self.assertTrue(service.observe_tip(fixture.TIP_A, observation_sequence=2))
        self.assertFalse(service.observe_tip(fixture.TIP_B, observation_sequence=1))
        self.assertTrue(service.observe_tip(fixture.TIP_A, observation_sequence=1))
        self.assertEqual(service.snapshot().latest_detected_tip, (fixture.TIP_A, 2))

    def test_pending_clear_requires_exact_completion_token(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        snapshot = fixture.snapshot
        self.assertTrue(
            service.publish_tip(
                fixture.tip,
                observation_sequence=1,
                published_snapshot=snapshot,
            )
        )
        stale_token = service.mark_pending()
        current_token = service.mark_pending()

        self.assertFalse(
            service.clear_pending_for_completed_refresh(
                snapshot,
                1,
                fixture.payout.generation,
                stale_token,
            )
        )
        self.assertEqual(service.snapshot().pending_token, current_token)
        self.assertTrue(
            service.clear_pending_for_completed_refresh(
                snapshot,
                1,
                fixture.payout.generation,
                current_token,
            )
        )
        self.assertFalse(service.snapshot().pending)

    def test_divergence_lease_is_anchored_to_first_departure(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(service.publish_tip(fixture.TIP_A, observation_sequence=1))

        fixture.now = 110.0
        self.assertTrue(service.observe_tip(fixture.TIP_B, observation_sequence=2))
        self.assertEqual(service.snapshot().divergence_started_monotonic, 110.0)
        fixture.now = 120.0
        self.assertTrue(service.observe_tip(fixture.TIP_C, observation_sequence=3))
        self.assertEqual(service.snapshot().divergence_started_monotonic, 110.0)
        fixture.now = 130.0
        self.assertTrue(service.observe_tip(fixture.TIP_A, observation_sequence=4))
        self.assertEqual(service.snapshot().divergence_started_monotonic, 110.0)
        self.assertTrue(service.publication_failure_expired(140.0))
        self.assertTrue(service.publish_tip(fixture.TIP_A, observation_sequence=4))
        self.assertIsNone(service.snapshot().divergence_started_monotonic)
        self.assertFalse(service.publication_failure_expired(10_000.0))

    def test_post_accept_failure_wakes_retry_without_counting_supersession(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        trigger = dataclass_replace(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A),
            post_accept_block=(10, fixture.TIP_B),
            post_accept_admission_sequence=1,
        )

        with patch("lab.prism.tip_refresh.traceback.print_exception"):
            service._handle_scheduled_failure(trigger, RuntimeError("rpc failed"))
        self.assertTrue(service.snapshot().retry_requested)
        self.assertEqual(service.snapshot().post_accept_refresh_failure_count, 1)

        service.clear_retry_for_test()
        service._handle_scheduled_failure(
            trigger,
            TemplateRefreshSuperseded("newer observation"),
        )
        self.assertTrue(service.snapshot().retry_requested)
        self.assertEqual(service.snapshot().post_accept_refresh_failure_count, 1)

    def test_post_accept_notification_stamps_heartbeat_and_runs_scheduler(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        heartbeats: list[str] = []
        service.reconfigure_ports_for_test(heartbeat=heartbeats.append)

        self.assertEqual(
            service.refresh_after_accepted_block(
                block_height=10,
                block_hash=fixture.TIP_B,
                heartbeat_name="block_submitter",
            ),
            0,
        )

        self.assertEqual(heartbeats[0], "block_submitter")
        self.assertGreaterEqual(heartbeats.count("block_submitter"), 2)
        self.assertIn("tip_refresh_scheduler", heartbeats)
        self.assertFalse(service.snapshot().retry_requested)
        self.assertEqual(service.snapshot().post_accept_refresh_failure_count, 0)

    def test_post_accept_rpc_failure_wakes_driver_and_counts_failure(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        service.reconfigure_ports_for_test(
            rpc_call=lambda _method, _params: (_ for _ in ()).throw(
                RuntimeError("best-tip unavailable")
            )
        )

        with patch("lab.prism.tip_refresh.traceback.print_exc"):
            self.assertEqual(
                service.refresh_after_accepted_block(
                    block_height=10,
                    block_hash=fixture.TIP_B,
                ),
                0,
            )

        self.assertTrue(service.snapshot().retry_requested)
        self.assertEqual(service.snapshot().post_accept_refresh_failure_count, 1)

    def test_post_accept_supersession_wakes_driver_without_counting_failure(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        service.observe_tip = lambda *_args, **_kwargs: False  # type: ignore[method-assign]

        self.assertEqual(
            service.refresh_after_accepted_block(
                block_height=10,
                block_hash=fixture.TIP_B,
            ),
            0,
        )

        self.assertTrue(service.snapshot().retry_requested)
        self.assertEqual(service.snapshot().post_accept_refresh_failure_count, 0)

    def test_retry_generation_preserves_each_visible_wake(self) -> None:
        service = _TipRefreshFixture().service

        self.assertFalse(service.consume_retry())
        service.schedule_retry()
        self.assertTrue(service.consume_retry())
        self.assertFalse(service.consume_retry())
        service.schedule_retry()
        self.assertTrue(service.consume_retry())
        self.assertFalse(service.consume_retry())

    def test_fanout_cancellation_closes_admission_then_drains(self) -> None:
        cancellation = FanoutCancellation()
        self.assertTrue(cancellation.begin_delivery())
        drained = threading.Event()

        def cancel_and_drain() -> None:
            cancellation.set()
            drained.set()

        thread = threading.Thread(target=cancel_and_drain)
        thread.start()
        self.assertFalse(drained.wait(0.05))
        self.assertFalse(cancellation.begin_delivery())
        cancellation.end_delivery()
        self.assertTrue(drained.wait(1.0))
        thread.join(1.0)
        self.assertFalse(thread.is_alive())

    def test_failure_budget_excludes_coordination_supersession(self) -> None:
        coordination = _TipRefreshFixture()
        coordination.fetch_error = TemplateRefreshSuperseded("newer observation")
        with self.assertRaises(TemplateRefreshSuperseded):
            coordination.service.poll_once()
        self.assertIsNone(
            coordination.service.snapshot().failure_started_monotonic
        )

        unhealthy = _TipRefreshFixture()
        unhealthy.fetch_error = TemplateRefreshBlocked("invalid template")
        with self.assertRaises(TemplateRefreshBlocked):
            unhealthy.service.poll_once()
        self.assertEqual(
            unhealthy.service.snapshot().failure_started_monotonic,
            unhealthy.now,
        )

    def test_same_tip_refresh_rechecks_readiness_without_pending_work(self) -> None:
        fixture = _TipRefreshFixture()
        fixture.jobs.ready = True
        self.assertEqual(fixture.service.poll_once(), 0)
        fixture.snapshot = fixture.make_snapshot(fixture.tip, "22" * 32)
        self.assertEqual(fixture.service.poll_once(), 0)

        state = fixture.service.snapshot()
        self.assertFalse(state.pending)
        self.assertFalse(state.retry_requested)
        self.assertEqual(fixture.jobs.readiness_checks, 2)
        self.assertEqual(fixture.jobs.prepared_clears, 0)
        self.assertIs(
            fixture.service.published_snapshot().template,
            fixture.snapshot,
        )

    def test_g1_progress_order_follows_coherence_and_publication(self) -> None:
        fixture = _TipRefreshFixture()
        fixture.jobs.ready = True

        self.assertEqual(fixture.service.poll_once(), 0)

        self.assertEqual(
            fixture.events,
            ["coherent", "start", "readiness", "publish", "coherent", "finish"],
        )

    def test_detector_thread_only_enqueues_scheduler_refresh_work(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        executed_by: list[str] = []
        results: list[int] = []

        def execute(_trigger: object) -> int:
            executed_by.append(threading.current_thread().name)
            return 0

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        detector = threading.Thread(
            target=lambda: results.append(service.poll_once()),
            name="test-blockpoll-detector",
        )
        detector.start()
        detector.join(1.0)

        self.assertFalse(detector.is_alive())
        self.assertEqual(results, [0])
        self.assertEqual(executed_by, ["prism-tip-refresh-scheduler"])
        self.assertTrue(service.shutdown())

    def test_blockpoll_await_renews_caller_heartbeat_while_worker_is_blocked(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        execute_started = threading.Event()
        release_execute = threading.Event()
        repeated_heartbeat = threading.Event()
        heartbeats: list[str] = []
        executed_by: list[str] = []

        def heartbeat(name: str) -> None:
            heartbeats.append(name)
            if heartbeats.count("qbit_blockpoll") >= 3:
                repeated_heartbeat.set()

        def execute(_trigger: object) -> int:
            executed_by.append(threading.current_thread().name)
            execute_started.set()
            self.assertTrue(release_execute.wait(2.0))
            return 0

        service.reconfigure_ports_for_test(heartbeat=heartbeat)
        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        results: list[int] = []
        caller = threading.Thread(target=lambda: results.append(service.poll_once()))
        caller.start()
        self.assertTrue(execute_started.wait(1.0))
        self.assertTrue(repeated_heartbeat.wait(1.0))
        release_execute.set()
        caller.join(1.0)

        self.assertFalse(caller.is_alive())
        self.assertEqual(results, [0])
        self.assertEqual(executed_by, ["prism-tip-refresh-scheduler"])
        self.assertGreaterEqual(heartbeats.count("qbit_blockpoll"), 3)
        self.assertTrue(service.shutdown())

    def test_blockwait_changed_tip_only_notifies_refresh_driver(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        stop = threading.Event()
        heartbeats: list[str] = []
        executions: list[object] = []

        def heartbeat(name: str) -> None:
            heartbeats.append(name)

        def blockwait_once(_known_tip: str) -> str:
            fixture.tip = fixture.TIP_B
            fixture.snapshot = fixture.make_snapshot(fixture.TIP_B, "22" * 32)
            stop.set()
            return fixture.TIP_B

        def execute(trigger: object) -> int:
            executions.append(trigger)
            return 0

        service.blockwait_once = blockwait_once  # type: ignore[method-assign]
        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        service.reconfigure_ports_for_test(
            heartbeat=heartbeat,
            stop_requested=stop.is_set,
            wait_for_stop=stop.wait,
        )
        caller = threading.Thread(target=service.blockwait_loop)
        caller.start()
        caller.join(1.0)

        self.assertFalse(caller.is_alive())
        self.assertEqual(executions, [])
        self.assertEqual(service.newest_observed_tip(), fixture.TIP_B)
        self.assertTrue(service.snapshot().pending)
        self.assertTrue(service.snapshot().retry_requested)
        self.assertEqual(heartbeats, ["qbit_blockwait"])
        metrics = service.metrics_snapshot()
        self.assertEqual(metrics["trigger_latency"]["count"], 0)  # type: ignore[index]
        self.assertEqual(metrics["trigger_coalesces"], 0)
        self.assertEqual(metrics["trigger_supersessions"], 0)
        self.assertTrue(service.shutdown())

    def test_scheduler_waits_for_refresh_execution_permit(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        permit_wait_started = threading.Event()
        permit = threading.Event()

        def wait_for_permit(timeout_seconds: float) -> bool:
            permit_wait_started.set()
            return permit.wait(timeout_seconds)

        service.reconfigure_ports_for_test(
            wait_for_execution_permit=wait_for_permit,
        )
        completion = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )

        self.assertTrue(permit_wait_started.wait(1.0))
        self.assertFalse(completion.done())
        self.assertEqual(fixture.events, [])
        permit.set()
        self.assertEqual(completion.result(timeout=1.0), 0)
        self.assertTrue(service.shutdown())
        self.assertIn("tip_refresh_scheduler", fixture.removed_heartbeats)

    def test_newer_trigger_supersedes_active_while_execution_permit_is_blocked(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        permit_wait_started = threading.Event()
        permit = threading.Event()

        def wait_for_permit(timeout_seconds: float) -> bool:
            permit_wait_started.set()
            return permit.wait(timeout_seconds)

        service.reconfigure_ports_for_test(
            wait_for_execution_permit=wait_for_permit,
        )
        first = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(permit_wait_started.wait(1.0))

        fixture.tip = fixture.TIP_B
        fixture.snapshot = fixture.make_snapshot(fixture.TIP_B, "22" * 32)
        second = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=2, tip=fixture.TIP_B)  # type: ignore[arg-type]
        )
        scheduler = service.scheduler_snapshot()
        self.assertEqual(scheduler.active.tip_hash, fixture.TIP_A)  # type: ignore[union-attr]
        self.assertEqual(scheduler.pending.tip_hash, fixture.TIP_B)  # type: ignore[union-attr]

        with self.assertRaises(TemplateRefreshSuperseded):
            first.result(timeout=1.0)
        self.assertFalse(second.done())
        self.assertEqual(fixture.events, [])

        permit.set()
        self.assertEqual(second.result(timeout=1.0), 0)
        self.assertEqual(service.published_snapshot().tip_hash, fixture.TIP_B)
        self.assertEqual(fixture.events.count("coherent"), 2)
        self.assertTrue(service.shutdown())

    def test_stale_retained_wake_cannot_roll_back_blocked_live_tip(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        template = {"previousblockhash": fixture.TIP_A, "transactions": []}
        fingerprint = qbit_template_fingerprint(template)
        artifacts = CachedTemplateArtifacts(
            template=template,
            fingerprint=fingerprint,
            previousblockhash=fixture.TIP_A,
            transaction_hexes=(),
            witness_merkle_leaves_hex=(),
            network_difficulty=1,
            fetched_monotonic=fixture.now,
            generation=4,
        )
        retained_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=fixture.TIP_A,
            previousblockhash=fixture.TIP_A,
            template_fingerprint=fingerprint,
            template_generation=4,
            template_artifacts=artifacts,
        )
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=retained_snapshot,
            )
        )
        service.retain_collection_refresh(retained_snapshot, 1, 0)
        self.assertIsNotNone(service.retained_collection_refresh_snapshot())

        permit_wait_started = threading.Event()
        permit = threading.Event()
        executed: list[str | None] = []
        original_execute = service._execute_refresh_trigger

        def wait_for_permit(timeout_seconds: float) -> bool:
            permit_wait_started.set()
            return permit.wait(timeout_seconds)

        def record_execute(trigger: object) -> int:
            executed.append(getattr(trigger, "tip_hash"))
            return original_execute(trigger)  # type: ignore[arg-type]

        service.reconfigure_ports_for_test(wait_for_execution_permit=wait_for_permit)
        service._execute_refresh_trigger = record_execute  # type: ignore[method-assign]
        fixture.tip = fixture.TIP_B
        fixture.snapshot = fixture.make_snapshot(fixture.TIP_B, "22" * 32)
        admission = service.submit_tip_observation_admission(
            fixture.TIP_B,
            reason="blockwait",
        )
        assert admission.completion is not None
        self.assertTrue(permit_wait_started.wait(1.0))
        sequence_before_wake = service.observation_sequence()

        client = object()
        fixture.delivery.client_can_receive_jobs = lambda _client: True  # type: ignore[method-assign]
        fixture.delivery.eligible_clients = lambda: (client,)  # type: ignore[method-assign]
        service.note_collection_identity_available(client)

        scheduler = service.scheduler_snapshot()
        self.assertEqual(scheduler.active.tip_hash, fixture.TIP_B)  # type: ignore[union-attr]
        self.assertIsNone(scheduler.pending)
        self.assertEqual(service.observation_sequence(), sequence_before_wake)
        permit.set()
        self.assertEqual(admission.completion.result(timeout=1.0), 0)
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        self.assertEqual(executed, [fixture.TIP_B])
        self.assertEqual(service.published_snapshot().tip_hash, fixture.TIP_B)
        self.assertTrue(service.shutdown())

    def test_exact_template_and_payout_merge_on_current_chain_axis(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        active_started = threading.Event()
        release_active = threading.Event()

        def execute(trigger: object) -> int:
            if int(getattr(trigger, "observation_sequence")) == 1:
                active_started.set()
                self.assertTrue(release_active.wait(2.0))
                service._raise_if_scheduler_superseded(trigger)  # type: ignore[arg-type]
            return 0

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        first = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        self.assertTrue(
            service.observe_tip(
                fixture.TIP_C,
                observation_sequence=3,
                mark_pending=False,
            )
        )
        exact_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=fixture.TIP_C,
            previousblockhash=fixture.TIP_C,
            template_fingerprint="99" * 32,
            template_generation=9,
        )
        exact_token = service.mark_pending(9)
        exact = service.submit_trigger(
            service._new_trigger(
                observation_sequence=3,
                tip_hash=fixture.TIP_C,
                payout_state_generation=2,
                ready_required=False,
                reasons=("template",),
                pending_signal_token=exact_token,
                snapshot=exact_snapshot,
            )
        )
        fixture.payout.generation = 7
        service.payout_generation_changed(7)
        pending = service.scheduler_snapshot().pending
        assert pending is not None
        self.assertEqual(pending.tip_hash, fixture.TIP_C)
        self.assertEqual(pending.observation_sequence, 3)
        self.assertEqual(pending.template_generation, 9)
        self.assertIs(pending.snapshot, exact_snapshot)
        self.assertEqual(pending.payout_state_generation, 7)
        release_active.set()
        with self.assertRaises(TemplateRefreshSuperseded):
            first.result(timeout=1.0)
        self.assertEqual(exact.result(timeout=1.0), 0)
        self.assertTrue(service.shutdown())

    def test_payout_admitted_between_live_observation_and_exact_template_submit(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        active_started = threading.Event()
        release_active = threading.Event()

        def execute(trigger: object) -> int:
            if int(getattr(trigger, "observation_sequence")) == 1:
                active_started.set()
                self.assertTrue(release_active.wait(2.0))
                service._raise_if_scheduler_superseded(trigger)  # type: ignore[arg-type]
            return 0

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        first = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        self.assertTrue(
            service.observe_tip(
                fixture.TIP_C,
                observation_sequence=3,
                mark_pending=False,
            )
        )
        fixture.payout.generation = 8
        service.payout_generation_changed(8)
        payout_pending = service.scheduler_snapshot().pending
        assert payout_pending is not None
        self.assertEqual(payout_pending.tip_hash, fixture.TIP_C)
        self.assertEqual(payout_pending.observation_sequence, 3)

        exact_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=fixture.TIP_C,
            previousblockhash=fixture.TIP_C,
            template_fingerprint="99" * 32,
            template_generation=9,
        )
        exact_token = service.mark_pending(9)
        exact = service.submit_trigger(
            service._new_trigger(
                observation_sequence=3,
                tip_hash=fixture.TIP_C,
                payout_state_generation=3,
                ready_required=False,
                reasons=("template",),
                pending_signal_token=exact_token,
                snapshot=exact_snapshot,
            )
        )
        pending = service.scheduler_snapshot().pending
        assert pending is not None
        self.assertEqual(pending.tip_hash, fixture.TIP_C)
        self.assertEqual(pending.observation_sequence, 3)
        self.assertEqual(pending.template_generation, 9)
        self.assertIs(pending.snapshot, exact_snapshot)
        self.assertEqual(pending.payout_state_generation, 8)
        release_active.set()
        with self.assertRaises(TemplateRefreshSuperseded):
            first.result(timeout=1.0)
        self.assertEqual(exact.result(timeout=1.0), 0)
        self.assertTrue(service.shutdown())

    def test_admission_during_idle_heartbeat_removal_reuses_scheduler_worker(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        heartbeat_removal_started = threading.Event()
        release_heartbeat_removal = threading.Event()
        executed: list[tuple[str | None, int | None]] = []

        def remove_heartbeat(name: str) -> None:
            if name == "tip_refresh_scheduler" and not heartbeat_removal_started.is_set():
                heartbeat_removal_started.set()
                self.assertTrue(release_heartbeat_removal.wait(2.0))

        def execute(trigger: object) -> int:
            executed.append(
                (
                    getattr(trigger, "tip_hash"),
                    threading.current_thread().ident,
                )
            )
            return 0

        service.reconfigure_ports_for_test(remove_heartbeat=remove_heartbeat)
        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        first = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertEqual(first.result(timeout=1.0), 0)
        self.assertTrue(heartbeat_removal_started.wait(1.0))

        second = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=2, tip=fixture.TIP_B)  # type: ignore[arg-type]
        )
        scheduler = service.scheduler_snapshot()
        self.assertTrue(scheduler.worker_alive)
        self.assertIsNone(scheduler.active)
        self.assertEqual(scheduler.pending.tip_hash, fixture.TIP_B)  # type: ignore[union-attr]
        release_heartbeat_removal.set()

        self.assertEqual(second.result(timeout=1.0), 0)
        self.assertEqual([tip for tip, _ident in executed], [fixture.TIP_A, fixture.TIP_B])
        self.assertEqual(len({ident for _tip, ident in executed}), 1)
        self.assertTrue(service.shutdown())

    def test_nested_callback_suppression_survives_coherent_capture(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service

        with service.suppress_trigger_callbacks_for_test():
            self.assertTrue(service._trigger_capture_local.active)
            service.detect_poll_trigger()
            self.assertTrue(service._trigger_capture_local.active)

        self.assertFalse(service._trigger_capture_local.active)
        self.assertTrue(service.shutdown())

    def test_scheduler_replaces_blocked_active_with_newer_tip(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        active_started = threading.Event()
        release_active = threading.Event()
        executed: list[str | None] = []

        def execute(trigger: object) -> int:
            tip_hash = getattr(trigger, "tip_hash")
            executed.append(tip_hash)
            if tip_hash == fixture.TIP_A:
                active_started.set()
                self.assertTrue(release_active.wait(2.0))
                service._raise_if_scheduler_superseded(trigger)  # type: ignore[arg-type]
            return 1

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        first = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        second = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=2, tip=fixture.TIP_B)  # type: ignore[arg-type]
        )
        snapshot = service.scheduler_snapshot()
        self.assertEqual(snapshot.active.tip_hash, fixture.TIP_A)  # type: ignore[union-attr]
        self.assertEqual(snapshot.pending.tip_hash, fixture.TIP_B)  # type: ignore[union-attr]
        release_active.set()
        with self.assertRaises(TemplateRefreshSuperseded):
            first.result(timeout=1.0)
        self.assertEqual(second.result(timeout=1.0), 1)
        self.assertEqual(executed, [fixture.TIP_A, fixture.TIP_B])
        self.assertTrue(service.shutdown())

    def test_same_tip_newer_payout_generation_survives_coalescing(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        active_started = threading.Event()
        release_active = threading.Event()
        executed: list[int] = []

        def execute(trigger: object) -> int:
            generation = int(getattr(trigger, "payout_state_generation"))
            executed.append(generation)
            if generation == 1:
                active_started.set()
                self.assertTrue(release_active.wait(2.0))
                service._raise_if_scheduler_superseded(trigger)  # type: ignore[arg-type]
            return generation

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        first = service.submit_trigger(
            self._scheduler_trigger(
                fixture,
                sequence=4,
                tip=fixture.TIP_A,
                payout_generation=1,
            )  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        second = service.submit_trigger(
            self._scheduler_trigger(
                fixture,
                sequence=4,
                tip=fixture.TIP_A,
                payout_generation=2,
                reason="payout",
            )  # type: ignore[arg-type]
        )
        release_active.set()
        with self.assertRaises(TemplateRefreshSuperseded):
            first.result(timeout=1.0)
        self.assertEqual(second.result(timeout=1.0), 2)
        self.assertEqual(executed, [1, 2])
        self.assertTrue(service.shutdown())

    def test_payout_invalidation_defers_one_trigger_until_publication(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        trigger_started = threading.Event()
        release_trigger = threading.Event()
        executed: list[object] = []

        def execute(trigger: object) -> int:
            executed.append(trigger)
            trigger_started.set()
            self.assertTrue(release_trigger.wait(2.0))
            return 0

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        service.payout_generation_invalidated(1)
        invalidated = service.snapshot()
        invalidated_token = invalidated.pending_token
        self.assertIsNotNone(invalidated_token)
        self.assertFalse(invalidated.retry_requested)
        self.assertIsNone(service.scheduler_snapshot().active)
        self.assertIsNone(service.scheduler_snapshot().pending)

        fixture.payout.generation = 1
        service.payout_generation_changed(1)
        self.assertTrue(trigger_started.wait(1.0))
        self.assertEqual(len(executed), 1)
        trigger = executed[0]
        self.assertEqual(getattr(trigger, "payout_state_generation"), 1)
        self.assertEqual(getattr(trigger, "pending_signal_token"), invalidated_token)
        self.assertEqual(getattr(trigger, "reasons"), ("payout",))
        self.assertEqual(service.snapshot().pending_token, invalidated_token)
        self.assertFalse(service.snapshot().retry_requested)

        release_trigger.set()
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        self.assertEqual(service.metrics_snapshot()["trigger_latency"]["count"], 1)  # type: ignore[index]
        self.assertTrue(service.shutdown())

    def test_accepted_writer_defers_payout_trigger_to_post_accept_marker(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        writer_active = True
        service.reconfigure_ports_for_test(
            wait_for_execution_permit=lambda _timeout: not writer_active,
        )
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        executed: list[object] = []
        service._execute_refresh_trigger = lambda trigger: executed.append(trigger) or 0  # type: ignore[method-assign]

        service.payout_generation_invalidated(1)
        invalidated_token = service.snapshot().pending_token
        fixture.payout.generation = 1
        service.payout_generation_changed(1)

        self.assertIsNotNone(invalidated_token)
        self.assertEqual(executed, [])
        self.assertIsNone(service.scheduler_snapshot().active)
        self.assertIsNone(service.scheduler_snapshot().pending)
        self.assertEqual(service.snapshot().pending_token, invalidated_token)

        writer_active = False
        completion = service.submit_post_accept_trigger(
            block_height=2,
            block_hash=fixture.TIP_B,
        )
        self.assertEqual(completion.result(timeout=1.0), 0)
        self.assertEqual(len(executed), 1)
        trigger = executed[0]
        self.assertEqual(getattr(trigger, "reasons"), ("post_accept",))
        self.assertEqual(getattr(trigger, "payout_state_generation"), 1)
        self.assertEqual(getattr(trigger, "pending_signal_token"), invalidated_token)
        self.assertTrue(getattr(trigger, "fresh_capture_required"))
        self.assertTrue(service.shutdown())

    def test_immediate_producers_do_not_duplicate_live_blockpoll_work(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        stopped = False
        detector_waiting = threading.Event()
        detector_heartbeats: list[str] = []

        def stop_requested() -> bool:
            detector_waiting.set()
            return stopped

        service.reconfigure_for_test(blockpoll_seconds=60.0)
        service.reconfigure_ports_for_test(
            stop_requested=stop_requested,
            heartbeat=detector_heartbeats.append,
        )
        executed: list[object] = []
        service._execute_refresh_trigger = lambda trigger: executed.append(trigger) or 0  # type: ignore[method-assign]
        detector = threading.Thread(
            target=service.blockpoll_loop,
            name="test-live-blockpoll",
        )
        detector.start()
        self.assertTrue(detector_waiting.wait(1.0))

        service.payout_generation_invalidated(1)
        self.assertEqual(executed, [])
        fixture.payout.generation = 1
        service.payout_generation_changed(1)
        self.assertTrue(service.wait_for_scheduler_idle_for_test())

        artifacts = SimpleNamespace(
            generation=2,
            previousblockhash=fixture.TIP_A,
            fingerprint="22" * 32,
        )
        service.template_artifacts_changed(artifacts)  # type: ignore[arg-type]
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        service.submit_tip_observation(fixture.TIP_B, reason="blockwait")
        self.assertTrue(service.wait_for_scheduler_idle_for_test())

        self.assertEqual(
            [getattr(trigger, "reasons") for trigger in executed],
            [("payout",), ("template",), ("blockwait",)],
        )
        self.assertIs(getattr(executed[1], "snapshot").template_artifacts, artifacts)
        metrics = service.metrics_snapshot()
        self.assertEqual(metrics["trigger_supersessions"], 0)
        self.assertEqual(metrics["trigger_coalesces"], 0)
        self.assertEqual(metrics["trigger_latency"]["count"], 3)  # type: ignore[index]
        self.assertEqual(detector_heartbeats, [])

        stopped = True
        service.schedule_retry()
        detector.join(1.0)
        self.assertFalse(detector.is_alive())
        self.assertTrue(service.shutdown())

    def test_async_capture_failure_arms_budget_but_supersession_does_not(self) -> None:
        failed = _TipRefreshFixture()
        failed_service = failed.service
        self.assertTrue(
            failed_service.publish_tip(
                failed.TIP_A,
                observation_sequence=1,
                published_snapshot=failed.snapshot,
            )
        )
        failed_service.reconfigure_ports_for_test(
            fetch_snapshot=lambda: (_ for _ in ()).throw(
                RuntimeError("template RPC unavailable")
            )
        )
        failed_service.payout_generation_invalidated(1)
        failed.payout.generation = 1
        failed_service.payout_generation_changed(1)
        self.assertTrue(failed_service.wait_for_scheduler_idle_for_test())
        self.assertEqual(failed_service.snapshot().failure_started_monotonic, failed.now)
        self.assertTrue(failed_service.shutdown())

        superseded = _TipRefreshFixture()
        superseded_service = superseded.service
        self.assertTrue(
            superseded_service.publish_tip(
                superseded.TIP_A,
                observation_sequence=1,
                published_snapshot=superseded.snapshot,
            )
        )
        superseded_service.reconfigure_ports_for_test(
            fetch_snapshot=lambda: (_ for _ in ()).throw(
                TemplateRefreshSuperseded("newer capture")
            )
        )
        superseded_service.payout_generation_invalidated(1)
        superseded.payout.generation = 1
        superseded_service.payout_generation_changed(1)
        self.assertTrue(superseded_service.wait_for_scheduler_idle_for_test())
        self.assertIsNone(
            superseded_service.snapshot().failure_started_monotonic
        )
        self.assertTrue(superseded_service.shutdown())

    def test_post_accept_context_never_becomes_detected_tip_authority(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        accepted_hash = fixture.TIP_C
        fixture.tip = fixture.TIP_B
        fixture.snapshot = fixture.make_snapshot(fixture.TIP_B, "22" * 32)
        observed: list[str] = []
        original_observe = service.observe_tip

        def record_observe(tip_hash: str, **kwargs: object) -> bool:
            observed.append(tip_hash)
            return original_observe(tip_hash, **kwargs)  # type: ignore[arg-type]

        service.observe_tip = record_observe  # type: ignore[method-assign]
        completion = service.submit_post_accept_trigger(
            block_height=11,
            block_hash=accepted_hash,
        )

        self.assertEqual(completion.result(timeout=1.0), 0)
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        self.assertNotIn(accepted_hash, observed)
        self.assertEqual(
            service.snapshot().latest_detected_tip,
            (fixture.TIP_B, 2),
        )
        self.assertEqual(
            [tip_hash for tip_hash, _cause, _stamp in fixture.payout.reservations],
            [fixture.TIP_B],
        )
        published = service.published_snapshot()
        self.assertEqual(published.tip_hash, fixture.TIP_B)
        self.assertEqual(published.observation_sequence, 2)
        self.assertTrue(service.shutdown())

    def test_post_accept_after_captured_blockpoll_gets_fresh_followup(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        permit_wait_started = threading.Event()
        permit = threading.Event()
        executions: list[object] = []
        reports: list[tuple[tuple[int, str] | None, int]] = []
        original_execute = service._execute_refresh_trigger

        def wait_for_permit(timeout_seconds: float) -> bool:
            permit_wait_started.set()
            return permit.wait(timeout_seconds)

        def execute(trigger: object) -> int:
            executions.append(trigger)
            return original_execute(trigger)  # type: ignore[arg-type]

        service.reconfigure_ports_for_test(wait_for_execution_permit=wait_for_permit)
        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        service._handle_scheduled_success = (  # type: ignore[method-assign]
            lambda trigger, result: reports.append(
                (getattr(trigger, "post_accept_block"), result)
            )
        )
        blockpoll = service.submit_trigger(
            service._new_trigger(
                observation_sequence=1,
                tip_hash=fixture.TIP_A,
                payout_state_generation=0,
                ready_required=False,
                reasons=("blockpoll",),
                pending_signal_token=None,
                snapshot=fixture.snapshot,
            )
        )
        self.assertTrue(permit_wait_started.wait(1.0))
        post_accept = service.submit_post_accept_trigger(
            block_height=10,
            block_hash=fixture.TIP_C,
        )

        self.assertIsNot(post_accept, blockpoll)
        scheduler = service.scheduler_snapshot()
        pending = scheduler.pending
        assert pending is not None
        self.assertTrue(pending.fresh_capture_required)
        self.assertIsNone(pending.snapshot)
        self.assertIsNone(pending.template_generation)
        self.assertEqual(pending.post_accept_block, (10, fixture.TIP_C))
        fixture.tip = fixture.TIP_B
        fixture.snapshot = fixture.make_snapshot(fixture.TIP_B, "22" * 32)
        permit.set()

        self.assertEqual(blockpoll.result(timeout=1.0), 0)
        self.assertEqual(post_accept.result(timeout=1.0), 0)
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        self.assertEqual(len(executions), 2)
        self.assertIsNotNone(getattr(executions[0], "snapshot"))
        self.assertIsNone(getattr(executions[0], "post_accept_block"))
        self.assertTrue(getattr(executions[1], "fresh_capture_required"))
        self.assertEqual(
            getattr(executions[1], "post_accept_block"),
            (10, fixture.TIP_C),
        )
        self.assertEqual(reports, [(None, 0), ((10, fixture.TIP_C), 0)])
        self.assertEqual(service.snapshot().latest_detected_tip, (fixture.TIP_B, 2))
        published = service.published_snapshot()
        self.assertEqual(published.tip_hash, fixture.TIP_B)
        self.assertEqual(published.observation_sequence, 2)
        self.assertTrue(service.shutdown())

    def test_post_accept_after_capture_start_uses_pending_latest_context(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        capture_port_entered = threading.Event()
        release_capture_port = threading.Event()
        getbest_calls = 0
        reports: list[tuple[int, str] | None] = []

        def rpc_call(method: str, params: list[object] | None) -> object:
            nonlocal getbest_calls
            if method == "getbestblockhash":
                getbest_calls += 1
                if getbest_calls == 1:
                    capture_port_entered.set()
                    self.assertTrue(release_capture_port.wait(2.0))
                return fixture.TIP_A
            return fixture.rpc_call(method, params)

        service.reconfigure_ports_for_test(rpc_call=rpc_call)
        service._handle_scheduled_success = (  # type: ignore[method-assign]
            lambda trigger, _result: reports.append(
                getattr(trigger, "post_accept_block")
            )
        )
        first = service.submit_post_accept_trigger(
            block_height=10,
            block_hash=fixture.TIP_A,
        )
        self.assertTrue(capture_port_entered.wait(1.0))
        second = service.submit_post_accept_trigger(
            block_height=11,
            block_hash=fixture.TIP_B,
        )
        latest = service.submit_post_accept_trigger(
            block_height=12,
            block_hash=fixture.TIP_C,
        )

        self.assertIsNot(first, second)
        self.assertIs(latest, second)
        pending = service.scheduler_snapshot().pending
        assert pending is not None
        self.assertTrue(pending.fresh_capture_required)
        self.assertEqual(pending.post_accept_block, (12, fixture.TIP_C))
        self.assertIsNone(pending.snapshot)
        release_capture_port.set()

        self.assertEqual(first.result(timeout=1.0), 0)
        self.assertEqual(second.result(timeout=1.0), 0)
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        self.assertEqual(getbest_calls, 2)
        self.assertEqual(
            reports,
            [(10, fixture.TIP_A), (12, fixture.TIP_C)],
        )
        self.assertEqual(service.snapshot().latest_detected_tip, (fixture.TIP_A, 3))
        self.assertEqual(service.published_snapshot().observation_sequence, 3)
        self.assertEqual(service.snapshot().post_accept_refresh_failure_count, 0)
        self.assertTrue(service.shutdown())

    def test_post_accept_before_capture_uses_pending_latest_context(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        permit_wait_started = threading.Event()
        permit = threading.Event()
        getbest_calls = 0
        reports: list[tuple[int, str] | None] = []

        def wait_for_permit(timeout_seconds: float) -> bool:
            permit_wait_started.set()
            return permit.wait(timeout_seconds)

        def rpc_call(method: str, params: list[object] | None) -> object:
            nonlocal getbest_calls
            if method == "getbestblockhash":
                getbest_calls += 1
            return fixture.rpc_call(method, params)

        service.reconfigure_ports_for_test(
            rpc_call=rpc_call,
            wait_for_execution_permit=wait_for_permit,
        )
        service._handle_scheduled_success = (  # type: ignore[method-assign]
            lambda trigger, _result: reports.append(
                getattr(trigger, "post_accept_block")
            )
        )
        active = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(permit_wait_started.wait(1.0))
        first = service.submit_post_accept_trigger(
            block_height=10,
            block_hash=fixture.TIP_B,
        )
        latest = service.submit_post_accept_trigger(
            block_height=11,
            block_hash=fixture.TIP_C,
        )

        self.assertIsNot(first, active)
        self.assertIs(latest, first)
        pending = service.scheduler_snapshot().pending
        assert pending is not None
        self.assertTrue(pending.fresh_capture_required)
        self.assertEqual(pending.post_accept_block, (11, fixture.TIP_C))
        fixture.tip = fixture.TIP_B
        fixture.snapshot = fixture.make_snapshot(fixture.TIP_B, "22" * 32)
        permit.set()

        self.assertEqual(active.result(timeout=1.0), 0)
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        self.assertEqual(getbest_calls, 2)
        self.assertEqual(reports, [None, (11, fixture.TIP_C)])
        self.assertEqual(service.snapshot().latest_detected_tip, (fixture.TIP_B, 2))
        self.assertEqual(service.published_snapshot().observation_sequence, 2)
        self.assertTrue(service.shutdown())

    def test_live_tip_supersession_preserves_pending_post_accept_future(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        permit_wait_started = threading.Event()
        permit = threading.Event()
        failures: list[tuple[tuple[int, str] | None, type[BaseException]]] = []
        successes: list[tuple[int, str] | None] = []

        def wait_for_permit(timeout_seconds: float) -> bool:
            permit_wait_started.set()
            return permit.wait(timeout_seconds)

        service.reconfigure_ports_for_test(wait_for_execution_permit=wait_for_permit)
        service._handle_scheduled_failure = (  # type: ignore[method-assign]
            lambda trigger, exc: failures.append(
                (getattr(trigger, "post_accept_block"), type(exc))
            )
        )
        service._handle_scheduled_success = (  # type: ignore[method-assign]
            lambda trigger, _result: successes.append(
                getattr(trigger, "post_accept_block")
            )
        )
        active = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(permit_wait_started.wait(1.0))
        post_accept = service.submit_post_accept_trigger(
            block_height=10,
            block_hash=fixture.TIP_C,
        )
        self.assertIsNot(post_accept, active)

        fixture.tip = fixture.TIP_B
        fixture.snapshot = fixture.make_snapshot(fixture.TIP_B, "22" * 32)
        live = service.submit_tip_observation_admission(
            fixture.TIP_B,
            reason="blockwait",
        )
        self.assertIs(live.completion, post_accept)
        pending = service.scheduler_snapshot().pending
        assert pending is not None
        self.assertEqual(pending.tip_hash, fixture.TIP_B)
        self.assertTrue(pending.fresh_capture_required)
        self.assertIsNone(pending.snapshot)
        self.assertEqual(pending.post_accept_block, (10, fixture.TIP_C))

        with self.assertRaises(TemplateRefreshSuperseded):
            active.result(timeout=1.0)
        permit.set()
        self.assertEqual(post_accept.result(timeout=1.0), 0)
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        self.assertEqual(failures, [(None, TemplateRefreshSuperseded)])
        self.assertEqual(successes, [(10, fixture.TIP_C)])
        self.assertEqual(service.snapshot().latest_detected_tip, (fixture.TIP_B, 3))
        published = service.published_snapshot()
        self.assertEqual(published.tip_hash, fixture.TIP_B)
        self.assertEqual(published.observation_sequence, 3)
        self.assertTrue(service.shutdown())

    def test_live_tip_superseding_active_post_accept_transfers_reporting(
        self,
    ) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        permit_wait_started = threading.Event()
        permit = threading.Event()
        successes: list[tuple[int, str] | None] = []

        def wait_for_permit(timeout_seconds: float) -> bool:
            permit_wait_started.set()
            return permit.wait(timeout_seconds)

        service.reconfigure_ports_for_test(wait_for_execution_permit=wait_for_permit)
        service._handle_scheduled_success = (  # type: ignore[method-assign]
            lambda trigger, _result: successes.append(
                getattr(trigger, "post_accept_block")
            )
        )
        active_post_accept = service.submit_post_accept_trigger(
            block_height=10,
            block_hash=fixture.TIP_C,
        )
        self.assertTrue(permit_wait_started.wait(1.0))

        fixture.tip = fixture.TIP_B
        fixture.snapshot = fixture.make_snapshot(fixture.TIP_B, "22" * 32)
        live = service.submit_tip_observation_admission(
            fixture.TIP_B,
            reason="blockwait",
        )
        assert live.completion is not None
        self.assertIsNot(live.completion, active_post_accept)
        pending = service.scheduler_snapshot().pending
        assert pending is not None
        self.assertEqual(pending.tip_hash, fixture.TIP_B)
        self.assertTrue(pending.fresh_capture_required)
        self.assertEqual(pending.post_accept_block, (10, fixture.TIP_C))

        with self.assertRaises(TemplateRefreshSuperseded):
            active_post_accept.result(timeout=1.0)
        self.assertEqual(
            service.snapshot().post_accept_refresh_failure_count,
            0,
        )
        permit.set()
        self.assertEqual(live.completion.result(timeout=1.0), 0)
        self.assertTrue(service.wait_for_scheduler_idle_for_test())
        self.assertEqual(successes, [(10, fixture.TIP_C)])
        self.assertEqual(
            service.snapshot().post_accept_refresh_failure_count,
            0,
        )
        self.assertEqual(service.snapshot().latest_detected_tip, (fixture.TIP_B, 3))
        published = service.published_snapshot()
        self.assertEqual(published.tip_hash, fixture.TIP_B)
        self.assertEqual(published.observation_sequence, 3)
        self.assertTrue(service.shutdown())

    def test_stale_slow_trigger_cannot_replace_newer_pending_authority(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        active_started = threading.Event()
        release_active = threading.Event()

        def execute(trigger: object) -> int:
            sequence = int(getattr(trigger, "observation_sequence"))
            if sequence == 5:
                active_started.set()
                self.assertTrue(release_active.wait(2.0))
                service._raise_if_scheduler_superseded(trigger)  # type: ignore[arg-type]
            return sequence

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=5, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        newest = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=7, tip=fixture.TIP_C)  # type: ignore[arg-type]
        )
        stale = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=6, tip=fixture.TIP_B)  # type: ignore[arg-type]
        )
        self.assertIs(stale, newest)
        self.assertEqual(
            service.scheduler_snapshot().pending.tip_hash,  # type: ignore[union-attr]
            fixture.TIP_C,
        )
        release_active.set()
        self.assertEqual(newest.result(timeout=1.0), 7)
        self.assertTrue(service.shutdown())

    def test_old_tip_template_event_cannot_reclaim_newer_tip_authority(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        self.assertTrue(
            service.publish_tip(
                fixture.TIP_A,
                observation_sequence=1,
                published_snapshot=fixture.snapshot,
            )
        )
        self.assertTrue(
            service.observe_tip(
                fixture.TIP_B,
                observation_sequence=2,
                mark_pending=False,
            )
        )
        executed: list[object] = []
        service._execute_refresh_trigger = lambda trigger: executed.append(trigger) or 0  # type: ignore[method-assign]
        sequence_before_callback = service.observation_sequence()

        service.template_artifacts_changed(
            SimpleNamespace(
                generation=99,
                previousblockhash=fixture.TIP_A,
                fingerprint="99" * 32,
            )
        )

        scheduler = service.scheduler_snapshot()
        self.assertIsNone(scheduler.active)
        self.assertIsNone(scheduler.pending)
        self.assertFalse(scheduler.worker_alive)
        self.assertEqual(executed, [])
        self.assertEqual(service.snapshot().latest_detected_tip, (fixture.TIP_B, 2))
        self.assertEqual(service.published_snapshot().tip_hash, fixture.TIP_A)
        self.assertEqual(service.observation_sequence(), sequence_before_callback)
        self.assertTrue(service.shutdown())

    def test_equivalent_blockwait_trigger_coalesces_with_active_blockpoll(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        active_started = threading.Event()
        release_active = threading.Event()
        executions = 0

        def execute(_trigger: object) -> int:
            nonlocal executions
            executions += 1
            active_started.set()
            self.assertTrue(release_active.wait(2.0))
            return 0

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        active = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=3, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        duplicate = service.submit_trigger(
            self._scheduler_trigger(
                fixture,
                sequence=3,
                tip=fixture.TIP_A,
                reason="blockwait",
            )  # type: ignore[arg-type]
        )
        self.assertIs(duplicate, active)
        self.assertIsNone(service.scheduler_snapshot().pending)
        release_active.set()
        self.assertEqual(active.result(timeout=1.0), 0)
        self.assertEqual(executions, 1)
        metrics = service.metrics_snapshot()
        self.assertEqual(metrics["trigger_coalesces"], 1)
        self.assertEqual(metrics["trigger_supersessions"], 0)
        self.assertTrue(service.shutdown())

    def test_same_tip_template_and_readiness_requirements_are_not_dropped(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        active_started = threading.Event()
        release_active = threading.Event()

        def execute(trigger: object) -> int:
            if int(getattr(trigger, "observation_sequence")) == 1:
                active_started.set()
                self.assertTrue(release_active.wait(2.0))
                service._raise_if_scheduler_superseded(trigger)  # type: ignore[arg-type]
            return 0

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        service.submit_trigger(
            self._scheduler_trigger(
                fixture,
                sequence=2,
                tip=fixture.TIP_A,
                payout_generation=4,
                reason="template",
                pending_token=8,
            )  # type: ignore[arg-type]
        )
        completion = service.submit_trigger(
            self._scheduler_trigger(
                fixture,
                sequence=2,
                tip=fixture.TIP_A,
                payout_generation=5,
                ready_required=True,
                reason="readiness",
                pending_token=9,
            )  # type: ignore[arg-type]
        )
        pending = service.scheduler_snapshot().pending
        assert pending is not None
        self.assertEqual(pending.tip_hash, fixture.TIP_A)
        self.assertEqual(pending.payout_state_generation, 5)
        self.assertTrue(pending.ready_required)
        self.assertEqual(pending.pending_signal_token, 9)
        self.assertEqual(pending.reasons, ("readiness", "template"))
        release_active.set()
        self.assertEqual(completion.result(timeout=1.0), 0)
        self.assertTrue(service.shutdown())

    def test_scheduler_metrics_are_exact_and_snapshot_lock_order_is_safe(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        active_started = threading.Event()
        release_active = threading.Event()

        def execute(trigger: object) -> int:
            if int(getattr(trigger, "observation_sequence")) == 1:
                active_started.set()
                self.assertTrue(release_active.wait(2.0))
                service._raise_if_scheduler_superseded(trigger)  # type: ignore[arg-type]
            return 0

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        fixture.now = 100.25
        pending = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=2, tip=fixture.TIP_B)  # type: ignore[arg-type]
        )
        duplicate = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=2, tip=fixture.TIP_B)  # type: ignore[arg-type]
        )
        self.assertIs(duplicate, pending)

        snapshots: list[dict[str, object]] = []
        snapshot_thread = threading.Thread(
            target=lambda: snapshots.append(service.metrics_snapshot())
        )
        snapshot_thread.start()
        snapshot_thread.join(1.0)
        self.assertFalse(snapshot_thread.is_alive())
        self.assertEqual(snapshots[0]["trigger_queue_depth"], 1)
        self.assertEqual(snapshots[0]["trigger_queue_capacity"], 1)
        self.assertEqual(snapshots[0]["trigger_coalesces"], 1)
        self.assertEqual(snapshots[0]["trigger_supersessions"], 1)
        release_active.set()
        self.assertEqual(pending.result(timeout=1.0), 0)
        final = service.metrics_snapshot()
        latency = final["trigger_latency"]
        assert isinstance(latency, dict)
        self.assertEqual(final["trigger_queue_depth"], 0)
        self.assertEqual(latency["count"], 2)
        self.assertEqual(latency["sum"], 0.0)
        self.assertEqual(latency["buckets"][0.01], 2)  # type: ignore[index]
        self.assertTrue(service.shutdown())

    def test_scheduler_callbacks_converge_without_recursive_trigger(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        readiness_callbacks = 0
        template_callbacks = 0
        original_snapshot = fixture.snapshot

        def latch_readiness() -> bool:
            nonlocal readiness_callbacks
            readiness_callbacks += 1
            service.readiness_promoted()
            return True

        def fetch_with_template_callback() -> QbitTipTemplateSnapshot:
            nonlocal template_callbacks
            template_callbacks += 1
            service.template_artifacts_changed(
                SimpleNamespace(
                    generation=1,
                    previousblockhash=fixture.tip,
                    fingerprint=original_snapshot.template_fingerprint,
                )
            )
            return original_snapshot

        fixture.jobs.pool_readiness_latched = latch_readiness  # type: ignore[method-assign]
        service.reconfigure_ports_for_test(fetch_snapshot=fetch_with_template_callback)
        self.assertEqual(service.poll_once(), 0)
        self.assertEqual(readiness_callbacks, 1)
        self.assertEqual(template_callbacks, 1)
        self.assertIsNone(service.scheduler_snapshot().pending)
        self.assertIsNone(service.snapshot().pending_token)
        self.assertEqual(service.metrics_snapshot()["trigger_supersessions"], 0)
        self.assertTrue(service.shutdown())

    def test_shutdown_closes_admission_cancels_pending_and_joins_worker(self) -> None:
        fixture = _TipRefreshFixture()
        service = fixture.service
        active_started = threading.Event()
        release_active = threading.Event()

        def execute(trigger: object) -> int:
            active_started.set()
            self.assertTrue(release_active.wait(2.0))
            service._raise_if_scheduler_superseded(trigger)  # type: ignore[arg-type]
            return 0

        service._execute_refresh_trigger = execute  # type: ignore[method-assign]
        active = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=1, tip=fixture.TIP_A)  # type: ignore[arg-type]
        )
        self.assertTrue(active_started.wait(1.0))
        pending = service.submit_trigger(
            self._scheduler_trigger(fixture, sequence=2, tip=fixture.TIP_B)  # type: ignore[arg-type]
        )
        shutdown_results: list[bool] = []
        shutdown_thread = threading.Thread(
            target=lambda: shutdown_results.append(service.shutdown())
        )
        shutdown_thread.start()
        while service.scheduler_snapshot().admission_open:
            self.assertTrue(shutdown_thread.is_alive())
        release_active.set()
        shutdown_thread.join(1.0)
        self.assertFalse(shutdown_thread.is_alive())
        self.assertEqual(shutdown_results, [True])
        with self.assertRaises(ShutdownInProgress):
            pending.result(timeout=1.0)
        with self.assertRaises(TemplateRefreshSuperseded):
            active.result(timeout=1.0)
        with self.assertRaises(ShutdownInProgress):
            service.submit_trigger(
                self._scheduler_trigger(fixture, sequence=3, tip=fixture.TIP_C)  # type: ignore[arg-type]
            )


if __name__ == "__main__":
    unittest.main()
