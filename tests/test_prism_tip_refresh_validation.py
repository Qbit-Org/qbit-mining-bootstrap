#!/usr/bin/env python3
"""Once-per-refresh chain validation and prepared-fanout cancellation tests."""

from __future__ import annotations

import threading
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import FrozenInstanceError
from unittest.mock import patch

from lab.prism.prism_coordinator import (
    ShutdownInProgress,
    TemplateRefreshBlocked,
    TipRefreshValidationToken,
    _FanoutCancellation,
    _PayoutStateDeliveryGate,
)
from tests.test_prism_coordinator_job_cache import (
    FakeLedger,
    FakeRpc,
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
)


class _ControlledTipRefreshLock:
    """Expose first contention per thread without wall-clock sleeps."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._condition = threading.Condition()
        self._controlled_threads: set[int] = set()
        self._probe_gates: list[threading.Event] = []
        self.allow_waiters_to_acquire = threading.Event()

    def acquire(self, timeout: float = -1) -> bool:
        if self._lock.acquire(blocking=False):
            return True
        thread_id = threading.get_ident()
        with self._condition:
            if thread_id not in self._controlled_threads:
                self._controlled_threads.add(thread_id)
                probe_gate = threading.Event()
                self._probe_gates.append(probe_gate)
                self._condition.notify_all()
            else:
                probe_gate = None
        if probe_gate is not None:
            if not probe_gate.wait(5):
                raise AssertionError("test did not release contention probe")
            return False
        if not self.allow_waiters_to_acquire.wait(5):
            raise AssertionError("test did not release refresh lock waiters")
        return self._lock.acquire(timeout=timeout)

    def release(self) -> None:
        self._lock.release()

    def next_probe_gate(self) -> threading.Event:
        with self._condition:
            if not self._condition.wait_for(lambda: bool(self._probe_gates), timeout=5):
                raise AssertionError("poll did not contend for the refresh lock")
            return self._probe_gates.pop(0)


def _advance_fake_tip(rpc: FakeRpc, tip_hash: str, height: int) -> None:
    rpc.tip = tip_hash
    rpc.template = base_template(height=height, prevhash=tip_hash)


class ObservedRLock:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.acquire_attempted = threading.Event()

    def acquire(self, *args: object, **kwargs: object) -> bool:
        self.acquire_attempted.set()
        return self.lock.acquire(*args, **kwargs)  # type: ignore[arg-type]

    def release(self) -> None:
        self.lock.release()

    def __enter__(self) -> ObservedRLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


class ObservedPayoutGate:
    def __init__(self, delegate: object) -> None:
        self.delegate = delegate
        self.delivery_wait_started = threading.Event()

    def delivery(self) -> object:
        return self.delegate.delivery()  # type: ignore[attr-defined,no-any-return]

    @contextmanager
    def delivery_cancelable(
        self,
        cancelled: object,
        **kwargs: object,
    ) -> object:
        self.delivery_wait_started.set()
        with self.delegate.delivery_cancelable(cancelled, **kwargs) as admitted:  # type: ignore[attr-defined]
            yield admitted

    def mutation(self) -> object:
        return self.delegate.mutation()  # type: ignore[attr-defined,no-any-return]


class TipRefreshValidationTests(unittest.TestCase):
    def advance_tip(
        self,
        server: object,
        rpc: object,
        tip_hash: str,
        *,
        height: int,
    ) -> None:
        rpc.tip = tip_hash  # type: ignore[attr-defined]
        rpc.template = base_template(height=height, prevhash=tip_hash)  # type: ignore[attr-defined]
        self.assertTrue(server.observe_tip_first_seen(tip_hash))  # type: ignore[attr-defined]

    def test_collection_refresh_reconciles_once_for_multiple_clients(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        install_fake_bundle_builder(server)
        clients = [client(index + 1) for index in range(5)]
        notifications: set[int] = set()
        for state in clients:
            state.send = (  # type: ignore[method-assign]
                lambda payload, connection_id=state.connection_id: (
                    notifications.add(connection_id)
                    if payload["method"] == "mining.notify"
                    else None
                )
            )
        server.clients = clients  # type: ignore[assignment]
        reconciliation_calls: list[str] = []
        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            lambda tip_hash: reconciliation_calls.append(tip_hash) or True
        )
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("collection fanout repeated chain validation")
        )

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, len(clients))
        self.assertEqual(reconciliation_calls, [rpc.tip])
        self.assertEqual(notifications, {state.connection_id for state in clients})
        self.assertTrue(all(state.active_job is not None for state in clients))
        self.assertTrue(all(state.active_job.collection_only for state in clients))
        self.assertLessEqual(rpc.count("getbestblockhash"), 3)

    def test_external_tip_preparation_does_not_hold_delivery_gate(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class BlockingExternalTipLedger(FakeLedger):
            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                return [
                    {
                        "block_height": active_tip_height + 1,
                        "block_hash": "aa" * 32,
                        "chain_state": "confirmed",
                    }
                ]

            def mark_pool_block_inactive(
                self,
                *,
                block_hash: str,
                active_tip_height: int,
            ) -> dict[str, object]:
                self.assert_not_atomic()
                entered.set()
                if not release.wait(5):
                    raise AssertionError("test did not release external-tip preparation")
                return {"inactive_count": 1}

            def mark_mature_pool_payouts(
                self,
                *,
                active_tip_height: int,
            ) -> dict[str, object]:
                return {"matured_count": 0}

        ledger = BlockingExternalTipLedger()
        server, rpc = coordinator(ledger=ledger)
        server.reorg_reconciler_enabled = True
        server._ensure_job_cache_state()
        ledger.assert_not_atomic = lambda: self.assertIsNone(  # type: ignore[attr-defined]
            server._payout_state_delivery_gate._mutation_owner
        )
        results: list[dict[str, object]] = []
        errors: list[BaseException] = []

        def reconcile() -> None:
            try:
                results.append(
                    server.reconcile_prism_pool_blocks_once(tip_hash=rpc.tip)
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        thread = threading.Thread(target=reconcile)
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            with server._payout_state_delivery_gate.delivery_cancelable(
                lambda: False,
                generation=0,
            ) as admission:
                self.assertTrue(admission)
                release.set()
                with server._payout_state_delivery_gate._condition:
                    self.assertTrue(
                        server._payout_state_delivery_gate._condition.wait_for(
                            lambda: (
                                server._payout_state_delivery_gate._publisher_waiting
                            ),
                            timeout=5,
                        )
                    )
                self.assertTrue(
                    server._payout_state_prepare_lock.acquire(timeout=1)
                )
                server._payout_state_prepare_lock.release()
        finally:
            release.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results[0]["inactive_blocks"], 1)
        self.assertEqual(server._payout_state_generation, 1)

    def test_payout_only_preparation_does_not_hold_delivery_gate(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class BlockingMaturityLedger(FakeLedger):
            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                return []

            def mark_mature_pool_payouts(
                self,
                *,
                active_tip_height: int,
            ) -> dict[str, object]:
                self.assert_not_atomic()
                entered.set()
                if not release.wait(5):
                    raise AssertionError("test did not release payout-only preparation")
                return {"matured_count": 1}

        ledger = BlockingMaturityLedger()
        server, rpc = coordinator(ledger=ledger)
        server.reorg_reconciler_enabled = True
        server._ensure_job_cache_state()
        ledger.assert_not_atomic = lambda: self.assertIsNone(  # type: ignore[attr-defined]
            server._payout_state_delivery_gate._mutation_owner
        )
        results: list[dict[str, object]] = []

        thread = threading.Thread(
            target=lambda: results.append(
                server.reconcile_prism_pool_blocks_once(tip_hash=rpc.tip)
            )
        )
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            with server._payout_state_delivery_gate.delivery_cancelable(
                lambda: False,
                generation=0,
            ) as admission:
                self.assertTrue(admission)
                release.set()
                with server._payout_state_delivery_gate._condition:
                    self.assertTrue(
                        server._payout_state_delivery_gate._condition.wait_for(
                            lambda: (
                                server._payout_state_delivery_gate._publisher_waiting
                            ),
                            timeout=5,
                        )
                    )
                self.assertTrue(
                    server._payout_state_prepare_lock.acquire(timeout=1)
                )
                server._payout_state_prepare_lock.release()
        finally:
            release.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(results[0]["matured_payouts"], 1)
        self.assertEqual(server._payout_state_generation, 1)

    def test_tip_poll_reuses_reconciled_external_source(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        initial_snapshot = server.fetch_qbit_tip_template_snapshot()
        initial_sequence = server._reserve_tip_observation_sequence()
        self.assertTrue(
            server.observe_tip_first_seen(
                initial_snapshot.bestblockhash,
                observation_sequence=initial_sequence,
                publish_refresh_observation=True,
            )
        )
        with server.lock:
            server.tip_template_snapshot = initial_snapshot

        next_tip = "76" * 32
        _advance_fake_tip(rpc, next_tip, 11)

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._payout_state_source[0], 1)
        self.assertEqual(server._published_payout_state.source_generation, 1)
        self.assertEqual(server._published_payout_state.source_tip_hash, next_tip)
        self.assertFalse(server.tip_refresh_is_pending())

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._payout_state_source[0], 1)

    def test_newer_tip_discards_in_progress_payout_candidate(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class SupersededLedger(FakeLedger):
            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                return [
                    {
                        "block_height": active_tip_height + 1,
                        "block_hash": "bb" * 32,
                        "chain_state": "confirmed",
                    }
                ]

            def mark_pool_block_inactive(
                self,
                *,
                block_hash: str,
                active_tip_height: int,
            ) -> dict[str, object]:
                entered.set()
                if not release.wait(5):
                    raise AssertionError("test did not release superseded preparation")
                return {"inactive_count": 1}

            def mark_mature_pool_payouts(
                self,
                *,
                active_tip_height: int,
            ) -> dict[str, object]:
                return {"matured_count": 0}

        server, rpc = coordinator(ledger=SupersededLedger())
        server.reorg_reconciler_enabled = True
        self.assertTrue(server.observe_tip_first_seen(rpc.tip))
        old_tip = rpc.tip
        errors: list[BaseException] = []

        def reconcile() -> None:
            try:
                server.reconcile_prism_pool_blocks_once(tip_hash=old_tip)
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        thread = threading.Thread(target=reconcile)
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            new_tip = "77" * 32
            rpc.tip = new_tip
            rpc.template = base_template(height=11, prevhash=new_tip)
            self.assertTrue(server.observe_tip_first_seen(new_tip))
        finally:
            release.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server.payout_state_candidates_discarded, 1)
        self.assertEqual(server._published_payout_state.source_tip_hash, new_tip)

    def test_supersession_retry_preserves_durable_reorg_counts(self) -> None:
        new_tip = "78" * 32

        class SupersedingLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.server: object | None = None
                self.rpc: FakeRpc | None = None
                self.inactive_calls = 0

            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                return [
                    {
                        "block_height": active_tip_height + 1,
                        "block_hash": "bc" * 32,
                        "chain_state": "confirmed",
                    }
                ]

            def mark_pool_block_inactive(
                self,
                *,
                block_hash: str,
                active_tip_height: int,
            ) -> dict[str, object]:
                self.inactive_calls += 1
                if self.inactive_calls == 1:
                    assert self.server is not None
                    assert self.rpc is not None
                    _advance_fake_tip(self.rpc, new_tip, 11)
                    self.server._reserve_payout_state_source(  # type: ignore[attr-defined]
                        "external_tip",
                        tip_hash=new_tip,
                    )
                    return {"inactive_count": 1}
                return {"inactive_count": 0}

            def mark_mature_pool_payouts(
                self,
                *,
                active_tip_height: int,
            ) -> dict[str, object]:
                return {"matured_count": 0}

        ledger = SupersedingLedger()
        server, rpc = coordinator(ledger=ledger)
        ledger.server = server
        ledger.rpc = rpc
        server.reorg_reconciler_enabled = True
        old_tip = rpc.tip

        summary = server.reconcile_prism_pool_blocks_once(tip_hash=old_tip)

        self.assertEqual(ledger.inactive_calls, 2)
        self.assertEqual(summary["watched_blocks"], 1)
        self.assertEqual(summary["inactive_blocks"], 1)
        self.assertEqual(server.reorg_inactive_block_count, 1)
        self.assertEqual(summary["published_generation"], 1)
        self.assertEqual(server.payout_state_candidates_discarded, 1)
        self.assertEqual(server._published_payout_state.source_tip_hash, new_tip)

    def test_failed_superseded_reconcile_does_not_publish_latest_source(self) -> None:
        newer_tip = "79" * 32

        class FailingSupersededLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.server: object | None = None

            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                return [
                    {
                        "block_height": active_tip_height + 1,
                        "block_hash": "bd" * 32,
                        "chain_state": "confirmed",
                    }
                ]

            def mark_pool_block_inactive(
                self,
                *,
                block_hash: str,
                active_tip_height: int,
            ) -> dict[str, object]:
                assert self.server is not None
                self.server._reserve_payout_state_source(  # type: ignore[attr-defined]
                    "external_tip",
                    tip_hash=newer_tip,
                )
                return {"inactive_count": 1}

            def mark_mature_pool_payouts(
                self,
                *,
                active_tip_height: int,
            ) -> dict[str, object]:
                raise RuntimeError("maturity RPC failed")

        ledger = FailingSupersededLedger()
        server, rpc = coordinator(ledger=ledger)
        ledger.server = server
        server.reorg_reconciler_enabled = True

        with self.assertRaisesRegex(RuntimeError, "maturity RPC failed"):
            server.reconcile_prism_pool_blocks_once(tip_hash=rpc.tip)

        self.assertEqual(server._payout_state_generation, 0)
        self.assertIsNone(server._published_payout_state.source_tip_hash)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertEqual(server.payout_state_candidates_discarded, 1)
        self.assertEqual(server.reorg_inactive_block_count, 1)
        self.assertTrue(server.tip_refresh_is_pending())

    def test_tip_churn_bounds_reconcile_retries_and_fences_job_builds(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server.payout_reconcile_supersession_retries = 2
        server.qbit_chain_view_untrusted = lambda: True  # type: ignore[method-assign]
        real_publish = server._publish_payout_state_candidate
        publish_attempts = 0

        def supersede_before_publish(candidate: object) -> int | None:
            nonlocal publish_attempts
            publish_attempts += 1
            newer_tip = f"{publish_attempts + 1:064x}"
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=newer_tip,
            )
            return real_publish(candidate)  # type: ignore[arg-type]

        server._publish_payout_state_candidate = supersede_before_publish  # type: ignore[method-assign]

        summary = server.reconcile_prism_pool_blocks_once(
            tip_hash=rpc.tip,
            _force_publish=True,
        )

        self.assertTrue(summary["superseded"])
        self.assertEqual(publish_attempts, 3)
        self.assertEqual(server.reorg_reconcile_skip_count, 1)
        self.assertEqual(server._payout_state_generation, 0)
        self.assertEqual(server.payout_state_candidates_discarded, 3)
        self.assertTrue(server._payout_state_publication_blocked)
        state = client(1)
        assert state.worker is not None
        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "pending publication",
        ):
            server.build_shared_job_bundle(
                server.current_template_artifacts(),
                state.worker,
            )

    def test_published_generation_prioritizes_current_tip_over_waiters(self) -> None:
        gate = _PayoutStateDeliveryGate()
        active_entered = threading.Event()
        release_active = threading.Event()
        publication_done = threading.Event()
        stale_done = threading.Event()
        admitted_order: list[str] = []

        def active_delivery() -> None:
            with gate.delivery_cancelable(lambda: False, generation=0) as admission:
                self.assertTrue(admission)
                active_entered.set()
                self.assertTrue(release_active.wait(5))

        def publish() -> None:
            with gate.publication():
                gate.publish_generation(1, prioritize_delivery=True)
            publication_done.set()

        def wait_for_delivery(name: str, *, priority: bool) -> None:
            with gate.delivery_cancelable(
                lambda: False,
                generation=1,
                priority=priority,
            ) as admission:
                if admission:
                    admitted_order.append(name)
                    admission.mark_delivered()

        def stale_waiter() -> None:
            with gate.delivery_cancelable(lambda: False, generation=0) as admission:
                self.assertFalse(admission)
            stale_done.set()

        active_thread = threading.Thread(target=active_delivery)
        publish_thread = threading.Thread(target=publish)
        routine_thread = threading.Thread(
            target=wait_for_delivery,
            args=("routine",),
            kwargs={"priority": False},
        )
        priority_thread = threading.Thread(
            target=wait_for_delivery,
            args=("priority",),
            kwargs={"priority": True},
        )
        stale_thread = threading.Thread(target=stale_waiter)
        active_thread.start()
        self.assertTrue(active_entered.wait(5))
        publish_thread.start()
        with gate._condition:
            self.assertTrue(
                gate._condition.wait_for(lambda: gate._publisher_waiting, timeout=5)
            )
        routine_thread.start()
        stale_thread.start()
        priority_thread.start()
        release_active.set()
        for thread in (
            active_thread,
            publish_thread,
            routine_thread,
            priority_thread,
            stale_thread,
        ):
            thread.join(5)

        self.assertTrue(publication_done.is_set())
        self.assertTrue(stale_done.is_set())
        self.assertFalse(routine_thread.is_alive())
        self.assertEqual(admitted_order[:1], ["priority"])
        if admitted_order == ["priority"]:
            with gate.delivery_cancelable(
                lambda: False,
                generation=1,
                priority=False,
            ) as admission:
                self.assertTrue(admission)
                admitted_order.append("routine")
        self.assertEqual(admitted_order, ["priority", "routine"])

    def test_priority_reservation_rejects_nonpriority_without_waiting(self) -> None:
        gate = _PayoutStateDeliveryGate()
        with gate.publication():
            gate.publish_generation(1, prioritize_delivery=True)
        cancellation_checks = 0

        def cancel_after_first_wait() -> bool:
            nonlocal cancellation_checks
            cancellation_checks += 1
            return cancellation_checks > 1

        with gate.delivery_cancelable(
            cancel_after_first_wait,
            generation=1,
            priority=False,
            poll_seconds=0.0,
        ) as admission:
            self.assertFalse(admission)

        self.assertEqual(cancellation_checks, 1)
        with gate.delivery_cancelable(
            lambda: False,
            generation=1,
            priority=True,
        ) as admission:
            self.assertTrue(admission)
            admission.mark_delivered()

    def test_publication_updates_gate_without_coordinator_locks(self) -> None:
        server, _rpc = coordinator()
        server._reserve_payout_state_source("payout_only")
        original_publish_generation = (
            server._payout_state_delivery_gate.publish_generation
        )
        lock_snapshots: list[tuple[bool, bool]] = []

        def observe_locks(
            generation: int,
            *,
            prioritize_delivery: bool,
        ) -> None:
            lock_snapshots.append(
                (
                    server._job_cache_lock.locked(),
                    server.lock._is_owned(),  # type: ignore[attr-defined]
                )
            )
            original_publish_generation(
                generation,
                prioritize_delivery=prioritize_delivery,
            )

        server._payout_state_delivery_gate.publish_generation = observe_locks  # type: ignore[method-assign]

        self.assertEqual(
            server._publish_payout_state_candidate(
                server._current_payout_state_candidate()
            ),
            1,
        )
        self.assertEqual(lock_snapshots, [(False, False)])

    def test_pending_clear_does_not_consume_first_delivery_priority(self) -> None:
        server, _rpc = coordinator()
        snapshot = server.fetch_qbit_tip_template_snapshot()
        sequence = server._reserve_tip_observation_sequence()
        self.assertTrue(
            server.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=sequence,
                publish_refresh_observation=True,
            )
        )
        with server.lock:
            server.tip_template_snapshot = snapshot
        server._reserve_payout_state_source("payout_only")
        self.assertEqual(
            server._publish_payout_state_candidate(
                server._current_payout_state_candidate()
            ),
            1,
        )

        self.assertTrue(
            server._clear_tip_refresh_pending_for_completed_refresh(
                snapshot,
                sequence,
                1,
            )
        )
        self.assertEqual(
            server._payout_state_delivery_gate._priority_generation,
            1,
        )
        with server._payout_state_delivery_gate.delivery_cancelable(
            lambda: False,
            generation=1,
            priority=False,
        ) as routine_admission:
            self.assertFalse(routine_admission)
        with server._payout_state_delivery_gate.delivery_cancelable(
            lambda: False,
            generation=1,
            priority=True,
        ) as first_delivery:
            self.assertTrue(first_delivery)
            first_delivery.mark_delivered()
        self.assertIsNone(
            server._payout_state_delivery_gate._priority_generation
        )

    def test_collection_refresh_stops_when_chain_becomes_untrusted(self) -> None:
        server, _rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        first, second = client(1), client(2)
        notifications: list[int] = []
        for state in (first, second):
            state.send = (  # type: ignore[method-assign]
                lambda payload, connection_id=state.connection_id: (
                    notifications.append(connection_id)
                    if payload["method"] == "mining.notify"
                    else None
                )
            )
        server.clients = [first, second]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        chain_view_checks = 0

        def chain_view_untrusted() -> bool:
            nonlocal chain_view_checks
            chain_view_checks += 1
            return chain_view_checks == 2

        server.qbit_chain_view_untrusted = chain_view_untrusted  # type: ignore[method-assign]

        with self.assertRaises(TemplateRefreshBlocked):
            server.poll_qbit_tip_template_once()

        self.assertEqual(chain_view_checks, 2)
        self.assertEqual(notifications, [first.connection_id])
        self.assertIsNotNone(first.active_job)
        self.assertIsNone(second.active_job)
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_orphan_reconciliation_rebuilds_bundle_from_post_reorg_balances(self) -> None:
        orphaned_balance = {
            "recipient_id": "orphaned-miner",
            "order_key": "orphaned-miner",
            "p2mr_program_hex": "44" * 32,
            "balance_sats": 12_345,
        }

        class OrphanBalanceLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.prior_balances = [dict(orphaned_balance)]
                self.inactive_calls = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                return [dict(balance) for balance in self.prior_balances]

            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                self.assert_active_tip_height = active_tip_height
                return [
                    {
                        "block_height": 99,
                        "block_hash": "aa" * 32,
                        "chain_state": "confirmed",
                    }
                ]

            def mark_pool_block_inactive(
                self,
                *,
                block_hash: str,
                active_tip_height: int,
            ) -> dict[str, object]:
                self.inactive_calls += 1
                self.prior_balances.clear()
                events.append("reconcile")
                return {"inactive_count": 1}

            def mark_mature_pool_payouts(
                self,
                *,
                active_tip_height: int,
            ) -> dict[str, object]:
                return {"matured_count": 0}

        events: list[str] = []
        ledger = OrphanBalanceLedger()
        server, rpc = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        original_rpc_call = rpc.call

        def reorg_rpc_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash" and params == [99]:
                return "bb" * 32
            return original_rpc_call(method, params)

        rpc.call = reorg_rpc_call  # type: ignore[method-assign]
        original_build = server.build_audit_bundle
        signed_balance_snapshots: list[list[dict[str, object]]] = []

        def record_signed_balances(**kwargs: object) -> dict[str, object]:
            balances = [
                dict(balance)
                for balance in kwargs["prior_balances"]  # type: ignore[union-attr]
            ]
            signed_balance_snapshots.append(balances)
            events.append(f"bundle:{sum(int(row['balance_sats']) for row in balances)}")
            bundle = original_build(**kwargs)
            bundle["prior_balances"] = balances
            return bundle

        server.build_audit_bundle = record_signed_balances  # type: ignore[method-assign]

        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        stale_bundle = server.shared_job_bundle(artifacts, state.worker)  # type: ignore[arg-type]
        self.assertEqual(stale_bundle.prior_balances, [orphaned_balance])
        self.assertEqual(recorded["calls"], 1)
        stale_context = server.stamp_job_for_client(
            state,
            stale_bundle,
            clean_jobs=False,
        )
        state.active_job = stale_context
        state.active_job_ids.add(stale_context.job.job_id)
        server.jobs[stale_context.job.job_id] = stale_context
        events.clear()
        clean_notifications: list[bool] = []

        def record_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                events.append("fanout")
                clean_notifications.append(bool(payload["params"][-1]))  # type: ignore[index]

        state.send = record_send  # type: ignore[method-assign]

        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(events, ["reconcile", "bundle:0", "fanout"])
        self.assertEqual(ledger.inactive_calls, 1)
        self.assertEqual(server.reorg_inactive_block_count, 1)
        self.assertEqual(
            signed_balance_snapshots,
            [[orphaned_balance], []],
        )
        self.assertEqual(recorded["calls"], 2)
        self.assertIsNotNone(state.active_job)
        self.assertEqual(state.active_job.prior_balances, [])
        self.assertFalse(hasattr(state.active_job, "bundle"))
        self.assertFalse(hasattr(stale_bundle, "bundle"))
        refreshed_bundle = next(iter(server._job_bundle_cache.values()))
        self.assertEqual(refreshed_bundle.prior_balances, [])
        self.assertIsNot(refreshed_bundle.coinbase_manifest, stale_bundle.coinbase_manifest)
        self.assertIn("coinbase_tx_hex", refreshed_bundle.coinbase_manifest)
        self.assertEqual(stale_bundle.payout_state_generation, 0)
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(state.active_job.payout_state_generation, 1)
        self.assertEqual(clean_notifications, [True])
        self.assertNotIn(stale_context.job.job_id, state.active_job_ids)

    def test_hundred_client_refresh_validates_chain_once(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        server.tip_refresh_max_workers = 4
        clients = [client(index + 1) for index in range(100)]
        notifications: set[int] = set()
        notifications_lock = threading.Lock()

        for state in clients:
            def record_send(
                payload: dict[str, object],
                *,
                connection_id: int = state.connection_id,
            ) -> None:
                if payload["method"] == "mining.notify":
                    with notifications_lock:
                        notifications.add(connection_id)

            state.send = record_send  # type: ignore[method-assign]
        # A list makes task submission order deterministic while retaining the
        # membership behavior the coordinator needs.
        server.clients = clients  # type: ignore[assignment]

        reconciliation_calls: list[str] = []
        chain_view_checks = 0
        forbidden_worker_checks = 0
        validation_tokens: list[TipRefreshValidationToken] = []

        def reconcile_once(tip_hash: str) -> bool:
            reconciliation_calls.append(tip_hash)
            return True

        def chain_view_untrusted() -> bool:
            nonlocal chain_view_checks
            chain_view_checks += 1
            return False

        def reject_per_client_validation(
            *,
            expected_tip_hash: str | None = None,
        ) -> bool:
            nonlocal forbidden_worker_checks
            forbidden_worker_checks += 1
            raise AssertionError(
                "prepared fanout worker performed live chain validation "
                f"for {expected_tip_hash}"
            )

        original_validate = server._validate_prepared_tip_refresh

        def record_validation(
            bundle: object,
            snapshot: object,
            observation_sequence: int,
        ) -> TipRefreshValidationToken:
            token = original_validate(bundle, snapshot, observation_sequence)
            validation_tokens.append(token)
            return token

        server.ensure_reorg_reconciled_for_tip = reconcile_once  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = chain_view_untrusted  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            reject_per_client_validation
        )
        server._validate_prepared_tip_refresh = record_validation  # type: ignore[method-assign]

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 100)
        self.assertEqual(notifications, {state.connection_id for state in clients})
        self.assertTrue(all(state.active_job is not None for state in clients))
        self.assertEqual(reconciliation_calls, [rpc.tip])
        self.assertEqual(chain_view_checks, 2)
        self.assertEqual(forbidden_worker_checks, 0)
        self.assertEqual(len(validation_tokens), 1)

        token = validation_tokens[0]
        self.assertEqual(token.tip_hash, rpc.tip)
        self.assertEqual(
            token.template_fingerprint,
            server.tip_template_snapshot.template_fingerprint,
        )
        self.assertEqual(
            token.template_generation,
            server.tip_template_snapshot.template_generation,
        )
        self.assertEqual(
            token.payout_state_generation,
            server._payout_state_generation,
        )
        self.assertEqual(
            token.observation_sequence,
            server.current_tip_observation_sequence,
        )
        with self.assertRaises(FrozenInstanceError):
            token.tip_hash = "ff" * 32  # type: ignore[misc]

        # One fetch check, bounded pre-fanout checks, and one post-fanout check
        # are acceptable. The count must not scale to the 100-client fanout.
        self.assertLessEqual(rpc.count("getbestblockhash"), 4)

    def test_untrusted_prevalidation_sends_zero_jobs(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        clients = [client(1), client(2)]
        sent: list[dict[str, object]] = []
        for state in clients:
            state.send = sent.append  # type: ignore[method-assign]
        server.clients = clients  # type: ignore[assignment]
        reconciliation_calls: list[str] = []
        chain_view_checks = 0

        def reconcile(tip_hash: str) -> bool:
            reconciliation_calls.append(tip_hash)
            return True

        def untrusted() -> bool:
            nonlocal chain_view_checks
            chain_view_checks += 1
            return True

        server.ensure_reorg_reconciled_for_tip = reconcile  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = untrusted  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("prepared worker performed chain validation")
        )

        try:
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(reconciliation_calls, [server.rpc.tip])
        self.assertEqual(chain_view_checks, 1)
        self.assertEqual(sent, [])
        self.assertTrue(all(state.active_job is None for state in clients))
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_untrusted_post_fanout_does_not_report_refresh_success(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        clients = [client(1), client(2)]
        notifications: list[int] = []
        for state in clients:
            state.send = (  # type: ignore[method-assign]
                lambda payload, connection_id=state.connection_id: (
                    notifications.append(connection_id)
                    if payload["method"] == "mining.notify"
                    else None
                )
            )
        server.clients = clients  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        chain_view_checks = 0

        def becomes_untrusted() -> bool:
            nonlocal chain_view_checks
            chain_view_checks += 1
            return chain_view_checks == 2

        server.qbit_chain_view_untrusted = becomes_untrusted  # type: ignore[method-assign]

        try:
            with self.assertRaisesRegex(
                TemplateRefreshBlocked,
                "qbit chain view became untrusted during prepared fanout",
            ):
                server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(chain_view_checks, 2)
        self.assertEqual(
            set(notifications),
            {state.connection_id for state in clients},
        )
        self.assertTrue(server._tip_refresh_retry.is_set())
        self.assertIsNone(
            getattr(server, "last_successful_template_refresh_monotonic", None)
        )

    def test_shutdown_during_live_fanout_trust_check_is_not_reclassified(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        notification_started = threading.Event()
        release_notification = threading.Event()

        def block_notification(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            notification_started.set()
            self.assertTrue(release_notification.wait(5))

        def shutdown_during_reconciliation(**_kwargs: object) -> bool:
            server.request_shutdown()
            raise ShutdownInProgress("PRISM coordinator is shutting down")

        state.send = block_notification  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            shutdown_during_reconciliation
        )

        try:
            with self.assertRaises(ShutdownInProgress):
                server.poll_qbit_tip_template_once()
            self.assertTrue(notification_started.is_set())
            self.assertFalse(server._tip_refresh_retry.is_set())
        finally:
            release_notification.set()
            server.shutdown_tip_refresh_executor()

    def test_prevalidation_rpc_failure_schedules_immediate_retry(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        snapshot = server.fetch_qbit_tip_template_snapshot()
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        original_rpc_call = rpc.call

        def fail_tip_validation(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getbestblockhash":
                raise RuntimeError("qbit rpc unavailable")
            return original_rpc_call(method, params)

        rpc.call = fail_tip_validation  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "qbit tip validation failed before prepared fanout",
        ):
            server._validate_prepared_tip_refresh(bundle, snapshot, 1)

        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_prevalidation_trust_check_failure_schedules_immediate_retry(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        snapshot = server.fetch_qbit_tip_template_snapshot()
        bundle = server.prepare_tip_refresh_bundle(snapshot)

        def fail_chain_trust_check() -> bool:
            raise RuntimeError("getblockchaininfo unavailable")

        server.qbit_chain_view_untrusted = fail_chain_trust_check  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "qbit chain trust check failed before prepared fanout",
        ):
            server._validate_prepared_tip_refresh(bundle, snapshot, 1)

        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_reconciliation_failure_before_fanout_sends_zero_jobs(self) -> None:
        server, _rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        clients = [client(1), client(2)]
        sent: list[dict[str, object]] = []
        for state in clients:
            state.send = sent.append  # type: ignore[method-assign]
        server.clients = clients  # type: ignore[assignment]
        reconciliation_calls: list[str] = []

        def fail_reconciliation(tip_hash: str) -> bool:
            reconciliation_calls.append(tip_hash)
            raise RuntimeError("ledger reconciliation unavailable")

        server.ensure_reorg_reconciled_for_tip = fail_reconciliation  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = (  # type: ignore[method-assign]
            lambda: self.fail("chain view checked after reconciliation failure")
        )
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("prepared worker performed chain validation")
        )

        try:
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(reconciliation_calls, [server.rpc.tip])
        self.assertEqual(recorded["calls"], 0)
        self.assertEqual(sent, [])
        self.assertTrue(all(state.active_job is None for state in clients))

    def test_newer_observation_cancels_pending_fanout_tasks(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        server.tip_refresh_max_workers = 1
        first, second, third = client(1), client(2), client(3)
        clients = [first, second, third]
        server.clients = clients  # type: ignore[assignment]
        first_send_started = threading.Event()
        release_first_send = threading.Event()
        later_sends: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            first_send_started.set()
            self.assertTrue(release_first_send.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = later_sends.append  # type: ignore[method-assign]
        third.send = later_sends.append  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("prepared worker performed chain validation")
        )
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface thread errors
                errors.append(exc)

        poll_thread = threading.Thread(target=poll)
        poll_thread.start()
        try:
            self.assertTrue(first_send_started.wait(5))
            old_sequence = server.current_tip_observation_sequence
            new_tip = "33" * 32
            rpc.tip = new_tip
            self.assertTrue(server.observe_tip_first_seen(new_tip))
            self.assertGreater(server.current_tip_observation_sequence, old_sequence)
        finally:
            release_first_send.set()
            poll_thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(poll_thread.is_alive())
        self.assertEqual(later_sends, [])
        self.assertIsNotNone(first.active_job)
        self.assertIsNone(second.active_job)
        self.assertIsNone(third.active_job)
        self.assertLessEqual(sum(refreshed), 1)
        self.assertTrue(
            all(isinstance(error, TemplateRefreshBlocked) for error in errors),
            errors,
        )

    def test_waiting_poll_observes_new_tip_and_supersedes_slow_fanout(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        server.tip_refresh_max_workers = 1
        first, second = client(1), client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        server._ensure_tip_refresh_state()
        refresh_lock = _ControlledTipRefreshLock()
        server._tip_refresh_lock = refresh_lock  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: True
        )
        self.assertTrue(server.observe_tip_first_seen(rpc.tip))
        server._tip_refresh_retry.clear()

        tip_a = rpc.tip
        tip_b = "33" * 32
        first_send_started = threading.Event()
        release_first_send = threading.Event()
        tip_b_observed = threading.Event()
        sent_tips: dict[int, list[str]] = {1: [], 2: []}
        first_notification_count = 0

        def record_first_send(payload: dict[str, object]) -> None:
            nonlocal first_notification_count
            if payload["method"] != "mining.notify":
                return
            assert first.active_job is not None
            sent_tips[1].append(str(first.active_job.template["previousblockhash"]))
            first_notification_count += 1
            if first_notification_count == 1:
                first_send_started.set()
                self.assertTrue(release_first_send.wait(5))

        def record_second_send(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            assert second.active_job is not None
            sent_tips[2].append(str(second.active_job.template["previousblockhash"]))

        first.send = record_first_send  # type: ignore[method-assign]
        second.send = record_second_send  # type: ignore[method-assign]
        original_observe = server.observe_tip_first_seen

        def record_observation(*args: object, **kwargs: object) -> bool:
            observed = original_observe(*args, **kwargs)  # type: ignore[arg-type]
            if args and args[0] == tip_b and observed:
                tip_b_observed.set()
            return observed

        server.observe_tip_first_seen = record_observation  # type: ignore[method-assign]
        results: dict[str, list[int]] = {"old": [], "new": []}
        errors: dict[str, list[BaseException]] = {"old": [], "new": []}

        def poll(label: str) -> None:
            try:
                results[label].append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface thread errors
                errors[label].append(exc)

        old_poll = threading.Thread(target=poll, args=("old",))
        new_poll = threading.Thread(target=poll, args=("new",))
        old_poll.start()
        try:
            self.assertTrue(first_send_started.wait(5))
            with server.lock:
                active = server._active_tip_refresh
            self.assertIsNotNone(active)
            assert active is not None

            new_poll.start()
            probe_gate = refresh_lock.next_probe_gate()
            _advance_fake_tip(rpc, tip_b, 11)
            probe_gate.set()

            self.assertTrue(tip_b_observed.wait(5))
            self.assertTrue(active[1].is_set())
            pending_tip_b = server._tip_refresh_pending_token
            self.assertIsNotNone(pending_tip_b)
        finally:
            release_first_send.set()
            old_poll.join(5)

        self.assertFalse(old_poll.is_alive())
        self.assertEqual(results["old"], [])
        self.assertEqual(len(errors["old"]), 1)
        self.assertIsInstance(errors["old"][0], TemplateRefreshBlocked)
        self.assertEqual(server._tip_refresh_pending_token, pending_tip_b)
        self.assertTrue(server.tip_refresh_is_pending())

        refresh_lock.allow_waiters_to_acquire.set()
        new_poll.join(5)
        server.shutdown_tip_refresh_executor()

        self.assertFalse(new_poll.is_alive())
        self.assertEqual(errors["new"], [])
        self.assertEqual(results["new"], [2])
        self.assertEqual(sent_tips, {1: [tip_a, tip_b], 2: [tip_b]})
        self.assertFalse(server.tip_refresh_is_pending())

    def test_waiting_poll_supersedes_slow_bundle_without_parallel_build(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        server._ensure_tip_refresh_state()
        refresh_lock = _ControlledTipRefreshLock()
        server._tip_refresh_lock = refresh_lock  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        self.assertTrue(server.observe_tip_first_seen(rpc.tip))
        server._tip_refresh_retry.clear()

        tip_b = "44" * 32
        bundle_started = threading.Event()
        release_bundle = threading.Event()
        tip_b_observed = threading.Event()
        builder_lock = threading.Lock()
        active_builders = 0
        max_active_builders = 0
        build_calls = 0
        original_shared_job_bundle = server.shared_job_bundle

        def blocking_shared_job_bundle(*args: object, **kwargs: object) -> object:
            nonlocal active_builders, max_active_builders, build_calls
            with builder_lock:
                build_calls += 1
                active_builders += 1
                max_active_builders = max(max_active_builders, active_builders)
                should_block = build_calls == 1
            try:
                if should_block:
                    bundle_started.set()
                    if not release_bundle.wait(5):
                        raise AssertionError("test did not release bundle construction")
                return original_shared_job_bundle(*args, **kwargs)  # type: ignore[arg-type]
            finally:
                with builder_lock:
                    active_builders -= 1

        server.shared_job_bundle = blocking_shared_job_bundle  # type: ignore[method-assign]
        sent_tips: list[str] = []

        def record_send(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            assert state.active_job is not None
            sent_tips.append(str(state.active_job.template["previousblockhash"]))

        state.send = record_send  # type: ignore[method-assign]
        original_observe = server.observe_tip_first_seen

        def record_observation(*args: object, **kwargs: object) -> bool:
            observed = original_observe(*args, **kwargs)  # type: ignore[arg-type]
            if args and args[0] == tip_b and observed:
                tip_b_observed.set()
            return observed

        server.observe_tip_first_seen = record_observation  # type: ignore[method-assign]
        results: dict[str, list[int]] = {"old": [], "new": []}
        errors: dict[str, list[BaseException]] = {"old": [], "new": []}

        def poll(label: str) -> None:
            try:
                results[label].append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface thread errors
                errors[label].append(exc)

        old_poll = threading.Thread(target=poll, args=("old",))
        new_poll = threading.Thread(target=poll, args=("new",))
        old_poll.start()
        try:
            self.assertTrue(bundle_started.wait(5))
            new_poll.start()
            probe_gate = refresh_lock.next_probe_gate()
            _advance_fake_tip(rpc, tip_b, 11)
            probe_gate.set()
            self.assertTrue(tip_b_observed.wait(5))
            pending_tip_b = server._tip_refresh_pending_token
            self.assertIsNotNone(pending_tip_b)
            self.assertEqual(sent_tips, [])
        finally:
            release_bundle.set()
            old_poll.join(5)

        self.assertFalse(old_poll.is_alive())
        self.assertEqual(results["old"], [])
        self.assertEqual(len(errors["old"]), 1)
        self.assertIsInstance(errors["old"][0], TemplateRefreshBlocked)
        self.assertEqual(sent_tips, [])
        self.assertEqual(server._tip_refresh_pending_token, pending_tip_b)

        refresh_lock.allow_waiters_to_acquire.set()
        new_poll.join(5)
        server.shutdown_tip_refresh_executor()

        self.assertFalse(new_poll.is_alive())
        self.assertEqual(errors["new"], [])
        self.assertEqual(results["new"], [1])
        self.assertEqual(sent_tips, [tip_b])
        self.assertEqual(max_active_builders, 1)
        self.assertFalse(server.tip_refresh_is_pending())

    def test_rapid_contention_observations_are_latest_wins(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        server._ensure_tip_refresh_state()
        refresh_lock = _ControlledTipRefreshLock()
        server._tip_refresh_lock = refresh_lock  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        self.assertTrue(server.observe_tip_first_seen(rpc.tip))
        server._tip_refresh_retry.clear()

        tip_b = "55" * 32
        tip_c = "66" * 32
        bundle_started = threading.Event()
        release_bundle = threading.Event()
        original_shared_job_bundle = server.shared_job_bundle
        build_calls = 0

        def blocking_shared_job_bundle(*args: object, **kwargs: object) -> object:
            nonlocal build_calls
            build_calls += 1
            if build_calls == 1:
                bundle_started.set()
                if not release_bundle.wait(5):
                    raise AssertionError("test did not release bundle construction")
            return original_shared_job_bundle(*args, **kwargs)  # type: ignore[arg-type]

        server.shared_job_bundle = blocking_shared_job_bundle  # type: ignore[method-assign]
        stale_probe_started = threading.Event()
        release_stale_probe = threading.Event()
        tip_b_observed = threading.Event()
        tip_c_observed = threading.Event()
        stale_probe_finished = threading.Event()
        stale_probe_results: list[bool] = []
        original_rpc_call = rpc.call
        stale_probe: threading.Thread
        tip_b_poll: threading.Thread
        tip_c_poll: threading.Thread
        stale_rpc_intercepted = False

        def ordered_rpc_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            nonlocal stale_rpc_intercepted
            if (
                method == "getbestblockhash"
                and threading.current_thread() is stale_probe
                and not stale_rpc_intercepted
            ):
                stale_rpc_intercepted = True
                stale_probe_started.set()
                if not release_stale_probe.wait(5):
                    raise AssertionError("test did not release stale contention probe")
                return tip_b
            return original_rpc_call(method, params)

        rpc.call = ordered_rpc_call  # type: ignore[method-assign]
        original_observe = server.observe_tip_first_seen

        def record_observation(*args: object, **kwargs: object) -> bool:
            observed = original_observe(*args, **kwargs)  # type: ignore[arg-type]
            if threading.current_thread() is stale_probe and args and args[0] == tip_b:
                stale_probe_results.append(observed)
                stale_probe_finished.set()
            elif threading.current_thread() is tip_b_poll and args and args[0] == tip_b:
                if observed:
                    tip_b_observed.set()
            elif threading.current_thread() is tip_c_poll and args and args[0] == tip_c:
                if observed:
                    tip_c_observed.set()
            return observed

        server.observe_tip_first_seen = record_observation  # type: ignore[method-assign]
        sent_tips: list[str] = []

        def record_send(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            assert state.active_job is not None
            sent_tips.append(str(state.active_job.template["previousblockhash"]))

        state.send = record_send  # type: ignore[method-assign]
        results: dict[str, list[int]] = {
            "old": [],
            "stale": [],
            "b": [],
            "c": [],
        }
        errors: dict[str, list[BaseException]] = {
            "old": [],
            "stale": [],
            "b": [],
            "c": [],
        }

        def poll(label: str) -> None:
            try:
                results[label].append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface thread errors
                errors[label].append(exc)

        old_poll = threading.Thread(target=poll, args=("old",))
        stale_probe = threading.Thread(target=poll, args=("stale",))
        tip_b_poll = threading.Thread(target=poll, args=("b",))
        tip_c_poll = threading.Thread(target=poll, args=("c",))
        old_poll.start()
        try:
            self.assertTrue(bundle_started.wait(5))

            stale_probe.start()
            refresh_lock.next_probe_gate().set()
            self.assertTrue(stale_probe_started.wait(5))

            _advance_fake_tip(rpc, tip_b, 11)
            tip_b_poll.start()
            refresh_lock.next_probe_gate().set()
            self.assertTrue(tip_b_observed.wait(5))

            _advance_fake_tip(rpc, tip_c, 12)
            tip_c_poll.start()
            refresh_lock.next_probe_gate().set()
            self.assertTrue(tip_c_observed.wait(5))
            pending_tip_c = server._tip_refresh_pending_token
            published_tip_c_sequence = server.current_tip_observation_sequence

            release_stale_probe.set()
            self.assertTrue(stale_probe_finished.wait(5))
            self.assertEqual(stale_probe_results, [False])
            self.assertEqual(server.current_tip_first_seen[0], tip_c)
            self.assertEqual(
                server.current_tip_observation_sequence,
                published_tip_c_sequence,
            )
            self.assertEqual(server._tip_refresh_pending_token, pending_tip_c)
        finally:
            release_stale_probe.set()
            release_bundle.set()
            old_poll.join(5)

        self.assertFalse(old_poll.is_alive())
        self.assertEqual(results["old"], [])
        self.assertEqual(len(errors["old"]), 1)
        self.assertIsInstance(errors["old"][0], TemplateRefreshBlocked)
        self.assertEqual(server._tip_refresh_pending_token, pending_tip_c)
        self.assertEqual(sent_tips, [])

        refresh_lock.allow_waiters_to_acquire.set()
        for poll_thread in (stale_probe, tip_b_poll, tip_c_poll):
            poll_thread.join(5)
        server.shutdown_tip_refresh_executor()

        self.assertTrue(
            all(not poll_thread.is_alive() for poll_thread in (stale_probe, tip_b_poll, tip_c_poll))
        )
        self.assertEqual(errors["stale"] + errors["b"] + errors["c"], [])
        self.assertEqual(
            sorted(results["stale"] + results["b"] + results["c"]),
            [0, 0, 1],
        )
        self.assertEqual(sent_tips, [tip_c])
        self.assertFalse(server.tip_refresh_is_pending())

    def test_client_lock_waiter_cancels_before_lock_owner_releases(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        observed_lock = ObservedRLock()
        state.job_update_lock = observed_lock  # type: ignore[assignment]
        observed_lock.acquire()
        observed_lock.acquire_attempted.clear()
        notifications: list[dict[str, object]] = []
        state.send = notifications.append  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        poll_done = threading.Event()
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                poll_done.set()

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(observed_lock.acquire_attempted.wait(5))
            self.advance_tip(server, rpc, "33" * 32, height=11)
            self.assertTrue(poll_done.wait(5))
            self.assertEqual(notifications, [])
            self.assertIsNone(state.active_job)
        finally:
            observed_lock.release()
            thread.join(5)

        try:
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], TemplateRefreshBlocked)
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(
            sum(payload["method"] == "mining.notify" for payload in notifications),
            1,
        )
        self.assertEqual(state.active_job.template["previousblockhash"], rpc.tip)
        self.assertGreaterEqual(
            server.tip_refresh_cancellation_counts["client_lock"],
            1,
        )

    def test_payout_gate_waiter_cancels_while_mutation_remains_held(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        notifications: list[dict[str, object]] = []
        state.send = notifications.append  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        gate = ObservedPayoutGate(server._payout_state_delivery_gate)
        server._payout_state_delivery_gate = gate  # type: ignore[assignment]
        poll_done = threading.Event()
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                poll_done.set()

        thread = threading.Thread(target=poll)
        with gate.mutation():  # type: ignore[attr-defined]
            thread.start()
            self.assertTrue(gate.delivery_wait_started.wait(5))
            self.advance_tip(server, rpc, "44" * 32, height=11)
            self.assertTrue(poll_done.wait(5))
            self.assertEqual(notifications, [])
            self.assertIsNone(state.active_job)
        thread.join(5)

        try:
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], TemplateRefreshBlocked)
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(
            sum(payload["method"] == "mining.notify" for payload in notifications),
            1,
        )
        self.assertEqual(state.active_job.template["previousblockhash"], rpc.tip)
        self.assertGreaterEqual(
            server.tip_refresh_cancellation_counts["payout_gate"],
            1,
        )

    def test_obsolete_backlog_never_starts_the_unsubmitted_fleet(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 2
        clients = [client(index + 1) for index in range(8)]
        observed_locks = [ObservedRLock(), ObservedRLock()]
        for state, observed_lock in zip(clients, observed_locks):
            state.job_update_lock = observed_lock  # type: ignore[assignment]
            observed_lock.acquire()
            observed_lock.acquire_attempted.clear()
        notifications: set[int] = set()
        for state in clients:
            state.send = (  # type: ignore[method-assign]
                lambda payload, connection_id=state.connection_id: (
                    notifications.add(connection_id)
                    if payload["method"] == "mining.notify"
                    else None
                )
            )
        server.clients = clients  # type: ignore[assignment]
        started_clients: list[int] = []
        started_lock = threading.Lock()
        original_send_prepared_job = server.send_prepared_job

        def tracked_send_prepared_job(state: object, *args: object) -> object:
            with started_lock:
                started_clients.append(state.connection_id)  # type: ignore[attr-defined]
            return original_send_prepared_job(state, *args)  # type: ignore[arg-type]

        server.send_prepared_job = tracked_send_prepared_job  # type: ignore[method-assign]
        poll_done = threading.Event()
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                poll_done.set()

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(observed_locks[0].acquire_attempted.wait(5))
            self.assertTrue(observed_locks[1].acquire_attempted.wait(5))
            self.advance_tip(server, rpc, "55" * 32, height=11)
            self.assertTrue(poll_done.wait(5))
            with started_lock:
                obsolete_started = list(started_clients)
            self.assertEqual(set(obsolete_started), {1, 2})
            self.assertEqual(notifications, set())
        finally:
            for observed_lock in observed_locks:
                observed_lock.release()
            thread.join(5)

        try:
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], TemplateRefreshBlocked)
            self.assertEqual(server.poll_qbit_tip_template_once(), len(clients))
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(notifications, {state.connection_id for state in clients})
        self.assertGreaterEqual(
            server.tip_refresh_cancellation_counts["client_lock"],
            2,
        )

    def test_obsolete_executor_queue_entry_is_canceled_and_counted(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 2
        server._ensure_tip_refresh_state()

        class ObservedExecutor:
            def __init__(self) -> None:
                self.delegate = ThreadPoolExecutor(max_workers=1)
                self.submission_count = 0
                self.second_submitted = threading.Event()

            def submit(self, function: object, *args: object) -> Future[object]:
                self.submission_count += 1
                future = self.delegate.submit(function, *args)  # type: ignore[arg-type]
                if self.submission_count == 2:
                    self.second_submitted.set()
                return future

            def shutdown(self, **kwargs: object) -> None:
                self.delegate.shutdown(**kwargs)  # type: ignore[arg-type]

        observed_executor = ObservedExecutor()
        server._tip_refresh_executor = observed_executor  # type: ignore[assignment]
        admitted = client(1)
        queued = client(2)
        server.clients = [admitted, queued]  # type: ignore[assignment]
        admitted_send_started = threading.Event()
        release_admitted_send = threading.Event()
        queued_canceled = threading.Event()
        queued_notifications: list[dict[str, object]] = []

        def block_notify(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                admitted_send_started.set()
                self.assertTrue(release_admitted_send.wait(5))

        admitted.send = block_notify  # type: ignore[method-assign]
        queued.send = queued_notifications.append  # type: ignore[method-assign]
        original_record_cancellation = server._record_tip_refresh_cancellation

        def record_cancellation(stage: str) -> None:
            original_record_cancellation(stage)
            if stage == "executor_queue":
                queued_canceled.set()

        server._record_tip_refresh_cancellation = record_cancellation  # type: ignore[method-assign]
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(admitted_send_started.wait(5))
            self.assertTrue(observed_executor.second_submitted.wait(5))
            self.advance_tip(server, rpc, "59" * 32, height=11)
            self.assertTrue(queued_canceled.wait(5))
            self.assertEqual(queued_notifications, [])
            self.assertIsNone(queued.active_job)
        finally:
            release_admitted_send.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertEqual(
            server.tip_refresh_cancellation_counts["executor_queue"],
            1,
        )

    def test_latest_pending_tip_wins_across_a_b_c_observations(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        observed_lock = ObservedRLock()
        state.job_update_lock = observed_lock  # type: ignore[assignment]
        observed_lock.acquire()
        observed_lock.acquire_attempted.clear()
        notifications: list[dict[str, object]] = []
        state.send = notifications.append  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        poll_done = threading.Event()
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                poll_done.set()

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(observed_lock.acquire_attempted.wait(5))
            self.advance_tip(server, rpc, "66" * 32, height=11)
            self.advance_tip(server, rpc, "77" * 32, height=12)
            newest_pending_token = server._tip_refresh_pending_token
            self.assertTrue(poll_done.wait(5))
            self.assertTrue(server.tip_refresh_is_pending())
            self.assertEqual(server._tip_refresh_pending_token, newest_pending_token)
            self.assertEqual(notifications, [])
        finally:
            observed_lock.release()
            thread.join(5)

        try:
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], TemplateRefreshBlocked)
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertFalse(server.tip_refresh_is_pending())
        self.assertEqual(
            sum(payload["method"] == "mining.notify" for payload in notifications),
            1,
        )
        self.assertEqual(state.active_job.template["previousblockhash"], "77" * 32)

    def test_admitted_old_send_finishes_before_new_tip_delivery(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 2
        admitted = client(1)
        waiting = client(2)
        waiting_lock = ObservedRLock()
        waiting.job_update_lock = waiting_lock  # type: ignore[assignment]
        waiting_lock.acquire()
        waiting_lock.acquire_attempted.clear()
        server.clients = [admitted, waiting]  # type: ignore[assignment]
        admitted_send_started = threading.Event()
        release_admitted_send = threading.Event()
        waiting_worker_finished = threading.Event()
        notifications: dict[int, list[str]] = {1: [], 2: []}

        def block_old_notify(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                notifications[1].append("A")
                admitted_send_started.set()
                self.assertTrue(release_admitted_send.wait(5))

        admitted.send = block_old_notify  # type: ignore[method-assign]
        waiting.send = (  # type: ignore[method-assign]
            lambda payload: notifications[2].append("A")
            if payload["method"] == "mining.notify"
            else None
        )
        original_send_prepared_job = server.send_prepared_job

        def tracked_send_prepared_job(state: object, *args: object) -> object:
            try:
                return original_send_prepared_job(state, *args)  # type: ignore[arg-type]
            finally:
                if state is waiting:
                    waiting_worker_finished.set()

        server.send_prepared_job = tracked_send_prepared_job  # type: ignore[method-assign]
        poll_done = threading.Event()
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                poll_done.set()

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(admitted_send_started.wait(5))
            self.assertTrue(waiting_lock.acquire_attempted.wait(5))
            self.advance_tip(server, rpc, "88" * 32, height=11)
            self.assertTrue(waiting_worker_finished.wait(5))
            self.assertFalse(poll_done.is_set())
            self.assertEqual(notifications[2], [])
        finally:
            release_admitted_send.set()
            thread.join(5)
            waiting_lock.release()

        admitted.send = (  # type: ignore[method-assign]
            lambda payload: notifications[1].append("B")
            if payload["method"] == "mining.notify"
            else None
        )
        waiting.send = (  # type: ignore[method-assign]
            lambda payload: notifications[2].append("B")
            if payload["method"] == "mining.notify"
            else None
        )
        try:
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], TemplateRefreshBlocked)
            self.assertEqual(server.poll_qbit_tip_template_once(), 2)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(notifications[1], ["A", "B"])
        self.assertEqual(notifications[2], ["B"])

    def test_prepared_wait_phases_account_for_total_elapsed(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        snapshot = server.fetch_qbit_tip_template_snapshot()
        sequence = server._reserve_tip_observation_sequence()
        self.assertTrue(
            server.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=sequence,
                publish_refresh_observation=True,
            )
        )
        with server.lock:
            server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        token = server._validate_prepared_tip_refresh(bundle, snapshot, sequence)

        class ManualClock:
            def __init__(self) -> None:
                self.now = 10.0

            def monotonic(self) -> float:
                return self.now

            def advance(self, seconds: float) -> None:
                self.now += seconds

        clock = ManualClock()

        class AdvancingLock:
            def acquire(self, *, timeout: float) -> bool:
                self.timeout = timeout
                clock.advance(2.0)
                return True

            def release(self) -> None:
                return None

        class AdvancingGate:
            class Admission:
                def __bool__(self) -> bool:
                    return True

                @staticmethod
                def mark_delivered() -> None:
                    pass

            @contextmanager
            def delivery_cancelable(
                self,
                _cancelled: object,
                **_kwargs: object,
            ) -> object:
                clock.advance(4.0)
                yield self.Admission()

        state.job_update_lock = AdvancingLock()  # type: ignore[assignment]
        server._payout_state_delivery_gate = AdvancingGate()  # type: ignore[assignment]
        original_stamp = server.stamp_job_for_client

        def advancing_stamp(*args: object, **kwargs: object) -> object:
            clock.advance(5.0)
            return original_stamp(*args, **kwargs)  # type: ignore[arg-type]

        server.stamp_job_for_client = advancing_stamp  # type: ignore[method-assign]
        state.send = lambda _payload: clock.advance(3.0)  # type: ignore[method-assign]

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            clock.monotonic,
        ):
            result = server.send_prepared_job(
                state,
                bundle,
                snapshot,
                token,
                state.connection_id,
                None,
                _FanoutCancellation(),
                submitted_monotonic=7.0,
            )

        self.assertEqual(result.result, "sent")
        expected_phases = {
            "executor_queue": 3.0,
            "client_lock": 2.0,
            "payout_gate": 4.0,
            "stamp": 5.0,
            "socket_send": 6.0,
        }
        for phase, expected in expected_phases.items():
            self.assertAlmostEqual(
                server.job_build_phase_seconds[phase],
                expected,
            )
        self.assertAlmostEqual(server.job_build_seconds_sum, 20.0)
        self.assertAlmostEqual(sum(expected_phases.values()), 20.0)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_tip_refresh_cancellations_total{stage="executor_queue"} 0',
            metrics,
        )
        self.assertIn(
            'qbit_prism_job_build_phase_seconds_total{phase="payout_gate"} 4.000000',
            metrics,
        )

    def test_routine_same_tip_observation_keeps_active_fanout_valid(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        server.tip_refresh_max_workers = 1
        first, second, third = client(1), client(2), client(3)
        clients = [first, second, third]
        server.clients = clients  # type: ignore[assignment]
        first_send_started = threading.Event()
        release_first_send = threading.Event()
        notifications: set[int] = set()

        def record_send(
            payload: dict[str, object],
            *,
            connection_id: int,
            block: bool = False,
        ) -> None:
            if payload["method"] != "mining.notify":
                return
            notifications.add(connection_id)
            if block:
                first_send_started.set()
                self.assertTrue(release_first_send.wait(5))

        first.send = lambda payload: record_send(  # type: ignore[method-assign]
            payload,
            connection_id=first.connection_id,
            block=True,
        )
        second.send = lambda payload: record_send(  # type: ignore[method-assign]
            payload,
            connection_id=second.connection_id,
        )
        third.send = lambda payload: record_send(  # type: ignore[method-assign]
            payload,
            connection_id=third.connection_id,
        )
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        results: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        poll_thread = threading.Thread(target=poll)
        poll_thread.start()
        try:
            self.assertTrue(first_send_started.wait(5))
            active_sequence = server.current_tip_observation_sequence
            self.assertTrue(server.observe_tip_first_seen(rpc.tip))
            self.assertEqual(
                server.current_tip_observation_sequence,
                active_sequence,
            )
        finally:
            release_first_send.set()
            poll_thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(poll_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [3])
        self.assertEqual(notifications, {1, 2, 3})
        self.assertFalse(server._tip_refresh_retry.is_set())

    def test_same_tip_contention_probe_keeps_active_fanout_valid(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first, second = client(1), client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        server._ensure_tip_refresh_state()
        refresh_lock = _ControlledTipRefreshLock()
        server._tip_refresh_lock = refresh_lock  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: True
        )
        self.assertTrue(server.observe_tip_first_seen(rpc.tip))
        server._tip_refresh_retry.clear()

        first_send_started = threading.Event()
        release_first_send = threading.Event()
        contention_probe_finished = threading.Event()
        notifications: list[int] = []

        def record_send(
            payload: dict[str, object],
            *,
            connection_id: int,
            block_first: bool = False,
        ) -> None:
            if payload["method"] != "mining.notify":
                return
            notifications.append(connection_id)
            if block_first:
                first_send_started.set()
                self.assertTrue(release_first_send.wait(5))

        first.send = lambda payload: record_send(  # type: ignore[method-assign]
            payload,
            connection_id=1,
            block_first=True,
        )
        second.send = lambda payload: record_send(  # type: ignore[method-assign]
            payload,
            connection_id=2,
        )
        original_contention_probe = server._probe_tip_while_refresh_waiting

        def record_contention_probe() -> None:
            original_contention_probe()
            contention_probe_finished.set()

        server._probe_tip_while_refresh_waiting = record_contention_probe  # type: ignore[method-assign]
        results: dict[str, list[int]] = {"active": [], "waiting": []}
        errors: dict[str, list[BaseException]] = {"active": [], "waiting": []}

        def poll(label: str) -> None:
            try:
                results[label].append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface thread errors
                errors[label].append(exc)

        active_poll = threading.Thread(target=poll, args=("active",))
        waiting_poll = threading.Thread(target=poll, args=("waiting",))
        active_poll.start()
        try:
            self.assertTrue(first_send_started.wait(5))
            with server.lock:
                active = server._active_tip_refresh
                active_sequence = server.current_tip_observation_sequence
            self.assertIsNotNone(active)
            assert active is not None

            waiting_poll.start()
            refresh_lock.next_probe_gate().set()
            self.assertTrue(contention_probe_finished.wait(5))
            self.assertFalse(active[1].is_set())
            self.assertEqual(
                server.current_tip_observation_sequence,
                active_sequence,
            )
            self.assertFalse(server.tip_refresh_is_pending())
        finally:
            release_first_send.set()
            active_poll.join(5)

        self.assertFalse(active_poll.is_alive())
        self.assertEqual(errors["active"], [])
        self.assertEqual(results["active"], [2])

        refresh_lock.allow_waiters_to_acquire.set()
        waiting_poll.join(5)
        server.shutdown_tip_refresh_executor()

        self.assertFalse(waiting_poll.is_alive())
        self.assertEqual(errors["waiting"], [])
        self.assertEqual(results["waiting"], [0])
        self.assertEqual(notifications, [1, 2])
        self.assertFalse(server.tip_refresh_is_pending())

    def test_post_fanout_payout_change_schedules_immediate_retry(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        original_rpc_call = rpc.call

        def advance_during_post_fanout_tip_check(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            result = original_rpc_call(method, params)
            if method == "getbestblockhash" and rpc.count(method) == 4:
                server._advance_payout_state_generation()
            return result

        rpc.call = advance_during_post_fanout_tip_check  # type: ignore[method-assign]

        try:
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertIsNotNone(state.active_job)
        self.assertEqual(state.active_job.payout_state_generation, 0)
        self.assertEqual(server._payout_state_generation, 1)
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_payout_mutation_waits_for_prepared_network_delivery(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        send_started = threading.Event()
        release_send = threading.Event()
        mutation_started = threading.Event()
        mutation_completed = threading.Event()
        poll_results: list[int] = []
        poll_errors: list[BaseException] = []

        def block_notify(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                send_started.set()
                self.assertTrue(release_send.wait(5))

        state.send = block_notify  # type: ignore[method-assign]

        def poll() -> None:
            try:
                poll_results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - asserted below
                poll_errors.append(exc)

        def mutate() -> None:
            mutation_started.set()
            server._advance_payout_state_generation()
            mutation_completed.set()

        poll_thread = threading.Thread(target=poll)
        mutation_thread = threading.Thread(target=mutate)
        poll_thread.start()
        try:
            self.assertTrue(send_started.wait(5))
            mutation_thread.start()
            self.assertTrue(mutation_started.wait(5))
            self.assertFalse(mutation_completed.wait(0.1))
            self.assertEqual(server._payout_state_generation, 0)
            with server._payout_state_delivery_gate._condition:
                self.assertTrue(
                    server._payout_state_delivery_gate._publisher_waiting
                )
                self.assertIsNone(
                    server._payout_state_delivery_gate._mutation_owner
                )
            self.assertTrue(
                server._payout_state_prepare_lock.acquire(timeout=1)
            )
            server._payout_state_prepare_lock.release()
        finally:
            release_send.set()
            mutation_thread.join(5)
            poll_thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(mutation_thread.is_alive())
        self.assertFalse(poll_thread.is_alive())
        self.assertTrue(mutation_completed.is_set())
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(len(poll_results) + len(poll_errors), 1)
        if poll_errors:
            self.assertIsInstance(poll_errors[0], TemplateRefreshBlocked)
        else:
            self.assertEqual(poll_results, [1])
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_prepared_disconnection_after_admission_releases_cancellation_gate(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        snapshot = server.fetch_qbit_tip_template_snapshot()
        sequence = server._reserve_tip_observation_sequence()
        self.assertTrue(
            server.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=sequence,
                publish_refresh_observation=True,
            )
        )
        with server.lock:
            server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        token = server._validate_prepared_tip_refresh(
            bundle,
            snapshot,
            sequence,
        )
        cancellation = _FanoutCancellation()
        original_gate = server._payout_state_delivery_gate

        class RemoveClientAtAdmission:
            @contextmanager
            def delivery(self) -> object:
                with original_gate.delivery():
                    server.clients = []  # type: ignore[assignment]
                    yield

            @contextmanager
            def delivery_cancelable(
                self,
                cancelled: object,
                **kwargs: object,
            ) -> object:
                with original_gate.delivery_cancelable(cancelled, **kwargs) as admitted:
                    if admitted:
                        server.clients = []  # type: ignore[assignment]
                    yield admitted

            def mutation(self) -> object:
                return original_gate.mutation()

        server._payout_state_delivery_gate = RemoveClientAtAdmission()

        result = server.send_prepared_job(
            state,
            bundle,
            snapshot,
            token,
            state.connection_id,
            None,
            cancellation,
        )

        self.assertEqual(result.result, "disconnected")
        self.assertEqual(cancellation._active_deliveries, 0)
        cancellation.set()

    def test_publication_block_fences_escaped_prepared_bundle(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        snapshot = server.fetch_qbit_tip_template_snapshot()
        sequence = server._reserve_tip_observation_sequence()
        self.assertTrue(
            server.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=sequence,
                publish_refresh_observation=True,
            )
        )
        with server.lock:
            server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        token = server._validate_prepared_tip_refresh(
            bundle,
            snapshot,
            sequence,
        )
        cancellation = _FanoutCancellation()

        server._reserve_payout_state_source("payout_only")
        server._block_payout_state_publication()

        with server.lock:
            self.assertTrue(
                server._tip_refresh_token_current_locked(token, bundle, snapshot)
            )
        self.assertFalse(cancellation.is_set())
        result = server.send_prepared_job(
            state,
            bundle,
            snapshot,
            token,
            state.connection_id,
            None,
            cancellation,
        )

        self.assertEqual(result.result, "skipped")
        self.assertEqual(sent, [])
        self.assertIsNone(state.active_job)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)

        published = server._publish_payout_state_candidate(
            server._current_payout_state_candidate()
        )
        self.assertEqual(published, 1)
        with server._payout_state_delivery_gate.delivery_cancelable(
            lambda: False,
            generation=1,
            priority=True,
        ) as admission:
            self.assertTrue(admission)

    def test_sequential_payout_change_schedules_immediate_retry(self) -> None:
        server, _rpc = coordinator(ledger=FakeLedger(miners=["solo"]))
        install_fake_bundle_builder(server)
        first, second = client(1), client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        first_notifications = 0

        def record_first_send(payload: dict[str, object]) -> None:
            nonlocal first_notifications
            if payload["method"] == "mining.notify":
                first_notifications += 1

        first.send = record_first_send  # type: ignore[method-assign]
        second.send = lambda _payload: None  # type: ignore[method-assign]
        original_maybe_send_job = server.maybe_send_job
        send_calls = 0

        def advance_between_clients(*args: object, **kwargs: object) -> bool:
            nonlocal send_calls
            sent = original_maybe_send_job(*args, **kwargs)  # type: ignore[arg-type]
            send_calls += 1
            if send_calls == 1:
                server._advance_payout_state_generation()
            return sent

        server.maybe_send_job = advance_between_clients  # type: ignore[method-assign]

        with self.assertRaises(TemplateRefreshBlocked):
            server.poll_qbit_tip_template_once()

        self.assertEqual(first_notifications, 1)
        self.assertIsNotNone(first.active_job)
        self.assertIsNotNone(second.active_job)
        self.assertEqual(first.active_job.payout_state_generation, 0)
        self.assertEqual(second.active_job.payout_state_generation, 1)
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_payout_change_after_client_selection_retries_full_same_tip_set(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        first = client(1)
        second = client(2)
        first_sent: list[dict[str, object]] = []
        second_sent: list[dict[str, object]] = []
        first.send = first_sent.append  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        server.clients = [second]  # type: ignore[assignment]

        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            self.assertIsNotNone(second.active_job)
            self.assertEqual(second.active_job.payout_state_generation, 0)

            server.clients = [first, second]  # type: ignore[assignment]
            original_prepare = server.prepare_tip_refresh_bundle
            advanced = False

            def advance_before_prepare(*args: object, **kwargs: object) -> object:
                nonlocal advanced
                if not advanced:
                    advanced = True
                    server._advance_payout_state_generation()
                return original_prepare(*args, **kwargs)  # type: ignore[arg-type]

            server.prepare_tip_refresh_bundle = advance_before_prepare  # type: ignore[method-assign]

            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()

            self.assertIsNone(first.active_job)
            self.assertEqual(second.active_job.payout_state_generation, 0)
            self.assertTrue(server._tip_refresh_retry.is_set())

            self.assertEqual(server.poll_qbit_tip_template_once(), 2)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertIsNotNone(first.active_job)
        self.assertIsNotNone(second.active_job)
        self.assertEqual(first.active_job.payout_state_generation, 1)
        self.assertEqual(second.active_job.payout_state_generation, 1)
        self.assertEqual(
            sum(payload["method"] == "mining.notify" for payload in first_sent),
            1,
        )
        self.assertEqual(
            sum(payload["method"] == "mining.notify" for payload in second_sent),
            2,
        )

    def test_executor_submission_failure_cancels_and_drains_started_work(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        first, second = client(1), client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        first_send_started = threading.Event()
        release_first_send = threading.Event()
        first_notifications: list[dict[str, object]] = []
        later_notifications: list[dict[str, object]] = []

        def blocking_send(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            first_notifications.append(payload)
            first_send_started.set()
            self.assertTrue(release_first_send.wait(5))

        first.send = blocking_send  # type: ignore[method-assign]
        second.send = later_notifications.append  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        backing_executor = ThreadPoolExecutor(max_workers=1)

        class RejectSecondSubmission:
            def __init__(self) -> None:
                self.submission_count = 0

            def submit(self, function: object, *args: object) -> Future[object]:
                self.submission_count += 1
                if self.submission_count == 1:
                    future = backing_executor.submit(function, *args)  # type: ignore[arg-type]
                    if not first_send_started.wait(5):
                        raise AssertionError("first fanout task did not start")
                    return future
                raise RuntimeError("executor rejected queued fanout task")

        server.tip_refresh_executor = lambda: RejectSecondSubmission()  # type: ignore[method-assign]
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        poll_thread = threading.Thread(target=poll)
        poll_thread.start()
        self.assertTrue(first_send_started.wait(5))
        release_first_send.set()
        poll_thread.join(5)
        backing_executor.shutdown(wait=True, cancel_futures=True)

        self.assertFalse(poll_thread.is_alive())
        self.assertEqual(len(first_notifications), 1)
        self.assertEqual(later_notifications, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertIsNone(server._active_tip_refresh)
        self.assertTrue(server._tip_refresh_retry.is_set())


if __name__ == "__main__":
    unittest.main()
