#!/usr/bin/env python3
"""Once-per-refresh chain validation and prepared-fanout cancellation tests."""

from __future__ import annotations

import threading
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import FrozenInstanceError

from lab.prism.prism_coordinator import (
    TemplateRefreshBlocked,
    TipRefreshValidationToken,
    _FanoutCancellation,
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


class TipRefreshValidationTests(unittest.TestCase):
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
        self.assertEqual(state.active_job.bundle["prior_balances"], [])
        self.assertIsNot(state.active_job.bundle, stale_bundle.bundle)
        self.assertIn("signed_coinbase_manifest", state.active_job.bundle)
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

    def test_prevalidation_rpc_failure_schedules_immediate_retry(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        snapshot = server.fetch_qbit_tip_template_snapshot()
        bundle = server.prepare_tip_refresh_bundle(snapshot, [state])
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
        bundle = server.prepare_tip_refresh_bundle(snapshot, [state])

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

    def test_prepared_skip_after_admission_releases_cancellation_gate(self) -> None:
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
        bundle = server.prepare_tip_refresh_bundle(snapshot, [state])
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

        self.assertEqual(result.result, "skipped")
        self.assertEqual(cancellation._active_deliveries, 0)
        cancellation.set()

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
