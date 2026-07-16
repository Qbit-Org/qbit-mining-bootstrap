#!/usr/bin/env python3
"""Once-per-refresh chain validation and prepared-fanout cancellation tests."""

from __future__ import annotations

import threading
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import FrozenInstanceError

from lab.prism.prism_coordinator import (
    TemplateRefreshBlocked,
    TipRefreshValidationToken,
)
from tests.test_prism_coordinator_job_cache import (
    FakeLedger,
    client,
    coordinator,
    install_fake_bundle_builder,
)


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

    def test_reconciliation_runs_after_bundle_and_immediately_before_fanout(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        events: list[str] = []
        original_prepare = server.prepare_tip_refresh_bundle

        def prepare(bundle_snapshot: object, clients: object) -> object:
            result = original_prepare(bundle_snapshot, clients)  # type: ignore[arg-type]
            events.append("bundle")
            return result

        def reconcile(_tip_hash: str) -> bool:
            events.append("reconcile")
            return True

        def chain_view_untrusted() -> bool:
            events.append("chain_view")
            return False

        def record_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                events.append("fanout")

        server.prepare_tip_refresh_bundle = prepare  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = reconcile  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = chain_view_untrusted  # type: ignore[method-assign]
        state.send = record_send  # type: ignore[method-assign]

        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(events, ["bundle", "reconcile", "chain_view", "fanout"])

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
        self.assertEqual(chain_view_checks, 1)
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

    def test_reconciliation_failure_before_fanout_sends_zero_jobs(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
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
