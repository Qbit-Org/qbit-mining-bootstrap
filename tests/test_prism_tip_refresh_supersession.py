#!/usr/bin/env python3
"""Latest-tip logical supersession regressions for PRISM job preparation."""

from __future__ import annotations

import threading
import time
import unittest
from unittest.mock import patch

from lab.prism.prism_coordinator import (
    JobBuildSuperseded,
    canonical_json_text,
)
from tests.test_prism_coordinator_job_cache import (
    FakeLedger,
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
)


class TipRefreshLogicalSupersessionTests(unittest.TestCase):
    def test_blocked_b_retires_while_d_starts_and_c_is_coalesced(self) -> None:
        tip_a = "11" * 32
        tip_b = "22" * 32
        tip_c = "33" * 32
        tip_d = "44" * 32
        server, rpc = coordinator(template=base_template(prevhash=tip_a))
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]

        release_b_physical = threading.Event()
        b_physical_waiting = threading.Event()
        b_physical_returning = threading.Event()
        d_physical_started = threading.Event()
        allow_b_owner_release = threading.Event()
        b_owner_observed_supersession = threading.Event()
        build_tips: list[str] = []
        active_physical = 0
        max_active_physical = 0
        physical_lock = threading.Lock()
        b_poll_errors: list[BaseException] = []
        b_poll_thread: threading.Thread
        b_cleanup_done = threading.Event()

        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            self.assertEqual(server.current_tip_first_seen[0], tip_a)

            original_build = server.build_shared_job_bundle

            def controlled_build(
                artifacts: object,
                worker: object = None,
                **kwargs: object,
            ) -> object:
                nonlocal active_physical, max_active_physical
                tip_hash = str(artifacts.previousblockhash)  # type: ignore[union-attr]
                with physical_lock:
                    build_tips.append(tip_hash)
                    active_physical += 1
                    max_active_physical = max(
                        max_active_physical,
                        active_physical,
                    )
                try:
                    if tip_hash == tip_d:
                        d_physical_started.set()
                    result = original_build(
                        artifacts,  # type: ignore[arg-type]
                        worker,  # type: ignore[arg-type]
                        **kwargs,
                    )
                    if tip_hash == tip_b:
                        b_physical_waiting.set()
                        if not release_b_physical.wait(5):
                            raise AssertionError("test did not release retired B build")
                        b_physical_returning.set()
                    return result
                finally:
                    with physical_lock:
                        active_physical -= 1

            server.build_shared_job_bundle = controlled_build  # type: ignore[method-assign]
            original_note_failed = server._note_tip_refresh_attempt_failed

            def hold_b_logical_owner(tip_hash: str | None) -> None:
                if threading.current_thread() is b_poll_thread:
                    b_owner_observed_supersession.set()
                    if not allow_b_owner_release.wait(5):
                        raise AssertionError("test did not release superseded B owner")
                original_note_failed(tip_hash)

            server._note_tip_refresh_attempt_failed = hold_b_logical_owner  # type: ignore[method-assign]

            def poll_b() -> None:
                try:
                    server.poll_qbit_tip_template_once()
                except BaseException as exc:  # noqa: BLE001 - thread handoff
                    b_poll_errors.append(exc)

            rpc.tip = tip_b
            rpc.template = base_template(height=11, prevhash=tip_b)
            b_poll_thread = threading.Thread(target=poll_b)
            b_poll_thread.start()
            self.assertTrue(b_physical_waiting.wait(5))

            rpc.tip = tip_c
            rpc.template = base_template(height=12, prevhash=tip_c)
            self.assertEqual(server.poll_qbit_tip_template_once(), 0)
            self.assertTrue(b_owner_observed_supersession.wait(5))

            with server._job_build_scheduler_lock:
                b_flight = server._job_build_retiring
                self.assertIsNotNone(b_flight)
                assert b_flight is not None and b_flight.future is not None
                self.assertEqual(
                    b_flight.request.key.previous_block_hash,
                    tip_b,
                )
                self.assertTrue(b_flight.request.promise.done())
                self.assertIsInstance(
                    b_flight.request.promise.exception(),
                    JobBuildSuperseded,
                )
                self.assertFalse(b_flight.future.done())
                b_flight.future.add_done_callback(
                    lambda _future: b_cleanup_done.set()
                )

            allow_b_owner_release.set()
            b_poll_thread.join(5)
            self.assertFalse(b_poll_thread.is_alive())
            self.assertEqual(len(b_poll_errors), 1)
            self.assertIsInstance(b_poll_errors[0], JobBuildSuperseded)
            self.assertFalse(b_physical_returning.is_set())

            rpc.tip = tip_d
            rpc.template = base_template(height=13, prevhash=tip_d)
            d_results: list[int] = []
            d_errors: list[BaseException] = []

            def poll_d() -> None:
                try:
                    d_results.append(server.poll_qbit_tip_template_once())
                except BaseException as exc:  # noqa: BLE001 - thread handoff
                    d_errors.append(exc)

            d_poll_thread = threading.Thread(target=poll_d)
            d_poll_thread.start()
            self.assertTrue(d_physical_started.wait(5))
            self.assertFalse(b_physical_returning.is_set())
            d_poll_thread.join(5)

            self.assertFalse(d_poll_thread.is_alive())
            self.assertEqual(d_errors, [])
            self.assertEqual(d_results, [1])
            self.assertEqual(server.current_tip_first_seen[0], tip_d)
            assert state.active_job is not None
            self.assertEqual(
                str(state.active_job.template["previousblockhash"]),
                tip_d,
            )
            self.assertEqual(build_tips, [tip_b, tip_d])
            self.assertNotIn(tip_c, build_tips)
            self.assertEqual(max_active_physical, 2)
            self.assertFalse(
                any(
                    str(bundle.template["previousblockhash"]) == tip_b
                    for bundle in server._job_bundle_cache.values()
                )
            )

            release_b_physical.set()
            self.assertTrue(b_physical_returning.wait(5))
            self.assertTrue(b_cleanup_done.wait(5))
            self.assertIsInstance(
                b_flight.request.promise.exception(),
                JobBuildSuperseded,
            )
            self.assertFalse(
                any(
                    str(bundle.template["previousblockhash"]) == tip_b
                    for bundle in server._job_bundle_cache.values()
                )
            )
            with server._job_build_scheduler_lock:
                self.assertIsNone(server._job_build_active)
                self.assertIsNone(server._job_build_retiring)
                self.assertIsNone(server._job_build_pending)
        finally:
            allow_b_owner_release.set()
            release_b_physical.set()
            if "b_poll_thread" in locals():
                b_poll_thread.join(5)
            server.shutdown_tip_refresh_executor()
            server.shutdown_job_build_executor()
            server.shutdown_payout_artifact_executor()

    def test_repeated_supersession_keeps_two_workers_and_one_latest_pending(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        tips = [f"{index:064x}" for index in range(2, 10)]
        requests = []
        payout_generation = server._payout_state_generation
        for generation, tip_hash in enumerate(tips, start=1):
            artifacts = server._derive_template_artifacts(
                base_template(height=10 + generation, prevhash=tip_hash),
                generation=generation,
            )
            requests.append(
                server._new_job_build_request(
                    artifacts,
                    None,
                    mode="ready",
                    payout_state_generation=payout_generation,
                    cache_key=server._job_bundle_key(
                        artifacts,
                        mode="ready",
                        payout_state_generation=payout_generation,
                        worker=None,
                    ),
                    publication_critical=True,
                    request_source="tip_refresh",
                )
            )

        release_first = threading.Event()
        release_second = threading.Event()
        first_started = threading.Event()
        second_started = threading.Event()
        latest_started = threading.Event()
        physical_starts: list[str] = []
        physical_lock = threading.Lock()

        def controlled_execute(request: object) -> object:
            tip_hash = str(request.key.previous_block_hash)  # type: ignore[union-attr]
            with physical_lock:
                physical_starts.append(tip_hash)
            if tip_hash == tips[0]:
                first_started.set()
                if not release_first.wait(5):
                    raise AssertionError("test did not release first retired build")
            elif tip_hash == tips[1]:
                second_started.set()
                if not release_second.wait(5):
                    raise AssertionError("test did not release second retired build")
            elif tip_hash == tips[-1]:
                latest_started.set()
            return tip_hash

        server._execute_job_build_request = controlled_execute  # type: ignore[method-assign]
        promises = []
        try:
            promises.append(server._request_job_build(requests[0]))
            self.assertTrue(first_started.wait(5))
            promises.append(server._request_job_build(requests[1]))
            self.assertTrue(second_started.wait(5))
            for request in requests[2:]:
                promises.append(server._request_job_build(request))

            self.assertEqual(physical_starts, tips[:2])
            self.assertTrue(
                all(
                    promise.done()
                    and isinstance(promise.exception(), JobBuildSuperseded)
                    for promise in promises[:-1]
                )
            )
            self.assertFalse(promises[-1].done())
            with server._job_build_scheduler_lock:
                self.assertIsNotNone(server._job_build_active)
                self.assertIsNotNone(server._job_build_retiring)
                self.assertIs(server._job_build_pending, requests[-1])
                self.assertTrue(server._job_build_active.request.cancellation.is_set())
                self.assertTrue(server._job_build_retiring.request.cancellation.is_set())

            release_second.set()
            self.assertTrue(latest_started.wait(5))
            self.assertEqual(promises[-1].result(timeout=5), tips[-1])
            self.assertEqual(physical_starts, [tips[0], tips[1], tips[-1]])
            self.assertFalse(release_first.is_set())

            release_first.set()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with server._job_build_scheduler_lock:
                    if (
                        server._job_build_active is None
                        and server._job_build_retiring is None
                        and server._job_build_pending is None
                    ):
                        break
                time.sleep(0.01)
            else:
                self.fail("retired scheduler state did not drain")

            self.assertTrue(
                all(
                    isinstance(promise.exception(), JobBuildSuperseded)
                    for promise in promises[:-1]
                )
            )
            self.assertEqual(server.job_build_scheduler_counts["starts"], 3)
            self.assertEqual(server.job_build_scheduler_counts["completions"], 3)
        finally:
            release_first.set()
            release_second.set()
            server.shutdown_job_build_executor()

    def test_supersession_wins_before_executor_result_can_complete_promise(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        artifacts = server._derive_template_artifacts(
            base_template(height=11, prevhash="22" * 32),
            generation=1,
        )
        payout_generation = server._payout_state_generation
        request = server._new_job_build_request(
            artifacts,
            None,
            mode="ready",
            payout_state_generation=payout_generation,
            cache_key=server._job_bundle_key(
                artifacts,
                mode="ready",
                payout_state_generation=payout_generation,
                worker=None,
            ),
            publication_critical=True,
            request_source="tip_refresh",
        )
        execute_started = threading.Event()
        release_execute = threading.Event()

        def controlled_execute(_request: object) -> object:
            execute_started.set()
            if not release_execute.wait(5):
                raise AssertionError("test did not release executor result")
            return "late obsolete result"

        server._execute_job_build_request = controlled_execute  # type: ignore[method-assign]
        try:
            promise = server._request_job_build(request)
            self.assertTrue(execute_started.wait(5))
            with server._job_build_scheduler_lock:
                flight = server._job_build_active
                self.assertIsNotNone(flight)
                assert flight is not None and flight.future is not None
                release_execute.set()
                deadline = time.monotonic() + 5
                while not flight.future.done() and time.monotonic() < deadline:
                    time.sleep(0.001)
                self.assertTrue(flight.future.done())

                # The executor callback has its result but cannot enter the
                # scheduler yet. Supersession owns the promise atomically.
                self.assertTrue(
                    server._cancel_job_build_flight_locked(
                        flight,
                        "chain tip superseded",
                    )
                )
                self.assertTrue(promise.done())
                self.assertIsInstance(promise.exception(), JobBuildSuperseded)

            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with server._job_build_scheduler_lock:
                    if (
                        server._job_build_active is None
                        and server._job_build_retiring is None
                    ):
                        break
                time.sleep(0.01)
            else:
                self.fail("late executor callback did not drain")
            self.assertIsInstance(promise.exception(), JobBuildSuperseded)
            self.assertEqual(server.job_build_scheduler_counts["obsolete_results"], 1)
        finally:
            release_execute.set()
            server.shutdown_job_build_executor()


class PublicationPreparationCancellationTests(unittest.TestCase):
    def test_tip_generation_cancels_after_blocked_payout_preparation(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        payout_entered = threading.Event()
        release_payout = threading.Event()

        class BlockingLedger(FakeLedger):
            def current_prior_balances(self) -> list[dict[str, object]]:
                payout_entered.set()
                if not release_payout.wait(5):
                    raise AssertionError("test did not release payout preparation")
                return []

        server, rpc = coordinator(
            ledger=BlockingLedger(),
            template=base_template(prevhash=old_tip),
        )
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.observe_tip_for_refresh(
            old_tip,
            observation_sequence=1,
            mark_pending=False,
        )
        errors: list[BaseException] = []

        def prepare() -> None:
            try:
                server.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                    publication_critical=True,
                    request_source="tip_refresh",
                )
            except BaseException as exc:  # noqa: BLE001 - thread handoff
                errors.append(exc)

        thread = threading.Thread(target=prepare)
        thread.start()
        try:
            self.assertTrue(payout_entered.wait(5))
            server.observe_tip_for_refresh(
                new_tip,
                observation_sequence=2,
                mark_pending=False,
            )
        finally:
            release_payout.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        self.assertEqual(server.job_build_scheduler_counts["starts"], 0)
        with server._job_build_scheduler_lock:
            self.assertEqual(server._job_build_priority_preparations, {})

    def test_tip_generation_cancels_after_payout_serialization(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        balances = [{"recipient_id": "miner-a", "balance_sats": 1}]
        serialization_entered = threading.Event()
        release_serialization = threading.Event()

        class BalanceLedger(FakeLedger):
            def current_prior_balances(self) -> list[dict[str, object]]:
                return balances

        server, rpc = coordinator(
            ledger=BalanceLedger(),
            template=base_template(prevhash=old_tip),
        )
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.observe_tip_for_refresh(
            old_tip,
            observation_sequence=1,
            mark_pending=False,
        )
        errors: list[BaseException] = []

        def blocking_json(value: object) -> str:
            if value is balances:
                serialization_entered.set()
                if not release_serialization.wait(5):
                    raise AssertionError("test did not release payout serialization")
            return canonical_json_text(value)

        def prepare() -> None:
            try:
                server.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                    publication_critical=True,
                    request_source="tip_refresh",
                )
            except BaseException as exc:  # noqa: BLE001 - thread handoff
                errors.append(exc)

        with patch(
            "lab.prism.prism_coordinator.canonical_json_text",
            side_effect=blocking_json,
        ):
            thread = threading.Thread(target=prepare)
            thread.start()
            try:
                self.assertTrue(serialization_entered.wait(5))
                server.observe_tip_for_refresh(
                    new_tip,
                    observation_sequence=2,
                    mark_pending=False,
                )
            finally:
                release_serialization.set()
                thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        self.assertEqual(server.job_build_scheduler_counts["starts"], 0)
        with server._job_build_scheduler_lock:
            self.assertEqual(server._job_build_priority_preparations, {})


if __name__ == "__main__":
    unittest.main()
