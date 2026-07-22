#!/usr/bin/env python3
"""Focused PRISM coordinator payout state tests."""
# ruff: noqa: F403, F405

from __future__ import annotations

import unittest
from tests.prism_coordinator_test_support import *


class JobBundleCacheTests(unittest.TestCase):
    def test_child_bundle_waits_for_pending_parent_preview_without_confirmed_read(
        self,
    ) -> None:
        class PreviewLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.current_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.current_balance_reads += 1
                return []

        class ObservedCondition(threading.Condition):
            def __init__(self) -> None:
                super().__init__()
                self.wait_entered = threading.Event()

            def wait(self, timeout: float | None = None) -> bool:
                self.wait_entered.set()
                return super().wait(timeout)

        ledger = PreviewLedger()
        server, rpc = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        assert artifacts is not None
        parent_hash = str(rpc.template["previousblockhash"])
        preview_condition = ObservedCondition()
        server._accepted_block_payout_preview_condition = preview_condition
        server.accepted_block_payout_preview_wait_seconds = 10
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        bundles: list[object] = []
        errors: list[BaseException] = []

        def build_child_bundle() -> None:
            try:
                bundles.append(server.shared_job_bundle(artifacts, worker()))
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        server._begin_accepted_block_payout_preview(parent_hash)
        thread = threading.Thread(target=build_child_bundle, daemon=True)
        thread.start()
        try:
            preview_wait_reached = preview_condition.wait_entered.wait(2)
            waiting_for_preview = thread.is_alive()
            confirmed_reads_while_pending = ledger.current_balance_reads
            builds_while_pending = int(recorded["calls"])
        finally:
            server._publish_accepted_block_payout_preview(parent_hash, preview)
            thread.join(5)
            server._clear_accepted_block_payout_preview(parent_hash)

        self.assertTrue(preview_wait_reached)
        self.assertTrue(waiting_for_preview)
        self.assertEqual(confirmed_reads_while_pending, 0)
        self.assertEqual(builds_while_pending, 0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(bundles), 1)
        bundle = bundles[0]
        self.assertEqual(bundle.prior_balances, preview)  # type: ignore[union-attr]
        self.assertEqual(recorded["last_kwargs"]["prior_balances"], preview)  # type: ignore[index]
        # Preview publication prepares an artifact from one confirmed snapshot;
        # the zero count captured above proves the waiting child did not read it.
        self.assertEqual(ledger.current_balance_reads, 1)
        self.assertEqual(  # type: ignore[union-attr]
            bundle.payout_state_generation,
            server._payout_state_generation,
        )

        self.assertEqual(server._prior_balances_for_job_parent(parent_hash), [])
        self.assertEqual(ledger.current_balance_reads, 2)
    def test_parent_preview_publication_is_idempotent_and_withdrawal_invalidates(
        self,
    ) -> None:
        server, _rpc = coordinator()
        parent_hash = "ab" * 32
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]

        server._begin_accepted_block_payout_preview(parent_hash)
        self.assertEqual(
            server._publish_accepted_block_payout_preview(parent_hash, preview),
            preview,
        )
        self.assertEqual(server._payout_state_generation, 1)

        self.assertEqual(
            server._publish_accepted_block_payout_preview(parent_hash, preview),
            preview,
        )
        self.assertEqual(server._payout_state_generation, 1)
        with self.assertRaisesRegex(RuntimeError, "changed during retry"):
            server._publish_accepted_block_payout_preview(
                parent_hash,
                [{**preview[0], "balance_sats": 26}],
            )

        server._clear_accepted_block_payout_preview(
            parent_hash,
            invalidate_published=True,
        )
        self.assertEqual(server._payout_state_generation, 2)
        self.assertEqual(server._accepted_block_payout_previews, {})
        self.assertEqual(
            server._invalidated_accepted_block_payout_previews,
            {parent_hash: None},
        )
        with self.assertRaisesRegex(TemplateRefreshBlocked, "was withdrawn"):
            server._prior_balances_for_job_parent(parent_hash)

        server._begin_accepted_block_payout_preview(parent_hash)
        self.assertEqual(server._invalidated_accepted_block_payout_previews, {})
        server._clear_accepted_block_payout_preview(parent_hash)
    def test_unpublished_parent_preview_retries_and_reopens_delivery(self) -> None:
        server, _rpc = coordinator()
        server.payout_reconcile_supersession_retries = 2
        parent_hash = "ac" * 32
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]

        server._begin_accepted_block_payout_preview(parent_hash)
        with patch.object(
            server,
            "_publish_payout_state_candidate",
            return_value=None,
        ) as publish_candidate:
            self.assertEqual(
                server._publish_accepted_block_payout_preview(parent_hash, preview),
                preview,
            )

        transition = server._accepted_block_payout_previews[parent_hash]
        self.assertEqual(publish_candidate.call_count, 3)
        self.assertIsNotNone(transition.preview)
        self.assertIsNone(transition.published_generation)
        self.assertEqual(server._payout_state_generation, 0)
        self.assertTrue(server._payout_state_publication_fenced())
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)

        self.assertEqual(
            server._publish_accepted_block_payout_preview(parent_hash, preview),
            preview,
        )

        transition = server._accepted_block_payout_previews[parent_hash]
        self.assertEqual(transition.published_generation, 1)
        self.assertEqual(server._payout_state_generation, 1)
        self.assertFalse(server._payout_state_publication_fenced())
        self.assertFalse(server._payout_state_delivery_gate._delivery_blocked)
    def test_withdrawn_landed_transition_blocks_active_descendant_fallback(
        self,
    ) -> None:
        class CountingLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.current_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.current_balance_reads += 1
                return []

        ledger = CountingLedger()
        server, rpc = coordinator(ledger=ledger)
        accepted_hash = "bc" * 32
        descendant_hash = "bd" * 32
        original_rpc_call = rpc.call

        def active_chain_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return accepted_hash
            return original_rpc_call(method, params)

        rpc.call = active_chain_call  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            accepted_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            accepted_hash,
            block_height=10,
        )
        server._clear_accepted_block_payout_preview(
            accepted_hash,
            invalidate_published=True,
        )

        with self.assertRaisesRegex(TemplateRefreshBlocked, "was withdrawn"):
            server._prior_balances_for_job_parent(
                descendant_hash,
                parent_height=11,
            )
        self.assertEqual(ledger.current_balance_reads, 0)
        self.assertTrue(server._tip_refresh_retry.is_set())
    def test_inactive_landed_ancestor_rejects_preview_patched_artifact(
        self,
    ) -> None:
        accepted_hash = "c0" * 32
        alternate_tip = "c1" * 32
        server, rpc = coordinator(
            template=base_template(height=12, prevhash=alternate_tip)
        )
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        self.assertTrue(server.pool_readiness_latched())
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        server._begin_accepted_block_payout_preview(
            accepted_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            accepted_hash,
            block_height=10,
        )
        server._publish_accepted_block_payout_preview(accepted_hash, preview)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(list(artifact.prior_balances), preview)

        original_rpc_call = rpc.call

        def alternate_chain_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return "c2" * 32
            return original_rpc_call(method, params)

        rpc.call = alternate_chain_call  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "no longer active",
        ):
            server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(recorded["calls"], 0)
        self.assertTrue(server._tip_refresh_retry.is_set())
    def test_waiting_child_does_not_fall_back_after_transition_withdrawal(
        self,
    ) -> None:
        class CountingLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.current_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.current_balance_reads += 1
                return []

        class ObservedCondition(threading.Condition):
            def __init__(self) -> None:
                super().__init__()
                self.wait_entered = threading.Event()

            def wait(self, timeout: float | None = None) -> bool:
                self.wait_entered.set()
                return super().wait(timeout)

        ledger = CountingLedger()
        server, _rpc = coordinator(ledger=ledger)
        parent_hash = "be" * 32
        preview_condition = ObservedCondition()
        server._accepted_block_payout_preview_condition = preview_condition
        server.accepted_block_payout_preview_wait_seconds = 10
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            parent_hash,
            block_height=10,
        )
        errors: list[BaseException] = []

        def read_parent_balances() -> None:
            try:
                server._prior_balances_for_job_parent(
                    parent_hash,
                    parent_height=10,
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        thread = threading.Thread(target=read_parent_balances, daemon=True)
        thread.start()
        try:
            self.assertTrue(preview_condition.wait_entered.wait(2))
            self.assertTrue(thread.is_alive())
        finally:
            server._clear_accepted_block_payout_preview(
                parent_hash,
                invalidate_published=True,
            )
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIn("was withdrawn", str(errors[0]))
        self.assertEqual(ledger.current_balance_reads, 0)
    def test_pending_parent_preview_wait_is_bounded_and_retryable(self) -> None:
        server, _rpc = coordinator()
        parent_hash = "ac" * 32
        server.accepted_block_payout_preview_wait_seconds = 0.01
        server._begin_accepted_block_payout_preview(parent_hash)

        with self.assertRaisesRegex(TemplateRefreshBlocked, "not ready"):
            server._prior_balances_for_job_parent(parent_hash)

        self.assertTrue(server._tip_refresh_retry.is_set())
        server._clear_accepted_block_payout_preview(parent_hash)
    def test_replayed_active_ancestor_blocks_descendant_until_preview(self) -> None:
        class PreviewLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.current_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.current_balance_reads += 1
                return []

        class ObservedCondition(threading.Condition):
            def __init__(self) -> None:
                super().__init__()
                self.wait_entered = threading.Event()

            def wait(self, timeout: float | None = None) -> bool:
                self.wait_entered.set()
                return super().wait(timeout)

        ledger = PreviewLedger()
        server, rpc = coordinator(ledger=ledger)
        accepted_hash = "ad" * 32
        descendant_hash = "ae" * 32
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        original_rpc_call = rpc.call

        def active_chain_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return accepted_hash
            return original_rpc_call(method, params)

        rpc.call = active_chain_call  # type: ignore[method-assign]
        preview_condition = ObservedCondition()
        server._accepted_block_payout_preview_condition = preview_condition
        server.accepted_block_payout_preview_wait_seconds = 10
        server._begin_accepted_block_payout_preview(accepted_hash, block_height=10)
        balances: list[list[dict[str, object]]] = []
        errors: list[BaseException] = []

        def read_descendant_balances() -> None:
            try:
                balances.append(
                    server._prior_balances_for_job_parent(
                        descendant_hash,
                        parent_height=11,
                    )
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        thread = threading.Thread(target=read_descendant_balances, daemon=True)
        thread.start()
        try:
            self.assertTrue(preview_condition.wait_entered.wait(2))
            self.assertTrue(thread.is_alive())
            self.assertEqual(ledger.current_balance_reads, 0)
        finally:
            server._publish_accepted_block_payout_preview(accepted_hash, preview)
            thread.join(5)
            server._clear_accepted_block_payout_preview(accepted_hash)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(balances, [preview])
        self.assertEqual(ledger.current_balance_reads, 0)
    def test_landed_transition_bars_reconciliation_before_preview(self) -> None:
        server, _rpc = coordinator()
        block_hash = "af" * 32
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)

        with self.assertRaisesRegex(TemplateRefreshBlocked, "confirmation is still pending"):
            with server._payout_balance_mutation():
                self.fail("landed transition must bar payout reconciliation")

        server._clear_accepted_block_payout_preview(block_hash)
        with server._payout_balance_mutation():
            pass
    def test_readiness_latch_during_preparation_admission_reselects_ready_mode(self) -> None:
        ledger = FakeLedger(miners=["solo"])
        server, rpc = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        assert artifacts is not None
        first_lookup_entered = threading.Event()
        release_first_lookup = threading.Event()
        original_lookup = server._lookup_job_bundle
        lookup_calls = 0

        def block_first_lookup(key: tuple[object, ...]) -> object:
            nonlocal lookup_calls
            lookup_calls += 1
            if lookup_calls == 1:
                first_lookup_entered.set()
                self.assertTrue(release_first_lookup.wait(2.0))
            return original_lookup(key)

        server._lookup_job_bundle = block_first_lookup  # type: ignore[method-assign]
        bundles: list[object] = []
        errors: list[BaseException] = []

        def build_bundle() -> None:
            try:
                bundles.append(server.shared_job_bundle(artifacts, worker()))
            except BaseException as exc:  # pragma: no cover - assertion reports it
                errors.append(exc)

        build_thread = threading.Thread(target=build_bundle)
        build_thread.start()
        self.assertTrue(first_lookup_entered.wait(2.0))
        ledger.miners = ["miner-a", "miner-b", "miner-c"]
        release_first_lookup.set()

        build_thread.join(2.0)

        self.assertFalse(build_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(bundles), 1)
        bundle = bundles[0]
        self.assertFalse(bundle.collection_only)  # type: ignore[union-attr]
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(ledger.snapshot_calls, 1)
    def test_payout_generation_advance_does_not_cancel_new_generation_fanout(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        cancellations: list[str] = []

        class CurrentToken:
            payout_state_generation = 1

        class Cancellation:
            def cancel(self) -> None:
                cancellations.append("cancelled")

        class InjectCurrentFanoutLock:
            def __enter__(self) -> object:
                server._active_tip_refresh = (CurrentToken(), Cancellation())
                return self

            def __exit__(self, *_args: object) -> None:
                return None

        # Model a new-generation refresh registering after the cache-state
        # increment but before the invalidator reaches the coordinator lock.
        server.lock = InjectCurrentFanoutLock()  # type: ignore[assignment]

        self.assertEqual(server._advance_payout_state_generation(), 1)
        self.assertEqual(cancellations, [])
        self.assertFalse(server.tip_refresh_is_pending())
        self.assertFalse(server._tip_refresh_retry.is_set())
    def test_payout_generation_retry_marks_tip_refresh_pending(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()

        self.assertFalse(server.tip_refresh_is_pending())
        self.assertEqual(server._advance_payout_state_generation(), 1)

        self.assertTrue(server.tip_refresh_is_pending())
        self.assertTrue(server._tip_refresh_retry.is_set())

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertFalse(server.tip_refresh_is_pending())
    def test_payout_only_advance_bounds_publish_supersession(self) -> None:
        server, _rpc = coordinator()
        server.payout_reconcile_supersession_retries = 2
        real_publish = server._publish_payout_state_candidate
        publish_attempts = 0

        def supersede_before_publish(candidate: object) -> int | None:
            nonlocal publish_attempts
            publish_attempts += 1
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=f"{publish_attempts + 30:064x}",
            )
            return real_publish(candidate)  # type: ignore[arg-type]

        server._publish_payout_state_candidate = supersede_before_publish  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "payout-only invalidation was superseded",
        ):
            server._advance_payout_state_generation()

        self.assertEqual(publish_attempts, 3)
        self.assertEqual(server._payout_state_generation, 0)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)
        self.assertTrue(server.tip_refresh_is_pending())
    def test_payout_publication_fence_is_not_a_job_build_failure(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server._pool_ready_latched = True
        server._reserve_payout_state_source("payout_only")
        server._block_payout_state_publication()

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        with self.assertRaisesRegex(TemplateRefreshBlocked, "pending publication"):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.job_build_failure_count, 0)
        self.assertEqual(server.tip_refresh_client_counts["failed"], 0)
        self.assertEqual(server.tip_refresh_client_counts["skipped"], 1)
    def test_successful_poll_clears_payout_pending_created_during_reconcile(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        reconcile_entered = threading.Event()
        allow_reconcile = threading.Event()
        results: list[int] = []
        errors: list[BaseException] = []

        def reconcile(_tip_hash: str) -> bool:
            reconcile_entered.set()
            if not allow_reconcile.wait(5):
                raise AssertionError("test did not release reconciliation")
            return True

        def poll() -> None:
            try:
                results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        server.ensure_reorg_reconciled_for_tip = reconcile  # type: ignore[method-assign]
        server._mark_tip_refresh_pending("seed")
        poll_thread = threading.Thread(target=poll)
        poll_thread.start()
        try:
            self.assertTrue(reconcile_entered.wait(5))
            self.assertEqual(server._advance_payout_state_generation(), 1)
        finally:
            allow_reconcile.set()
            poll_thread.join(5)

        self.assertFalse(poll_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [0])
        self.assertEqual(server._payout_state_generation, 1)
        self.assertIsNotNone(server.last_successful_template_refresh_monotonic)
        self.assertFalse(server.tip_refresh_is_pending())
    def test_failed_poll_preserves_pending_signal_until_successful_retry(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        pending_token = server._mark_tip_refresh_pending("seed")

        def fail_reconciliation(_tip_hash: str) -> bool:
            server._schedule_tip_refresh_retry()
            raise RuntimeError("ledger unavailable")

        server.ensure_reorg_reconciled_for_tip = fail_reconciliation  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "qbit reorg reconciliation failed",
        ):
            server.poll_qbit_tip_template_once()

        self.assertTrue(server._tip_refresh_retry.is_set())
        self.assertTrue(server.tip_refresh_is_pending())
        self.assertEqual(server._tip_refresh_pending_token, pending_token)

        # Model blockpoll claiming the immediate wake and completing the retry.
        server._tip_refresh_retry.clear()
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertFalse(server.tip_refresh_is_pending())
    def test_shutdown_during_reconciliation_stops_poll_without_refresh_failure(self) -> None:
        server, _rpc = coordinator()

        def rejected_reconciliation(_tip_hash: str) -> bool:
            server.stop_event.set()
            raise ShutdownInProgress("PRISM coordinator is shutting down")

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            rejected_reconciliation
        )

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertIsNone(
            getattr(server, "last_successful_template_refresh_monotonic", None)
        )
    def test_completed_refresh_cannot_clear_newer_payout_pending(self) -> None:
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
        completed_generation = server._payout_state_generation
        self.assertEqual(server._advance_payout_state_generation(), 1)
        newer_token = server._tip_refresh_pending_token

        self.assertFalse(
            server._clear_tip_refresh_pending_for_completed_refresh(
                snapshot,
                sequence,
                completed_generation,
            )
        )
        self.assertEqual(server._tip_refresh_pending_token, newer_token)
        self.assertTrue(server.tip_refresh_is_pending())
    @staticmethod
    def _capture_error(
        errors: list[BaseException],
        operation: object,
    ) -> None:
        try:
            operation()  # type: ignore[operator]
        except BaseException as exc:  # noqa: BLE001 - test thread handoff
            errors.append(exc)
