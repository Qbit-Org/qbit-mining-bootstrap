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

        with self.assertRaisesRegex(
            _PayoutStatePublicationBlocked,
            "confirmation is still pending",
        ):
            with server._payout_balance_mutation():
                self.fail("landed transition must bar payout reconciliation")
        with self.assertRaisesRegex(
            _PayoutStatePublicationBlocked,
            "confirmation is still pending",
        ):
            server.reconcile_prism_pool_blocks_once()

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

    def test_preview_publication_degrades_when_artifact_preparation_times_out(
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

        def timing_out_preparation(*_args: object, **_kwargs: object) -> object:
            raise TimeoutError("postgres operation exceeded 30s")

        server._prepared_payout_state_candidate = (  # type: ignore[method-assign]
            timing_out_preparation
        )
        server._begin_accepted_block_payout_preview(parent_hash)
        with patch("builtins.print"):
            self.assertEqual(
                server._publish_accepted_block_payout_preview(parent_hash, preview),
                preview,
            )

        # Children of the pending parent receive the immutable compact
        # preview even though no payout-state generation was published.
        transition = server._accepted_block_payout_previews[parent_hash]
        self.assertIsNone(transition.published_generation)
        self.assertTrue(transition.landed)
        self.assertEqual(
            server._prior_balances_for_job_parent(parent_hash),
            preview,
        )
        # Payout-state publication itself stays gated until the landing
        # reaches a terminal durable state.
        with self.assertRaisesRegex(
            _PayoutStatePublicationBlocked, "still pending"
        ):
            with server._payout_balance_mutation():
                pass
        server._clear_accepted_block_payout_preview(parent_hash)

    def test_unresolved_accepted_parent_depth_cap_fails_closed(self) -> None:
        server, _rpc = coordinator()
        server.accepted_parent_unresolved_depth_max = 2
        server.accepted_block_payout_preview_wait_seconds = 0.01
        hashes = ["a1" * 32, "a2" * 32, "a3" * 32]
        for height, block_hash in enumerate(hashes, start=10):
            server._begin_accepted_block_payout_preview(
                block_hash, block_height=height
            )
            server._mark_accepted_block_payout_landed(
                block_hash, block_height=height
            )
        self.assertEqual(len(server.accepted_parent_unresolved_ages_seconds()), 3)
        with self.assertRaisesRegex(TemplateRefreshBlocked, "exceeds cap"):
            server._await_pending_parent_payout_preview(hashes[0])
        # At the cap issuance still blocks: a child issued now could land
        # and create a (cap + 1)th unresolved transition, so the configured
        # maximum would no longer bound the prospective balance chain.
        server._clear_accepted_block_payout_preview(hashes[2])
        with self.assertRaisesRegex(TemplateRefreshBlocked, "exceeds cap"):
            server._await_pending_parent_payout_preview(hashes[0])
        # Below the cap the ordinary bounded wait applies instead.
        server._clear_accepted_block_payout_preview(hashes[1])
        with self.assertRaisesRegex(TemplateRefreshBlocked, "not ready yet"):
            server._await_pending_parent_payout_preview(hashes[0])
        server._clear_accepted_block_payout_preview(hashes[0])
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])

    def test_landing_observability_metrics_exposition(self) -> None:
        import contextlib as _contextlib
        import time as _time

        server, _rpc = coordinator()
        server.block_landing_db_timeout_seconds = 45.0
        server.block_landing_db_timeout_max_seconds = 90.0

        @_contextlib.contextmanager
        def statement_timeout(seconds: float):
            yield

        original_ledger = server.ledger
        server.ledger = SimpleNamespace(
            statement_timeout=statement_timeout,
            prior_balances_read_stats=lambda: {
                "reads_total": 3,
                "last_seconds": 0.004,
                "max_seconds": 0.021,
            },
        )
        block_hash = "ab" * 32
        with self.assertRaises(TimeoutError):
            with server._block_landing_ledger_statement_timeout_scope(block_hash):
                raise LedgerOperationTimeout("postgres operation exceeded 45s")
        server.ledger = original_ledger

        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server.accepted_block_payout_preview_wait_seconds = 0.0
        with self.assertRaisesRegex(TemplateRefreshBlocked, "not ready yet"):
            server._await_pending_parent_payout_preview(block_hash)
        server._startup_phase_origin_monotonic = _time.monotonic()
        server._record_startup_phase_once("audit_listener_bound")

        prior_stats = {
            "reads_total": 3,
            "last_seconds": 0.004,
            "max_seconds": 0.021,
        }
        server.ledger.prior_balances_read_stats = (  # type: ignore[attr-defined]
            lambda: prior_stats
        )
        lines = "\n".join(server.landing_observability_metrics_lines())
        self.assertIn(
            'qbit_prism_block_ledger_call_timeouts_total{call_class="landing"} 1',
            lines,
        )
        self.assertIn(
            'qbit_prism_block_ledger_call_budget_seconds{call_class="landing"} 45.000000',
            lines,
        )
        self.assertIn(
            "qbit_prism_accepted_parent_unresolved_transitions 1",
            lines,
        )
        self.assertIn(
            "qbit_prism_accepted_parent_preview_wait_timeouts_total 1",
            lines,
        )
        self.assertNotIn(
            "qbit_prism_accepted_parent_unresolved_oldest_seconds -1",
            lines,
        )
        self.assertIn("qbit_prism_prior_balances_reads_total 3", lines)
        self.assertIn(
            "qbit_prism_prior_balances_read_max_seconds 0.021000",
            lines,
        )
        self.assertIn(
            'qbit_prism_startup_phase_seconds{phase="audit_listener_bound"}',
            lines,
        )
        server._clear_accepted_block_payout_preview(block_hash)

    def test_startup_replay_gate_blocks_job_builds_until_enumeration(self) -> None:
        server, _rpc = coordinator()
        enumerated: list[int] = []

        class EnumLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                enumerated.append(limit)
                return []

        server.ledger = EnumLedger()
        server._note_block_replay_enumeration_owed()
        with self.assertRaisesRegex(TemplateRefreshBlocked, "has not enumerated"):
            server._await_pending_parent_payout_preview("ab" * 32)
        self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(enumerated, [32])
        self.assertFalse(server._block_replay_enumeration_owed())
        self.assertIsNone(server._await_pending_parent_payout_preview("ab" * 32))

    def test_owed_enumeration_bypasses_live_candidate_short_circuits(self) -> None:
        server, _rpc = coordinator()
        enumerated: list[int] = []

        class EnumLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                enumerated.append(limit)
                return []

        server.ledger = EnumLedger()
        server.block_landing_db_timeout_seconds = 7.5
        server._retry_block_candidate = object()
        # Steady state: a retained retry short-circuits the outbox poll.
        self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(enumerated, [])
        # Owed enumeration is correctness-critical and bypasses the
        # short-circuits, running with the landing-class budget and
        # recording under the landing metrics class.
        server._note_block_replay_enumeration_owed()
        self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(enumerated, [32])
        self.assertFalse(server._block_replay_enumeration_owed())
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["last_budget_seconds"], 7.5)
        self.assertNotIn("fast", metrics)

    def test_worker_reported_deadline_counts_as_landing_timeout(self) -> None:
        """A server-side cancellation completes the ledger worker with a
        timeout error before the coordinator-side wait expires; the
        completion path must still count it for the landing-timeout alert."""
        server, _rpc = coordinator()

        class CancelledLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                raise LedgerOperationTimeout("postgres statement deadline expired")

        server.ledger = CancelledLedger()
        server._note_block_replay_enumeration_owed()
        with patch("builtins.print"):
            self.assertTrue(server._run_startup_block_candidate_replay())
        self.assertTrue(server._block_replay_enumeration_owed())
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["calls_total"], 1)
        self.assertEqual(metrics["landing"]["timeouts_total"], 1)
        self.assertNotIn("fast", metrics)

    def test_startup_enumeration_escalates_past_truncated_batches(self) -> None:
        """A full first batch must not end enumeration: rows beyond the batch
        window (potentially the active parent) still need their payout
        barriers armed before job builds unblock."""
        server, _rpc = coordinator()
        requested: list[int] = []
        total_pending = 33

        class TruncatingLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                requested.append(limit)
                return [
                    durable_candidate_row(index)
                    for index in range(min(limit, total_pending))
                ]

        server.ledger = TruncatingLedger()
        server._note_block_replay_enumeration_owed()
        self.assertEqual(server.replay_pending_block_candidates(), total_pending)
        self.assertEqual(requested, [32, 64])
        self.assertFalse(server._block_replay_enumeration_owed())
        self.assertEqual(
            server._block_replay_candidate_queue.qsize(), total_pending
        )
        # Every restored row armed its payout barrier before the gate opened.
        self.assertEqual(
            len(server._accepted_block_payout_previews), total_pending
        )

    def test_startup_enumeration_stays_owed_at_truncation_cap(self) -> None:
        """An outbox larger than the enumeration cap keeps job builds blocked
        instead of silently declaring enumeration complete."""
        server, _rpc = coordinator()
        requested: list[int] = []

        class EndlessLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                requested.append(limit)
                return [durable_candidate_row(index) for index in range(limit)]

        server.ledger = EndlessLedger()
        server._note_block_replay_enumeration_owed()
        with patch("builtins.print"):
            queued = server.replay_pending_block_candidates()
        self.assertEqual(queued, 1024)
        self.assertEqual(requested, [32, 64, 128, 256, 512, 1024])
        self.assertTrue(server._block_replay_enumeration_owed())
        with self.assertRaisesRegex(TemplateRefreshBlocked, "has not enumerated"):
            server._await_pending_parent_payout_preview("ab" * 32)

    def test_steady_state_poll_restores_a_single_batch(self) -> None:
        """Without an owed enumeration, a full batch stays a single query;
        the next poll picks up later rows after the queue drains."""
        server, _rpc = coordinator()
        requested: list[int] = []

        class FullBatchLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                requested.append(limit)
                return [durable_candidate_row(index) for index in range(limit)]

        server.ledger = FullBatchLedger()
        self.assertEqual(server.replay_pending_block_candidates(), 32)
        self.assertEqual(requested, [32])
        self.assertFalse(server._block_replay_enumeration_owed())

    def test_startup_replay_timeout_keeps_job_builds_blocked(self) -> None:
        server, _rpc = coordinator()
        release = threading.Event()

        class StallLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                release.wait(5)
                return []

        server.ledger = StallLedger()
        server.block_landing_db_timeout_seconds = 0.01
        server.block_landing_db_timeout_max_seconds = 0.01
        try:
            with patch("builtins.print"):
                self.assertTrue(server._run_startup_block_candidate_replay())
            self.assertTrue(server._block_replay_enumeration_owed())
            with self.assertRaisesRegex(
                TemplateRefreshBlocked, "has not enumerated"
            ):
                server._await_pending_parent_payout_preview("ab" * 32)
            # The startup enumeration ran with the landing budget, so its
            # timeout fires the documented landing-timeout alert instead of
            # hiding inside the fast-call series.
            metrics = server.block_ledger_call_class_metrics()
            self.assertEqual(metrics["landing"]["timeouts_total"], 1)
            self.assertNotIn("fast", metrics)
        finally:
            release.set()

    def test_startup_phase_seconds_records_first_occurrence_only(self) -> None:
        import time as _time

        server, _rpc = coordinator()
        self.assertEqual(server.startup_phase_seconds(), {})
        server._record_startup_phase_once("audit_listener_bound")
        self.assertEqual(server.startup_phase_seconds(), {})
        server._startup_phase_origin_monotonic = _time.monotonic() - 1.0
        server._record_startup_phase_once("audit_listener_bound")
        first = server.startup_phase_seconds()["audit_listener_bound"]
        self.assertGreaterEqual(first, 1.0)
        server._record_startup_phase_once("audit_listener_bound")
        self.assertEqual(
            server.startup_phase_seconds()["audit_listener_bound"], first
        )

    def test_job_issuance_continues_during_padded_landing(self) -> None:
        """Acceptance shape for issue #188: a landing padded past its normal
        duration must not stop child job issuance, and payout-state
        publication stays gated until the landing reaches a terminal state.
        The pad is logical (the barrier simply stays unresolved) so the test
        is instant while pinning the same ordering a 10s landing produces."""
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
        server._begin_accepted_block_payout_preview(parent_hash, block_height=10)
        server._mark_accepted_block_payout_landed(parent_hash, block_height=10)
        server._publish_accepted_block_payout_preview(parent_hash, preview)
        # The landing bookkeeping is still unresolved; every child build keeps
        # receiving the immutable preview instead of blocking or reading
        # confirmed balances.
        for _ in range(5):
            self.assertEqual(
                server._prior_balances_for_job_parent(parent_hash),
                preview,
            )
        ages = server.accepted_parent_unresolved_ages_seconds()
        self.assertEqual(len(ages), 1)
        with self.assertRaisesRegex(
            _PayoutStatePublicationBlocked, "still pending"
        ):
            with server._payout_balance_mutation():
                pass
        # Landing completes: the transition clears and confirmed reads resume.
        server._clear_accepted_block_payout_preview(parent_hash)
        self.assertEqual(
            server._prior_balances_for_job_parent(
                parent_hash, fallback_balances=[]
            ),
            [],
        )
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])

    def test_unmark_landed_clears_unresolved_age(self) -> None:
        server, _rpc = coordinator()
        block_hash = "cd" * 32
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        ages = server.accepted_parent_unresolved_ages_seconds()
        self.assertEqual(len(ages), 1)
        self.assertGreaterEqual(ages[0], 0.0)
        server._unmark_accepted_block_payout_landed(block_hash)
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])
        server._clear_accepted_block_payout_preview(block_hash)

    def test_preview_publication_survives_window_build_timeout(self) -> None:
        """Issue #188 acceptance: when the payout window build dies with the
        slow ledger during found-block publication, the preview must still
        cross the atomic publication boundary. Admission then reopens and
        child jobs build against the verified compact preview instead of
        stalling behind the global publication fence until the ledger
        recovers."""
        accepted_hash = "c4" * 32
        server, rpc = coordinator(
            template=base_template(height=12, prevhash=accepted_hash)
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

        def timing_out_window_build(
            *_args: object, **_kwargs: object
        ) -> PayoutLedgerArtifact:
            raise LedgerOperationTimeout("postgres statement deadline expired")

        server._build_payout_ledger_artifact = (  # type: ignore[method-assign]
            timing_out_window_build
        )
        server._begin_accepted_block_payout_preview(accepted_hash, block_height=11)
        server._mark_accepted_block_payout_landed(accepted_hash, block_height=11)
        generation_before = server._payout_state_generation
        with patch("builtins.print"):
            published = server._publish_accepted_block_payout_preview(
                accepted_hash, preview
            )
        self.assertEqual(published, preview)
        # The publication crossed the atomic boundary: a generation was
        # installed and delivery admission reopened despite the dead build.
        self.assertFalse(server._payout_state_publication_fenced())
        self.assertEqual(server._payout_state_generation, generation_before + 1)
        transition = server._accepted_block_payout_previews[accepted_hash]
        self.assertEqual(transition.published_generation, generation_before + 1)
        # Child job construction is admitted and consumes the preview.
        bundle = server.shared_job_bundle(artifacts, mode="ready")
        self.assertIsNotNone(bundle)
        self.assertEqual(recorded["calls"], 1)
        last_kwargs = recorded["last_kwargs"]
        assert isinstance(last_kwargs, dict)
        self.assertEqual(list(last_kwargs["prior_balances"]), preview)
        server._clear_accepted_block_payout_preview(accepted_hash)

    def test_landed_transition_poll_uses_coordination_blocked_budget(self) -> None:
        server, _rpc = coordinator()
        server.reorg_reconciler_enabled = True
        block_hash = "af" * 32
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)

        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                return_value=100.0,
            ),
            self.assertRaisesRegex(
                _PayoutStatePublicationBlocked,
                "confirmation is still pending",
            ),
        ):
            server.poll_qbit_tip_template_once()

        self.assertIsNone(
            getattr(server, "template_refresh_failure_started_monotonic", None)
        )
        self.assertEqual(
            server.coordination_blocked_streak_age_seconds(105.0),
            5.0,
        )

    def test_sync_ledger_snapshot_publishes_reusable_artifact(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        # Pin distinct per-generation anchors near the live clock so the
        # armed window stays anchored inside the audit ceiling.
        first_anchor_ms = now_ms() - 20
        with server._job_cache_lock:
            server._job_build_issued_at_ms[first.generation] = first_anchor_ms

        inline = server.shared_job_bundle(first, mode="ready")

        self.assertEqual(inline.payout_artifact_generation, 0)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertGreater(artifact.generation, 0)
        self.assertEqual(
            artifact.payout_state_generation,
            server._payout_state_generation,
        )
        self.assertEqual(
            artifact.accepted_share_count,
            len(server.ledger.miners),
        )
        self.assertEqual(list(artifact.shares_json), inline.shares_json)
        self.assertEqual(artifact.snapshot_anchor_ms, inline.issued_at_ms)

        second = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert second is not None
        with server._job_cache_lock:
            server._job_build_issued_at_ms[second.generation] = (
                first_anchor_ms + 10
            )

        reused = server.shared_job_bundle(second, mode="ready")

        self.assertEqual(reused.payout_artifact_generation, artifact.generation)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        self.assertEqual(reused.shares_json, inline.shares_json)
        self.assertNotEqual(reused.issued_at_ms, inline.issued_at_ms)
        self.assertEqual(
            reused.found_block["anchor_job_issued_at_ms"],
            artifact.snapshot_anchor_ms,
        )

    def test_sync_artifact_publication_survives_share_commit_mid_read(
        self,
    ) -> None:
        class MidReadCommitLedger(FakeLedger):
            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[FakeShare]:
                result = super().snapshot_at_job_issue(
                    anchor_job_issued_at_ms,
                    window_weight=window_weight,
                )
                # A share becomes durable while the window read is in
                # flight. The read is scoped by the frozen anchor -- the
                # commit lands above it and belongs to the next window -- so
                # anchor-scoped publication proceeds.
                self.miners = [*self.miners, f"mid-read-{self.snapshot_calls}"]
                return result

        server, rpc = coordinator(ledger=MidReadCommitLedger())
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertFalse(bundle.collection_only)
        self.assertEqual(bundle.payout_artifact_generation, 0)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, bundle.issued_at_ms)
        self.assertEqual(list(artifact.shares_json), bundle.shares_json)

        # The armed window keeps serving under continuous commits: the live
        # durable count has moved past the artifact's, and reuse must not
        # care.
        second = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert second is not None
        reused = server.shared_job_bundle(second, mode="ready")
        self.assertEqual(reused.payout_artifact_generation, artifact.generation)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        self.assertEqual(
            reused.found_block["anchor_job_issued_at_ms"],
            artifact.snapshot_anchor_ms,
        )

    def test_sync_publication_allowed_when_anchor_predates_durable_share(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        first = server.shared_job_bundle(artifacts, mode="ready")
        self.assertFalse(first.collection_only)
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)
            # A share becomes durable after this generation's anchor was
            # frozen. Force the synchronous path to run again for the same
            # frozen anchor.
            server._payout_ledger_artifact = None
            server._job_bundle_cache.clear()
        server.ledger.miners = [*server.ledger.miners, "post-anchor-share"]

        second = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(second.payout_artifact_generation, 0)
        # The window read at the frozen anchor deterministically excludes
        # the later durable share -- it belongs to the next window -- so the
        # anchor-scoped publication proceeds even though the live count has
        # moved past the frozen anchor.
        with server._job_cache_lock:
            republished = server._payout_ledger_artifact
        self.assertIsNotNone(republished)
        assert republished is not None
        self.assertEqual(republished.snapshot_anchor_ms, second.issued_at_ms)
        self.assertEqual(second.issued_at_ms, first.issued_at_ms)

    def test_racing_commit_does_not_disable_sync_artifact_seeding(self) -> None:
        class RacingStatsLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__(
                    miners=["miner-a", "miner-b", "miner-c", "miner-d"]
                )
                self._pending_counts = iter([3, 4])

            def accepted_share_stats(self) -> dict[str, int]:
                self.stats_calls += 1
                try:
                    count = next(self._pending_counts)
                except StopIteration:
                    count = 4
                return {
                    "accepted_share_count": count,
                    "distinct_miner_count": 4,
                }

        server, rpc = coordinator(ledger=RacingStatsLedger())
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        # A commit races the informational count read. The window is scoped
        # by the frozen anchor regardless of which side of the read the
        # commit landed on, so seeding proceeds; the count is diagnostics,
        # not a fence.
        bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertFalse(bundle.collection_only)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, bundle.issued_at_ms)
        self.assertEqual(list(artifact.shares_json), bundle.shares_json)

    def test_same_window_background_rebuild_keeps_artifact_generation(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        server.shared_job_bundle(artifacts, mode="ready")
        with server._job_cache_lock:
            installed = server._payout_ledger_artifact
        assert installed is not None

        # No share committed since: the speculative rebuild reads the same
        # window under a fresh anchor and must not spin the generation,
        # which would re-key bundle lookups for nothing -- but it is still a
        # successful preparation, so an accumulated re-arm backoff releases
        # and the fresher anchor advances the staleness clock in place.
        with server._payout_artifact_executor_lock:
            server._payout_artifact_rearm_backoff = 4
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            refreshed = server._payout_ledger_artifact
        assert refreshed is not None
        self.assertEqual(refreshed.generation, installed.generation)
        self.assertEqual(
            refreshed.share_snapshot_sha256,
            installed.share_snapshot_sha256,
        )
        assert installed.snapshot_anchor_ms is not None
        assert refreshed.snapshot_anchor_ms is not None
        self.assertGreaterEqual(
            int(refreshed.snapshot_anchor_ms),
            int(installed.snapshot_anchor_ms),
        )
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 1)

        # A new durable share makes the rebuild a genuine replacement.
        server.ledger.miners = [*server.ledger.miners, "late-share"]
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            replaced = server._payout_ledger_artifact
        assert replaced is not None
        self.assertGreater(replaced.generation, installed.generation)
        self.assertEqual(replaced.accepted_share_count, 4)

    def test_delayed_older_snapshot_does_not_regress_artifact(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.shared_job_bundle(artifacts, mode="ready")
        with server._job_cache_lock:
            current = server._payout_ledger_artifact
        assert current is not None
        assert current.snapshot_anchor_ms is not None

        # A snapshot taken at an earlier anchor finishes its window
        # conversion late, so its preparation timestamp is newer than the
        # installed artifact's; the anchor still proves it is the older
        # window.
        older_window = [
            {"share_seq": seq, "miner_id": "miner-a"} for seq in (1, 2)
        ]
        delayed = PayoutLedgerArtifact(
            generation=0,
            payout_state_generation=server._payout_state_generation,
            network_difficulty=int(current.network_difficulty),
            accepted_share_count=2,
            shares_json=tuple(older_window),
            prior_balances=(),
            prepared_monotonic=time.monotonic(),
            snapshot_anchor_ms=int(current.snapshot_anchor_ms) - 5,
            share_snapshot_sha256=canonical_json_sha256(older_window),
        )
        server._install_payout_ledger_artifact(delayed)
        with server._job_cache_lock:
            self.assertIs(server._payout_ledger_artifact, current)

        # A window snapshotted at a fresher anchor replaces regardless of
        # its preparation timestamp ordering.
        newer_window = [
            {"share_seq": seq, "miner_id": "miner-a"} for seq in (1, 2, 3, 4)
        ]
        fresher = dataclass_replace(
            delayed,
            accepted_share_count=4,
            shares_json=tuple(newer_window),
            prepared_monotonic=current.prepared_monotonic - 1.0,
            snapshot_anchor_ms=int(current.snapshot_anchor_ms) + 5,
            share_snapshot_sha256=canonical_json_sha256(newer_window),
        )
        server._install_payout_ledger_artifact(fresher)
        with server._job_cache_lock:
            replaced = server._payout_ledger_artifact
        assert replaced is not None
        self.assertGreater(replaced.generation, current.generation)
        self.assertEqual(replaced.accepted_share_count, 4)

    def test_difficulty_change_replaces_same_window_artifact(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.shared_job_bundle(artifacts, mode="ready")
        with server._job_cache_lock:
            installed = server._payout_ledger_artifact
        assert installed is not None

        # A retarget moves the live template to a new difficulty while the
        # share window stays identical. The rebuild at the live difficulty
        # must replace the artifact: keeping the old-difficulty artifact
        # would fail the reuse fence on every build while the same-window
        # skip kept discarding its replacement.
        retarget_difficulty = int(installed.network_difficulty) + 1
        with server._job_cache_lock:
            live = server._template_artifacts
            assert live is not None
            server._template_artifacts = dataclass_replace(
                live,
                network_difficulty=retarget_difficulty,
            )
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            retarget_difficulty,
        )
        with server._job_cache_lock:
            replaced = server._payout_ledger_artifact
        assert replaced is not None
        self.assertGreater(replaced.generation, installed.generation)
        self.assertEqual(replaced.network_difficulty, retarget_difficulty)

        # A pre-retarget build delayed after its snapshot carries the same
        # window at the old difficulty; whether its anchor trails or ties,
        # the live-difficulty artifact must stay.
        delayed = dataclass_replace(
            installed,
            generation=0,
            prepared_monotonic=time.monotonic(),
        )
        server._install_payout_ledger_artifact(delayed)
        with server._job_cache_lock:
            self.assertIs(server._payout_ledger_artifact, replaced)

        # Even a delayed pre-retarget build that clamped a fresher anchor
        # must not displace it: a wrong-difficulty install would fail every
        # reuse probe on the difficulty check, which never re-arms.
        assert replaced.snapshot_anchor_ms is not None
        leading = dataclass_replace(
            installed,
            generation=0,
            prepared_monotonic=time.monotonic(),
            snapshot_anchor_ms=int(replaced.snapshot_anchor_ms) + 5,
        )
        server._install_payout_ledger_artifact(leading)
        with server._job_cache_lock:
            self.assertIs(server._payout_ledger_artifact, replaced)

    def test_fence_failure_rearms_artifact_preparation_with_debounce(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.payout_artifact_rearm_min_seconds = 5.0
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )

        class RecordingExecutor:
            def __init__(self) -> None:
                self.submissions = 0

            def submit(self, _fn: object) -> Future[None]:
                # Recorded but never run: the request slot stays observable.
                self.submissions += 1
                return Future()

        executor = RecordingExecutor()
        with server._payout_artifact_executor_lock:
            server._payout_artifact_executor = executor  # type: ignore[assignment]

        usable = server._usable_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        self.assertIsNotNone(usable)
        with server._payout_artifact_executor_lock:
            self.assertIsNone(server._payout_artifact_requested)

        # Age the armed window past the re-anchor floor: the probe keeps
        # serving the artifact while the debounced background re-anchor is
        # scheduled.
        server.payout_artifact_reanchor_seconds = 10.0
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 11_000,
            )

        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        with server._payout_artifact_executor_lock:
            self.assertEqual(
                server._payout_artifact_requested,
                (
                    server._payout_state_generation,
                    int(artifacts.network_difficulty),
                ),
            )
            self.assertEqual(executor.submissions, 1)
            # Every past-floor probe counts, even when the rearm itself is
            # debounced -- the canary needs "serving past the floor" to be
            # visible independently of rearm_scheduled.
            self.assertEqual(
                server.payout_artifact_event_counts["probe_past_floor"],
                1,
            )
            # The one-slot worker dequeues the request; a served probe inside
            # the interval must not slip a duplicate into the emptied slot.
            server._payout_artifact_requested = None

        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        with server._payout_artifact_executor_lock:
            self.assertIsNone(server._payout_artifact_requested)

        # Once the interval has elapsed the next served probe re-arms.
        with server._payout_artifact_executor_lock:
            server._payout_artifact_last_schedule_monotonic = (
                time.monotonic() - 6.0
            )
        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        with server._payout_artifact_executor_lock:
            self.assertEqual(
                server._payout_artifact_requested,
                (
                    server._payout_state_generation,
                    int(artifacts.network_difficulty),
                ),
            )

    def test_aborted_speculative_rebuilds_back_off_and_reset(self) -> None:
        ledger = AnchorRecordingLedger()
        server, rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.payout_artifact_rearm_min_seconds = 5.0

        # A pending-commit floor older than the audit anchor ceiling aborts
        # the rebuild before the window walk (the artifact would arm born
        # expired); each abort doubles the re-arm interval instead of
        # retrying the reward-window walk at the floor forever.
        share = stamped_pending_share(now_ms() - 400_000)
        server._ensure_pending_share_commit_state()
        with server._pending_share_commit_lock:
            server._pending_share_commit_floor[id(share)] = [
                share,
                time.monotonic(),
                False,
            ]
        server._prepare_payout_ledger_artifact(0, artifacts.network_difficulty)
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 2)
        server._prepare_payout_ledger_artifact(0, artifacts.network_difficulty)
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 4)
        self.assertEqual(ledger.snapshot_calls, 0)

        # Elapsed time beyond the floor but inside the scaled interval must
        # not re-arm.
        with server._payout_artifact_executor_lock:
            server._payout_artifact_last_schedule_monotonic = (
                time.monotonic() - 10.0
            )
        server._rearm_payout_ledger_artifact_after_fence_failure(
            0,
            artifacts.network_difficulty,
        )
        with server._payout_artifact_executor_lock:
            self.assertIsNone(server._payout_artifact_requested)

        # A rebuild that finally arms resets the backoff to the floor.
        server._finish_pending_share_commit(share)
        server._prepare_payout_ledger_artifact(0, artifacts.network_difficulty)
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 1)

    def test_payout_publication_resets_rearm_backoff(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        with server._payout_artifact_executor_lock:
            server._payout_artifact_rearm_backoff = 8

        # The accepted-block preview publishes a new payout generation whose
        # candidate artifact installs through the atomic publication pointer
        # swap; the accumulated backoff must release with it.
        parent_hash = str(rpc.template["previousblockhash"])
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        server._publish_accepted_block_payout_preview(
            parent_hash,
            [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 25,
                }
            ],
        )

        self.assertGreater(server._payout_state_generation, 0)
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 1)

    def test_landed_preview_suppresses_fence_failure_rearm(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        scheduled: list[tuple[int, int]] = []
        server._schedule_payout_ledger_artifact_preparation = (  # type: ignore[method-assign]
            lambda generation, difficulty, **_kwargs: scheduled.append(
                (generation, difficulty)
            )
        )
        accepted_hash = "d0" * 32
        server._begin_accepted_block_payout_preview(
            accepted_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        server._mark_accepted_block_payout_landed(
            accepted_hash,
            block_height=int(rpc.template["height"]) - 1,
        )

        # Age the armed window past the re-anchor floor: the probe keeps
        # serving while attempting the background re-anchor.
        server.payout_artifact_reanchor_seconds = 10.0
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 11_000,
            )

        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        # A speculative rebuild would read database balances the landed
        # prospective state supersedes; preparation resumes only through the
        # durable-confirmation call site.
        self.assertEqual(scheduled, [])

    def test_artifact_reuse_serves_past_install_age_to_anchor_ceiling(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
        assert armed is not None
        assert armed.snapshot_anchor_ms is not None
        # The balances digest is memoized at construction so the serving
        # probe and the reused-build fences never re-canonicalize the
        # tuple on the request path.
        self.assertEqual(
            armed.prior_balances_sha256,
            canonical_json_sha256(armed.prior_balances),
        )

        # A window whose anchor is past the re-anchor floor still serves --
        # post-anchor shares belong to the next window by construction, and
        # validity is carried by the generation/balances fences, never by
        # wall-clock age below the ceiling. (The re-anchor and
        # anchor-ceiling env knobs wire these same attributes in __init__.)
        server.payout_artifact_reanchor_seconds = 10.0
        server.ledger.miners = [*server.ledger.miners, "late-1", "late-2"]
        with server._job_cache_lock:
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 15_000,
            )
        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
                rearm_on_fence_failure=False,
            )
        )

        # 2026-07-30 brownout regression: install age must never retire the
        # window. A 10s install budget at the production build cadence (a
        # shared build every 10-14s, a 4-8s walk) expired between
        # consecutive builds, sending every build back through the
        # synchronous reward-window walk. Only payout events and the audit
        # ceiling end reuse.
        with server._job_cache_lock:
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                prepared_monotonic=float(armed.prepared_monotonic) - 3600.0,
            )
        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
                rearm_on_fence_failure=False,
            )
        )

        # The wall-clock anchor ceiling stays as the audit-facing backstop.
        server.payout_artifact_max_anchor_age_seconds = 5.0
        with server._job_cache_lock:
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 6_000,
            )
        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
                rearm_on_fence_failure=False,
            )
        )

    def test_reuse_kill_switch_disables_probe_and_background_walks(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            assert server._payout_ledger_artifact is not None

        # Off: the probe refuses a perfectly valid armed artifact and the
        # scheduler runs no background reward-window walks -- the disabled
        # state restores pre-reuse delivery economics (the synchronous
        # path keeps the re-landed anchor-selection semantics), so a
        # delivery regression is an env flip instead of a rollback.
        server.payout_artifact_reuse_enabled = False
        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        server._schedule_payout_ledger_artifact_preparation(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._payout_artifact_executor_lock:
            self.assertIsNone(server._payout_artifact_requested)
            self.assertIsNone(server._payout_artifact_future)

        # Back on: the armed artifact serves again without a rebuild.
        server.payout_artifact_reuse_enabled = True
        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )

        # Off also gates arming itself: a prepared window is discarded at
        # install, so nothing churns install events or re-keys the idle
        # bundle fast path to an unusable generation while disabled.
        server.payout_artifact_reuse_enabled = False
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)

    def test_anchor_ceiling_must_clear_reanchor_floor(self) -> None:
        # One env var used to be able to re-create the 2026-07-30 incident:
        # a ceiling the background re-anchor cannot beat (floor + debounce +
        # the window walk) rejects every probe while the rebuild runs
        # debounced in parallel -- walk-per-build. __init__ refuses the
        # configuration at startup through this validation.
        with self.assertRaises(SystemExit):
            prism_coordinator_module.validate_payout_artifact_age_bounds(
                60.0,
                60.0,
            )
        with self.assertRaises(SystemExit):
            prism_coordinator_module.validate_payout_artifact_age_bounds(
                120.0,
                60.0,
            )
        # The same margin applies to a custom build cadence; validating only
        # the re-anchor floor allowed 299s builds under a 300s ceiling.
        with self.assertRaises(SystemExit):
            prism_coordinator_module.validate_payout_artifact_age_bounds(
                300.0,
                299.0,
            )
        # The default split (300s ceiling, 60s floor) passes.
        prism_coordinator_module.validate_payout_artifact_age_bounds(
            300.0,
            60.0,
        )

    def test_slow_window_walk_still_arms_reusable_artifact(self) -> None:
        # 2026-07-29 regression: the reward-window walk takes multiple
        # seconds at production volume, and freshness measured from the
        # wall-clock anchor rejected every slow build on arrival. The walk
        # here advances the wall clock past the freshness budget; the
        # artifact must still arm, serve reuse, and reset the re-arm
        # backoff.
        clock = [now_ms()]

        class SlowWalkLedger(FakeLedger):
            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[FakeShare]:
                result = super().snapshot_at_job_issue(
                    anchor_job_issued_at_ms,
                    window_weight=window_weight,
                )
                clock[0] += 15_000
                return result

        server, rpc = coordinator(ledger=SlowWalkLedger())
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        with server._payout_artifact_executor_lock:
            server._payout_artifact_rearm_backoff = 4
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock[0],
        ):
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._job_cache_lock:
                armed = server._payout_ledger_artifact
            self.assertIsNotNone(armed)
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            self.assertGreater(
                clock[0] - int(armed.snapshot_anchor_ms),
                10_000,
            )
            self.assertIs(
                server._usable_payout_ledger_artifact(
                    server._payout_state_generation,
                    artifacts.network_difficulty,
                ),
                armed,
            )
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 1)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="installed"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="built"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="incremental"}',
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="self_check_failed"}',
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="found_block_cached"}',
            metrics,
        )

    def test_born_expired_install_paces_backoff_instead_of_resetting(
        self,
    ) -> None:
        # A walk that outlives the audit ceiling arms nothing. Crediting it
        # as a success reset the re-arm backoff to the floor and re-walked
        # the reward window continuously -- the 2026-07-29 livelock. It
        # must pace exactly like a failed preparation.
        clock = [now_ms()]

        class GlacialWalkLedger(FakeLedger):
            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[FakeShare]:
                result = super().snapshot_at_job_issue(
                    anchor_job_issued_at_ms,
                    window_weight=window_weight,
                )
                clock[0] += 400_000
                return result

        server, rpc = coordinator(ledger=GlacialWalkLedger())
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock[0],
        ):
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)
            with server._payout_artifact_executor_lock:
                self.assertEqual(server._payout_artifact_rearm_backoff, 2)
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._payout_artifact_executor_lock:
                self.assertEqual(server._payout_artifact_rearm_backoff, 4)
        with server._payout_artifact_executor_lock:
            self.assertEqual(
                server.payout_artifact_event_counts["born_expired"],
                2,
            )

    def test_equal_anchor_reprove_credits_freshness(self) -> None:
        # A pending-commit floor pins the snapshot anchor, so a fence-failure
        # rebuild re-proves the same window at the SAME anchor. That re-prove
        # must advance the freshness clock: no fresher window is
        # constructible while the floor holds, and rejecting the credit
        # would re-walk the reward window (with backoff reset each round)
        # until the audit ceiling -- the livelock shape this re-land
        # removes.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        share = stamped_pending_share(now_ms() - 5)
        server._ensure_pending_share_commit_state()
        with server._pending_share_commit_lock:
            server._pending_share_commit_floor[id(share)] = [
                share,
                time.monotonic(),
                False,
            ]
        try:
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._job_cache_lock:
                armed = server._payout_ledger_artifact
                assert armed is not None
                server._payout_ledger_artifact = dataclass_replace(
                    armed,
                    prepared_monotonic=float(armed.prepared_monotonic) - 11.0,
                )
            # Install age no longer gates the probe (2026-07-30 brownout
            # regression); the armed window keeps serving while pinned.
            self.assertIsNotNone(
                server._usable_payout_ledger_artifact(
                    server._payout_state_generation,
                    artifacts.network_difficulty,
                    rearm_on_fence_failure=False,
                )
            )

            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._job_cache_lock:
                reproved = server._payout_ledger_artifact
            assert reproved is not None
            self.assertEqual(reproved.generation, armed.generation)
            self.assertEqual(
                reproved.snapshot_anchor_ms,
                armed.snapshot_anchor_ms,
            )
            self.assertIsNotNone(
                server._usable_payout_ledger_artifact(
                    server._payout_state_generation,
                    artifacts.network_difficulty,
                )
            )
        finally:
            server._finish_pending_share_commit(share)

    def test_publication_restamps_candidate_artifact_freshness(self) -> None:
        # A payout-state candidate builds its artifact before the atomic
        # publication, and the delivery-gate drain between the two can
        # outlive the reuse budget. The install must restamp freshness so a
        # freshly published generation never arms an already-stale artifact.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        real_build = server._build_payout_ledger_artifact

        def slow_publication_build(*args: object) -> object:
            built = real_build(*args)
            if built is None:
                return None
            return dataclass_replace(
                built,
                prepared_monotonic=time.monotonic() - 11.0,
            )

        server._build_payout_ledger_artifact = slow_publication_build  # type: ignore[method-assign]
        parent_hash = str(rpc.template["previousblockhash"])
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        server._publish_accepted_block_payout_preview(
            parent_hash,
            [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 25,
                }
            ],
        )

        self.assertGreater(server._payout_state_generation, 0)
        usable = server._usable_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        self.assertIsNotNone(usable)

    def test_usable_probe_survives_equal_window_freshness_restamp(
        self,
    ) -> None:
        # An equal-window re-prove restamps the armed artifact by replacing
        # the object while a reuse probe is hashing balances outside the
        # cache lock. The probe must treat the restamped copy as the same
        # armed window instead of failing closed into a synchronous
        # reward-window walk: under a pinned pending-commit floor the
        # restamp IS the intentional recovery path, so the race is routine.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            # Strip the memoized balances digest: the race window this test
            # drives opens while the probe hashes balances outside the cache
            # lock, which only happens on the fallback path for artifacts
            # without a memo (legacy or test-constructed). The restamp
            # admission below must keep protecting that path.
            armed = dataclass_replace(armed, prior_balances_sha256=None)
            server._payout_ledger_artifact = armed

        real_sha256 = prism_coordinator_module.canonical_json_sha256
        restamped_once = [False]

        def restamp_during_hash(value: object) -> str:
            if not restamped_once[0]:
                restamped_once[0] = True
                self.assertTrue(
                    server._install_payout_ledger_artifact(
                        dataclass_replace(
                            armed,
                            generation=0,
                            prepared_monotonic=time.monotonic(),
                        )
                    )
                )
            return real_sha256(value)

        with patch.object(
            prism_coordinator_module,
            "canonical_json_sha256",
            restamp_during_hash,
        ):
            usable = server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        self.assertTrue(restamped_once[0])
        with server._job_cache_lock:
            restamped = server._payout_ledger_artifact
        assert restamped is not None
        self.assertIsNot(restamped, armed)
        self.assertIsNotNone(usable)
        self.assertIs(usable, restamped)
        self.assertEqual(restamped.generation, armed.generation)

    def test_publication_discards_born_expired_candidate_artifact(
        self,
    ) -> None:
        # Candidate construction plus the delivery-gate drain can push the
        # candidate artifact's declared anchor past the audit ceiling. The
        # atomic publication must apply the same born-expired admission rule
        # as _install_payout_ledger_artifact: arming the artifact would fail
        # every reuse probe on anchor age -- with the re-arm suppressed
        # while the accepted preview awaits durability -- instead of letting
        # the post-publication probe schedule recovery.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        real_build = server._build_payout_ledger_artifact
        ceiling_ms = server._payout_artifact_max_anchor_age_ms()

        def expired_anchor_build(*args: object) -> object:
            built = real_build(*args)
            if built is None:
                return None
            assert built.snapshot_anchor_ms is not None
            return dataclass_replace(
                built,
                snapshot_anchor_ms=int(built.snapshot_anchor_ms)
                - int(ceiling_ms)
                - 1_000,
            )

        server._build_payout_ledger_artifact = expired_anchor_build  # type: ignore[method-assign]
        parent_hash = str(rpc.template["previousblockhash"])
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        generation_before = server._payout_state_generation
        server._publish_accepted_block_payout_preview(
            parent_hash,
            [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 25,
                }
            ],
        )
        self.assertGreater(server._payout_state_generation, generation_before)
        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)
        with server._payout_artifact_executor_lock:
            born_expired = int(
                server.payout_artifact_event_counts.get("born_expired", 0)
            )
        self.assertGreaterEqual(born_expired, 1)
        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )

    def test_publication_records_installed_event(self) -> None:
        # The atomic publication arms the candidate artifact through its own
        # pointer swap, not _install_payout_ledger_artifact. It must still
        # count in the installed lifecycle event family, or every
        # publication-path install is invisible to the observability the
        # event counter exists for.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        with server._payout_artifact_executor_lock:
            installed_before = int(
                server.payout_artifact_event_counts.get("installed", 0)
            )
        parent_hash = str(rpc.template["previousblockhash"])
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        server._publish_accepted_block_payout_preview(
            parent_hash,
            [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 25,
                }
            ],
        )
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)
        with server._payout_artifact_executor_lock:
            installed_after = int(
                server.payout_artifact_event_counts.get("installed", 0)
            )
        self.assertEqual(installed_after, installed_before + 1)

    def test_reused_anchor_bundle_preview_matches_fresh_build_through_guard(
        self,
    ) -> None:
        # The #50 payout-preview guard compares the preview computed from
        # the issued job against one recomputed at landing. A reused-anchor
        # bundle must therefore reproduce, byte for byte, what a fresh
        # ledger read at the same anchor produces -- the audit
        # reproducibility contract the artifact's snapshot_anchor_ms
        # declaration documents.
        programs = {
            "miner-a": "aa" * 32,
            "miner-b": "bb" * 32,
            "miner-c": "cc" * 32,
            "miner-d": "dd" * 32,
        }

        class AnchorScopedLedger(FakeLedger):
            """Window reads respect the anchor like the real ledger."""

            def __init__(self) -> None:
                super().__init__(miners=[])
                self.stamped: list[tuple[int, FakeShare]] = []

            def add_share(self, miner_id: str, accepted_at_ms: int) -> None:
                self.stamped.append(
                    (
                        int(accepted_at_ms),
                        FakeShare(
                            miner_id=miner_id,
                            share_seq=len(self.stamped) + 1,
                        ),
                    )
                )

            def accepted_share_stats(self) -> dict[str, int]:
                self.stats_calls += 1
                return {
                    "accepted_share_count": len(self.stamped),
                    "distinct_miner_count": len(
                        {share.miner_id for _, share in self.stamped}
                    ),
                }

            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[FakeShare]:
                self.snapshot_calls += 1
                return [
                    share
                    for stamp, share in self.stamped
                    if stamp <= int(anchor_job_issued_at_ms)
                ]

        anchor_ms = now_ms() - 50
        ledger = AnchorScopedLedger()
        ledger.add_share("miner-a", anchor_ms - 30)
        ledger.add_share("miner-b", anchor_ms - 20)
        ledger.add_share("miner-c", anchor_ms - 10)
        server, rpc = coordinator(ledger=ledger)

        def preview_bundle_builder(**kwargs: object) -> dict[str, object]:
            suffix_hex = str(kwargs["coinbase_script_sig_suffix_hex"])
            weights: dict[str, int] = {}
            for share in kwargs["shares"]:  # type: ignore[union-attr]
                miner = str(share["miner_id"])  # type: ignore[index]
                weights[miner] = weights.get(miner, 0) + 1
            return {
                "found_block": dict(kwargs["found_block"]),  # type: ignore[call-overload]
                "payout_policy_manifest": {
                    "accounts": [
                        {
                            "account_type": "miner",
                            "recipient_id": miner,
                            "order_key": miner,
                            "p2mr_program_hex": programs[miner],
                            "carry_forward_balance_sats": 1_000 * weight,
                        }
                        for miner, weight in sorted(weights.items())
                    ]
                },
                "signed_coinbase_manifest": {
                    "manifest": {
                        "coinbase_tx_hex": synthetic_manifest_coinbase_hex(
                            suffix_hex
                        ),
                    }
                },
            }

        server.build_audit_bundle = preview_bundle_builder  # type: ignore[method-assign]

        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        with server._job_cache_lock:
            server._job_build_issued_at_ms[first.generation] = anchor_ms

        seeded_bundle = server.shared_job_bundle(first, mode="ready")
        self.assertEqual(seeded_bundle.payout_artifact_generation, 0)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, anchor_ms)

        # Shares keep landing after the anchor; the durable count moves past
        # the artifact's, which must not matter for reuse.
        ledger.add_share("miner-a", anchor_ms + 20)
        ledger.add_share("miner-d", anchor_ms + 25)

        second = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert second is not None
        reused = server.shared_job_bundle(second, mode="ready")
        self.assertEqual(reused.payout_artifact_generation, artifact.generation)
        self.assertEqual(
            reused.found_block["anchor_job_issued_at_ms"],
            anchor_ms,
        )

        # Control: a fresh synchronous build pinned to the same anchor pays
        # its own window read and must reproduce the reused output exactly,
        # excluding the post-anchor shares deterministically.
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
            server._job_bundle_cache.clear()
        third = server.store_template_artifacts(
            base_template(height=12, prevhash="33" * 32)
        )
        assert third is not None
        with server._job_cache_lock:
            server._job_build_issued_at_ms[third.generation] = anchor_ms
        snapshot_calls_before = ledger.snapshot_calls
        fresh = server.shared_job_bundle(third, mode="ready")
        self.assertEqual(ledger.snapshot_calls, snapshot_calls_before + 1)
        self.assertEqual(fresh.payout_artifact_generation, 0)

        self.assertEqual(reused.shares_json, fresh.shares_json)
        self.assertEqual(
            fresh.found_block["anchor_job_issued_at_ms"],
            anchor_ms,
        )
        assert reused.prospective_prior_balances is not None
        assert fresh.prospective_prior_balances is not None
        self.assertEqual(
            canonical_json_text(list(reused.prospective_prior_balances)),
            canonical_json_text(list(fresh.prospective_prior_balances)),
        )

        # A window at a fresher anchor covers the post-anchor shares and
        # produces a different preview; built now to prove the guard
        # equality below is load-bearing.
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
            server._job_bundle_cache.clear()
        fourth = server.store_template_artifacts(
            base_template(height=13, prevhash="44" * 32)
        )
        assert fourth is not None
        with server._job_cache_lock:
            server._job_build_issued_at_ms[fourth.generation] = anchor_ms + 30
        divergent = server.shared_job_bundle(fourth, mode="ready")
        assert divergent.prospective_prior_balances is not None
        self.assertNotEqual(
            canonical_json_text(list(divergent.prospective_prior_balances)),
            canonical_json_text(list(reused.prospective_prior_balances)),
        )

        # The #50 guard sequence at landing: the issued preview (from the
        # reused-anchor job) publishes first; the verified preview
        # recomputed at the same anchor must then publish idempotently.
        block_hash = "d1" * 32
        server._begin_accepted_block_payout_preview(
            block_hash,
            block_height=int(rpc.template["height"]),
        )
        issued = server._materialize_prior_balance_preview(
            reused.prospective_prior_balances
        )
        server._publish_accepted_block_payout_preview(block_hash, issued)
        generation_after_issued = server._payout_state_generation
        verified = server._materialize_prior_balance_preview(
            fresh.prospective_prior_balances
        )
        server._publish_accepted_block_payout_preview(block_hash, verified)
        self.assertEqual(
            server._payout_state_generation,
            generation_after_issued,
        )
        with server._accepted_block_payout_preview_condition:
            transition = server._accepted_block_payout_previews[block_hash]
        self.assertEqual(
            transition.preview,
            server._serialize_prior_balance_preview(issued),
        )

        # A preview from any other anchor's window trips the guard exactly
        # as submit_block_candidate's landing rebuild would.
        with self.assertRaisesRegex(RuntimeError, "changed during retry"):
            server._publish_accepted_block_payout_preview(
                block_hash,
                server._materialize_prior_balance_preview(
                    divergent.prospective_prior_balances
                ),
            )


class IncrementalPayoutArtifactTests(unittest.TestCase):
    def configured_server(
        self,
    ) -> tuple[PrismCoordinator, IncrementalRecordingLedger, object]:
        ledger = IncrementalRecordingLedger()
        append_incremental_share(ledger, share_seq=1, accepted_at_ms=999_900)
        append_incremental_share(ledger, share_seq=2, accepted_at_ms=999_910)
        append_incremental_share(ledger, share_seq=3, accepted_at_ms=999_920)
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 60.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        return server, ledger, artifacts

    @staticmethod
    def replay_shaped_append_entry(
        *,
        accepted_at_ms: int,
        block_hash_hex: str,
    ) -> PendingShareAppend:
        persisted = stamped_pending_share(accepted_at_ms)
        pending = PendingShare(**dict(vars(persisted)))
        return PendingShareAppend(
            pending_share=pending,
            username="miner-a",
            job_id=pending.job_id,
            block_hash_hex=block_hash_hex,
            collection_only=False,
            credit_policy=None,
            candidate_intent={
                "block_hash_hex": block_hash_hex,
                "pending_share": dict(vars(pending)),
                "credit_share_on_accept": True,
            },
        )

    def test_late_visible_replay_append_forces_next_build_to_full_oracle(
        self,
    ) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")

            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertTrue(server._install_payout_ledger_artifact(advanced))
            anchor_ms = int(advanced.snapshot_anchor_ms)

            entry = self.replay_shaped_append_entry(
                accepted_at_ms=anchor_ms - 10,
                block_hash_hex="44" * 32,
            )
            self.assertTrue(server._append_share_batch([entry]))
            self.assertIsNone(server._incremental_payout_artifact_window)
            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)
            self.assertFalse(server._install_payout_ledger_artifact(advanced))
            self.assertIsNone(
                server._cached_found_block_payout_artifact(
                    base_generation=0,
                    artifact_payout_state_generation=1,
                    network_difficulty=artifacts.network_difficulty,
                    fallback_reason="prepare_lock_busy",
                )
            )

            clock_ms[0] = 1_000_040
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert rebuilt is not None
        self.assertEqual(rebuilt.window_build_mode, "full_rescan")
        self.assertEqual(
            rebuilt.window_full_rescan_reason,
            "late_visible_append",
        )
        self.assertEqual(ledger.full_snapshot_calls, 2)
        self.assertIn(
            entry.pending_share.share_id,
            {str(share["share_id"]) for share in rebuilt.shares_json},
        )

    def test_replay_shaped_batch_commits_under_the_landing_fence(self) -> None:
        # The durable append and its epoch bump must share the landing fence
        # boundary: a bump that only starts after append_batch() returned
        # leaves a gap where a landing verifies the pre-bump epoch and enters
        # submitblock with the row already durable but unpaid. A batch holding
        # a replay-shaped row therefore holds the fence across the ledger
        # commit itself; an ordinary batch stays off the lock entirely.
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertTrue(server._install_payout_ledger_artifact(advanced))
            anchor_ms = int(advanced.snapshot_anchor_ms)

            fence_held_during_commit: list[bool] = []
            original_append_batch = ledger.append_batch

            def recording_append_batch(entries: list[object]) -> list[object]:
                fence_held_during_commit.append(
                    server._payout_append_landing_fence_lock.locked()
                )
                return original_append_batch(entries)

            ledger.append_batch = recording_append_batch  # type: ignore[method-assign]

            fresh = self.replay_shaped_append_entry(
                accepted_at_ms=anchor_ms + 50,
                block_hash_hex="88" * 32,
            )
            self.assertTrue(server._append_share_batch([fresh]))
            self.assertEqual(fence_held_during_commit, [False])
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 0
                )

            predating = self.replay_shaped_append_entry(
                accepted_at_ms=anchor_ms - 10,
                block_hash_hex="99" * 32,
            )
            self.assertTrue(server._append_share_batch([predating]))
            self.assertEqual(fence_held_during_commit, [False, True])
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 1
                )
                self.assertIsNone(server._payout_ledger_artifact)
            self.assertIsNone(server._incremental_payout_artifact_window)

    def test_replay_append_during_cold_scan_invalidates_inflight_walk(
        self,
    ) -> None:
        # A cold-start walk exposes no cached window or armed artifact while
        # its ledger read runs. A replayed row committing mid-walk with
        # timestamps below the walk's anchor must still record an
        # invalidation, or the walk installs a window that can never
        # rediscover the row through delta reads.
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        entry = self.replay_shaped_append_entry(
            accepted_at_ms=999_950,
            block_hash_hex="55" * 32,
        )
        original_snapshot = ledger.snapshot_at_job_issue
        raced = threading.Event()
        append_threads: list[threading.Thread] = []

        def racing_snapshot(
            anchor_job_issued_at_ms: int,
            *,
            window_weight: int | None = None,
        ) -> list[object]:
            # Capture the read result first: the replayed row committing
            # afterwards models a database snapshot taken before the commit.
            records = original_snapshot(
                anchor_job_issued_at_ms,
                window_weight=window_weight,
            )
            if not raced.is_set():
                raced.set()
                thread = threading.Thread(
                    target=server._append_share_batch,
                    args=([entry],),
                )
                thread.start()
                append_threads.append(thread)
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    with server._job_cache_lock:
                        if server._payout_ledger_append_invalidation_epoch:
                            break
                    time.sleep(0.001)
            return records

        ledger.snapshot_at_job_issue = racing_snapshot  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")
            self.assertNotIn(
                entry.pending_share.share_id,
                {str(share["share_id"]) for share in initial.shares_json},
            )
            self.assertTrue(append_threads)
            append_threads[0].join(timeout=2.0)
            self.assertFalse(append_threads[0].is_alive())
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 1
                )
            self.assertFalse(server._install_payout_ledger_artifact(initial))
            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)

            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_040
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert rebuilt is not None
        self.assertEqual(rebuilt.window_build_mode, "full_rescan")
        self.assertEqual(
            rebuilt.window_full_rescan_reason,
            "late_visible_append",
        )
        self.assertIn(
            entry.pending_share.share_id,
            {str(share["share_id"]) for share in rebuilt.shares_json},
        )

    def test_inflight_scan_anchor_records_invalidation_without_windows(
        self,
    ) -> None:
        # Directly exercise the published in-flight anchor: with no cached
        # window and no armed artifact, only a predating append may advance
        # the epoch, and fresh shares above the anchor must leave the walk
        # undisturbed.
        server, _ledger, _artifacts = self.configured_server()
        server._ensure_job_cache_state()
        predating = stamped_pending_share(999_950)
        fresh = stamped_pending_share(1_000_050)

        server._invalidate_incremental_payout_window_for_append(predating)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 0)

        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            token = server._expose_inflight_scan_anchor(999_999)
        try:
            server._invalidate_incremental_payout_window_for_append(fresh)
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 0
                )
            server._invalidate_incremental_payout_window_for_append(predating)
        finally:
            server._retire_inflight_scan_anchor(token)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
            self.assertEqual(server._payout_window_inflight_scan_anchors, {})
        self.assertEqual(
            server._incremental_payout_artifact_window_invalidation_reason,
            "late_visible_append",
        )

    def test_replay_append_during_bundle_construction_invalidates_build(
        self,
    ) -> None:
        # A cold synchronous ready build exposes no cached window or armed
        # artifact once its ledger read returns, while manifest construction
        # and cache publication still lie ahead. A replayed row committing in
        # that gap with timestamps below the read's anchor must still advance
        # the epoch -- the exposure outlives the read -- so the publication
        # fence supersedes the bundle instead of seeding a window that delta
        # reads can never rediscover the row from.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        clock_ms = [1_000_000]
        entry = self.replay_shaped_append_entry(
            accepted_at_ms=999_950,
            block_hash_hex="66" * 32,
        )
        raced = threading.Event()
        original_build = server.build_audit_bundle

        def racing_build(**kwargs: object) -> dict[str, object]:
            # The ledger read has already returned; the replayed row commits
            # while the coinbase manifest is being constructed.
            if not raced.is_set():
                raced.set()
                server._append_share_batch([entry])
            return original_build(**kwargs)

        server.build_audit_bundle = racing_build  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            with self.assertRaises(JobBuildSuperseded):
                server.build_shared_job_bundle(
                    server.current_template_artifacts(),
                    worker(),
                )
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 1
                )
                self.assertIsNone(server._payout_ledger_artifact)

            # A fresh build keyed to the advanced epoch re-reads the ledger
            # and rediscovers the replayed row.
            with server._job_cache_lock:
                server._job_build_issued_at_ms.clear()
            clock_ms[0] = 1_000_040
            rebuilt = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
        self.assertIn(
            entry.pending_share.share_id,
            {str(share["share_id"]) for share in rebuilt.shares_json},
        )

    def test_seeded_artifact_exposure_survives_until_install_fence(
        self,
    ) -> None:
        # The synchronous build's exposed scan anchor rides its seeded
        # artifact: a replayed row committing after the bundle returned but
        # before cache publication installs the seed must advance the epoch,
        # and the doomed seed's install fence must discard it (retiring the
        # exposure) instead of arming the pre-append window.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            bundle = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
            seed = bundle.prepared_ledger_artifact
            assert seed is not None
            self.assertIsNotNone(seed.inflight_scan_anchor_token)

            entry = self.replay_shaped_append_entry(
                accepted_at_ms=999_950,
                block_hash_hex="77" * 32,
            )
            self.assertTrue(server._append_share_batch([entry]))
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 1
                )

            self.assertFalse(server._install_payout_ledger_artifact(seed))
        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)
            self.assertEqual(server._payout_window_inflight_scan_anchors, {})

    def test_late_append_disarms_before_prepare_lock_is_available(self) -> None:
        server, _ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            self.assertTrue(server._install_payout_ledger_artifact(artifact))
            anchor_ms = int(artifact.snapshot_anchor_ms)
            pending = stamped_pending_share(anchor_ms - 10)

            server._payout_state_prepare_lock.acquire()
            # Exercise the artifact anchor independently: an already-cold
            # incremental cache must not hide an unsafe armed artifact.
            server._incremental_payout_artifact_window = None
            invalidation_finished = threading.Event()

            def invalidate() -> None:
                server._invalidate_incremental_payout_window_for_append(
                    pending
                )
                invalidation_finished.set()

            thread = threading.Thread(target=invalidate)
            thread.start()
            try:
                deadline = time.monotonic() + 1.0
                while time.monotonic() < deadline:
                    with server._job_cache_lock:
                        if server._payout_ledger_artifact is None:
                            break
                    time.sleep(0.001)
                with server._job_cache_lock:
                    self.assertIsNone(server._payout_ledger_artifact)
                    self.assertGreater(
                        server._payout_ledger_append_invalidation_epoch,
                        artifact.append_invalidation_epoch,
                    )
                self.assertFalse(invalidation_finished.is_set())
                self.assertFalse(
                    server._install_payout_ledger_artifact(artifact)
                )
            finally:
                server._payout_state_prepare_lock.release()
                thread.join(timeout=1.0)
            self.assertFalse(thread.is_alive())
            self.assertTrue(invalidation_finished.is_set())

    def test_atomic_publication_rejects_pre_append_artifact(self) -> None:
        server, _ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            candidate = server._current_payout_state_candidate()
            stale = candidate.ledger_artifact
            assert stale is not None
            anchor_ms = int(stale.snapshot_anchor_ms)
            server._invalidate_incremental_payout_window_for_append(
                stamped_pending_share(anchor_ms - 10)
            )
            with server._payout_artifact_executor_lock:
                server._payout_artifact_executor_shutdown = True
            published = server._publish_payout_state_candidate(candidate)

        self.assertIsNotNone(published)
        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)
            self.assertGreater(
                server._payout_ledger_append_invalidation_epoch,
                stale.append_invalidation_epoch,
            )

    def test_late_append_supersedes_in_flight_sync_seed_bundle(self) -> None:
        server, _ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            self.assertTrue(server._install_payout_ledger_artifact(artifact))
            pending = stamped_pending_share(
                int(artifact.snapshot_anchor_ms) - 10
            )

            def invalidating_builder(**kwargs: object) -> dict[str, object]:
                server._invalidate_incremental_payout_window_for_append(
                    pending
                )
                suffix_hex = str(kwargs["coinbase_script_sig_suffix_hex"])
                return {
                    "found_block": dict(kwargs["found_block"]),
                    "payout_policy_manifest": {"accounts": []},
                    "signed_coinbase_manifest": {
                        "manifest": {
                            "coinbase_tx_hex": synthetic_manifest_coinbase_hex(
                                suffix_hex
                            ),
                        }
                    },
                }

            server.build_audit_bundle = invalidating_builder  # type: ignore[method-assign]
            with self.assertRaises(JobBuildSuperseded):
                server.build_shared_job_bundle(
                    artifacts,
                    worker(),
                    payout_artifact=None,
                )

        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)

    def test_normal_append_preserves_incremental_window(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            cached = server._incremental_payout_artifact_window
            self.assertIsNotNone(cached)
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            with server._job_cache_lock:
                armed = server._payout_ledger_artifact
                append_epoch = (
                    server._payout_ledger_append_invalidation_epoch
                )
            self.assertIsNotNone(armed)

            entry = self.replay_shaped_append_entry(
                accepted_at_ms=1_000_010,
                block_hash_hex="55" * 32,
            )
            self.assertTrue(server._append_share_entry(entry))
            self.assertIs(server._incremental_payout_artifact_window, cached)
            with server._job_cache_lock:
                self.assertIs(server._payout_ledger_artifact, armed)
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch,
                    append_epoch,
                )
            self.assertIs(
                server._usable_payout_ledger_artifact(
                    0,
                    artifacts.network_difficulty,
                ),
                armed,
            )

            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert advanced is not None
        self.assertEqual(advanced.window_build_mode, "incremental")
        self.assertIsNone(advanced.window_full_rescan_reason)
        self.assertEqual(advanced.window_touched_pages, 1)
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 1)
        self.assertIn(
            entry.pending_share.share_id,
            {str(share["share_id"]) for share in advanced.shares_json},
        )

    def test_debounce_then_delta_and_forced_full_rescan(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")
            self.assertEqual(ledger.full_snapshot_calls, 1)

            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            debounced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert debounced is not None
            self.assertEqual(debounced.window_build_mode, "debounced")
            self.assertEqual(ledger.full_snapshot_calls, 1)
            self.assertEqual(ledger.delta_snapshot_calls, 0)
            self.assertEqual(len(debounced.shares_json), 3)

            server.payout_artifact_min_build_interval_seconds = 0.0
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertEqual(advanced.window_delta_rows, 1)
            self.assertEqual(ledger.delta_snapshot_calls, 1)
            self.assertEqual(len(advanced.shares_json), 4)

            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )
            assert forced is not None
            self.assertEqual(forced.window_build_mode, "full_rescan")
            self.assertEqual(
                forced.window_full_rescan_reason,
                "reconcile_invalidation",
            )
            self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_periodic_self_check_compares_delta_to_full_oracle(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            server.payout_artifact_min_build_interval_seconds = 0.0
            server.payout_artifact_full_rescan_seconds = 0.0

            checked = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert checked is not None
        self.assertEqual(checked.window_build_mode, "self_check_match")
        self.assertEqual(ledger.delta_snapshot_calls, 1)
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_routine_build_runs_overdue_self_check_despite_preview_refresh(
        self,
    ) -> None:
        # Accepted-block previews bypass the build interval and refresh
        # refreshed_monotonic on every block, yet never run the periodic
        # oracle themselves. Under a sustained sub-interval block cadence
        # every routine build used to return from the debounce branch
        # before reaching the self-check condition, so the configured
        # full-rescan window and balance oracle could be postponed
        # indefinitely. An overdue self-check now disarms the debounce
        # for routine builds while the urgent preview path stays
        # debounce-free and oracle-free.
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            preview = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty, False, True
            )
            assert preview is not None
            self.assertEqual(preview.window_build_mode, "incremental")
            # The next preview arrives with the self-check long overdue;
            # the urgent path must still not pay the oracle walk.
            server.payout_artifact_full_rescan_seconds = 0.0
            append_incremental_share(
                ledger,
                share_seq=5,
                accepted_at_ms=1_000_030,
            )
            clock_ms[0] = 1_000_040
            urgent = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty, False, True
            )
            assert urgent is not None
            self.assertEqual(urgent.window_build_mode, "incremental")
            self.assertEqual(ledger.full_snapshot_calls, 1)
            # A routine build inside the min build interval used to
            # debounce here -- forever, while blocks kept refreshing the
            # window above -- and the self-check never ran.
            checked = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert checked is not None
        self.assertEqual(checked.window_build_mode, "self_check_match")
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_periodic_self_check_replaces_divergent_delta_with_full_bytes(
        self,
    ) -> None:
        class OmittingDeltaLedger(IncrementalRecordingLedger):
            def snapshot_between_job_issues(
                self,
                previous_anchor_job_issued_at_ms: int,
                anchor_job_issued_at_ms: int,
            ) -> list[object]:
                del previous_anchor_job_issued_at_ms, anchor_job_issued_at_ms
                self.delta_snapshot_calls += 1
                return []

        ledger = OmittingDeltaLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            server.payout_artifact_min_build_interval_seconds = 0.0
            server.payout_artifact_full_rescan_seconds = 0.0
            repaired = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )

        assert repaired is not None and forced is not None
        self.assertEqual(repaired.window_build_mode, "self_check_mismatch")
        self.assertEqual(list(repaired.shares_json), list(forced.shares_json))
        self.assertEqual(
            repaired.share_snapshot_sha256,
            forced.share_snapshot_sha256,
        )

    def test_periodic_self_check_invalidates_drifted_published_balances(
        self,
    ) -> None:
        class DriftingCarryLedger(IncrementalRecordingLedger):
            def __init__(self) -> None:
                super().__init__()
                self.balance_sats = 546
                self.prior_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.prior_balance_reads += 1
                return [
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": self.balance_sats,
                    }
                ]

        ledger = DriftingCarryLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        build_logs: list[dict[str, object]] = []
        server._payout_artifact_log = (  # type: ignore[method-assign]
            lambda event, **fields: build_logs.append(
                {"event": event, **fields}
            )
        )

        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            published = server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))

            ledger.balance_sats = 777
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            monotonic_seconds[0] = 3_701.0
            checked = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )

            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)

            # The mismatch build publishes the repaired balances for the same
            # generation, so the next build resumes byte reuse from the
            # repaired snapshot instead of paying another carry read against
            # a still-stale publication.
            clock_ms[0] = 1_000_030
            monotonic_seconds[0] = 3_702.0
            following = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )

        assert checked is not None and following is not None
        self.assertEqual(checked.window_build_mode, "self_check_match")
        self.assertEqual(checked.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(following.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(ledger.prior_balance_reads, 2)
        self.assertEqual(ledger.full_snapshot_calls, 2)
        repaired = server._published_payout_state.artifact
        assert repaired is not None
        self.assertNotEqual(
            repaired.prior_balances_sha256,
            published.prior_balances_sha256,
        )
        self.assertEqual(
            repaired.prior_balances_sha256,
            checked.prior_balances_sha256,
        )
        self.assertIsNone(
            server._payout_prior_balances_reuse_invalidated_sha256
        )
        self.assertEqual(
            server.payout_artifact_event_counts["balance_check_mismatch"],
            1,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["balance_repair_published"],
            1,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["self_check_match"],
            1,
        )
        built_sources = [
            entry["prior_balances_source"]
            for entry in build_logs
            if entry["event"] == "payout_artifact_built"
        ]
        self.assertEqual(built_sources, ["published", "ledger", "published"])
        mismatch_logs = [
            entry
            for entry in build_logs
            if entry["event"] == "payout_artifact_balance_check_mismatch"
        ]
        self.assertEqual(len(mismatch_logs), 1)
        repair_logs = [
            entry
            for entry in build_logs
            if entry["event"] == "payout_artifact_balance_repair_published"
        ]
        self.assertEqual(len(repair_logs), 1)

    def test_balance_backstop_failure_retains_oracle_repaired_window(
        self,
    ) -> None:
        # The share-window oracle and the carry-balance backstop are
        # independent reads. A backstop failure after a successful oracle
        # must not resurrect the delta window the oracle just refuted.
        class FailingCarryOmittingDeltaLedger(IncrementalRecordingLedger):
            def __init__(self) -> None:
                super().__init__()
                self.fail_balance_read = False
                self.balance_read_attempts = 0

            def snapshot_between_job_issues(
                self,
                previous_anchor_job_issued_at_ms: int,
                anchor_job_issued_at_ms: int,
            ) -> list[object]:
                del previous_anchor_job_issued_at_ms, anchor_job_issued_at_ms
                self.delta_snapshot_calls += 1
                return []

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.balance_read_attempts += 1
                if self.fail_balance_read:
                    raise RuntimeError("carry aggregate unavailable")
                return [
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": 546,
                    }
                ]

        ledger = FailingCarryOmittingDeltaLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))

            ledger.fail_balance_read = True
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            monotonic_seconds[0] = 3_701.0
            checked = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )

            append_incremental_share(
                ledger,
                share_seq=5,
                accepted_at_ms=1_000_021,
            )
            clock_ms[0] = 1_000_030
            monotonic_seconds[0] = 3_702.0
            following = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )

        assert checked is not None and following is not None
        self.assertEqual(checked.window_build_mode, "self_check_mismatch")
        self.assertEqual(
            checked.window_full_rescan_reason,
            "periodic_self_check_balance_check_failed",
        )
        # The omitting delta produced three retained shares; the oracle's
        # four-share window must be the one served and cached.
        self.assertEqual(len(checked.shares_json), 4)
        # One read published the priming state; the only other attempt is
        # the backstop that failed.
        self.assertEqual(ledger.balance_read_attempts, 2)
        # The unverified balances keep reusing the published snapshot.
        self.assertEqual(
            checked.prior_balances_sha256,
            initial.prior_balances_sha256,
        )
        self.assertIsNone(
            server._payout_prior_balances_reuse_invalidated_sha256
        )
        self.assertEqual(
            server.payout_artifact_event_counts["self_check_mismatch"],
            1,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["self_check_failed"],
            0,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["balance_check_mismatch"],
            0,
        )
        # The failed backstop still stamps the attempt: the next build stays
        # on the delta path instead of retrying the oracle immediately.
        self.assertEqual(following.window_build_mode, "incremental")
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_periodic_self_check_repair_arms_and_serves(self) -> None:
        # The repaired publication is what lets the mismatch build's fresh
        # balances actually reach miners: its artifact must arm against the
        # repaired digest and the reuse probe must serve it, instead of the
        # armed-artifact fence clearing the repair against a stale published
        # snapshot until an unrelated generation change.
        class DriftingCarryLedger(IncrementalRecordingLedger):
            def __init__(self) -> None:
                super().__init__()
                self.balance_sats = 546

            def current_prior_balances(self) -> list[dict[str, object]]:
                return [
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": self.balance_sats,
                    }
                ]

        ledger = DriftingCarryLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]

        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))

            ledger.balance_sats = 777
            clock_ms[0] = 1_000_020
            monotonic_seconds[0] = 3_701.0
            checked = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert checked is not None
            self.assertEqual(checked.prior_balances[0]["balance_sats"], 777)

            self.assertTrue(server._install_payout_ledger_artifact(checked))
            served = server._usable_payout_ledger_artifact(
                0,
                artifacts.network_difficulty,
            )

        assert served is not None
        self.assertEqual(served.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(
            served.prior_balances_sha256,
            checked.prior_balances_sha256,
        )
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)

    def test_self_check_repair_supersedes_active_jobs(self) -> None:
        # The in-place repair changes neither the payout generation nor the
        # append epoch, so an already-stamped job passes every admission
        # fence while carrying the refuted balances. The repair must
        # schedule a refresh wave, and the wave's reselection must identify
        # such jobs by their payout digest, or they stay mineable (and a
        # block solve commits the stale allocation) until an unrelated
        # generation change.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            bundle = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
            context = server.stamp_job_for_client(
                state,
                bundle,
                clean_jobs=True,
            )
        state.active_job = context
        snapshot = SimpleNamespace(
            bestblockhash=str(bundle.template["previousblockhash"]),
            previousblockhash=str(bundle.template["previousblockhash"]),
            template_fingerprint=bundle.template_fingerprint,
        )
        with server._job_cache_lock:
            published = server._published_payout_state.artifact
        assert published is not None
        self.assertEqual(
            context.payout_artifact_sha256,
            published.prior_balances_sha256,
        )
        self.assertFalse(
            server.client_needs_tip_template_refresh(state, snapshot)
        )

        self.assertTrue(
            server._publish_self_check_repaired_balances(
                0,
                stale_prior_balances_sha256=published.prior_balances_sha256,
                balances=[
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": 999,
                    }
                ],
            )
        )

        self.assertTrue(
            server.client_needs_tip_template_refresh(state, snapshot)
        )
        self.assertTrue(server._tip_refresh_pending_event.is_set())
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_self_check_repair_retires_refuted_jobs_clean(self) -> None:
        # Reselection alone is not enough: the wave stamps the replacement
        # with clean_jobs from client_tip_changed_for_snapshot, and a
        # clean_jobs=False delivery leaves the refuted job ID admissible in
        # active_job_ids. The digest mismatch must force clean-job
        # retirement, or a late block submission on the old job still
        # commits the refuted allocation after corrected work is delivered.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            bundle = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
            context = server.stamp_job_for_client(
                state,
                bundle,
                clean_jobs=True,
            )
        state.active_job = context
        snapshot = SimpleNamespace(
            bestblockhash=str(bundle.template["previousblockhash"]),
            previousblockhash=str(bundle.template["previousblockhash"]),
            template_fingerprint=bundle.template_fingerprint,
        )
        with server._job_cache_lock:
            published = server._published_payout_state.artifact
        assert published is not None
        self.assertFalse(
            server.client_tip_changed_for_snapshot(state, snapshot)
        )

        self.assertTrue(
            server._publish_self_check_repaired_balances(
                0,
                stale_prior_balances_sha256=published.prior_balances_sha256,
                balances=[
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": 999,
                    }
                ],
            )
        )

        self.assertTrue(
            server.client_tip_changed_for_snapshot(state, snapshot)
        )

    def test_late_append_invalidation_supersedes_active_jobs(self) -> None:
        # A late-visible append advances only the append epoch: the payout
        # generation and published digest both survive, so an active job
        # mining the pre-append window passes every other fence. The
        # invalidation must schedule a refresh wave, the reselection must
        # identify the job by its stamped epoch, and the replacement must
        # retire it clean -- otherwise miners keep working (and can solve a
        # block from) a window that omitted the late share until some
        # unrelated tip event.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            bundle = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
            context = server.stamp_job_for_client(
                state,
                bundle,
                clean_jobs=True,
            )
        state.active_job = context
        snapshot = SimpleNamespace(
            bestblockhash=str(bundle.template["previousblockhash"]),
            previousblockhash=str(bundle.template["previousblockhash"]),
            template_fingerprint=bundle.template_fingerprint,
        )
        self.assertFalse(
            server.client_needs_tip_template_refresh(state, snapshot)
        )
        self.assertFalse(
            server.client_tip_changed_for_snapshot(state, snapshot)
        )

        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            token = server._expose_inflight_scan_anchor(999_999)
        try:
            server._invalidate_incremental_payout_window_for_append(
                stamped_pending_share(999_950)
            )
        finally:
            server._retire_inflight_scan_anchor(token)
        with server._job_cache_lock:
            self.assertEqual(
                server._payout_ledger_append_invalidation_epoch, 1
            )

        self.assertTrue(
            server.client_needs_tip_template_refresh(state, snapshot)
        )
        self.assertTrue(
            server.client_tip_changed_for_snapshot(state, snapshot)
        )
        self.assertTrue(server._tip_refresh_pending_event.is_set())
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_direct_delivery_rechecks_published_digest_at_commit(self) -> None:
        # A periodic self-check repair swaps the published balance snapshot
        # in place: neither the payout generation nor the append epoch moves,
        # so only the digest identifies a direct-delivery context keyed to
        # the refuted balances. When the repair lands after the build but
        # before the commit boundary, the delivery must re-check the digest
        # or the miner's active job carries the refuted allocation.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]
        original_build_job_for_client = server.build_job_for_client
        repaired = False

        def repair_after_build(*args: object, **kwargs: object) -> object:
            nonlocal repaired
            context = original_build_job_for_client(*args, **kwargs)  # type: ignore[arg-type]
            if not repaired:
                repaired = True
                with server._job_cache_lock:
                    published = server._published_payout_state.artifact
                assert published is not None
                self.assertEqual(
                    context.payout_artifact_sha256,
                    published.prior_balances_sha256,
                )
                self.assertTrue(
                    server._publish_self_check_repaired_balances(
                        0,
                        stale_prior_balances_sha256=(
                            published.prior_balances_sha256
                        ),
                        balances=[
                            {
                                "recipient_id": "carry",
                                "order_key": "01:carry",
                                "p2mr_program_hex": "66" * 32,
                                "balance_sats": 999,
                            }
                        ],
                    )
                )
            return context

        server.build_job_for_client = repair_after_build  # type: ignore[method-assign]

        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(sent, [])
        self.assertIsNone(state.active_job)
        self.assertEqual(state.active_job_ids, set())
        self.assertEqual(server.jobs, {})

    def test_incremental_and_forced_full_artifacts_match_with_carry_boundaries(
        self,
    ) -> None:
        class CarryLedger(IncrementalRecordingLedger):
            def current_prior_balances(self) -> list[dict[str, object]]:
                return [
                    {
                        "recipient_id": "below",
                        "order_key": "01:below",
                        "p2mr_program_hex": "11" * 32,
                        "balance_sats": 545,
                    },
                    {
                        "recipient_id": "at",
                        "order_key": "02:at",
                        "p2mr_program_hex": "22" * 32,
                        "balance_sats": 546,
                    },
                    {
                        "recipient_id": "above",
                        "order_key": "03:above",
                        "p2mr_program_hex": "33" * 32,
                        "balance_sats": 547,
                    },
                ]

        ledger = CarryLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            incremental = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )

        assert incremental is not None and forced is not None
        incremental_bytes = canonical_json_text(
            {
                "shares": list(incremental.shares_json),
                "prior_balances": list(incremental.prior_balances),
            }
        ).encode()
        forced_bytes = canonical_json_text(
            {
                "shares": list(forced.shares_json),
                "prior_balances": list(forced.prior_balances),
            }
        ).encode()
        self.assertEqual(incremental_bytes, forced_bytes)
        self.assertEqual(
            incremental.share_snapshot_sha256,
            forced.share_snapshot_sha256,
        )
        self.assertEqual(
            incremental.prior_balances_sha256,
            forced.prior_balances_sha256,
        )

    def test_normal_refresh_reuses_generation_owned_prior_balances(self) -> None:
        class CountingCarryLedger(IncrementalRecordingLedger):
            prior_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.prior_balance_reads += 1
                return [
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": 545,
                    }
                ]

        ledger = CountingCarryLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            published = server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            incremental = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )

        assert initial is not None and incremental is not None and forced is not None
        self.assertEqual(ledger.prior_balance_reads, 2)
        self.assertEqual(
            initial.prior_balances_sha256,
            published.prior_balances_sha256,
        )
        self.assertEqual(
            incremental.prior_balances_sha256,
            published.prior_balances_sha256,
        )

    def test_fifteen_second_generation_bumps_refresh_once_per_minute(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        modes: list[str | None] = []
        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            modes.append(initial.window_build_mode)
            for bump in range(1, 5):
                monotonic_seconds[0] = 100.0 + (15.0 * bump)
                clock_ms[0] = 1_000_000 + (15_000 * bump)
                append_incremental_share(
                    ledger,
                    share_seq=3 + bump,
                    accepted_at_ms=clock_ms[0] - 2,
                )
                built = server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
                assert built is not None
                modes.append(built.window_build_mode)

        self.assertEqual(
            modes,
            ["full_rescan", "debounced", "debounced", "debounced", "incremental"],
        )
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 1)

    def test_found_block_candidate_bypasses_normal_interval(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            # Even an overdue routine oracle must be deferred from the
            # immediate found-block publication path.
            server.payout_artifact_full_rescan_seconds = 0.0
            clock_ms[0] = 1_000_020
            candidate = server._prepared_payout_state_candidate(
                (
                    0,
                    1,
                    str(server.rpc.tip),
                    "accepted_block_preview",
                    time.monotonic(),
                )
            )
            candidate = server._accepted_block_preview_candidate(
                candidate,
                block_hash="44" * 32,
                preview=(("winner", "01:winner", "55" * 32, 546),),
            )

        artifact = candidate.ledger_artifact
        assert artifact is not None
        self.assertEqual(artifact.window_build_mode, "incremental")
        self.assertEqual(artifact.window_delta_rows, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 1)
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(len(artifact.shares_json), 4)

    def test_failed_periodic_oracle_is_spaced_from_next_delta_build(self) -> None:
        class FailingOracleLedger(IncrementalRecordingLedger):
            fail_full_snapshot = False

            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[object]:
                if self.fail_full_snapshot:
                    self.full_snapshot_calls += 1
                    raise RuntimeError("oracle unavailable")
                return super().snapshot_at_job_issue(
                    anchor_job_issued_at_ms,
                    window_weight=window_weight,
                )

        ledger = FailingOracleLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 10.0
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            ledger.fail_full_snapshot = True
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            monotonic_seconds[0] = 110.0
            failed_check = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert failed_check is not None
            self.assertEqual(
                failed_check.window_build_mode,
                "incremental_self_check_failed",
            )

            append_incremental_share(
                ledger,
                share_seq=5,
                accepted_at_ms=1_000_021,
            )
            clock_ms[0] = 1_000_030
            monotonic_seconds[0] = 111.0
            next_delta = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert next_delta is not None
        self.assertEqual(next_delta.window_build_mode, "incremental")
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_found_block_reuses_armed_window_when_prepare_lock_is_busy(
        self,
    ) -> None:
        class BusyPrepareLock:
            def acquire(self, blocking: bool = True) -> bool:
                self.assert_nonblocking(blocking)
                return False

            @staticmethod
            def assert_nonblocking(blocking: bool) -> None:
                if blocking:
                    raise AssertionError("found-block path attempted to wait")

            def release(self) -> None:
                raise AssertionError("unacquired lock was released")

        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            server._payout_state_prepare_lock = BusyPrepareLock()  # type: ignore[assignment]
            candidate = server._prepared_payout_state_candidate(
                (
                    0,
                    1,
                    str(server.rpc.tip),
                    "accepted_block_preview",
                    time.monotonic(),
                )
            )
            candidate = server._accepted_block_preview_candidate(
                candidate,
                block_hash="44" * 32,
                preview=(("winner", "01:winner", "55" * 32, 546),),
            )

        artifact = candidate.ledger_artifact
        assert artifact is not None
        self.assertEqual(artifact.window_build_mode, "found_block_cached")
        self.assertEqual(artifact.payout_state_generation, 1)
        self.assertEqual(
            list(artifact.prior_balances),
            [
                {
                    "recipient_id": "winner",
                    "order_key": "01:winner",
                    "p2mr_program_hex": "55" * 32,
                    "balance_sats": 546,
                }
            ],
        )
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 0)
        self.assertEqual(
            server.payout_artifact_event_counts["found_block_cached"],
            1,
        )

    def test_found_block_build_failure_is_visible_and_reuses_exact_window(
        self,
    ) -> None:
        class FailingDeltaLedger(IncrementalRecordingLedger):
            fail_delta = False

            def snapshot_between_job_issues(
                self,
                previous_anchor_job_issued_at_ms: int,
                anchor_job_issued_at_ms: int,
            ) -> list[object]:
                if self.fail_delta:
                    self.delta_snapshot_calls += 1
                    raise RuntimeError("delta unavailable")
                return super().snapshot_between_job_issues(
                    previous_anchor_job_issued_at_ms,
                    anchor_job_issued_at_ms,
                )

        ledger = FailingDeltaLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            ledger.fail_delta = True
            clock_ms[0] = 1_000_020
            candidate = server._prepared_payout_state_candidate(
                (
                    0,
                    1,
                    str(server.rpc.tip),
                    "accepted_block_preview",
                    time.monotonic(),
                )
            )

        artifact = candidate.ledger_artifact
        assert artifact is not None
        self.assertEqual(artifact.window_build_mode, "found_block_cached")
        self.assertEqual(
            server.payout_artifact_event_counts["build_aborted"],
            1,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["found_block_cached"],
            1,
        )

    def test_old_anchor_does_not_rearm_before_window_cadence(self) -> None:
        class RecordingExecutor:
            def __init__(self) -> None:
                self.submissions = 0

            def submit(self, _fn: object) -> Future[None]:
                self.submissions += 1
                return Future()

        server, _ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            executor = RecordingExecutor()
            with server._payout_artifact_executor_lock:
                server._payout_artifact_executor = executor  # type: ignore[assignment]

            # A pending-share floor can leave the declared anchor old even
            # though its exact pages were just re-proved. Past-floor probes
            # must not enqueue five-second debounced balance-only builds.
            clock_ms[0] = 1_061_000
            monotonic_seconds[0] = 105.0
            self.assertIsNotNone(
                server._usable_payout_ledger_artifact(
                    0,
                    artifacts.network_difficulty,
                )
            )
            with server._payout_artifact_executor_lock:
                self.assertIsNone(server._payout_artifact_requested)

            monotonic_seconds[0] = 160.0
            self.assertIsNotNone(
                server._usable_payout_ledger_artifact(
                    0,
                    artifacts.network_difficulty,
                )
            )

        with server._payout_artifact_executor_lock:
            self.assertEqual(
                server._payout_artifact_requested,
                (0, int(artifacts.network_difficulty)),
            )
            self.assertEqual(executor.submissions, 1)


class UnfencedAppendDrainTests(unittest.TestCase):
    def test_drain_waits_in_stamped_slices_without_returning_early(self) -> None:
        """The drain stays unbounded; only its silence is broken up.

        A landing calls this inline on the watchdog-monitored block-work
        thread right after exposing its declared anchor. Bounding the wait is
        not an option -- it enforces the fencing contract, and returning
        early would let the landing arm its epoch fences ahead of an append
        that must be drained first -- so the fix is to wait in
        watchdog-sized slices and stamp each one. The ``while`` re-check
        makes the timed wait semantically identical to the untimed one.
        """
        server, _rpc = coordinator()
        server._ensure_job_cache_state()
        stamps: list[tuple[str, object]] = []
        server._record_heartbeat = (  # type: ignore[method-assign]
            lambda name, phase=None: stamps.append((name, phase))
        )
        server._block_work_wait_slice = lambda: 0.01  # type: ignore[method-assign]
        anchor = 1000
        with server._payout_unfenced_append_drained:
            server._payout_unfenced_append_inflight_stamps[1] = anchor - 1
        returned = threading.Event()

        def drain() -> None:
            server._block_submitter_thread_ident = threading.get_ident()
            server._await_unfenced_appends_predating_anchor(anchor)
            returned.set()

        worker_thread = threading.Thread(target=drain)
        worker_thread.start()
        try:
            # The predating append is still in flight: no number of expired
            # slices may let the drain return.
            self.assertFalse(returned.wait(0.2))
            drain_stamps = [
                stamp
                for stamp in stamps
                if stamp == ("block_submitter", "wait-unfenced-append-drain")
            ]
            self.assertGreaterEqual(len(drain_stamps), 2)
            with server._payout_unfenced_append_drained:
                server._payout_unfenced_append_inflight_stamps.pop(1, None)
                server._payout_unfenced_append_drained.notify_all()
            self.assertTrue(returned.wait(5))
        finally:
            with server._payout_unfenced_append_drained:
                server._payout_unfenced_append_inflight_stamps.pop(1, None)
                server._payout_unfenced_append_drained.notify_all()
            worker_thread.join(5)

    def test_drain_with_nothing_in_flight_stamps_nothing(self) -> None:
        """The common path never enters the loop, so it never stamps."""
        server, _rpc = coordinator()
        server._ensure_job_cache_state()
        stamps: list[object] = []
        server._record_heartbeat = (  # type: ignore[method-assign]
            lambda name, phase=None: stamps.append((name, phase))
        )
        server._block_submitter_thread_ident = threading.get_ident()
        server._await_unfenced_appends_predating_anchor(1000)
        self.assertEqual(stamps, [])


if __name__ == "__main__":
    unittest.main()
