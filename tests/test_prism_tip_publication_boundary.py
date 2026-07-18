#!/usr/bin/env python3
"""Regressions for detected-tip versus published-tip refresh authority."""

from __future__ import annotations

import time
import unittest

from lab.prism.prism_coordinator import TemplateRefreshBlocked
from tests.test_prism_coordinator_job_cache import (
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
)


class TipPublicationBoundaryTests(unittest.TestCase):
    def test_final_validation_failure_keeps_previous_authority_and_snapshot(self) -> None:
        server, rpc = coordinator()
        builds = install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            published_tip = server.current_tip_first_seen
            published_snapshot = server.tip_template_snapshot
            initial_builds = int(builds["calls"])

            next_tip = "33" * 32
            rpc.tip = next_tip
            rpc.template = base_template(height=11, prevhash=next_tip)
            validation_reached = False

            def fail_final_validation(*_args: object, **_kwargs: object) -> object:
                nonlocal validation_reached
                validation_reached = True
                raise TemplateRefreshBlocked("forced final validation failure")

            server._validate_prepared_tip_refresh = fail_final_validation  # type: ignore[method-assign]

            with self.assertRaisesRegex(
                TemplateRefreshBlocked,
                "forced final validation failure",
            ):
                server.poll_qbit_tip_template_once()

            self.assertTrue(validation_reached)
            self.assertGreater(int(builds["calls"]), initial_builds)
            self.assertEqual(server.current_tip_first_seen, published_tip)
            self.assertIs(server.tip_template_snapshot, published_snapshot)
            self.assertEqual(server.latest_detected_tip[0], next_tip)
            self.assertIsNone(server._active_tip_refresh)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_newer_detection_before_atomic_activation_prevents_publication(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        sent_tips: list[str] = []

        def record_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                assert state.active_job is not None
                sent_tips.append(
                    str(state.active_job.template["previousblockhash"])
                )

        state.send = record_send  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            published_tip = server.current_tip_first_seen
            published_snapshot = server.tip_template_snapshot
            self.assertEqual(sent_tips, [rpc.tip])

            candidate_tip = "44" * 32
            rpc.tip = candidate_tip
            rpc.template = base_template(height=11, prevhash=candidate_tip)
            sequence = server._reserve_tip_observation_sequence()
            self.assertTrue(
                server.observe_tip_for_refresh(
                    candidate_tip,
                    observation_sequence=sequence,
                    mark_pending=False,
                )
            )
            snapshot = server.fetch_qbit_tip_template_snapshot()
            bundle = server.prepare_tip_refresh_bundle(snapshot)
            token = server._validate_prepared_tip_refresh(
                bundle,
                snapshot,
                sequence,
            )

            winning_tip = "55" * 32
            rpc.tip = winning_tip
            rpc.template = base_template(height=12, prevhash=winning_tip)
            self.assertTrue(server.observe_tip_for_refresh(winning_tip))

            with self.assertRaisesRegex(
                TemplateRefreshBlocked,
                "superseded before atomic publication",
            ):
                server._publish_prepared_tip_refresh(
                    token,
                    bundle,
                    snapshot,
                    parent_hash=None,
                )

            self.assertEqual(server.current_tip_first_seen, published_tip)
            self.assertIs(server.tip_template_snapshot, published_snapshot)
            self.assertEqual(server.latest_detected_tip[0], winning_tip)
            self.assertEqual(len(sent_tips), 1)
            self.assertIsNone(server._active_tip_refresh)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_executor_failure_happens_before_prepared_publication(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            published_tip = server.current_tip_first_seen
            published_snapshot = server.tip_template_snapshot

            next_tip = "66" * 32
            rpc.tip = next_tip
            rpc.template = base_template(height=11, prevhash=next_tip)

            def fail_executor() -> object:
                raise RuntimeError("executor unavailable")

            server.tip_refresh_executor = fail_executor  # type: ignore[method-assign]
            with self.assertRaisesRegex(RuntimeError, "executor unavailable"):
                server.poll_qbit_tip_template_once()

            self.assertEqual(server.current_tip_first_seen, published_tip)
            self.assertIs(server.tip_template_snapshot, published_snapshot)
            self.assertEqual(server.latest_detected_tip[0], next_tip)
            self.assertIsNone(server._active_tip_refresh)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_payout_mutation_before_publication_keeps_previous_authority(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            published_tip = server.current_tip_first_seen
            published_snapshot = server.tip_template_snapshot

            candidate_tip = "77" * 32
            rpc.tip = candidate_tip
            rpc.template = base_template(height=11, prevhash=candidate_tip)
            sequence = server._reserve_tip_observation_sequence()
            self.assertTrue(
                server.observe_tip_for_refresh(
                    candidate_tip,
                    observation_sequence=sequence,
                    mark_pending=False,
                )
            )
            snapshot = server.fetch_qbit_tip_template_snapshot()
            bundle = server.prepare_tip_refresh_bundle(snapshot)
            token = server._validate_prepared_tip_refresh(
                bundle,
                snapshot,
                sequence,
            )
            self.assertEqual(server._advance_payout_state_generation(), 1)

            with self.assertRaisesRegex(
                TemplateRefreshBlocked,
                "superseded before atomic publication",
            ):
                server._publish_prepared_tip_refresh(
                    token,
                    bundle,
                    snapshot,
                    parent_hash=None,
                )

            self.assertEqual(server.current_tip_first_seen, published_tip)
            self.assertIs(server.tip_template_snapshot, published_snapshot)
            self.assertIsNone(server._active_tip_refresh)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_payout_publication_block_keeps_previous_authority(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            published_tip = server.current_tip_first_seen
            published_snapshot = server.tip_template_snapshot

            candidate_tip = "88" * 32
            rpc.tip = candidate_tip
            rpc.template = base_template(height=11, prevhash=candidate_tip)
            sequence = server._reserve_tip_observation_sequence()
            self.assertTrue(
                server.observe_tip_for_refresh(
                    candidate_tip,
                    observation_sequence=sequence,
                    mark_pending=False,
                )
            )
            snapshot = server.fetch_qbit_tip_template_snapshot()
            bundle = server.prepare_tip_refresh_bundle(snapshot)
            token = server._validate_prepared_tip_refresh(
                bundle,
                snapshot,
                sequence,
            )

            server._block_payout_state_publication()
            self.assertTrue(server._payout_state_publication_blocked)
            with self.assertRaisesRegex(
                TemplateRefreshBlocked,
                "superseded before atomic publication",
            ):
                server._publish_prepared_tip_refresh(
                    token,
                    bundle,
                    snapshot,
                    parent_hash=None,
                )

            self.assertEqual(server.current_tip_first_seen, published_tip)
            self.assertIs(server.tip_template_snapshot, published_snapshot)
            self.assertIsNone(server._active_tip_refresh)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_tip_publication_preserves_first_payout_delivery_priority(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            server._reserve_payout_state_source("payout_only")
            self.assertEqual(
                server._publish_payout_state_candidate(
                    server._current_payout_state_candidate()
                ),
                1,
            )
            self.assertEqual(
                server._payout_state_delivery_gate._priority_generation,
                1,
            )

            sequence = server._reserve_tip_observation_sequence()
            self.assertTrue(
                server.observe_tip_for_refresh(
                    rpc.tip,
                    observation_sequence=sequence,
                    mark_pending=False,
                )
            )
            snapshot = server.fetch_qbit_tip_template_snapshot()
            bundle = server.prepare_tip_refresh_bundle(snapshot)
            token = server._validate_prepared_tip_refresh(
                bundle,
                snapshot,
                sequence,
            )
            cancellation = server._publish_prepared_tip_refresh(
                token,
                bundle,
                snapshot,
                parent_hash=None,
            )
            try:
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
            finally:
                server._clear_active_tip_refresh(token, cancellation)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_same_tip_republication_preserves_parent_on_lookup_failure(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            published_tip = server.current_tip_first_seen
            assert published_tip is not None
            cached_parent = (published_tip[0], "aa" * 32)
            with server.lock:
                server.current_tip_parent = cached_parent

            sequence = server._reserve_tip_observation_sequence()
            snapshot = server.fetch_qbit_tip_template_snapshot()
            bundle = server.prepare_tip_refresh_bundle(snapshot)
            token = server._validate_prepared_tip_refresh(bundle, snapshot, sequence)
            cancellation = server._publish_prepared_tip_refresh(
                token,
                bundle,
                snapshot,
                parent_hash=None,
            )
            try:
                # A transient parent lookup failure during a same-tip
                # republication must not wipe the still-valid cached parent.
                self.assertEqual(server.current_tip_parent, cached_parent)
                self.assertEqual(server.current_tip_first_seen[0], published_tip[0])
            finally:
                server._clear_active_tip_refresh(token, cancellation)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_direct_issuance_pins_published_snapshot_during_unpublished_window(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            published_tip = server.current_tip_first_seen
            assert published_tip is not None

            # Detect a replacement tip and let its template fetch install the
            # new global artifacts, exactly like an in-flight refresh does
            # before publication.
            next_tip = "77" * 32
            rpc.tip = next_tip
            rpc.template = base_template(height=11, prevhash=next_tip)
            sequence = server._reserve_tip_observation_sequence()
            self.assertTrue(
                server.observe_tip_for_refresh(
                    next_tip,
                    observation_sequence=sequence,
                    mark_pending=False,
                )
            )
            server.fetch_qbit_tip_template_snapshot()
            with server._job_cache_lock:
                self.assertEqual(
                    server._template_artifacts.previousblockhash,
                    next_tip,
                )

            # Direct issuance stays pinned to the published snapshot while the
            # published tip still owns share classification, so the issued job
            # is immediately creditable.
            artifacts = server.job_issuance_template_artifacts()
            self.assertEqual(artifacts.previousblockhash, published_tip[0])
            context = server.build_job_for_client(state, clean_jobs=False)
            self.assertEqual(
                str(context.template["previousblockhash"]),
                published_tip[0],
            )

            # Delivery-side currency accepts the pinned published snapshot
            # even though the detected-tip globals have moved on, so initial
            # delivery does not defer for the construction window.
            self.assertFalse(server._template_artifacts_are_current(artifacts))
            self.assertTrue(server._issuance_artifacts_current(artifacts))

            # A pruned bundle cache (payout mutation, LRU pressure) must not
            # strand pinned issuance either: published-parent work stays
            # buildable and cacheable while its authority holds.
            with server._job_cache_lock:
                server._job_bundle_cache.clear()
            rebuilt = server.build_job_for_client(state, clean_jobs=False)
            self.assertEqual(
                str(rebuilt.template["previousblockhash"]),
                published_tip[0],
            )

            # Once the published authority lapses, issuance falls through to
            # the live template, mirroring the submit-path RPC fallback.
            server.current_tip_observed_monotonic = (
                time.monotonic()
                - float(getattr(server, "submit_tip_max_age_seconds", 10.0))
                - 1.0
            )
            server.template_refresh_failure_exit_seconds = 0.0
            lapsed = server.job_issuance_template_artifacts()
            self.assertEqual(lapsed.previousblockhash, next_tip)
        finally:
            server.shutdown_tip_refresh_executor()

    def test_tip_flip_publication_clears_mismatched_parent_on_lookup_failure(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = [state]  # type: ignore[assignment]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = lambda: False  # type: ignore[method-assign]
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 1)
            published_tip = server.current_tip_first_seen
            assert published_tip is not None
            with server.lock:
                server.current_tip_parent = (published_tip[0], "aa" * 32)

            next_tip = "66" * 32
            rpc.tip = next_tip
            rpc.template = base_template(height=11, prevhash=next_tip)
            sequence = server._reserve_tip_observation_sequence()
            self.assertTrue(
                server.observe_tip_for_refresh(
                    next_tip,
                    observation_sequence=sequence,
                    mark_pending=False,
                )
            )
            snapshot = server.fetch_qbit_tip_template_snapshot()
            bundle = server.prepare_tip_refresh_bundle(snapshot)
            token = server._validate_prepared_tip_refresh(bundle, snapshot, sequence)
            cancellation = server._publish_prepared_tip_refresh(
                token,
                bundle,
                snapshot,
                parent_hash=None,
            )
            try:
                # The old tip's parent must never survive a flip; submit
                # classification would otherwise trust stale lineage.
                self.assertIsNone(server.current_tip_parent)
                self.assertEqual(server.current_tip_first_seen[0], next_tip)
            finally:
                server._clear_active_tip_refresh(token, cancellation)
        finally:
            server.shutdown_tip_refresh_executor()


if __name__ == "__main__":
    unittest.main()
