#!/usr/bin/env python3
"""Deterministic epoch ordering and refresh-wave invariants."""

from __future__ import annotations

import json
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism.prism_coordinator import (
    PRISM_TIP_REFRESH_WAVE_PASS_BUDGET,
    JobBuildSuperseded,
    PendingInitialJob,
    PrismCoordinator,
    TipRefreshValidationToken,
    _PayoutStatePublicationBlocked,
    _TipRefreshFanoutSuperseded,
    _TipRefreshTrustBlocked,
)
from tests.test_prism_coordinator_job_cache import (
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
)
from tests.test_prism_coordinator_vardiff import RecordingLedger


TIP_A = "11" * 32
TIP_B = "22" * 32
TIP_C = "33" * 32
TIP_D = "44" * 32


def validated_refresh(
    server: PrismCoordinator,
) -> tuple[object, object, TipRefreshValidationToken]:
    install_fake_bundle_builder(server)
    snapshot = server.fetch_qbit_tip_template_snapshot()
    sequence = server._reserve_tip_observation_sequence()
    if not server.observe_tip_first_seen(
        snapshot.bestblockhash,
        observation_sequence=sequence,
        publish_refresh_observation=True,
        published_snapshot=snapshot,
    ):
        raise AssertionError("fixture refresh observation lost")
    bundle = server.prepare_tip_refresh_bundle(snapshot)
    token = server._validate_prepared_tip_refresh(
        bundle,
        snapshot,
        sequence,
    )
    return snapshot, bundle, token


def delivered_context(
    *,
    connection_id: int,
    epoch_sequence: int,
    tip_hash: str,
    payout_generation: int = 0,
) -> SimpleNamespace:
    return SimpleNamespace(
        connection_id=connection_id,
        tip_refresh_epoch_sequence=epoch_sequence,
        template={"previousblockhash": tip_hash},
        payout_state_generation=payout_generation,
        template_fingerprint=None,
        template_generation=1,
        collection_only=False,
    )


class TipRefreshEpochTests(unittest.TestCase):
    def test_feature_fallback_keeps_epoch_ordering_disabled(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        state = client(1)
        state.active_job = delivered_context(
            connection_id=state.connection_id,
            epoch_sequence=7,
            tip_hash=TIP_A,
        )

        self.assertFalse(server.tip_refresh_epoch_fanout)
        self.assertEqual(
            server._mint_tip_refresh_epoch_locked(
                tip_hash=TIP_A,
                payout_state_generation=0,
                started_monotonic=1.0,
            ),
            0,
        )
        self.assertEqual(server._tip_refresh_epoch_sequence, 0)
        self.assertTrue(
            server._admit_client_tip_refresh_epoch_locked(state, 1)
        )
        self.assertEqual(
            getattr(state, "_tip_refresh_admitted_epoch_sequence", 0),
            0,
        )
        self.assertTrue(server._tip_refresh_epoch_fixpoint_reached())

    def test_returning_tip_mints_a_new_epoch(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()

        observed = []
        for sequence, tip_hash in enumerate((TIP_A, TIP_B, TIP_A), start=1):
            self.assertTrue(
                server.observe_tip_for_refresh(
                    tip_hash,
                    observation_sequence=sequence,
                    mark_pending=False,
                )
            )
            observed.append(
                (
                    server._tip_refresh_epoch_sequence,
                    server._tip_refresh_epoch_tip_hash,
                )
            )

        self.assertEqual(
            observed,
            [(1, TIP_A), (2, TIP_B), (3, TIP_A)],
        )

    def test_stamping_uses_published_token_not_pending_target(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        snapshot, bundle, token = validated_refresh(server)
        with server.lock:
            server._publish_tip_refresh_epoch_identity_locked(snapshot)
            pending_epoch = server._mint_tip_refresh_epoch_locked(
                tip_hash=TIP_B,
                payout_state_generation=token.payout_state_generation,
                started_monotonic=2.0,
            )

        context = server.stamp_job_for_client(
            state,
            bundle,
            clean_jobs=True,
        )

        self.assertGreater(pending_epoch, token.epoch_sequence)
        self.assertEqual(
            context.tip_refresh_epoch_sequence,
            token.epoch_sequence,
        )

    def test_returning_tip_invalidates_an_old_token(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        snapshot, bundle, token = validated_refresh(server)

        self.assertTrue(
            server.observe_tip_for_refresh(
                TIP_B,
                observation_sequence=token.observation_sequence + 1,
                mark_pending=False,
            )
        )
        self.assertTrue(
            server.observe_tip_for_refresh(
                TIP_A,
                observation_sequence=token.observation_sequence + 2,
                mark_pending=False,
            )
        )

        with server.lock:
            self.assertFalse(
                server._detected_tip_supersedes_locked(
                    token.tip_hash,
                    token.observation_sequence,
                )
            )
            self.assertFalse(
                server._tip_refresh_token_prepublication_current_locked(
                    token,
                    bundle,
                    snapshot,
                )
            )

    def test_admission_floor_orders_dequeued_work_across_tips(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        state = client(1)
        snapshot = SimpleNamespace(
            bestblockhash=TIP_A,
            previousblockhash=TIP_A,
            template_generation=1,
        )
        later = delivered_context(
            connection_id=state.connection_id,
            epoch_sequence=2,
            tip_hash=TIP_B,
        )
        later.template_generation = 9
        state.active_job = later

        self.assertFalse(
            server.intervening_job_supersedes_snapshot(
                later,
                object(),  # type: ignore[arg-type]
                snapshot,  # type: ignore[arg-type]
            )
        )
        self.assertFalse(
            server._admit_client_tip_refresh_epoch_locked(state, 1)
        )

        state.active_job = None
        self.assertTrue(
            server._admit_client_tip_refresh_epoch_locked(state, 3)
        )
        self.assertFalse(
            server._admit_client_tip_refresh_epoch_locked(state, 2)
        )

    def test_fence_blocks_stale_write_the_comparator_admits(self) -> None:
        """Negative control: the fence is load-bearing on the wire.

        A queued delivery that already passed global validation carries an
        older epoch than the connection has since advanced to. The
        intervening-job comparator cannot express that ordering (an
        old-tip snapshot must normally not be blocked by a newer-tip
        active job), so with the fence disabled the stale write reaches
        registration and the socket. Only the per-connection epoch fence
        prevents the regression.
        """
        server, rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        wire: list[tuple[int, str]] = []

        def sendall(data: bytes) -> None:
            payloads = [
                json.loads(line)
                for line in data.decode("utf-8").splitlines()
            ]
            self.assertEqual(
                [payload["method"] for payload in payloads],
                ["mining.set_difficulty", "mining.notify"],
            )
            context = state.active_job
            assert context is not None
            wire.append(
                (
                    int(context.tip_refresh_epoch_sequence),
                    str(context.template["previousblockhash"]),
                )
            )

        state.sock = SimpleNamespace(
            sendall=sendall,
            shutdown=lambda *_args: None,
            close=lambda: None,
        )

        snapshot_a, bundle_a, token_a = validated_refresh(server)
        rpc.tip = TIP_B
        rpc.template = base_template(height=11, prevhash=TIP_B)
        snapshot_b, bundle_b, token_b = validated_refresh(server)
        self.assertGreater(token_b.epoch_sequence, token_a.epoch_sequence)

        fresh = server.send_prepared_job(
            state,
            bundle_b,
            snapshot_b,
            token_b,
            state.connection_id,
            None,
        )
        self.assertEqual(fresh.result, "sent")
        current = state.active_job
        assert current is not None
        self.assertEqual(
            int(current.tip_refresh_epoch_sequence),
            token_b.epoch_sequence,
        )

        # Model the queued worker whose global validation already passed
        # before the connection advanced; identical in both arms so only
        # the fence distinguishes them.
        server._prepared_tip_refresh_obsolete = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: False
        )
        server._tip_refresh_token_current_locked = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: True
        )

        blocked = server.send_prepared_job(
            state,
            bundle_a,
            snapshot_a,
            token_a,
            state.connection_id,
            None,
        )
        self.assertEqual(blocked.result, "skipped")
        self.assertIs(state.active_job, current)
        self.assertEqual(len(wire), 1)

        server._client_tip_refresh_epoch_blocked_locked = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: False
        )
        server._admit_client_tip_refresh_epoch_locked = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: True
        )

        regressed = server.send_prepared_job(
            state,
            bundle_a,
            snapshot_a,
            token_a,
            state.connection_id,
            None,
        )
        self.assertEqual(regressed.result, "sent")
        active = state.active_job
        assert active is not None
        self.assertEqual(
            str(active.template["previousblockhash"]),
            TIP_A,
        )
        self.assertEqual(
            [epoch_sequence for epoch_sequence, _tip in wire],
            [token_b.epoch_sequence, token_a.epoch_sequence],
        )

    def test_hashrate_coverage_records_fixed_thresholds_after_delivery(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        clients = [client(index) for index in (1, 2, 3)]
        for state, weight in zip(
            clients,
            (Decimal("60"), Decimal("36"), Decimal("4")),
        ):
            state.vardiff_difficulty_estimate = weight
        server.clients = clients  # type: ignore[assignment]
        started = 10.0
        epoch = server._mint_tip_refresh_epoch_locked(
            tip_hash=TIP_A,
            payout_state_generation=0,
            started_monotonic=started,
        )

        for state, delivered_at in zip(clients, (11.0, 12.0, 13.0)):
            server._record_progress_delivery(
                state,
                delivered_context(
                    connection_id=state.connection_id,
                    epoch_sequence=epoch,
                    tip_hash=TIP_A,
                ),
                delivered_at,
            )

        self.assertEqual(
            {
                target: (
                    histogram["count"],
                    histogram["sum"],
                )
                for target, histogram in (
                    server.tip_refresh_coverage_histograms.items()
                )
            },
            {
                "50": (1, 1.0),
                "95": (1, 2.0),
                "99": (1, 3.0),
            },
        )
        metrics = "\n".join(server.tip_refresh_metrics_lines())
        coverage_series = [
            line
            for line in metrics.splitlines()
            if line.startswith(
                "qbit_prism_tip_refresh_hashrate_coverage_seconds"
            )
        ]
        self.assertTrue(coverage_series)
        self.assertTrue(
            all("connection" not in line for line in coverage_series)
        )
        self.assertTrue(
            all("username" not in line for line in coverage_series)
        )
        for target in ("50", "95", "99"):
            self.assertIn(
                "qbit_prism_tip_refresh_hashrate_coverage_seconds_count"
                f'{{coverage="{target}"}} 1',
                coverage_series,
            )

    def test_hashrate_coverage_keeps_epoch_cohort_fixed(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        first, departed, remainder = (
            client(index)
            for index in (1, 2, 3)
        )
        for state, weight in (
            (first, Decimal("60")),
            (departed, Decimal("36")),
            (remainder, Decimal("4")),
        ):
            state.vardiff_difficulty_estimate = weight
        server.clients = {first, departed, remainder}
        epoch = server._mint_tip_refresh_epoch_locked(
            tip_hash=TIP_A,
            payout_state_generation=0,
            started_monotonic=10.0,
        )
        server._record_progress_delivery(
            first,
            delivered_context(
                connection_id=first.connection_id,
                epoch_sequence=epoch,
                tip_hash=TIP_A,
            ),
            11.0,
        )
        with server.lock:
            server.clients.remove(departed)
            newcomer = client(4)
            newcomer.vardiff_difficulty_estimate = Decimal("1000")
            server.clients.add(newcomer)
        server._record_progress_delivery(
            newcomer,
            delivered_context(
                connection_id=newcomer.connection_id,
                epoch_sequence=epoch,
                tip_hash=TIP_A,
            ),
            12.0,
        )
        server._record_progress_delivery(
            remainder,
            delivered_context(
                connection_id=remainder.connection_id,
                epoch_sequence=epoch,
                tip_hash=TIP_A,
            ),
            13.0,
        )

        self.assertEqual(
            {
                target: histogram["count"]
                for target, histogram in (
                    server.tip_refresh_coverage_histograms.items()
                )
            },
            {"50": 1, "95": 0, "99": 0},
        )

    def test_fixpoint_requires_delivery_not_registration(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        snapshot = SimpleNamespace(
            bestblockhash=TIP_A,
            previousblockhash=TIP_A,
            template_fingerprint="fixture-fingerprint",
        )
        epoch = server._mint_tip_refresh_epoch_locked(
            tip_hash=TIP_A,
            payout_state_generation=0,
            started_monotonic=1.0,
        )
        context = delivered_context(
            connection_id=state.connection_id,
            epoch_sequence=epoch,
            tip_hash=TIP_A,
        )
        context.template_fingerprint = snapshot.template_fingerprint
        state.active_job = context
        state.active_job_ids.add("registered")
        server.jobs["registered"] = context  # type: ignore[assignment]

        self.assertTrue(
            server.client_needs_tip_template_refresh(
                state,
                snapshot,  # type: ignore[arg-type]
            )
        )
        self.assertFalse(server._tip_refresh_epoch_fixpoint_reached())

        state._progress_delivered_context = context
        self.assertFalse(
            server.client_needs_tip_template_refresh(
                state,
                snapshot,  # type: ignore[arg-type]
            )
        )
        self.assertTrue(server._tip_refresh_epoch_fixpoint_reached())

    def test_ready_latch_reselects_delivered_collection_job(self) -> None:
        """Reselection must cover every fixpoint-incomplete condition.

        A windowed registration can advance past a delivered collection
        job when its send fails. The delivered collection job then keeps
        the fixpoint open, so the client must stay selectable or the
        owning wave spins forever with no candidate to serve.
        """
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        state = client(1)
        server.clients = [state]  # type: ignore[assignment]
        snapshot = SimpleNamespace(
            bestblockhash=TIP_A,
            previousblockhash=TIP_A,
            template_fingerprint="fixture-fingerprint",
        )
        epoch = server._mint_tip_refresh_epoch_locked(
            tip_hash=TIP_A,
            payout_state_generation=0,
            started_monotonic=1.0,
        )
        delivered = delivered_context(
            connection_id=state.connection_id,
            epoch_sequence=epoch,
            tip_hash=TIP_A,
        )
        delivered.template_fingerprint = snapshot.template_fingerprint
        delivered.collection_only = True
        registered = delivered_context(
            connection_id=state.connection_id,
            epoch_sequence=epoch,
            tip_hash=TIP_A,
        )
        registered.template_fingerprint = snapshot.template_fingerprint
        state.active_job = registered
        state.active_job_ids.add("registered")
        server.jobs["registered"] = registered  # type: ignore[assignment]
        state._progress_delivered_context = delivered
        server._pool_ready_latched = True

        self.assertFalse(server._tip_refresh_epoch_fixpoint_reached())
        self.assertTrue(
            server.client_needs_tip_template_refresh(
                state,
                snapshot,  # type: ignore[arg-type]
            )
        )

    def test_wave_outcomes_are_recorded_once_per_owner(self) -> None:
        cases = (
            ("completed", None),
            ("fanout_superseded", _TipRefreshFanoutSuperseded("superseded")),
            ("build_superseded", JobBuildSuperseded("superseded")),
            ("payout_blocked", _PayoutStatePublicationBlocked("blocked")),
            ("trust_blocked", _TipRefreshTrustBlocked("blocked")),
            ("error", RuntimeError("failed")),
        )

        for expected, failure in cases:
            feature_states = (
                (False,)
                if expected in {"fanout_superseded", "build_superseded"}
                else (False, True)
            )
            for feature_enabled in feature_states:
                with self.subTest(
                    expected=expected,
                    feature_enabled=feature_enabled,
                ):
                    server, _rpc = coordinator()
                    server.tip_refresh_epoch_fanout = feature_enabled
                    server._ensure_tip_refresh_state()

                    def pass_once(**_kwargs: object) -> int:
                        if failure is not None:
                            raise failure
                        return 0

                    server._poll_qbit_tip_template_pass_once = pass_once  # type: ignore[method-assign]
                    if failure is None:
                        self.assertEqual(
                            server.poll_qbit_tip_template_once(),
                            0,
                        )
                    else:
                        with self.assertRaises(type(failure)):
                            server.poll_qbit_tip_template_once()
                    self.assertEqual(
                        server.tip_refresh_wave_outcome_counts[expected],
                        1,
                    )
                    self.assertEqual(
                        sum(
                            server.tip_refresh_wave_outcome_counts.values()
                        ),
                        1,
                    )
                    metrics = "\n".join(
                        server.tip_refresh_metrics_lines()
                    )
                    for outcome, _failure in cases:
                        expected_count = 1 if outcome == expected else 0
                        self.assertIn(
                            "qbit_prism_tip_refresh_wave_outcomes_total"
                            f'{{outcome="{outcome}"}} {expected_count}',
                            metrics,
                        )

    def test_reentered_wave_keeps_one_supersession_outcome(self) -> None:
        cases = (
            (
                "fanout_superseded",
                _TipRefreshFanoutSuperseded("superseded"),
            ),
            ("build_superseded", JobBuildSuperseded("superseded")),
        )

        for expected, supersession in cases:
            with self.subTest(expected=expected):
                server, _rpc = coordinator()
                server.tip_refresh_epoch_fanout = True
                server._ensure_tip_refresh_state()
                attempts = 0

                def pass_once(**_kwargs: object) -> int:
                    nonlocal attempts
                    attempts += 1
                    if attempts == 1:
                        raise supersession
                    return 0

                server._poll_qbit_tip_template_pass_once = pass_once  # type: ignore[method-assign]
                server._tip_refresh_epoch_fixpoint_reached = lambda: True  # type: ignore[method-assign]

                self.assertEqual(server.poll_qbit_tip_template_once(), 0)
                self.assertEqual(attempts, 2)
                self.assertEqual(
                    server.tip_refresh_wave_outcome_counts[expected],
                    1,
                )
                self.assertEqual(
                    sum(server.tip_refresh_wave_outcome_counts.values()),
                    1,
                )

    def test_shutdown_before_fixpoint_records_shutdown_outcome(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()

        def pass_once(**_kwargs: object) -> int:
            server.stop_event.set()
            return 0

        server._poll_qbit_tip_template_pass_once = pass_once  # type: ignore[method-assign]
        server._tip_refresh_epoch_fixpoint_reached = lambda: False  # type: ignore[method-assign]

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertEqual(
            server.tip_refresh_wave_outcome_counts["shutdown"],
            1,
        )
        self.assertEqual(
            sum(server.tip_refresh_wave_outcome_counts.values()),
            1,
        )
        metrics = "\n".join(server.tip_refresh_metrics_lines())
        self.assertIn(
            'qbit_prism_tip_refresh_wave_outcomes_total{outcome="shutdown"} 1',
            metrics,
        )

    def test_wave_pass_budget_falls_back_to_scheduled_retry(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        passes = 0

        def pass_once(**_kwargs: object) -> int:
            nonlocal passes
            passes += 1
            return 0

        server._poll_qbit_tip_template_pass_once = pass_once  # type: ignore[method-assign]
        server._tip_refresh_epoch_fixpoint_reached = lambda: False  # type: ignore[method-assign]

        with patch(
            "lab.prism.prism_coordinator."
            "PRISM_TIP_REFRESH_REENTRY_BACKOFF_SECONDS",
            0.0,
        ):
            self.assertEqual(server.poll_qbit_tip_template_once(), 0)

        self.assertEqual(passes, PRISM_TIP_REFRESH_WAVE_PASS_BUDGET)
        self.assertEqual(
            server.tip_refresh_wave_outcome_counts["fanout_superseded"],
            1,
        )
        self.assertEqual(
            sum(server.tip_refresh_wave_outcome_counts.values()),
            1,
        )
        self.assertTrue(server._consume_tip_refresh_retry())

    def test_shutdown_during_reentry_supersession_records_shutdown(self) -> None:
        cases = (
            ("build", JobBuildSuperseded("superseded")),
            ("fanout", _TipRefreshFanoutSuperseded("superseded")),
        )
        for label, supersession in cases:
            with self.subTest(supersession=label):
                server, _rpc = coordinator()
                server.tip_refresh_epoch_fanout = True
                server._ensure_tip_refresh_state()

                def pass_once(**_kwargs: object) -> int:
                    server.stop_event.set()
                    raise supersession

                server._poll_qbit_tip_template_pass_once = pass_once  # type: ignore[method-assign]
                with self.assertRaises(type(supersession)):
                    server.poll_qbit_tip_template_once()
                self.assertEqual(
                    server.tip_refresh_wave_outcome_counts["shutdown"],
                    1,
                )
                self.assertEqual(
                    sum(server.tip_refresh_wave_outcome_counts.values()),
                    1,
                )

    def test_legacy_supersession_outcome_survives_shutdown(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()

        def pass_once(**_kwargs: object) -> int:
            server.stop_event.set()
            raise _TipRefreshFanoutSuperseded("superseded")

        server._poll_qbit_tip_template_pass_once = pass_once  # type: ignore[method-assign]
        with self.assertRaises(_TipRefreshFanoutSuperseded):
            server.poll_qbit_tip_template_once()
        self.assertEqual(
            server.tip_refresh_wave_outcome_counts["fanout_superseded"],
            1,
        )
        self.assertEqual(
            sum(server.tip_refresh_wave_outcome_counts.values()),
            1,
        )

    def test_converged_reentry_consumes_prewave_retry(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        attempts = 0

        def pass_once(**_kwargs: object) -> int:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                server._schedule_tip_refresh_retry()
                raise _TipRefreshFanoutSuperseded("superseded")
            return 0

        server._poll_qbit_tip_template_pass_once = pass_once  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator."
            "PRISM_TIP_REFRESH_REENTRY_BACKOFF_SECONDS",
            0.0,
        ):
            self.assertEqual(server.poll_qbit_tip_template_once(), 0)

        self.assertEqual(attempts, 2)
        self.assertEqual(
            server.tip_refresh_wave_outcome_counts["fanout_superseded"],
            1,
        )
        self.assertFalse(server._tip_refresh_retry.is_set())
        self.assertFalse(server._consume_tip_refresh_retry())

    def test_payout_epoch_tip_prefers_observed_refresh_target(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()

        self.assertIsNone(server._payout_epoch_tip_hash_locked())
        server.current_tip_first_seen = (TIP_C, 1.0)
        self.assertEqual(server._payout_epoch_tip_hash_locked(), TIP_C)
        server.latest_detected_tip = (TIP_B, 2.0)
        self.assertEqual(server._payout_epoch_tip_hash_locked(), TIP_B)
        server._tip_refresh_epoch_tip_hash = TIP_A
        self.assertEqual(server._payout_epoch_tip_hash_locked(), TIP_A)

    def test_accepted_block_preview_keeps_epoch_on_observed_tip(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        self.assertTrue(
            server.observe_tip_for_refresh(
                TIP_A,
                observation_sequence=1,
                mark_pending=False,
            )
        )
        minted = server._tip_refresh_epoch_sequence
        block_hash = "bb" * 32
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._publish_accepted_block_payout_preview(block_hash, preview)

        # The accepted block hash is not a template parent; the payout
        # mint must keep the epoch on the observed refresh target.
        self.assertEqual(server._tip_refresh_epoch_tip_hash, TIP_A)
        self.assertGreater(server._tip_refresh_epoch_sequence, minted)
        self.assertEqual(
            server._tip_refresh_epoch_payout_generation,
            server._payout_state_generation,
        )

    def test_payout_mint_republishes_stamping_identity(self) -> None:
        server, _rpc = coordinator()
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        snapshot, bundle, token = validated_refresh(server)
        with server.lock:
            server._publish_tip_refresh_epoch_identity_locked(snapshot)
        self.assertEqual(
            server._tip_refresh_epoch_for_bundle_locked(bundle),
            token.epoch_sequence,
        )

        block_hash = "bb" * 32
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._publish_accepted_block_payout_preview(block_hash, preview)

        identity = server._published_tip_refresh_epoch_identity
        assert identity is not None
        self.assertEqual(identity[0], server._tip_refresh_epoch_sequence)
        self.assertGreater(identity[0], token.epoch_sequence)
        self.assertEqual(identity[2], server._payout_state_generation)
        # Rebuilt bundles carry the published payout generation and must
        # stamp the minted epoch, not fall to 0 behind a stale identity.
        rebuilt = server.prepare_tip_refresh_bundle(snapshot)
        self.assertEqual(
            int(rebuilt.payout_state_generation),
            int(server._payout_state_generation),
        )
        self.assertEqual(
            server._tip_refresh_epoch_for_bundle_locked(rebuilt),
            server._tip_refresh_epoch_sequence,
        )

    def test_initial_fence_miss_completes_request_with_newer_work(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        server._ensure_initial_job_state()
        bundle = server.prewarm_current_tip_ready_bundle()
        assert bundle is not None
        artifacts = server.current_template_artifacts()
        state = client(1)
        state.authorization_generation = 1
        state.difficulty_generation = 0
        state.authorized_monotonic = time.monotonic()
        sent_payloads: list[dict[str, object]] = []
        state.send = sent_payloads.append  # type: ignore[method-assign]
        server.clients = {state}
        snapshot = server.tip_template_snapshot
        assert snapshot is not None
        assert snapshot.template_artifacts is not None
        newer = SimpleNamespace(
            connection_id=state.connection_id,
            tip_refresh_epoch_sequence=99,
            template=snapshot.template_artifacts.template,
            template_fingerprint=snapshot.template_fingerprint,
            template_generation=snapshot.template_generation,
            payout_state_generation=int(
                getattr(server, "_payout_state_generation", 0)
            ),
            authorization_generation=1,
            difficulty_generation=0,
            collection_only=False,
            job=SimpleNamespace(job_id="newer"),
        )
        state.active_job = newer
        state._tip_refresh_admitted_epoch_sequence = 99
        request = PendingInitialJob(
            client=state,
            authorization_generation=1,
            worker=state.worker,
            requested_monotonic=time.monotonic(),
            deadline_monotonic=None,
            connection_id=state.connection_id,
            difficulty_generation=0,
        )
        server.pending_initial_jobs[state] = request

        self.assertIs(
            server._deliver_initial_bundle(request, artifacts, bundle),
            False,
        )
        self.assertNotIn(state, server.pending_initial_jobs)
        self.assertTrue(request.cancelled.is_set())
        self.assertEqual(server.initial_job_sent_count, 1)
        self.assertIs(state.active_job, newer)
        self.assertEqual(sent_payloads, [])

    def test_wave_reentry_gates_on_feature_shutdown_and_budget(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()

        self.assertFalse(server._tip_refresh_wave_reenters(1))

        server.tip_refresh_epoch_fanout = True
        with patch(
            "lab.prism.prism_coordinator."
            "PRISM_TIP_REFRESH_REENTRY_BACKOFF_SECONDS",
            0.0,
        ):
            self.assertTrue(server._tip_refresh_wave_reenters(1))
            self.assertFalse(server._consume_tip_refresh_retry())
            self.assertFalse(
                server._tip_refresh_wave_reenters(
                    PRISM_TIP_REFRESH_WAVE_PASS_BUDGET
                )
            )
            self.assertTrue(server._consume_tip_refresh_retry())

        server.stop_event.set()
        self.assertFalse(server._tip_refresh_wave_reenters(1))

    def test_reconciliation_failure_records_trust_blocked(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()

        def fail_reconciliation(_tip_hash: str) -> bool:
            raise RuntimeError("synthetic reconciliation failure")

        server.ensure_reorg_reconciled_for_tip = fail_reconciliation  # type: ignore[method-assign]

        with self.assertRaises(_TipRefreshTrustBlocked):
            server.poll_qbit_tip_template_once()

        self.assertEqual(
            server.tip_refresh_wave_outcome_counts["trust_blocked"],
            1,
        )
        self.assertEqual(
            sum(server.tip_refresh_wave_outcome_counts.values()),
            1,
        )

    def _assert_feature_wave_converges_after_supersession(
        self,
        transitions: tuple[tuple[str, str, int], ...],
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_epoch_fanout = True
        server.tip_refresh_max_workers = 16
        server._ensure_tip_refresh_state()
        clients = [client(index + 1) for index in range(200)]
        for state in clients:
            state.vardiff_difficulty_estimate = Decimal(
                len(clients) + 1 - state.connection_id
            )
        server.clients = clients  # type: ignore[assignment]
        superseded_tips = tuple(
            old_tip
            for old_tip, _new_tip, _height in transitions
        )
        final_tip = transitions[-1][1]
        tip_order = {
            tip_hash: rank
            for rank, tip_hash in enumerate(
                (
                    transitions[0][0],
                    *(
                        new_tip
                        for _old_tip, new_tip, _height in transitions
                    ),
                )
            )
        }
        release_sends = {
            tip_hash: threading.Event()
            for tip_hash in superseded_tips
        }
        sends_admitted = {
            tip_hash: threading.Event()
            for tip_hash in superseded_tips
        }
        send_lock = threading.Lock()
        notify_starts: list[tuple[str, int, int]] = []
        wire_batches: dict[int, list[tuple[int, bytes]]] = {
            state.connection_id: []
            for state in clients
        }
        active_sends = 0
        active_sends_by_tip = {
            tip_hash: 0
            for tip_hash in superseded_tips
        }
        maximum_active_sends = 0

        def client_sendall(state: object) -> object:
            def sendall(data: bytes) -> None:
                nonlocal active_sends, maximum_active_sends
                payloads = [
                    json.loads(line)
                    for line in data.decode("utf-8").splitlines()
                ]
                self.assertEqual(
                    [payload["method"] for payload in payloads],
                    ["mining.set_difficulty", "mining.notify"],
                )
                context = state.active_job  # type: ignore[attr-defined]
                if context is None:
                    raise AssertionError("notify started without registered work")
                self.assertEqual(
                    payloads,
                    [
                        server.difficulty_payload(
                            context.job.share_difficulty
                        ),
                        server.job_payload(context.job),
                    ],
                )
                tip_hash = str(context.template["previousblockhash"])
                epoch_sequence = int(context.tip_refresh_epoch_sequence)
                with send_lock:
                    active_sends += 1
                    if tip_hash in active_sends_by_tip:
                        active_sends_by_tip[tip_hash] += 1
                    maximum_active_sends = max(
                        maximum_active_sends,
                        active_sends,
                    )
                    notify_starts.append(
                        (
                            tip_hash,
                            epoch_sequence,
                            int(state.connection_id),  # type: ignore[attr-defined]
                        )
                    )
                    wire_batches[int(state.connection_id)].append(  # type: ignore[attr-defined]
                        (epoch_sequence, data)
                    )
                    if (
                        tip_hash in active_sends_by_tip
                        and active_sends_by_tip[tip_hash]
                        == server.tip_refresh_max_workers
                    ):
                        sends_admitted[tip_hash].set()
                try:
                    if (
                        tip_hash in release_sends
                        and not release_sends[tip_hash].wait(10)
                    ):
                        raise AssertionError("test did not release admitted sends")
                finally:
                    with send_lock:
                        active_sends -= 1
                        if tip_hash in active_sends_by_tip:
                            active_sends_by_tip[tip_hash] -= 1

            return sendall

        for state in clients:
            state.sock = SimpleNamespace(
                sendall=client_sendall(state),
                shutdown=lambda *_args: None,
                close=lambda: None,
            )

        results: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:
                errors.append(exc)

        owner = threading.Thread(target=poll)
        owner.start()
        try:
            for old_tip, new_tip, height in transitions:
                self.assertTrue(sends_admitted[old_tip].wait(5))
                rpc.tip = new_tip
                rpc.template = base_template(
                    height=height,
                    prevhash=new_tip,
                )
                self.assertTrue(server.observe_tip_for_refresh(new_tip))
                with send_lock:
                    admitted_work = [
                        (epoch_sequence, connection_id)
                        for (
                            tip_hash,
                            epoch_sequence,
                            connection_id,
                        ) in notify_starts
                        if tip_hash == old_tip
                    ]
                self.assertEqual(
                    len(admitted_work),
                    server.tip_refresh_max_workers,
                )
                self.assertEqual(
                    {
                        connection_id
                        for _epoch_sequence, connection_id in admitted_work
                    },
                    set(
                        range(
                            1,
                            server.tip_refresh_max_workers + 1,
                        )
                    ),
                )
                clients_by_connection = {
                    state.connection_id: state
                    for state in clients
                }
                for epoch_sequence, connection_id in admitted_work:
                    active = clients_by_connection[
                        connection_id
                    ].active_job
                    self.assertIsNotNone(active)
                    assert active is not None
                    self.assertEqual(
                        str(active.template["previousblockhash"]),
                        old_tip,
                    )
                    self.assertEqual(
                        active.tip_refresh_epoch_sequence,
                        epoch_sequence,
                    )
                release_sends[old_tip].set()
            owner.join(20)

            self.assertFalse(owner.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(results, [len(clients)])
            final_epoch = server._tip_refresh_epoch_sequence
            self.assertEqual(server.current_tip_first_seen[0], final_tip)
            self.assertEqual(server._tip_refresh_epoch_tip_hash, final_tip)
            self.assertTrue(server._tip_refresh_epoch_fixpoint_reached())
            self.assertFalse(server.tip_refresh_is_pending())
            for state in clients:
                active = state.active_job
                delivered = getattr(state, "_progress_delivered_context", None)
                self.assertIsNotNone(active)
                self.assertIsNotNone(delivered)
                assert active is not None and delivered is not None
                self.assertEqual(
                    str(active.template["previousblockhash"]),
                    final_tip,
                )
                self.assertEqual(
                    str(delivered.template["previousblockhash"]),
                    final_tip,
                )
                self.assertEqual(
                    active.tip_refresh_epoch_sequence,
                    final_epoch,
                )
                self.assertEqual(
                    delivered.tip_refresh_epoch_sequence,
                    final_epoch,
                )

            with send_lock:
                start_ranks = [
                    tip_order[tip_hash]
                    for tip_hash, _epoch, _connection_id in notify_starts
                ]
                self.assertEqual(start_ranks, sorted(start_ranks))
                self.assertEqual(active_sends, 0)
                self.assertEqual(
                    maximum_active_sends,
                    server.tip_refresh_max_workers,
                )
                for state in clients:
                    batches = wire_batches[state.connection_id]
                    self.assertGreaterEqual(len(batches), 1)
                    epochs = [
                        epoch_sequence
                        for epoch_sequence, _data in batches
                    ]
                    self.assertEqual(epochs, sorted(epochs))
                    for _epoch_sequence, data in batches:
                        methods = [
                            json.loads(line)["method"]
                            for line in data.decode("utf-8").splitlines()
                        ]
                        self.assertEqual(
                            methods,
                            ["mining.set_difficulty", "mining.notify"],
                        )
            self.assertEqual(server.tip_refresh_inflight, 0)
            self.assertEqual(server.tip_refresh_build_inflight, 0)
            self.assertEqual(server.tip_refresh_build_queue_depth, 0)
            self.assertEqual(server._tip_refresh_executor.stats(), (0, 0))
            self.assertIsNone(server._active_tip_refresh)
            self.assertEqual(
                server.tip_refresh_wave_outcome_counts["fanout_superseded"],
                1,
            )
            self.assertEqual(
                sum(server.tip_refresh_wave_outcome_counts.values()),
                1,
            )
        finally:
            for release in release_sends.values():
                release.set()
            owner.join(5)
            server.shutdown_tip_refresh_executor()

    def test_feature_wave_converges_after_repeated_supersession(self) -> None:
        cases = (
            (
                (TIP_A, TIP_B, 11),
                (TIP_B, TIP_C, 12),
            ),
            (
                (TIP_A, TIP_B, 11),
                (TIP_B, TIP_C, 12),
                (TIP_C, TIP_D, 13),
            ),
        )
        for transitions in cases:
            with self.subTest(supersessions=len(transitions)):
                self._assert_feature_wave_converges_after_supersession(
                    transitions
                )

    def test_build_checkpoint_supersession_reenters_latest_epoch(self) -> None:
        checkpoints = (
            "ledger_snapshot_complete",
            "payout_derivation",
            "ctv_manifest",
            "signing_verification",
            "bundle_assembly",
            "serialization",
            "bundle_publication",
        )

        for index, blocked_checkpoint in enumerate(checkpoints):
            with self.subTest(checkpoint=blocked_checkpoint):
                server, rpc = coordinator()
                install_fake_bundle_builder(server)
                server.tip_refresh_epoch_fanout = True
                server._ensure_tip_refresh_state()
                state = client(index + 1)
                notifications: list[tuple[str, int]] = []

                def send(payload: dict[str, object]) -> None:
                    if payload["method"] != "mining.notify":
                        return
                    context = state.active_job
                    if context is None:
                        raise AssertionError(
                            "notify started without registered work"
                        )
                    notifications.append(
                        (
                            str(context.template["previousblockhash"]),
                            int(context.tip_refresh_epoch_sequence),
                        )
                    )

                state.send = send  # type: ignore[method-assign]
                server.clients = [state]  # type: ignore[assignment]
                checkpoint_entered = threading.Event()
                release_checkpoint = threading.Event()
                checkpoint_hits = 0
                original_checkpoint = server._job_build_checkpoint

                def controlled_checkpoint(
                    phase: str,
                    cancellation: object,
                ) -> None:
                    nonlocal checkpoint_hits
                    if phase == blocked_checkpoint:
                        checkpoint_hits += 1
                        if checkpoint_hits == 1:
                            checkpoint_entered.set()
                            if not release_checkpoint.wait(10):
                                raise AssertionError(
                                    "test did not release build checkpoint"
                                )
                    original_checkpoint(phase, cancellation)  # type: ignore[arg-type]

                server._job_build_checkpoint = controlled_checkpoint  # type: ignore[method-assign]
                next_tip = f"{64 + index:02x}" * 32
                results: list[int] = []
                errors: list[BaseException] = []

                def poll() -> None:
                    try:
                        results.append(
                            server.poll_qbit_tip_template_once()
                        )
                    except BaseException as exc:
                        errors.append(exc)

                owner = threading.Thread(target=poll)
                owner.start()
                try:
                    self.assertTrue(checkpoint_entered.wait(5))
                    rpc.tip = next_tip
                    rpc.template = base_template(
                        height=20 + index,
                        prevhash=next_tip,
                    )
                    self.assertTrue(
                        server.observe_tip_for_refresh(next_tip)
                    )
                    release_checkpoint.set()
                    owner.join(20)

                    self.assertFalse(owner.is_alive())
                    self.assertEqual(errors, [])
                    self.assertEqual(results, [1])
                    self.assertEqual(checkpoint_hits, 2)
                    final_epoch = server._tip_refresh_epoch_sequence
                    self.assertEqual(
                        notifications,
                        [(next_tip, final_epoch)],
                    )
                    self.assertEqual(
                        server.current_tip_first_seen[0],
                        next_tip,
                    )
                    self.assertIsNotNone(state.active_job)
                    delivered = getattr(
                        state,
                        "_progress_delivered_context",
                        None,
                    )
                    self.assertIsNotNone(delivered)
                    assert state.active_job is not None and delivered is not None
                    self.assertEqual(
                        state.active_job.tip_refresh_epoch_sequence,
                        final_epoch,
                    )
                    self.assertEqual(
                        delivered.tip_refresh_epoch_sequence,
                        final_epoch,
                    )
                    self.assertTrue(
                        server._tip_refresh_epoch_fixpoint_reached()
                    )
                    self.assertEqual(server.tip_refresh_inflight, 0)
                    self.assertEqual(server.tip_refresh_build_inflight, 0)
                    self.assertEqual(
                        server.tip_refresh_build_queue_depth,
                        0,
                    )
                    self.assertEqual(
                        server._tip_refresh_executor.stats(),
                        (0, 0),
                    )
                    with server._job_build_scheduler_lock:
                        self.assertIsNone(server._job_build_active)
                        self.assertIsNone(server._job_build_retiring)
                    self.assertEqual(
                        server.tip_refresh_wave_outcome_counts[
                            "build_superseded"
                        ],
                        1,
                    )
                    self.assertEqual(
                        sum(
                            server.tip_refresh_wave_outcome_counts.values()
                        ),
                        1,
                    )
                finally:
                    release_checkpoint.set()
                    owner.join(5)
                    server.shutdown_tip_refresh_executor()

    def test_same_tip_payout_publications_recover_newest_epoch(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        clients = [client(index + 1) for index in range(4)]
        server.clients = clients  # type: ignore[assignment]
        initial_sends_started = threading.Event()
        release_initial_sends = threading.Event()
        mutation_started = threading.Event()
        mutation_completed = threading.Event()
        send_lock = threading.Lock()
        sends_inflight = 0
        delivery_history: dict[int, list[tuple[int, int]]] = {
            state.connection_id: []
            for state in clients
        }

        def client_send(state: object) -> object:
            def send(payload: dict[str, object]) -> None:
                nonlocal sends_inflight
                if payload["method"] != "mining.notify":
                    return
                context = state.active_job  # type: ignore[attr-defined]
                if context is None:
                    raise AssertionError("notify started without registered work")
                generation = int(context.payout_state_generation)
                epoch_sequence = int(context.tip_refresh_epoch_sequence)
                with send_lock:
                    sends_inflight += 1
                    delivery_history[int(state.connection_id)].append(  # type: ignore[attr-defined]
                        (generation, epoch_sequence)
                    )
                    if generation == 0 and sends_inflight == len(clients):
                        initial_sends_started.set()
                try:
                    if (
                        generation == 0
                        and not release_initial_sends.wait(10)
                    ):
                        raise AssertionError("test did not release initial sends")
                finally:
                    with send_lock:
                        sends_inflight -= 1

            return send

        for state in clients:
            state.send = client_send(state)  # type: ignore[method-assign,assignment]

        poll_results: list[int] = []
        poll_errors: list[BaseException] = []
        published_generations: list[int] = []
        published_epochs: list[int] = []

        def poll() -> None:
            try:
                poll_results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:
                poll_errors.append(exc)

        def mutate_payouts() -> None:
            mutation_started.set()
            try:
                for _ in range(3):
                    generation = server._advance_payout_state_generation()
                    published_generations.append(generation)
                    published_epochs.append(
                        server._tip_refresh_epoch_sequence
                    )
            finally:
                mutation_completed.set()

        owner = threading.Thread(target=poll)
        mutator = threading.Thread(target=mutate_payouts)
        owner.start()
        try:
            self.assertTrue(initial_sends_started.wait(5))
            first_epoch = server._tip_refresh_epoch_sequence
            mutator.start()
            self.assertTrue(mutation_started.wait(5))
            self.assertFalse(mutation_completed.wait(0.1))
            release_initial_sends.set()
            mutator.join(20)
            owner.join(20)

            self.assertFalse(mutator.is_alive())
            self.assertFalse(owner.is_alive())
            self.assertEqual(poll_errors, [])
            self.assertEqual(len(poll_results), 1)
            self.assertEqual(published_generations, [1, 2, 3])
            self.assertEqual(
                published_epochs,
                list(
                    range(
                        published_epochs[0],
                        published_epochs[0] + len(published_epochs),
                    )
                ),
            )
            newest_payout_generation = published_generations[-1]
            newest_epoch = server._tip_refresh_epoch_sequence
            self.assertGreater(newest_epoch, first_epoch)
            self.assertEqual(server._tip_refresh_epoch_tip_hash, rpc.tip)
            self.assertTrue(server._tip_refresh_epoch_fixpoint_reached())
            for state in clients:
                active = state.active_job
                delivered = getattr(
                    state,
                    "_progress_delivered_context",
                    None,
                )
                self.assertIsNotNone(active)
                self.assertIsNotNone(delivered)
                assert active is not None and delivered is not None
                self.assertEqual(
                    active.payout_state_generation,
                    newest_payout_generation,
                )
                self.assertEqual(
                    delivered.payout_state_generation,
                    newest_payout_generation,
                )
                self.assertEqual(
                    active.tip_refresh_epoch_sequence,
                    newest_epoch,
                )
                self.assertEqual(
                    delivered.tip_refresh_epoch_sequence,
                    newest_epoch,
                )
                with send_lock:
                    client_deliveries = list(
                        delivery_history[state.connection_id]
                    )
                self.assertGreaterEqual(len(client_deliveries), 2)
                self.assertEqual(
                    [generation for generation, _epoch in client_deliveries],
                    sorted(
                        generation
                        for generation, _epoch in client_deliveries
                    ),
                )
                self.assertEqual(
                    [epoch for _generation, epoch in client_deliveries],
                    sorted(
                        epoch
                        for _generation, epoch in client_deliveries
                    ),
                )
                self.assertEqual(
                    client_deliveries[-1],
                    (newest_payout_generation, newest_epoch),
                )
            with send_lock:
                self.assertEqual(sends_inflight, 0)
            self.assertEqual(server.tip_refresh_inflight, 0)
            self.assertEqual(server.tip_refresh_build_inflight, 0)
            self.assertEqual(server.tip_refresh_build_queue_depth, 0)
            self.assertEqual(server._tip_refresh_executor.stats(), (0, 0))
            self.assertEqual(
                server.tip_refresh_wave_outcome_counts[
                    "fanout_superseded"
                ],
                1,
            )
            self.assertEqual(
                sum(server.tip_refresh_wave_outcome_counts.values()),
                1,
            )
        finally:
            release_initial_sends.set()
            if mutator.ident is not None:
                mutator.join(5)
            owner.join(5)
            server.shutdown_tip_refresh_executor()

    def test_broken_pipe_retires_only_failed_client(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        failed = client(1)
        healthy = [client(2), client(3)]
        healthy_notifications: dict[int, list[dict[str, object]]] = {
            state.connection_id: []
            for state in healthy
        }

        def fail_notify(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                raise BrokenPipeError("synthetic closed connection")

        failed.send = fail_notify  # type: ignore[method-assign]
        failed.close = lambda: None  # type: ignore[method-assign]
        for state in healthy:
            state.send = healthy_notifications[state.connection_id].append  # type: ignore[method-assign]
        server.clients = {failed, *healthy}

        try:
            self.assertEqual(
                server.poll_qbit_tip_template_once(),
                len(healthy),
            )
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertNotIn(failed, server.clients)
        self.assertTrue(failed.closing)
        self.assertIsNone(failed.active_job)
        self.assertEqual(failed.active_job_ids, set())
        self.assertIsNone(failed.worker)
        final_epoch = server._tip_refresh_epoch_sequence
        for state in healthy:
            self.assertIn(state, server.clients)
            self.assertIsNotNone(state.active_job)
            assert state.active_job is not None
            self.assertEqual(
                state.active_job.tip_refresh_epoch_sequence,
                final_epoch,
            )
            self.assertEqual(
                [
                    payload["method"]
                    for payload in healthy_notifications[
                        state.connection_id
                    ]
                ],
                ["mining.set_difficulty", "mining.notify"],
            )
        self.assertTrue(server._tip_refresh_epoch_fixpoint_reached())
        self.assertEqual(
            server.tip_refresh_client_counts["disconnected"],
            1,
        )
        self.assertEqual(
            server.tip_refresh_wave_outcome_counts["completed"],
            1,
        )
        self.assertEqual(
            sum(server.tip_refresh_wave_outcome_counts.values()),
            1,
        )

    def test_retained_same_tip_submits_survive_inflight_epoch_swap(self) -> None:
        class ShareLedger(RecordingLedger):
            def accepted_share_stats(self) -> dict[str, int]:
                return {
                    "accepted_share_count": self.shares,
                    "distinct_miner_count": 0,
                }

            def snapshot_at_job_issue(
                self,
                _anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[object]:
                del window_weight
                return []

        ledger = ShareLedger()
        server, _rpc = coordinator(ledger=ledger)
        server.vardiff_config = SimpleNamespace(enabled=False)
        install_fake_bundle_builder(server)
        server.tip_refresh_epoch_fanout = True
        server._ensure_tip_refresh_state()
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        assert state.active_job is not None
        retained_job_id = state.active_job.job.job_id
        server.bury_evicted_job(state, retained_job_id)
        server.jobs.pop(retained_job_id)
        state.active_job_ids.clear()

        def submit(index: int) -> None:
            submission = SimpleNamespace(
                header_hex=f"{index:02x}" * 80,
                block_hash_hex=f"{index + 16:02x}" * 32,
                share_pass=True,
                block_pass=False,
            )
            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                self.assertFalse(
                    server.handle_submit(
                        state,
                        [
                            state.username,
                            retained_job_id,
                            "00" * 8,
                            f"{index:08x}",
                            f"{index + 32:08x}",
                        ],
                    )
                )

        submit(1)
        server._advance_payout_state_generation()
        send_started = threading.Event()
        release_send = threading.Event()
        poll_results: list[int] = []
        poll_errors: list[BaseException] = []

        def block_notify(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                send_started.set()
                if not release_send.wait(10):
                    raise AssertionError("test did not release epoch delivery")

        def poll() -> None:
            try:
                poll_results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:
                poll_errors.append(exc)

        state.send = block_notify  # type: ignore[method-assign]
        owner = threading.Thread(target=poll)
        owner.start()
        try:
            self.assertTrue(send_started.wait(5))
            submit(2)
            release_send.set()
            owner.join(10)
            self.assertFalse(owner.is_alive())
            self.assertEqual(poll_errors, [])
            self.assertEqual(poll_results, [1])
            submit(3)
        finally:
            release_send.set()
            owner.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertEqual(len(ledger.pending), 3)
        self.assertTrue(
            all(share.credit_policy is None for share in ledger.pending)
        )
        self.assertEqual(
            server.evicted_job_submit_counts["accepted_same_tip"],
            3,
        )
        self.assertIn(retained_job_id, server.evicted_job_graveyard)
        self.assertTrue(server._tip_refresh_epoch_fixpoint_reached())

    def test_shutdown_drains_admitted_send_and_drops_cleaned_queue(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_epoch_fanout = True
        server.tip_refresh_max_workers = 2
        server._ensure_tip_refresh_state()
        admitted, queued, untouched = (
            client(index)
            for index in (1, 2, 3)
        )
        admitted.share_difficulty = Decimal("3")
        queued.share_difficulty = Decimal("2")
        untouched.share_difficulty = Decimal("1")
        server.clients = {admitted, queued, untouched}
        admitted_send_started = threading.Event()
        admitted_send_finished = threading.Event()
        release_admitted_send = threading.Event()
        queued_submitted = threading.Event()
        queued_socket_called = threading.Event()
        untouched_socket_called = threading.Event()

        def block_admitted_send(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            admitted_send_started.set()
            try:
                if not release_admitted_send.wait(10):
                    raise AssertionError("test did not release admitted send")
            finally:
                admitted_send_finished.set()

        admitted.send = block_admitted_send  # type: ignore[method-assign]
        queued.send = lambda _payload: queued_socket_called.set()  # type: ignore[method-assign]
        queued.close = lambda: None  # type: ignore[method-assign]
        untouched.send = lambda _payload: untouched_socket_called.set()  # type: ignore[method-assign]

        class ObservedExecutor:
            def __init__(self) -> None:
                self.delegate = ThreadPoolExecutor(max_workers=1)
                self.submission_count = 0

            def submit(self, function: object, *args: object) -> object:
                self.submission_count += 1
                future = self.delegate.submit(function, *args)  # type: ignore[arg-type]
                if self.submission_count == 2:
                    queued_submitted.set()
                return future

            def shutdown(self, **kwargs: object) -> None:
                self.delegate.shutdown(**kwargs)  # type: ignore[arg-type]

        executor = ObservedExecutor()
        server._tip_refresh_executor = executor  # type: ignore[assignment]
        poll_results: list[int] = []
        poll_errors: list[BaseException] = []
        shutdown_complete = threading.Event()

        def poll() -> None:
            try:
                poll_results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:
                poll_errors.append(exc)

        def shutdown() -> None:
            server.shutdown_tip_refresh_executor()
            shutdown_complete.set()

        owner = threading.Thread(target=poll)
        shutdown_thread = threading.Thread(target=shutdown)
        owner.start()
        try:
            self.assertTrue(admitted_send_started.wait(5))
            self.assertTrue(queued_submitted.wait(5))
            server.disconnect_client(queued)
            server.stop_event.set()
            shutdown_thread.start()
            self.assertFalse(shutdown_complete.wait(0.1))
            self.assertFalse(admitted_send_finished.is_set())
            self.assertFalse(queued_socket_called.is_set())
            self.assertFalse(untouched_socket_called.is_set())
        finally:
            release_admitted_send.set()
            if shutdown_thread.ident is not None:
                shutdown_thread.join(10)
            owner.join(10)

        self.assertFalse(shutdown_thread.is_alive())
        self.assertFalse(owner.is_alive())
        self.assertTrue(shutdown_complete.is_set())
        self.assertTrue(admitted_send_finished.is_set())
        self.assertEqual(poll_errors, [])
        self.assertEqual(poll_results, [1])
        self.assertFalse(queued_socket_called.is_set())
        self.assertFalse(untouched_socket_called.is_set())
        self.assertNotIn(queued, server.clients)
        self.assertTrue(queued.closing)
        self.assertIsNone(queued.active_job)
        self.assertEqual(queued.active_job_ids, set())
        self.assertEqual(server.tip_refresh_inflight, 0)
        self.assertIsNone(server._active_tip_refresh)
        # Shutdown interrupted convergence before the untouched client got
        # newest delivered work; the wave must not report completed.
        self.assertEqual(
            server.tip_refresh_wave_outcome_counts["shutdown"],
            1,
        )
        self.assertEqual(
            sum(server.tip_refresh_wave_outcome_counts.values()),
            1,
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "tip refresh executor is shut down",
        ):
            server.tip_refresh_executor()

    def test_coalesced_observer_does_not_record_an_owner_outcome(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()

        def unexpected_pass(**_kwargs: object) -> int:
            raise AssertionError("coalesced observer entered owner pass")

        server._poll_qbit_tip_template_pass_once = unexpected_pass  # type: ignore[method-assign]
        server._tip_refresh_singleflight_lock.acquire()
        try:
            self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        finally:
            server._tip_refresh_singleflight_lock.release()

        self.assertEqual(
            sum(server.tip_refresh_wave_outcome_counts.values()),
            0,
        )


if __name__ == "__main__":
    unittest.main()
