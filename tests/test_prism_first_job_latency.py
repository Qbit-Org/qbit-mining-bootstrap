#!/usr/bin/env python3
"""New-connection first-job latency regressions.

Covers the coordinator changes that flatten the first-notify tail: initial
requests subscribing to in-flight publication-priority builds, retryable
payout-gate non-admission, per-generation share-window serialization caching,
worker-count env knobs, and hashrate-ordered tip-refresh fanout admission.
"""

from __future__ import annotations

import dataclasses
import json
import re
import threading
import time
import unittest
from concurrent.futures import Future
from decimal import Decimal
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Iterator
from unittest.mock import patch

import lab.prism.prism_coordinator as prism_coordinator
from lab.prism.prism_coordinator import (
    DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
    DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
    PRISM_DELIVERY_PRIORITY_INITIAL,
    PRISM_DELIVERY_PRIORITY_NEW_TIP,
    PRISM_DELIVERY_PRIORITY_SAME_TIP,
    PayoutLedgerArtifact,
    _compact_share_payload,
    _JobBuildCancellation,
    _PayoutDeliveryAdmission,
    canonical_json_sha256,
)
from lab.prism.prism_coordinator import ClientState, PrismCoordinator
from tests.test_prism_coordinator_job_cache import (
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
)
from tests.test_prism_coordinator_vardiff import fake_audit_bundle_popen
from tests.test_prism_initial_job_delivery import wait_until


def payout_ledger_artifact(
    shares: list[dict[str, object]],
    *,
    generation: int = 1,
    payout_state_generation: int = 0,
    network_difficulty: int = 2,
) -> PayoutLedgerArtifact:
    return PayoutLedgerArtifact(
        generation=generation,
        payout_state_generation=payout_state_generation,
        network_difficulty=network_difficulty,
        accepted_share_count=len(shares),
        shares_json=tuple(shares),
        prior_balances=(),
        prepared_monotonic=time.monotonic(),
        snapshot_anchor_ms=1_700_000_000_000,
    )


def window_shares(count: int = 4) -> list[dict[str, object]]:
    return [
        {
            "share_seq": index + 1,
            "share_id": f"share-{index + 1}",
            "miner_id": "miner-a" if index % 2 == 0 else "miner-b",
            "order_key": "miner-a" if index % 2 == 0 else "miner-b",
            "p2mr_program_hex": "22" * 32,
            "share_difficulty": 1,
            "network_difficulty": 2,
            "template_height": 9,
            "job_id": f"job-{index + 1}",
            "job_issued_at_ms": 1_000 + index,
            "accepted_at_ms": 2_000 + index,
            "ntime": 1_700_000_000,
        }
        for index in range(count)
    ]


class InitialBuildSubscriptionTests(unittest.TestCase):
    def test_initial_request_rides_priority_build_result(self) -> None:
        """A deferred initial request consumes the priority build's bundle
        instead of rebuilding it after the priority window closes."""
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        expected = server.shared_job_bundle(artifacts, mode="ready")
        with server._job_cache_lock:
            server._job_bundle_cache.clear()

        priority_request = SimpleNamespace(
            idle_retarget=False,
            mode="ready",
            equivalence_key=("priority",),
            cache_key=("priority",),
            cancellation=_JobBuildCancellation(timeout_seconds=30.0),
            promise=Future(),
            publication_critical=True,
            request_source="tip_refresh",
            requested_monotonic=time.monotonic(),
            priority_admission_recorded=True,
        )
        with server._job_build_scheduler_lock:
            server._job_build_active = SimpleNamespace(request=priority_request)
            server._job_build_retiring = None
            server._job_build_pending = None

        constructed_sources: list[object] = []
        original_new_request = server._new_job_build_request

        def observed_new_request(*args: object, **kwargs: object) -> object:
            constructed_sources.append(kwargs.get("request_source"))
            return original_new_request(*args, **kwargs)  # type: ignore[arg-type]

        server._new_job_build_request = observed_new_request  # type: ignore[method-assign]

        results: list[object] = []
        errors: list[BaseException] = []

        def run_initial() -> None:
            try:
                results.append(
                    server.shared_job_bundle(
                        artifacts,
                        mode="ready",
                        request_source="initial",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        thread = threading.Thread(target=run_initial)
        thread.start()
        try:
            wait_until(
                lambda: (
                    server.initial_job_prepared_work_counts["subscribed"] == 1
                )
            )
            self.assertTrue(thread.is_alive())
            # Production ordering: the build result resolves, then the
            # scheduler sweeps the finished flight and signals the change.
            priority_request.promise.set_result(expected)
            with server._job_build_scheduler_lock:
                server._job_build_active = None
            server._job_build_priority_changed.set()
            thread.join(5)
        finally:
            with server._job_build_scheduler_lock:
                server._job_build_active = None
            server._job_build_priority_changed.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertIs(results[0], expected)
        self.assertEqual(server.initial_job_prepared_work_counts["deferred"], 1)
        self.assertEqual(
            server.initial_job_prepared_work_counts["subscribed"],
            1,
        )
        self.assertEqual(
            server.initial_job_prepared_work_counts["cache_hit"],
            1,
        )
        # The whole point: no second immutable request was ever constructed.
        self.assertEqual(constructed_sources, [])

    def test_cached_bundle_served_without_waiting_out_priority_window(
        self,
    ) -> None:
        """An initial request whose bundle is already cached must not queue
        behind an unrelated publication-priority reservation."""
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        expected = server.shared_job_bundle(artifacts, mode="ready")

        priority_token, _requested = (
            server._begin_job_build_priority_preparation()
        )
        try:
            started = time.monotonic()
            served = server.shared_job_bundle(
                artifacts,
                mode="ready",
                request_source="initial",
            )
            elapsed = time.monotonic() - started
        finally:
            server._finish_job_build_priority_preparation(priority_token)

        self.assertIs(served, expected)
        # The old admission gate would have parked this request for the whole
        # priority window; the probe returns without any deferred accounting.
        self.assertLess(elapsed, 1.0)
        self.assertEqual(server.initial_job_prepared_work_counts["deferred"], 0)
        self.assertGreaterEqual(
            server.initial_job_prepared_work_counts["cache_hit"],
            1,
        )

    def test_metrics_render_subscribed_prepared_work(self) -> None:
        server, _rpc = coordinator()
        metrics = "\n".join(server.job_build_metrics_lines())
        self.assertIn(
            'qbit_prism_initial_job_prepared_work_total{result="subscribed"} 0',
            metrics,
        )


class RetryablePayoutGateTests(unittest.TestCase):
    def test_gate_non_admission_retries_and_delivers_without_disconnect(
        self,
    ) -> None:
        """Transient gate rejections retry with the request's own backoff and
        deadline instead of retiring the connection."""
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()

        real_gate = server._payout_state_delivery_gate

        class FlakyGate:
            def __init__(self, delegate: object, failures: int) -> None:
                self.delegate = delegate
                self.failures = failures
                self.non_admissions = 0

            @contextmanager
            def delivery_cancelable(
                self,
                cancelled: object,
                **kwargs: object,
            ) -> Iterator[object]:
                if self.failures > 0:
                    self.failures -= 1
                    self.non_admissions += 1
                    generation = int(kwargs.get("generation") or 0)
                    yield _PayoutDeliveryAdmission(
                        admitted=False,
                        wait_seconds=0.0,
                        generation=generation,
                        published_generation=generation,
                        relation="current",
                    )
                    return
                with self.delegate.delivery_cancelable(  # type: ignore[attr-defined]
                    cancelled,
                    **kwargs,
                ) as admission:
                    yield admission

        flaky = FlakyGate(real_gate, failures=2)
        server._payout_state_delivery_gate = flaky  # type: ignore[assignment]
        disconnected: list[ClientState] = []
        server.disconnect_client = disconnected.append  # type: ignore[method-assign]

        state = client(1)
        state.authorization_generation = 1
        state.authorized_monotonic = time.monotonic()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.request_initial_job_delivery(state)
        try:
            wait_until(lambda: state.active_job is not None)
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(flaky.non_admissions, 2)
        self.assertEqual(disconnected, [])
        self.assertEqual(server.initial_job_failed_count, 0)
        self.assertEqual(server.initial_job_cancelled_count, 0)
        self.assertEqual(server.initial_job_sent_count, 1)

    def test_cancelled_request_still_terminal_on_non_admission(self) -> None:
        """A cancelled request must not spin the retry loop: cancellation
        stays terminal even when the gate would also have rejected."""
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server.prewarm_current_tip_ready_bundle()
        artifacts = server.job_issuance_template_artifacts()
        bundle = server.shared_job_bundle(artifacts, mode="ready")

        state = client(1)
        state.authorization_generation = 1
        server.clients = {state}
        request = prism_coordinator.PendingInitialJob(
            client=state,
            connection_id=state.connection_id,
            authorization_generation=1,
            worker=state.worker,
            requested_monotonic=time.monotonic(),
            deadline_monotonic=None,
        )
        request.cancelled.set()

        self.assertIs(
            server._deliver_initial_bundle(request, artifacts, bundle),
            False,
        )


class ShareWindowSerializationCacheTests(unittest.TestCase):
    def test_cache_hits_within_generation_and_invalidates_on_bump(self) -> None:
        server, _rpc = coordinator()
        shares = window_shares()
        artifact = payout_ledger_artifact(shares)

        first = server._share_window_serialization_for_artifact(
            artifact,
            shares,
        )
        self.assertEqual(
            first.share_snapshot_sha256,
            canonical_json_sha256(shares),
        )
        self.assertIs(
            server._share_window_serialization_for_artifact(artifact, shares),
            first,
        )

        identities_json, compact_json = first.compact_fragments(shares)
        expected_identities, expected_compact = _compact_share_payload(shares)
        self.assertEqual(
            json.loads(identities_json),
            [list(identity) for identity in expected_identities],
        )
        self.assertEqual(
            json.loads(compact_json),
            [list(entry) for entry in expected_compact],
        )
        # Fragments encode once per generation and are reused verbatim.
        self.assertIs(first.compact_fragments(shares)[0], identities_json)

        artifact_bump = dataclasses.replace(artifact, generation=2)
        rebuilt = server._share_window_serialization_for_artifact(
            artifact_bump,
            shares,
        )
        self.assertIsNot(rebuilt, first)

        payout_bump = dataclasses.replace(
            artifact,
            generation=3,
            payout_state_generation=artifact.payout_state_generation + 1,
        )
        self.assertIsNot(
            server._share_window_serialization_for_artifact(
                payout_bump,
                shares,
            ),
            rebuilt,
        )

    def test_concurrent_builders_share_one_digest_computation(self) -> None:
        server, _rpc = coordinator()
        shares = window_shares()
        artifact = payout_ledger_artifact(shares)
        digest_calls = 0
        original_sha = prism_coordinator.canonical_json_sha256

        def counting_sha(value: object) -> str:
            nonlocal digest_calls
            digest_calls += 1
            return original_sha(value)

        barrier = threading.Barrier(2)
        results: list[object] = []
        results_lock = threading.Lock()

        def compute() -> None:
            barrier.wait()
            serialization = server._share_window_serialization_for_artifact(
                artifact,
                shares,
            )
            with results_lock:
                results.append(serialization)

        with patch.object(
            prism_coordinator,
            "canonical_json_sha256",
            counting_sha,
        ):
            threads = [threading.Thread(target=compute) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(5)

        self.assertEqual(len(results), 2)
        self.assertIs(results[0], results[1])
        self.assertEqual(digest_calls, 1)

    def test_precomposed_builder_payload_matches_inline_encoding(self) -> None:
        """The composed pipe write must be byte-for-byte JSON-equivalent to
        the historical single json.dump of the same payload."""
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        shares = window_shares()
        artifact = payout_ledger_artifact(shares)
        serialization = server._share_window_serialization_for_artifact(
            artifact,
            shares,
        )
        summary = {
            "found_block": {"block_height": 10},
            "signed_coinbase_manifest": {
                "manifest": {"coinbase_tx_hex": "c0ffee"},
            },
        }
        build_kwargs = dict(
            shares=shares,
            found_block={
                "block_height": 10,
                "coinbase_value_sats": 50_00000000,
                "network_difficulty": 2,
                "anchor_job_issued_at_ms": 1_700_000_000_000,
            },
            prior_balances=[{"miner_id": "miner-a", "balance_sats": 1}],
            coinbase_script_sig_suffix_hex="00",
            witness_merkle_leaves_hex=["aa" * 32],
            summary_only=True,
            payout_policy={"policy": "test"},
            ctv_settlement={"kind": "test"},
        )

        inline_captured: dict[str, object] = {}
        with patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(
                inline_captured,
                output_text=json.dumps(summary, separators=(",", ":")),
            ),
        ):
            server.build_audit_bundle(**build_kwargs)

        cached_captured: dict[str, object] = {}
        with patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(
                cached_captured,
                output_text=json.dumps(summary, separators=(",", ":")),
            ),
        ):
            server.build_audit_bundle(
                **build_kwargs,
                share_serialization=serialization,
            )

        self.assertIn("compact_shares", inline_captured["payload"])
        self.assertEqual(
            cached_captured["payload"],
            inline_captured["payload"],
        )

    def test_share_count_mismatch_falls_back_to_inline_encoding(self) -> None:
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        shares = window_shares()
        stale = prism_coordinator._ShareWindowSerialization(
            key=(0, 1, 32),
            share_count=len(shares) + 1,
            share_snapshot_sha256="00" * 32,
        )
        summary = {
            "found_block": {"block_height": 10},
            "signed_coinbase_manifest": {
                "manifest": {"coinbase_tx_hex": "c0ffee"},
            },
        }
        captured: dict[str, object] = {}
        with patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(
                captured,
                output_text=json.dumps(summary, separators=(",", ":")),
            ),
        ):
            server.build_audit_bundle(
                shares=shares,
                found_block={
                    "block_height": 10,
                    "coinbase_value_sats": 50_00000000,
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
                payout_policy={"policy": "test"},
                ctv_settlement={"kind": "test"},
                share_serialization=stale,
            )

        payload = captured["payload"]
        assert isinstance(payload, dict)
        expected_identities, expected_compact = _compact_share_payload(shares)
        self.assertEqual(
            payload["compact_share_identities"],
            [list(identity) for identity in expected_identities],
        )
        self.assertEqual(
            payload["compact_shares"],
            [list(entry) for entry in expected_compact],
        )


class WorkerKnobTests(unittest.TestCase):
    def test_defaults_unchanged(self) -> None:
        self.assertEqual(DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS, 4)
        self.assertEqual(DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS, 2)

    def test_knobs_are_env_wired(self) -> None:
        with open(prism_coordinator.__file__, encoding="utf-8") as module_source:
            source = module_source.read()
        self.assertRegex(
            source,
            r"resolve_initial_job_max_workers\(\s*"
            r'env_optional_positive_int\("PRISM_INITIAL_JOB_MAX_WORKERS"\)',
        )
        self.assertRegex(
            source,
            r'env_positive_int\(\s*"PRISM_JOB_BUILD_EXECUTOR_WORKERS",\s*'
            r"DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,",
        )

    def test_metrics_render_configured_worker_knobs(self) -> None:
        server, _rpc = coordinator()
        server.initial_job_max_workers = 7
        server.job_build_executor_workers = 1
        initial_metrics = "\n".join(server.initial_delivery_metrics_lines())
        build_metrics = "\n".join(server.job_build_metrics_lines())
        self.assertIn(
            "qbit_prism_initial_job_delivery_configured_workers 7",
            initial_metrics,
        )
        self.assertIn(
            "qbit_prism_job_build_configured_workers 1",
            build_metrics,
        )

    def test_job_build_executor_honors_configured_workers(self) -> None:
        server, _rpc = coordinator()
        server.job_build_executor_workers = 1
        try:
            with server._job_build_scheduler_lock:
                executor = server._job_build_executor_locked()
            self.assertEqual(executor._max_workers, 1)
        finally:
            server.shutdown_job_build_executor()

    def test_initial_job_workers_reject_widths_past_pending_bound(self) -> None:
        validate = prism_coordinator.validate_initial_job_max_workers
        self.assertEqual(validate(4, 128), 4)
        self.assertEqual(validate(128, 128), 128)
        with self.assertRaisesRegex(SystemExit, "cannot exceed"):
            validate(129, 128)
        with self.assertRaisesRegex(SystemExit, "positive"):
            validate(0, 128)

    def test_implicit_initial_worker_default_caps_to_pending_bound(self) -> None:
        resolve = prism_coordinator.resolve_initial_job_max_workers
        # Pre-knob deployments with tiny pending bounds stay bootable.
        self.assertEqual(resolve(None, 2), 2)
        self.assertEqual(
            resolve(None, 128),
            DEFAULT_PRISM_INITIAL_JOB_MAX_WORKERS,
        )
        self.assertEqual(resolve(2, 2), 2)
        with self.assertRaisesRegex(SystemExit, "cannot exceed"):
            resolve(4, 2)

    def test_job_build_executor_workers_rejects_unusable_widths(self) -> None:
        validate = prism_coordinator.validate_job_build_executor_workers
        self.assertEqual(validate(1), 1)
        self.assertEqual(
            validate(DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS),
            DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS,
        )
        with self.assertRaisesRegex(SystemExit, "cannot exceed"):
            validate(DEFAULT_PRISM_JOB_BUILD_EXECUTOR_WORKERS + 1)
        with self.assertRaisesRegex(SystemExit, "positive"):
            validate(0)

    def test_initial_job_executor_honors_configured_workers(self) -> None:
        server, _rpc = coordinator()
        server._ensure_initial_job_state()
        server.initial_job_max_workers = 3
        try:
            executor = server.initial_job_executor()
            self.assertEqual(executor.max_workers, 3)
        finally:
            server.shutdown_initial_job_executor()


class FanoutOrderingTests(unittest.TestCase):
    def test_fanout_wave_duration_histogram_is_observed(self) -> None:
        # One completed prepared wave records exactly one wall-clock span
        # (first task submission to last successful delivery).
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        states = [client(1), client(2)]
        for state in states:
            state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = set(states)

        snapshot = server.fetch_qbit_tip_template_snapshot()
        server.observe_tip_first_seen(snapshot.bestblockhash)
        server.pool_readiness_latched()
        server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        try:
            sent, first_delivery, last_delivery, failed = (
                server._fanout_prepared_tip_refresh(
                    states,
                    bundle,
                    snapshot,
                    heartbeat_name="qbit_blockpoll",
                )
            )
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(sent, 2)
        self.assertEqual(failed, 0)
        self.assertIsNotNone(first_delivery)
        self.assertIsNotNone(last_delivery)
        metrics = server.metrics_payload()
        self.assertIn(
            "qbit_prism_tip_refresh_fanout_wave_seconds_count 1",
            metrics,
        )

    def test_send_job_update_precomposed_bytes_match_full_serialization(
        self,
    ) -> None:
        # The precomposed notify line must be byte-identical to serializing
        # the payload dicts, or protocol bytes would silently diverge from
        # what monkeypatched-send tests observe.
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)

        class RecorderSock:
            def __init__(self) -> None:
                self.chunks: list[bytes] = []

            def sendall(self, data: bytes) -> None:
                self.chunks.append(data)

        sock = RecorderSock()
        state.sock = sock  # type: ignore[assignment]

        snapshot = server.fetch_qbit_tip_template_snapshot()
        server.observe_tip_first_seen(snapshot.bestblockhash)
        server.pool_readiness_latched()
        server.tip_template_snapshot = snapshot
        try:
            bundle = server.prepare_tip_refresh_bundle(snapshot)
        finally:
            server.shutdown_tip_refresh_executor()
        job = bundle.base_job
        self.assertIsNotNone(job.notify_shared_params_json)

        server.send_job_update(state, job)

        expected = (
            json.dumps(server.difficulty_payload(job.share_difficulty)).encode()
            + b"\n"
            + json.dumps(PrismCoordinator.job_payload(job)).encode()
            + b"\n"
        )
        self.assertEqual(b"".join(sock.chunks), expected)

    def test_priority_constants_put_first_jobs_ahead_of_the_wave(self) -> None:
        self.assertLess(
            PRISM_DELIVERY_PRIORITY_INITIAL,
            PRISM_DELIVERY_PRIORITY_NEW_TIP,
        )
        self.assertLess(
            PRISM_DELIVERY_PRIORITY_NEW_TIP,
            PRISM_DELIVERY_PRIORITY_SAME_TIP,
        )

    def test_fanout_wave_serves_descending_hashrate_first(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        slow_stale = client(1)
        newcomer = client(2)
        fast_stale = client(3)
        states = [slow_stale, newcomer, fast_stale]
        notified: list[int] = []

        def sender(state: ClientState) -> object:
            def send(payload: dict[str, object]) -> None:
                if payload["method"] == "mining.notify":
                    notified.append(state.connection_id)

            return send

        for state in states:
            state.send = sender(state)  # type: ignore[method-assign]
        server.clients = set(states)

        snapshot = server.fetch_qbit_tip_template_snapshot()
        server.observe_tip_first_seen(snapshot.bestblockhash)
        server.pool_readiness_latched()
        server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        try:
            server._fanout_prepared_tip_refresh(
                states,
                bundle,
                snapshot,
                heartbeat_name="qbit_blockpoll",
            )
            self.assertEqual(sorted(notified), [1, 2, 3])

            # A job-less newcomer burns zero hashrate while stale clients burn
            # their full rate on old-tip work, so the next wave must serve the
            # fastest stale client first and the newcomer last -- even when
            # the newcomer's configured difficulty is the highest in the wave,
            # it keeps only its initial-delivery queue priority. The fast
            # client ranks by its vardiff estimate; the slow one exercises the
            # share_difficulty fallback used before an estimate exists.
            newcomer.active_job = None
            newcomer.active_job_ids = set()
            newcomer.share_difficulty = Decimal("64")
            slow_stale.share_difficulty = Decimal("8")
            fast_stale.vardiff_difficulty_estimate = Decimal("32")
            notified.clear()
            rpc.tip = "22" * 32
            rpc.template = base_template(height=11, prevhash="22" * 32)
            replacement = server.fetch_qbit_tip_template_snapshot()
            server.observe_tip_first_seen(replacement.bestblockhash)
            server.tip_template_snapshot = replacement
            replacement_bundle = server.prepare_tip_refresh_bundle(replacement)

            submissions: list[tuple[int, int]] = []

            def recording_submit(
                executor: object,
                function: object,
                *args: object,
                priority: int,
            ) -> Future[object]:
                target = args[0]
                assert isinstance(target, ClientState)
                submissions.append((target.connection_id, priority))
                return PrismCoordinator._submit_delivery_task(
                    executor,
                    function,  # type: ignore[arg-type]
                    *args,
                    priority=priority,
                )

            server._submit_delivery_task = recording_submit  # type: ignore[method-assign]
            server._fanout_prepared_tip_refresh(
                [slow_stale, newcomer, fast_stale],
                replacement_bundle,
                replacement,
                heartbeat_name="qbit_blockpoll",
            )
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(notified, [3, 1, 2])
        self.assertEqual(
            [connection_id for connection_id, _priority in submissions],
            [3, 1, 2],
        )
        self.assertEqual(
            submissions[2],
            (2, PRISM_DELIVERY_PRIORITY_INITIAL),
        )
        self.assertEqual(
            {
                priority
                for connection_id, priority in submissions
                if connection_id != 2
            },
            {PRISM_DELIVERY_PRIORITY_NEW_TIP},
        )


class SimulatedChurnLatencyTests(unittest.TestCase):
    def test_p99_first_notify_stays_sub_second_under_tip_churn(self) -> None:
        """Clients joining through sustained tip churn get first work fast.

        Scaled-down version of the production complaint: ~45-150 clients
        churning at a ~47s tip cadence saw multi-second first notifies
        because every publication-priority build deferred, cancelled, and
        outcompeted initial work. Cadence and build cost are scaled ~1:100;
        the sub-second p99 bound is intentionally generous for slow CI.
        """
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        original_build = server.build_shared_job_bundle

        def costly_build(*args: object, **kwargs: object) -> object:
            time.sleep(0.02)
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        server.build_shared_job_bundle = costly_build  # type: ignore[method-assign]
        disconnected: list[ClientState] = []
        original_disconnect = server.disconnect_client

        def record_disconnect(state: ClientState) -> None:
            disconnected.append(state)
            original_disconnect(state)

        server.disconnect_client = record_disconnect  # type: ignore[method-assign]
        server.clients = set()

        stop_churn = threading.Event()
        poll_failures: list[BaseException] = []

        def churn_tips() -> None:
            height = 11
            while not stop_churn.is_set():
                if stop_churn.wait(0.3):
                    return
                prevhash = f"{height:02x}" * 32
                rpc.tip = prevhash
                rpc.template = base_template(height=height, prevhash=prevhash)
                try:
                    server.poll_qbit_tip_template_once()
                except BaseException as exc:  # noqa: BLE001 - surfaced below
                    poll_failures.append(exc)
                    return
                height += 1

        first_notify: dict[int, float] = {}
        notify_lock = threading.Lock()

        def notify_stamper(state: ClientState) -> object:
            def send(payload: dict[str, object]) -> None:
                if payload.get("method") == "mining.notify":
                    with notify_lock:
                        first_notify.setdefault(
                            state.connection_id,
                            time.monotonic(),
                        )

            return send

        churn_thread = threading.Thread(target=churn_tips)
        churn_thread.start()
        requested_at: dict[int, float] = {}
        try:
            server.poll_qbit_tip_template_once()
            for index in range(30):
                state = client(100 + index)
                state.authorization_generation = 1
                state.authorized_monotonic = time.monotonic()
                state.send = notify_stamper(state)  # type: ignore[method-assign]
                with server.lock:
                    server.clients.add(state)
                requested_at[state.connection_id] = time.monotonic()
                self.assertTrue(server.schedule_initial_job(state))
                time.sleep(0.1)
            wait_until(
                lambda: len(first_notify) == len(requested_at),
                timeout=30.0,
            )
        finally:
            stop_churn.set()
            churn_thread.join(10)
            server.shutdown_tip_refresh_executor()

        self.assertEqual(poll_failures, [])
        self.assertEqual(disconnected, [])
        latencies = [
            first_notify[connection_id] - requested
            for connection_id, requested in requested_at.items()
        ]
        self.assertEqual(len(latencies), 30)
        ranked = sorted(latencies)
        p99 = ranked[max(0, int(len(ranked) * 0.99) - 1)]
        self.assertLess(p99, 1.0)


if __name__ == "__main__":
    unittest.main()
