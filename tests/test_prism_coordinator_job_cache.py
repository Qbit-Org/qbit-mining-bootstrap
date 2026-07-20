#!/usr/bin/env python3
"""Direct ownership tests for extracted PRISM template and job services."""

from __future__ import annotations

import copy
from collections import OrderedDict
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import replace as dataclass_replace
import json
import threading
import time
from types import SimpleNamespace
import unittest

from lab.prism.job_bundle import (
    CachedJobBundle,
    JobBuildFlight,
    JobBuildKey,
    JobBuildSuperseded,
)
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    TemplateArtifactEventSink,
    TemplateArtifactPorts,
    TemplateArtifactRepository,
    qbit_template_fingerprint,
)
from tests.prism_coordinator_test_support import (
    base_template,
    coordinator,
    install_fake_bundle_builder,
    worker,
)


def cached_bundle(
    artifacts: CachedTemplateArtifacts,
    *,
    payout_generation: int,
    payout_artifact_sha256: str,
    key: tuple[object, ...] = ("candidate",),
    collection_only: bool = True,
) -> CachedJobBundle:
    build_key = JobBuildKey(
        best_tip_hash=artifacts.previousblockhash,
        previous_block_hash=artifacts.previousblockhash,
        template_fingerprint=artifacts.fingerprint,
        template_generation=artifacts.generation,
        payout_state_generation=payout_generation,
        payout_artifact_sha256=payout_artifact_sha256,
        mode="collection" if collection_only else "ready",
        collection_identity=("miner", "22" * 32) if collection_only else None,
        block_height=int(artifacts.template["height"]),
        coinbase_value_sats=int(artifacts.template["coinbasevalue"]),
        network_difficulty=artifacts.network_difficulty,
        issued_at_ms=1,
        payout_policy_sha256="payout",
        ctv_settlement_sha256=None,
        witness_merkle_sha256="witness",
        transaction_set_sha256="transactions",
        coinbase_suffix_hex="00",
        signing_key_sha256="signing",
        ledger_signing_key_sha256="ledger",
        numeric_context_sha256="numeric",
    )
    return CachedJobBundle(
        key=key,
        template=artifacts.template,
        template_fingerprint=artifacts.fingerprint,
        coinbase_manifest={"coinbase_tx_hex": "00"},
        shares_json=[],
        prior_balances=[],
        found_block={"network_difficulty": artifacts.network_difficulty},
        collection_only=collection_only,
        issued_at_ms=1,
        base_job=SimpleNamespace(),  # type: ignore[arg-type]
        built_monotonic=1.0,
        template_generation=artifacts.generation,
        payout_state_generation=payout_generation,
        collection_identity=build_key.collection_identity,
        build_key=build_key,
    )


def template_repository() -> TemplateArtifactRepository:
    repository = TemplateArtifactRepository(
        TemplateArtifactPorts(
            fetch_template=lambda: base_template(),
            fetch_bestblockhash=lambda: "11" * 32,
            newest_observed_tip=lambda: None,
            observe_tip=lambda _tip: None,
            schedule_refresh_retry=lambda: None,
            pinned_issuance_artifacts=lambda: None,
            repinned_issuance_artifacts=lambda _artifacts: None,
        ),
        cache_seconds=2.0,
        scale_network_difficulty=lambda _bits: 1,
    )
    repository.bind_event_sink(
        TemplateArtifactEventSink(
            record_cache_event=lambda _hit: None,
            record_build_phase=lambda _phase, _elapsed: None,
            artifacts_changed=lambda _artifacts, _fingerprint_changed: None,
            artifacts_cleared=lambda _artifacts: None,
        )
    )
    return repository


class ImmutableArtifactOwnershipTests(unittest.TestCase):
    def test_repository_event_sink_fails_fast_and_binds_once(self) -> None:
        repository = TemplateArtifactRepository(
            TemplateArtifactPorts(
                fetch_template=lambda: base_template(),
                fetch_bestblockhash=lambda: "11" * 32,
                newest_observed_tip=lambda: None,
                observe_tip=lambda _tip: None,
                schedule_refresh_retry=lambda: None,
                pinned_issuance_artifacts=lambda: None,
                repinned_issuance_artifacts=lambda _artifacts: None,
            ),
            cache_seconds=2.0,
            scale_network_difficulty=lambda _bits: 1,
        )
        sink = TemplateArtifactEventSink(
            record_cache_event=lambda _hit: None,
            record_build_phase=lambda _phase, _elapsed: None,
            artifacts_changed=lambda _artifacts, _changed: None,
            artifacts_cleared=lambda _artifacts: None,
        )

        with self.assertRaisesRegex(RuntimeError, "event sink is not bound"):
            repository.derive(base_template(), generation=1)
        repository.bind_event_sink(sink)
        with self.assertRaisesRegex(RuntimeError, "event sink is already bound"):
            repository.bind_event_sink(sink)

    def test_template_artifact_detaches_and_recursively_freezes_json(self) -> None:
        source = base_template()
        source["extension"] = {"rows": [{"value": 1}]}
        expected = copy.deepcopy(source)

        artifacts = CachedTemplateArtifacts(
            template=source,
            fingerprint=qbit_template_fingerprint(source),
            previousblockhash=str(source["previousblockhash"]),
            transaction_hexes=(),
            witness_merkle_leaves_hex=(),
            network_difficulty=1,
            fetched_monotonic=1.0,
            generation=1,
        )

        source["extension"]["rows"][0]["value"] = 2  # type: ignore[index]
        self.assertEqual(artifacts.template, expected)
        self.assertEqual(json.loads(json.dumps(artifacts.template)), expected)
        self.assertIs(copy.deepcopy(artifacts.template), artifacts.template)
        with self.assertRaises(TypeError):
            artifacts.template["height"] = 11
        with self.assertRaises(TypeError):
            artifacts.template["extension"]["rows"].append({})  # type: ignore[index,union-attr]
        with self.assertRaises(TypeError):
            artifacts.template["extension"]["rows"][0]["value"] = 3  # type: ignore[index]

    def test_bundle_detaches_and_recursively_freezes_owned_json(self) -> None:
        template = base_template()
        manifest = {"coinbase_tx_hex": "00", "nested": {"values": [1]}}
        shares = [{"miner_id": "miner-a", "proof": {"path": ["aa"]}}]
        balances = [{"miner_id": "miner-a", "balance_sats": 1}]
        found_block = {"network_difficulty": 1, "nested": {"values": [2]}}

        bundle = CachedJobBundle(
            key=("bundle",),
            template=template,
            template_fingerprint="fingerprint",
            coinbase_manifest=manifest,
            shares_json=shares,
            prior_balances=balances,
            found_block=found_block,
            collection_only=False,
            issued_at_ms=1,
            base_job=SimpleNamespace(),  # type: ignore[arg-type]
            built_monotonic=1.0,
        )

        manifest["nested"]["values"].append(9)  # type: ignore[index,union-attr]
        shares[0]["proof"]["path"].append("bb")  # type: ignore[index,union-attr]
        balances[0]["balance_sats"] = 2
        found_block["nested"]["values"].append(3)  # type: ignore[index,union-attr]
        self.assertEqual(bundle.coinbase_manifest["nested"]["values"], [1])  # type: ignore[index]
        self.assertEqual(bundle.shares_json[0]["proof"]["path"], ["aa"])  # type: ignore[index]
        self.assertEqual(bundle.prior_balances[0]["balance_sats"], 1)
        self.assertEqual(bundle.found_block["nested"]["values"], [2])  # type: ignore[index]
        self.assertIs(copy.deepcopy(bundle.shares_json), bundle.shares_json)
        with self.assertRaises(TypeError):
            bundle.shares_json.append({})
        with self.assertRaises(TypeError):
            bundle.coinbase_manifest["nested"]["values"].append(4)  # type: ignore[index,union-attr]

    def test_repository_derives_from_frozen_artifact_and_orders_stores(self) -> None:
        repository = template_repository()
        source = base_template()
        first = repository.derive(source, generation=1)
        self.assertTrue(repository.store_artifacts(first))

        second = repository.derive(first.template, generation=2)

        self.assertEqual(second.template, first.template)
        self.assertEqual(second.transaction_hexes, first.transaction_hexes)
        self.assertEqual(
            second.witness_merkle_leaves_hex,
            first.witness_merkle_leaves_hex,
        )
        self.assertTrue(repository.store_artifacts(second))
        self.assertFalse(repository.store_artifacts(first))
        self.assertIs(repository.current_artifacts(), second)

    def test_fingerprint_callback_finishes_before_newer_generation_wins(
        self,
    ) -> None:
        second_callback_started = threading.Event()
        release_second_callback = threading.Event()
        third_store_started = threading.Event()
        third_store_finished = threading.Event()
        callback_observations: list[tuple[int, int | None]] = []
        callback_wait_timed_out: list[int] = []
        repository: TemplateArtifactRepository

        def artifacts_changed(
            artifacts: CachedTemplateArtifacts,
            _fingerprint_changed: bool,
        ) -> None:
            if artifacts.generation == 2:
                second_callback_started.set()
                if not release_second_callback.wait(2.0):
                    callback_wait_timed_out.append(artifacts.generation)
            current = repository.current_artifacts()
            callback_observations.append(
                (
                    artifacts.generation,
                    None if current is None else current.generation,
                )
            )

        repository = TemplateArtifactRepository(
            TemplateArtifactPorts(
                fetch_template=lambda: base_template(),
                fetch_bestblockhash=lambda: "11" * 32,
                newest_observed_tip=lambda: None,
                observe_tip=lambda _tip: None,
                schedule_refresh_retry=lambda: None,
                pinned_issuance_artifacts=lambda: None,
                repinned_issuance_artifacts=lambda _artifacts: None,
            ),
            cache_seconds=2.0,
            scale_network_difficulty=lambda _bits: 1,
        )
        repository.bind_event_sink(
            TemplateArtifactEventSink(
                record_cache_event=lambda _hit: None,
                record_build_phase=lambda _phase, _elapsed: None,
                artifacts_changed=artifacts_changed,
                artifacts_cleared=lambda _artifacts: None,
            )
        )
        templates = [base_template() for _generation in range(3)]
        for generation, template in enumerate(templates, start=1):
            template["height"] = generation
        artifacts = [
            repository.derive(template, generation=generation)
            for generation, template in enumerate(templates, start=1)
        ]
        self.assertTrue(repository.store_artifacts(artifacts[0]))

        second_thread = threading.Thread(
            target=repository.store_artifacts,
            args=(artifacts[1],),
        )

        def store_third() -> None:
            third_store_started.set()
            repository.store_artifacts(artifacts[2])
            third_store_finished.set()

        third_thread = threading.Thread(target=store_third)
        second_thread.start()
        self.assertTrue(second_callback_started.wait(2.0))
        third_thread.start()
        self.assertTrue(third_store_started.wait(2.0))
        self.assertFalse(third_store_finished.wait(0.1))
        self.assertIs(repository.current_artifacts(), artifacts[1])

        release_second_callback.set()
        second_thread.join(2.0)
        third_thread.join(2.0)

        self.assertFalse(second_thread.is_alive())
        self.assertFalse(third_thread.is_alive())
        self.assertTrue(third_store_finished.is_set())
        self.assertIs(repository.current_artifacts(), artifacts[2])
        self.assertEqual(callback_wait_timed_out, [])
        self.assertEqual(callback_observations, [(2, 2), (3, 3)])


class JobBundleServiceOwnershipAndPriorityTests(unittest.TestCase):
    def test_coordinator_binds_leaf_callbacks_without_lazy_reentry(self) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        repository = service.template_repository
        compiler = server._ensure_bundle_compiler()
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        callback_finished = threading.Event()
        callback_errors: list[BaseException] = []
        init_lock = server._job_bundle_service_init_lock
        saved_service = server._job_bundle_service

        def use_bound_callback() -> None:
            try:
                repository.current()
            except BaseException as exc:  # noqa: BLE001 - surface thread errors
                callback_errors.append(exc)
            finally:
                callback_finished.set()

        server._job_bundle_service = None
        init_lock.acquire()
        try:
            callback_thread = threading.Thread(target=use_bound_callback)
            callback_thread.start()
            self.assertTrue(callback_finished.wait(2.0))
            callback_thread.join(2.0)
        finally:
            init_lock.release()
            server._job_bundle_service = saved_service

        self.assertEqual(callback_errors, [])
        self.assertIs(service.bundle_compiler(), compiler)
        with self.assertRaisesRegex(RuntimeError, "compiler is already bound"):
            service.bind_bundle_compiler(compiler)
    @staticmethod
    def request_for(
        server: object,
        artifacts: CachedTemplateArtifacts,
        *,
        mode: str = "ready",
        identity: object | None = None,
        publication_critical: bool = False,
        request_source: str = "initial",
        priority_requested_monotonic: float | None = None,
    ) -> object:
        service = server._ensure_job_bundle_service()  # type: ignore[attr-defined]
        payout_generation = (
            server._ensure_payout_state_service().snapshot().generation  # type: ignore[attr-defined]
        )
        cache_key = service.job_bundle_key(
            artifacts,
            mode=mode,
            payout_state_generation=payout_generation,
            worker=identity,
        )
        return service.new_build_request(
            artifacts,
            identity,
            mode=mode,
            payout_state_generation=payout_generation,
            cache_key=cache_key,
            publication_critical=publication_critical,
            request_source=request_source,
            priority_requested_monotonic=priority_requested_monotonic,
        )

    def test_publication_critical_build_cannot_be_displaced_by_initial_work(
        self,
    ) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        latest = self.request_for(
            server,
            new_artifacts,
            publication_critical=True,
            request_source="tip_refresh",
        )
        reconnect = self.request_for(server, old_artifacts)
        same_tip_reconnect = self.request_for(server, new_artifacts)
        latest_flight = JobBuildFlight(latest)  # type: ignore[arg-type]
        service._active = latest_flight

        deferred = service.request_build(reconnect)  # type: ignore[arg-type]
        coalesced = service.request_build(  # type: ignore[arg-type]
            same_tip_reconnect
        )

        self.assertIs(service._active, latest_flight)
        self.assertIs(coalesced, latest.promise)  # type: ignore[union-attr]
        self.assertFalse(latest.cancellation.is_set())  # type: ignore[union-attr]
        self.assertFalse(deferred.done())
        self.assertEqual(service._priority_counts["routine_deferred"], 1)
        self.assertEqual(service._initial_prepared_work_counts["deferred"], 1)
        self.assertEqual(
            service._initial_prepared_work_counts["singleflight"],
            1,
        )
        latest.promise.set_result(object())  # type: ignore[union-attr]
        with self.assertRaises(JobBuildSuperseded):
            deferred.result()
        metrics = "\n".join(server.job_build_metrics_lines())
        self.assertIn(
            'qbit_prism_job_build_priority_events_total{result="routine_deferred"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_initial_job_prepared_work_total{result="singleflight"} 1',
            metrics,
        )

    def test_publication_critical_build_preempts_routine_builder_capacity(
        self,
    ) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        routine = self.request_for(server, old_artifacts)
        latest = self.request_for(
            server,
            new_artifacts,
            publication_critical=True,
            request_source="tip_refresh",
        )
        routine_flight = JobBuildFlight(routine)  # type: ignore[arg-type]
        service._active = routine_flight
        service._start_locked = (  # type: ignore[method-assign]
            lambda request: JobBuildFlight(request)
        )
        service._arm_locked = lambda _flight: None  # type: ignore[method-assign]

        promise = service.request_build(latest)  # type: ignore[arg-type]

        self.assertIs(promise, latest.promise)  # type: ignore[union-attr]
        self.assertTrue(routine.cancellation.is_set())  # type: ignore[union-attr]
        self.assertIs(service._retiring, routine_flight)
        assert service._active is not None
        self.assertIs(service._active.request, latest)
        self.assertEqual(service._priority_counts["routine_preempted"], 1)

    def test_publication_critical_build_restarts_unhealthy_exact_flight(
        self,
    ) -> None:
        for unhealthy in ("almost_expired", "stalled"):
            with self.subTest(unhealthy=unhealthy):
                server, rpc = coordinator()
                service = server._ensure_job_bundle_service()
                artifacts = server.store_template_artifacts(dict(rpc.template))
                assert artifacts is not None
                routine = self.request_for(server, artifacts)
                now = time.monotonic()
                if unhealthy == "almost_expired":
                    routine.cancellation.started_monotonic = now - 59.99
                    routine.cancellation.deadline_monotonic = now + 0.01
                    routine.cancellation.last_checkpoint_monotonic = now
                else:
                    routine.cancellation.started_monotonic = now - 1.0
                    routine.cancellation.deadline_monotonic = now + 59.0
                    routine.cancellation.last_checkpoint_monotonic = (
                        now - service._config.cancel_grace_seconds - 0.01
                    )
                priority_requested = now - 59.0
                latest = self.request_for(
                    server,
                    artifacts,
                    publication_critical=True,
                    request_source="tip_refresh",
                    priority_requested_monotonic=priority_requested,
                )
                routine_flight = JobBuildFlight(routine)  # type: ignore[arg-type]
                service._active = routine_flight
                service._start_locked = (  # type: ignore[method-assign]
                    lambda request: JobBuildFlight(request)
                )
                service._arm_locked = lambda _flight: None  # type: ignore[method-assign]

                promise = service.request_build(latest)  # type: ignore[arg-type]

                self.assertIs(promise, latest.promise)  # type: ignore[union-attr]
                self.assertIsNot(promise, routine.promise)  # type: ignore[union-attr]
                self.assertTrue(routine.cancellation.is_set())  # type: ignore[union-attr]
                self.assertIs(service._retiring, routine_flight)
                assert service._active is not None
                self.assertIs(service._active.request, latest)
                self.assertEqual(
                    latest.requested_monotonic,  # type: ignore[union-attr]
                    priority_requested,
                )
                self.assertEqual(service._priority_counts["routine_preempted"], 1)

    def test_publication_priority_precedes_immutable_request_preparation(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        service = server._ensure_job_bundle_service()
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        critical_entered = threading.Event()
        release_critical = threading.Event()
        initial_entered = threading.Event()
        original_new_request = service.new_build_request

        def observed_new_request(*args: object, **kwargs: object) -> object:
            request_source = str(kwargs.get("request_source", "routine"))
            if request_source == "tip_refresh":
                critical_entered.set()
                if not release_critical.wait(5):
                    raise AssertionError("test did not release priority preparation")
            elif request_source == "initial":
                initial_entered.set()
            return original_new_request(*args, **kwargs)  # type: ignore[arg-type]

        service.new_build_request = observed_new_request  # type: ignore[method-assign]
        results: dict[str, list[object]] = {"critical": [], "initial": []}
        errors: list[BaseException] = []

        def build(label: str, *, publication_critical: bool) -> None:
            try:
                results[label].append(
                    service.shared_job_bundle(
                        artifacts,
                        mode="ready",
                        publication_critical=publication_critical,
                        request_source=(
                            "tip_refresh" if publication_critical else "initial"
                        ),
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        critical_thread = threading.Thread(
            target=build,
            args=("critical",),
            kwargs={"publication_critical": True},
        )
        initial_thread = threading.Thread(
            target=build,
            args=("initial",),
            kwargs={"publication_critical": False},
        )
        critical_thread.start()
        try:
            self.assertTrue(critical_entered.wait(5))
            initial_thread.start()
            self.assertFalse(initial_entered.wait(0.1))
            self.assertIn(
                "qbit_prism_job_build_priority_active 1",
                "\n".join(server.job_build_metrics_lines()),
            )
        finally:
            release_critical.set()
            critical_thread.join(5)
            initial_thread.join(5)
            service.shutdown()
        self.assertEqual(errors, [])
        self.assertEqual([len(results[label]) for label in results], [1, 1])
        self.assertIs(results["critical"][0], results["initial"][0])
        self.assertFalse(initial_entered.is_set())
        self.assertEqual(service._initial_prepared_work_counts["deferred"], 1)
        self.assertEqual(service._initial_prepared_work_counts["cache_hit"], 1)
        self.assertEqual(service._priority_admission_seconds["count"], 1)
        self.assertGreaterEqual(service._priority_admission_seconds["sum"], 0.1)

    def test_priority_reservation_cancels_admitted_routine_preparation(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        service = server._ensure_job_bundle_service()
        payout = server._ensure_payout_state_service()
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        routine_in_lookup = threading.Event()
        release_routine = threading.Event()
        routine_constructed = threading.Event()
        original_usable = payout.usable_ledger_artifact
        original_new_request = service.new_build_request
        routine_thread: threading.Thread

        def blocked_usable(*args: object, **kwargs: object) -> object:
            if threading.current_thread() is routine_thread:
                routine_in_lookup.set()
                if not release_routine.wait(5):
                    raise AssertionError("test did not release payout lookup")
            return original_usable(*args, **kwargs)  # type: ignore[arg-type]

        def observed_new_request(*args: object, **kwargs: object) -> object:
            if kwargs.get("request_source") == "initial":
                routine_constructed.set()
            return original_new_request(*args, **kwargs)  # type: ignore[arg-type]

        payout.usable_ledger_artifact = blocked_usable  # type: ignore[method-assign]
        service.new_build_request = observed_new_request  # type: ignore[method-assign]
        errors: list[BaseException] = []

        def build_routine() -> None:
            try:
                service.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                    request_source="initial",
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        routine_thread = threading.Thread(target=build_routine)
        routine_thread.start()
        priority_token: int | None = None
        try:
            self.assertTrue(routine_in_lookup.wait(5))
            with service._scheduler_lock:
                cancellations = tuple(
                    cancellation_ref()
                    for cancellation_ref in service._routine_preparations.values()
                )
            self.assertEqual(len(cancellations), 1)
            self.assertIsNotNone(cancellations[0])
            priority_token, _requested = service.begin_priority_preparation()
            assert cancellations[0] is not None
            self.assertTrue(cancellations[0].is_set())
        finally:
            release_routine.set()
            routine_thread.join(5)
            if priority_token is not None:
                service.finish_priority_preparation(priority_token)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        self.assertFalse(routine_constructed.is_set())
        with service._scheduler_lock:
            self.assertEqual(len(service._routine_preparations), 0)

    def test_publication_critical_collection_promotes_past_ready_work(self) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        ready = self.request_for(server, old_artifacts)
        latest = self.request_for(
            server,
            new_artifacts,
            mode="collection",
            identity=worker("tq1latest", "tq1latest.rig"),
            publication_critical=True,
            request_source="tip_refresh",
        )
        ready_flight = JobBuildFlight(ready)  # type: ignore[arg-type]
        service._active = ready_flight
        service._pending = latest  # type: ignore[assignment]
        armed: list[object] = []
        service._start_locked = (  # type: ignore[method-assign]
            lambda request: JobBuildFlight(request)
        )
        service._arm_locked = armed.append  # type: ignore[method-assign]

        service._promote_pending_locked()

        self.assertTrue(ready.cancellation.is_set())  # type: ignore[union-attr]
        self.assertIs(service._retiring, ready_flight)
        self.assertIsNone(service._pending)
        assert service._active is not None
        self.assertIs(service._active.request, latest)
        self.assertEqual(armed, [service._active])
        self.assertEqual(service._priority_counts["routine_preempted"], 1)

    def test_routine_pending_never_promotes_over_publication_critical_flight(
        self,
    ) -> None:
        for placement in ("active", "retiring"):
            with self.subTest(placement=placement):
                server, rpc = coordinator()
                service = server._ensure_job_bundle_service()
                critical_artifacts = server.store_template_artifacts(
                    dict(rpc.template)
                )
                routine_artifacts = server.store_template_artifacts(
                    base_template(height=11, prevhash="22" * 32)
                )
                assert critical_artifacts is not None
                assert routine_artifacts is not None
                critical = self.request_for(
                    server,
                    critical_artifacts,
                    publication_critical=True,
                    request_source="tip_refresh",
                )
                routine = self.request_for(server, routine_artifacts)
                critical_flight = JobBuildFlight(critical)  # type: ignore[arg-type]
                service._active = (
                    critical_flight if placement == "active" else None
                )
                service._retiring = (
                    critical_flight if placement == "retiring" else None
                )
                service._pending = routine  # type: ignore[assignment]
                service._start_locked = (  # type: ignore[method-assign]
                    lambda _request: self.fail(
                        "routine pending work displaced publication-critical work"
                    )
                )

                service._promote_pending_locked()

                self.assertIs(service._pending, routine)
                self.assertFalse(critical.cancellation.is_set())  # type: ignore[union-attr]
                self.assertIs(
                    service._active if placement == "active" else service._retiring,
                    critical_flight,
                )

    def test_service_owns_bundle_build_token_success_lifetime(self) -> None:
        server, _rpc = coordinator()
        service = server._ensure_job_bundle_service()
        events: list[str] = []
        expected = SimpleNamespace()

        @contextmanager
        def build_token():
            events.append("start")
            try:
                yield object()
            finally:
                events.append("finish")

        service._ports = dataclass_replace(  # type: ignore[assignment]
            service._ports,
            start_bundle_build=build_token,
        )
        service._shared_job_bundle = lambda *_args, **_kwargs: expected  # type: ignore[method-assign]

        actual = service.shared_job_bundle(SimpleNamespace())  # type: ignore[arg-type]

        self.assertIs(actual, expected)
        self.assertEqual(events, ["start", "finish"])

    def test_service_finishes_bundle_build_token_on_exception(self) -> None:
        server, _rpc = coordinator()
        service = server._ensure_job_bundle_service()
        events: list[str] = []

        @contextmanager
        def build_token():
            events.append("start")
            try:
                yield object()
            finally:
                events.append("finish")

        def fail(*_args: object, **_kwargs: object) -> CachedJobBundle:
            raise RuntimeError("build failed")

        service._ports = dataclass_replace(  # type: ignore[assignment]
            service._ports,
            start_bundle_build=build_token,
        )
        service._shared_job_bundle = fail  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "build failed"):
            service.shared_job_bundle(SimpleNamespace())  # type: ignore[arg-type]

        self.assertEqual(events, ["start", "finish"])

    def test_build_done_rechecks_cancellation_under_scheduler_lock(self) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        payout_state = server._ensure_payout_state_service()
        payout_artifact = payout_state.current_artifact()
        request = service.new_build_request(
            artifacts,
            None,
            mode="ready",
            payout_state_generation=payout_artifact.generation,
            cache_key=("build-done-race",),
        )
        result = cached_bundle(
            artifacts,
            payout_generation=payout_artifact.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
            collection_only=False,
        )
        completed: Future[CachedJobBundle] = Future()
        completed.set_result(result)
        flight = JobBuildFlight(request=request, future=completed)
        lock_attempted = threading.Event()
        original_lock = service._scheduler_lock

        class InstrumentedSchedulerLock:
            def __init__(self) -> None:
                self.entries = 0

            def __enter__(self) -> None:
                self.entries += 1
                if self.entries > 1:
                    lock_attempted.set()
                original_lock.acquire()

            def __exit__(
                self,
                _exc_type: object,
                _exc_value: object,
                _traceback: object,
            ) -> None:
                original_lock.release()

        instrumented_lock = InstrumentedSchedulerLock()
        service._scheduler_lock = instrumented_lock  # type: ignore[assignment]
        before = dict(service._scheduler_counts)
        shared_before = dict(service._shared_build_counts)
        try:
            with instrumented_lock:
                done_thread = threading.Thread(
                    target=service._build_done,
                    args=(flight, completed),
                )
                done_thread.start()
                self.assertTrue(lock_attempted.wait(2.0))
                self.assertTrue(request.cancellation.cancel("shutdown"))
            done_thread.join(2.0)
        finally:
            service._scheduler_lock = original_lock

        self.assertFalse(done_thread.is_alive())
        with self.assertRaises(JobBuildSuperseded):
            request.promise.result()
        self.assertEqual(
            service._scheduler_counts["completions"],
            before["completions"] + 1,
        )
        self.assertEqual(
            service._scheduler_counts["obsolete_results"],
            before["obsolete_results"] + 1,
        )
        self.assertEqual(
            service._shared_build_counts["superseded"],
            shared_before["superseded"] + 1,
        )
        self.assertEqual(
            service._shared_build_counts["completed"],
            shared_before["completed"],
        )
        with service._cache_lock:
            self.assertNotIn(result.key, service._bundle_cache)

    def test_readiness_promotion_is_one_way_and_emits_once(self) -> None:
        server, _rpc = coordinator()
        service = server._ensure_job_bundle_service()
        ready_miners = 0
        events: list[str] = []
        service.set_ready_for_test(False)
        service._ports = dataclass_replace(  # type: ignore[assignment]
            service._ports,
            accepted_share_stats=lambda: (0, ready_miners),
            clear_retained_collection_refresh=lambda: events.append("clear"),
            readiness_promoted=lambda: events.append("pending"),
        )

        self.assertFalse(service.pool_readiness_latched())
        ready_miners = 3
        self.assertTrue(service.pool_readiness_latched())
        self.assertTrue(service.pool_readiness_latched())
        self.assertEqual(events, ["clear", "pending"])


class JobBundleCacheAdmissionTests(unittest.TestCase):
    def test_collection_pinning_requires_exact_published_generation(self) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        repository = service.template_repository
        published = server.store_template_artifacts(dict(rpc.template))
        assert published is not None
        stale = repository.derive(
            published.template,
            generation=repository.reserve_generation(),
        )
        self.assertTrue(repository.store_artifacts(stale))
        current = repository.derive(
            stale.template,
            generation=repository.reserve_generation(),
        )
        self.assertTrue(repository.store_artifacts(current))
        payout_state = server._ensure_payout_state_service()
        payout_artifact = payout_state.current_artifact()
        service._ports = dataclass_replace(  # type: ignore[assignment]
            service._ports,
            artifacts_buildable=lambda _artifacts: True,
            published_snapshot_artifacts=lambda artifacts: (
                artifacts.fingerprint == published.fingerprint
                and artifacts.previousblockhash == published.previousblockhash
            ),
            published_artifacts=lambda: published,
        )
        stale_collection = cached_bundle(
            stale,
            payout_generation=payout_artifact.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
            key=("stale-collection",),
        )
        exact_published_collection = cached_bundle(
            published,
            payout_generation=payout_artifact.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
            key=("published-collection",),
        )
        reusable_ready = cached_bundle(
            stale,
            payout_generation=payout_artifact.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
            key=("reusable-ready",),
            collection_only=False,
        )
        sentinel = cached_bundle(
            current,
            payout_generation=payout_artifact.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
            collection_only=False,
        )
        with service._cache_lock:
            service._bundle_cache = OrderedDict(
                (("sentinel", index), sentinel)
                for index in range(128)
            )

        self.assertFalse(
            service.cache_bundle_if_current(stale_collection, stale)
        )
        with service._cache_lock:
            self.assertEqual(len(service._bundle_cache), 128)
            self.assertIn(("sentinel", 0), service._bundle_cache)
            self.assertNotIn(stale_collection.key, service._bundle_cache)
            service._bundle_cache.clear()

        self.assertTrue(
            service.cache_bundle_if_current(
                exact_published_collection,
                published,
            )
        )
        self.assertTrue(service.cache_bundle_if_current(reusable_ready, stale))

    def test_clear_cannot_linearize_inside_final_cache_admission(self) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        repository = service.template_repository
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        payout_state = server._ensure_payout_state_service()
        payout_artifact = payout_state.current_artifact()
        built = cached_bundle(
            artifacts,
            payout_generation=payout_artifact.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
        )
        final_validation_started = threading.Event()
        release_final_validation = threading.Event()
        validation_calls = 0

        def artifacts_buildable(_artifacts: CachedTemplateArtifacts) -> bool:
            nonlocal validation_calls
            validation_calls += 1
            if validation_calls == 2:
                final_validation_started.set()
                release_final_validation.wait(2.0)
            return True

        service._ports = dataclass_replace(  # type: ignore[assignment]
            service._ports,
            artifacts_buildable=artifacts_buildable,
            published_snapshot_artifacts=lambda _artifacts: False,
            published_artifacts=lambda: None,
        )
        cache_results: list[bool] = []
        clear_results: list[bool] = []
        clear_finished = threading.Event()
        cache_thread = threading.Thread(
            target=lambda: cache_results.append(
                service.cache_bundle_if_current(built, artifacts)
            )
        )

        def clear_current() -> None:
            clear_results.append(repository.clear_if_current(artifacts))
            clear_finished.set()

        cache_thread.start()
        self.assertTrue(final_validation_started.wait(2.0))
        clear_thread = threading.Thread(target=clear_current)
        clear_thread.start()
        self.assertFalse(clear_finished.wait(0.1))
        release_final_validation.set()
        cache_thread.join(2.0)
        clear_thread.join(2.0)

        self.assertFalse(cache_thread.is_alive())
        self.assertFalse(clear_thread.is_alive())
        self.assertEqual(cache_results, [True])
        self.assertEqual(clear_results, [True])
        self.assertIsNone(repository.current_artifacts())
        with service._cache_lock:
            self.assertNotIn(built.key, service._bundle_cache)

    def _template_race(self, *, same_fingerprint: bool) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        repository = service.template_repository
        old = server.store_template_artifacts(dict(rpc.template))
        assert old is not None
        payout_state = server._ensure_payout_state_service()
        payout_state.current_artifact()
        payout = payout_state.snapshot()
        payout_artifact = payout.published.artifact
        assert payout_artifact is not None
        new_template = json.loads(json.dumps(old.template))
        if not same_fingerprint:
            new_template["height"] = int(new_template["height"]) + 1
        new = repository.derive(
            new_template,
            generation=repository.reserve_generation(),
        )
        old_bundle = cached_bundle(
            old,
            payout_generation=payout.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
        )
        sentinel = cached_bundle(
            new,
            payout_generation=payout.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
            collection_only=False,
        )
        with service._cache_lock:
            service._bundle_cache = OrderedDict(
                (("sentinel", index), sentinel)
                for index in range(128)
            )
        first_validation = threading.Event()
        release_validation = threading.Event()
        validation_calls = 0
        validation_lock = threading.Lock()

        def artifacts_buildable(_artifacts: CachedTemplateArtifacts) -> bool:
            nonlocal validation_calls
            with validation_lock:
                validation_calls += 1
                block = validation_calls == 1
            if block:
                first_validation.set()
                release_validation.wait(2.0)
            return True

        service._ports = dataclass_replace(  # type: ignore[assignment]
            service._ports,
            artifacts_buildable=artifacts_buildable,
            published_snapshot_artifacts=lambda _artifacts: False,
            published_artifacts=lambda: None,
        )
        cache_results: list[bool] = []
        cache_thread = threading.Thread(
            target=lambda: cache_results.append(
                service.cache_bundle_if_current(old_bundle, old)
            )
        )
        cache_thread.start()
        self.assertTrue(first_validation.wait(2.0))

        self.assertTrue(repository.store_artifacts(new))
        self.assertIs(repository.current_artifacts(), new)
        release_validation.set()
        cache_thread.join(2.0)

        self.assertFalse(cache_thread.is_alive())
        self.assertEqual(cache_results, [False])
        with service._cache_lock:
            self.assertNotIn(old_bundle.key, service._bundle_cache)
            self.assertEqual(len(service._bundle_cache), 128)
            self.assertIn(("sentinel", 0), service._bundle_cache)

    def test_old_completion_cannot_cache_after_new_fingerprint_publication(
        self,
    ) -> None:
        self._template_race(same_fingerprint=False)

    def test_old_collection_cannot_cache_after_same_fingerprint_generation(
        self,
    ) -> None:
        self._template_race(same_fingerprint=True)

    def test_old_completion_cannot_cache_after_payout_invalidation(self) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        old = server.store_template_artifacts(dict(rpc.template))
        assert old is not None
        payout_state = server._ensure_payout_state_service()
        payout_state.current_artifact()
        payout = payout_state.snapshot()
        payout_artifact = payout.published.artifact
        assert payout_artifact is not None
        old_bundle = cached_bundle(
            old,
            payout_generation=payout.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
        )
        service._ports = dataclass_replace(  # type: ignore[assignment]
            service._ports,
            artifacts_buildable=lambda _artifacts: True,
            published_snapshot_artifacts=lambda _artifacts: False,
        )
        payout_validated = threading.Event()
        release_validation = threading.Event()
        original_snapshot = payout_state.snapshot
        snapshot_calls = 0

        def blocking_snapshot():  # type: ignore[no-untyped-def]
            nonlocal snapshot_calls
            snapshot = original_snapshot()
            snapshot_calls += 1
            if snapshot_calls == 1:
                payout_validated.set()
                release_validation.wait(2.0)
            return snapshot

        payout_state.snapshot = blocking_snapshot  # type: ignore[method-assign]
        cache_results: list[bool] = []
        cache_thread = threading.Thread(
            target=lambda: cache_results.append(
                service.cache_bundle_if_current(old_bundle, old)
            )
        )
        try:
            cache_thread.start()
            self.assertTrue(payout_validated.wait(2.0))
            payout_state.block_publication(force=True)
            release_validation.set()
            cache_thread.join(2.0)
        finally:
            payout_state.snapshot = original_snapshot  # type: ignore[method-assign]
            release_validation.set()

        self.assertFalse(cache_thread.is_alive())
        self.assertEqual(cache_results, [False])
        with service._cache_lock:
            self.assertNotIn(old_bundle.key, service._bundle_cache)

    def test_payout_fence_wait_does_not_block_template_publication(self) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        repository = service.template_repository
        old = server.store_template_artifacts(dict(rpc.template))
        assert old is not None
        payout_state = server._ensure_payout_state_service()
        payout_artifact = payout_state.current_artifact()
        payout = payout_state.snapshot()
        new_template = json.loads(json.dumps(old.template))
        new_template["height"] = int(new_template["height"]) + 1
        new = repository.derive(
            new_template,
            generation=repository.reserve_generation(),
        )
        old_bundle = cached_bundle(
            old,
            payout_generation=payout.generation,
            payout_artifact_sha256=payout_artifact.prior_balances_sha256,
        )
        service._ports = dataclass_replace(  # type: ignore[assignment]
            service._ports,
            artifacts_buildable=lambda _artifacts: True,
            published_snapshot_artifacts=lambda _artifacts: False,
            published_artifacts=lambda: None,
        )
        cache_results: list[bool] = []
        cache_admission_attempted = threading.Event()
        original_admission = payout_state.cache_publication_admission

        @contextmanager
        def signaling_admission():  # type: ignore[no-untyped-def]
            cache_admission_attempted.set()
            with original_admission():
                yield

        def cache_old() -> None:
            cache_results.append(service.cache_bundle_if_current(old_bundle, old))

        cache_thread = threading.Thread(
            target=cache_old,
        )
        new_store_finished = threading.Event()

        def store_new() -> None:
            repository.store_artifacts(new)
            new_store_finished.set()

        try:
            with original_admission():
                payout_state.cache_publication_admission = (  # type: ignore[method-assign]
                    signaling_admission
                )
                cache_thread.start()
                self.assertTrue(cache_admission_attempted.wait(2.0))
                store_thread = threading.Thread(target=store_new)
                store_thread.start()
                self.assertTrue(new_store_finished.wait(2.0))
                store_thread.join(2.0)
        finally:
            payout_state.cache_publication_admission = (  # type: ignore[method-assign]
                original_admission
            )

        cache_thread.join(2.0)
        self.assertFalse(cache_thread.is_alive())
        self.assertFalse(store_thread.is_alive())
        self.assertEqual(cache_results, [False])
        self.assertIs(repository.current_artifacts(), new)
        with service._cache_lock:
            self.assertNotIn(old_bundle.key, service._bundle_cache)


if __name__ == "__main__":
    unittest.main()
