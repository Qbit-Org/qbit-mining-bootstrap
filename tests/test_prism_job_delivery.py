#!/usr/bin/env python3
"""Direct contract tests for the coordinator-free S2 boundary."""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import replace
from decimal import Decimal
import inspect
import threading
import time
import unittest
from unittest.mock import Mock
from types import SimpleNamespace

from lab.auxpow import vardiff
from lab.prism.direct_stratum import DirectQbitStratumJob
from lab.prism.job_delivery import (
    AdmittedIdleBundleSource,
    DeliveryCompatibilityHooks,
    DeliverySourceAuthority,
    EvictedJobEntry,
    IdleDeliveryAuthority,
    InitialJobRuntimePort,
    InitialJobState,
    JobDeliveryRuntimePort,
    JobPreparationPort,
    JobDeliveryService,
    PayoutDeliveryPort,
    PendingInitialJob,
    PrismJobContext,
    ProgressDeliveryPort,
    RetainedJobIndex,
    TipAuthorityPort,
)
import lab.prism.job_delivery as job_delivery_module
from lab.prism.prism_coordinator import (
    EvictedJobEntry as FacadeEvictedJobEntry,
    PendingInitialJob as FacadePendingInitialJob,
    PrismCoordinator,
    PrismJobContext as FacadePrismJobContext,
)
from lab.prism.stratum_session import ClientState, SessionRegistry, WorkerIdentity
from lab.prism.template_artifacts import (
    CachedTemplateArtifacts,
    QbitTipTemplateSnapshot,
)
from tests.prism_vardiff_test_support import (
    client as compatibility_client,
    coordinator as compatibility_coordinator,
    install_idle_job_cache,
    prepare_idle_client,
)


TIP_A = "11" * 32
TIP_B = "22" * 32


def worker(name: str = "miner-a") -> WorkerIdentity:
    return WorkerIdentity(
        username=name,
        payout_address=name,
        worker_name=None,
        script_pubkey_hex="5220" + "33" * 32,
        p2mr_program_hex="33" * 32,
    )


def client(connection_id: int = 1) -> ClientState:
    state = ClientState(
        sock=object(),
        address=("127.0.0.1", 1),
        connection_id=connection_id,
        extranonce1_hex=f"{connection_id:08x}",
        share_difficulty=Decimal("1"),
    )
    state.subscribed = True
    state.authorized = True
    state.worker = worker()
    state.username = state.worker.username
    return state


def job(job_id: str, *, clean_jobs: bool = True) -> DirectQbitStratumJob:
    return DirectQbitStratumJob(
        job_id=job_id,
        previousblockhash_display=TIP_A,
        prevhash=TIP_A,
        coinb1="",
        coinb2="",
        full_coinbase_prefix="",
        full_coinbase_suffix="",
        merkle_branch=(),
        transaction_hexes=(),
        version="20000000",
        nbits="207fffff",
        ntime="6553f100",
        qbit_target=(1 << 255),
        share_target=(1 << 255),
        share_difficulty=Decimal("1"),
        extranonce1_hex="00000001",
        extranonce2_size=8,
        clean_jobs=clean_jobs,
    )


def context(
    job_id: str,
    *,
    parent: str = TIP_A,
    owner: WorkerIdentity | None = None,
    collection_only: bool = False,
    template_generation: int = 3,
    payout_generation: int = 7,
) -> PrismJobContext:
    return PrismJobContext(
        job=job(job_id),
        template={"previousblockhash": parent},
        shares_json=[],
        prior_balances=[],
        found_block={},
        share_weight=1,
        collection_only=collection_only,
        worker=owner or worker(),
        issued_at_ms=1,
        template_fingerprint="fingerprint",
        template_generation=template_generation,
        payout_state_generation=payout_generation,
        connection_id=1,
        authorization_generation=0,
        difficulty_generation=0,
    )


def tip_snapshot(
    tip_hash: str = TIP_A,
    *,
    generation: int = 3,
    fingerprint: str = "fingerprint",
) -> QbitTipTemplateSnapshot:
    artifacts = CachedTemplateArtifacts(
        template={"previousblockhash": tip_hash},
        fingerprint=fingerprint,
        previousblockhash=tip_hash,
        transaction_hexes=(),
        witness_merkle_leaves_hex=(),
        network_difficulty=1,
        fetched_monotonic=time.monotonic(),
        generation=generation,
    )
    return QbitTipTemplateSnapshot(
        bestblockhash=tip_hash,
        previousblockhash=tip_hash,
        template_fingerprint=fingerprint,
        template_generation=generation,
        template_artifacts=artifacts,
    )


class Runtime:
    def __init__(self) -> None:
        self.counter = 0
        self.payout_generation = 7
        self.ready = False
        self.events: list[str] = []

    def next_job_id(self) -> str:
        self.counter += 1
        return f"stamped-{self.counter}"

    def collection_identity(self, owner: WorkerIdentity) -> object:
        return (owner.payout_address, owner.script_pubkey_hex)

    def desired_share_difficulty(self, state: ClientState) -> Decimal:
        return state.pending_share_difficulty or state.share_difficulty

    def minimum_advertised_difficulty(self, _state: ClientState) -> Decimal:
        return Decimal("0")

    def share_weight(self, _owner: WorkerIdentity) -> int:
        return 5

    def current_payout_generation(self) -> int:
        return self.payout_generation

    def ready_latched(self) -> bool:
        return self.ready

    def template_fingerprint(self, _template: object) -> str:
        return "fingerprint"

    def send_difficulty(self, _state: ClientState, _job: object) -> None:
        self.events.append("difficulty")

    def send_job(self, _state: ClientState, _job: object) -> None:
        self.events.append("notify")

    def send_job_batch(self, _state: ClientState, _job: object) -> None:
        self.events.extend(("difficulty", "notify"))


def service(
    state: ClientState | None = None,
) -> tuple[JobDeliveryService, SessionRegistry, Runtime]:
    state = state or client()
    registry = SessionRegistry(
        lock=threading.RLock(),
        clients={state},
        connection_generation=state.connection_id,
        rejection_counts={"global": 0, "username": 0},
    )
    runtime = Runtime()
    delivery = JobDeliveryService(
        registry=registry,
        runtime=runtime,
        jobs={},
        retained=RetainedJobIndex(),
        preparation=SimpleNamespace(
            collection_identity=runtime.collection_identity,
            ready_latched=runtime.ready_latched,
            template_fingerprint=runtime.template_fingerprint,
        ),  # type: ignore[arg-type]
        payout=SimpleNamespace(
            generation=runtime.current_payout_generation,
        ),  # type: ignore[arg-type]
    )
    return delivery, registry, runtime


class PrismJobDeliveryTests(unittest.TestCase):
    def test_s2_ports_are_capability_scoped_and_hide_coordinator_context(self) -> None:
        self.assertFalse(hasattr(job_delivery_module, "JobDeliveryOperationsPort"))
        expected_methods = {
            JobDeliveryRuntimePort: {
                "desired_share_difficulty",
                "minimum_advertised_difficulty",
                "share_weight",
                "vardiff_config",
                "send_difficulty",
                "send_job",
                "send_job_batch",
            },
            JobPreparationPort: {
                "ensure_reorg_current",
                "issuance_artifacts",
                "shared_bundle",
                "artifacts_current",
                "clear_artifacts",
                "record_failure",
                "phases",
                "retained_artifacts",
                "chain_view_untrusted",
                "admit_idle_bundle_source",
                "observe_elapsed",
                "collection_identity",
                "ready_latched",
                "template_fingerprint",
            },
            TipAuthorityPort: {
                "live_tip",
                "observe_tip",
                "published_authority",
                "published_authoritative",
                "current_tip_locked",
                "published_template_locked",
                "snapshot_current_locked",
                "artifacts_parent_current_locked",
                "ensure_artifacts_parent_observed",
                "schedule_retry",
                "prepared_obsolete",
                "prepared_token_current_locked",
                "record_cancellation",
                "retention_authority_locked",
                "consume_retained_refresh",
                "published_current_locked",
            },
            PayoutDeliveryPort: {
                "snapshot",
                "generation",
                "initial_admission",
                "admission",
                "observe_admission",
                "record_first_delivery",
            },
            InitialJobRuntimePort: {
                "stopping",
                "wait",
                "disconnect",
                "submit_initial",
            },
            ProgressDeliveryPort: {
                "record_health_delivery",
                "reconcile_health_eligibility",
            },
        }
        for port, methods in expected_methods.items():
            with self.subTest(port=port.__name__):
                public = {
                    name
                    for name, value in port.__dict__.items()
                    if not name.startswith("_") and inspect.isfunction(value)
                }
                self.assertEqual(public, methods)

        server = compatibility_coordinator()
        delivery = server._ensure_job_delivery_service()
        for capability in (
            delivery.preparation,
            delivery.tip_authority,
            delivery.payout,
            delivery.initial_runtime,
            delivery.progress,
        ):
            self.assertFalse(hasattr(capability, "coordinator"))
        self.assertFalse(hasattr(delivery.initial_runtime, "executor"))
        self.assertFalse(hasattr(delivery.initial_runtime, "submit"))

    def test_compatibility_hooks_resolve_only_instance_overrides(self) -> None:
        server = compatibility_coordinator()
        hooks = server._ensure_job_delivery_service().hooks
        self.assertIsInstance(hooks, DeliveryCompatibilityHooks)
        assert hooks is not None
        self.assertIsNone(hooks.build_job_override())

        def replacement(*_args: object, **_kwargs: object) -> PrismJobContext:
            return context("replacement")

        server.build_job_for_client = replacement  # type: ignore[method-assign]
        self.assertIs(hooks.build_job_override(), replacement)

    def test_r1_prune_resolves_late_override_or_calls_s2_directly(self) -> None:
        server = compatibility_coordinator()
        tip_refresh = server._ensure_tip_refresh_service()
        delivery = server._ensure_job_delivery_service()
        direct_calls: list[tuple[float | None, bool]] = []

        def direct_prune(
            *,
            now: float | None = None,
            force: bool = True,
        ) -> None:
            direct_calls.append((now, force))

        delivery.prune_retained = direct_prune  # type: ignore[method-assign]
        tip_refresh._ports.prune_evicted_jobs(1.25, False)  # type: ignore[attr-defined]
        self.assertEqual(direct_calls, [(1.25, False)])

        saved_original = server.prune_evicted_job_graveyard
        override_calls: list[tuple[float | None, bool]] = []

        def wrapper(
            *,
            now: float | None = None,
            force: bool = True,
        ) -> None:
            override_calls.append((now, force))
            saved_original(now=now, force=force)

        server.prune_evicted_job_graveyard = wrapper  # type: ignore[method-assign]
        tip_refresh._ports.prune_evicted_jobs(2.5, True)  # type: ignore[attr-defined]
        self.assertEqual(override_calls, [(2.5, True)])
        self.assertEqual(direct_calls, [(1.25, False), (2.5, True)])

    def test_post_construction_send_update_override_is_late_and_nonrecursive(
        self,
    ) -> None:
        server = compatibility_coordinator()
        delivery = server._ensure_job_delivery_service()
        state = compatibility_client()
        events: list[tuple[ClientState, str]] = []

        def replacement(client_state: ClientState, candidate: object) -> None:
            events.append((client_state, candidate.job_id))

        server.send_job_update = replacement  # type: ignore[method-assign]
        delivery.send_update(state, job("late-override"), split_send=False)
        self.assertEqual(events, [(state, "late-override")])

    def test_send_update_override_can_call_saved_original_once(self) -> None:
        server = compatibility_coordinator()
        delivery = server._ensure_job_delivery_service()
        state = compatibility_client()
        events: list[str] = []
        original = server.send_job_update
        state.send_batch = lambda _payloads: events.append("batch")  # type: ignore[method-assign]

        def wrapper(client_state: ClientState, candidate: object) -> None:
            events.append("wrapper")
            original(client_state, candidate)

        server.send_job_update = wrapper  # type: ignore[method-assign]
        delivery.send_update(state, job("saved-original"), split_send=False)
        self.assertEqual(events, ["wrapper", "batch"])

    def test_facade_reexports_exact_s2_identities(self) -> None:
        self.assertIs(FacadePrismJobContext, PrismJobContext)
        self.assertIs(FacadeEvictedJobEntry, EvictedJobEntry)
        self.assertIs(FacadePendingInitialJob, PendingInitialJob)

    def test_stamp_freezes_worker_and_all_delivery_generations(self) -> None:
        state = client()
        delivery, _registry, runtime = service(state)
        base = job("shared")
        bundle = SimpleNamespace(
            collection_only=False,
            collection_identity=None,
            base_job=base,
            template={"previousblockhash": TIP_A},
            shares_json=[],
            prior_balances=[],
            found_block={},
            issued_at_ms=1,
            template_fingerprint="fingerprint",
            template_generation=3,
            payout_state_generation=7,
            prospective_prior_balances=None,
            payout_artifact_generation=4,
        )
        state.authorization_generation = 8
        state.difficulty_generation = 9

        stamped = delivery.stamp(state, bundle, clean_jobs=False)

        self.assertEqual(stamped.job.job_id, "prism-1")
        self.assertEqual(stamped.worker, state.worker)
        self.assertEqual(stamped.share_weight, 5)
        self.assertEqual(stamped.authorization_generation, 8)
        self.assertEqual(stamped.difficulty_generation, 9)
        self.assertFalse(stamped.job.clean_jobs)
        self.assertEqual(runtime.counter, 0)

    def test_difficulty_and_notify_are_adjacent_for_both_send_seams(self) -> None:
        state = client()
        delivery, _registry, runtime = service(state)
        delivery.send_update(state, job("one"), split_send=True)
        delivery.send_update(state, job("two"), split_send=False)
        self.assertEqual(
            runtime.events,
            ["difficulty", "notify", "difficulty", "notify"],
        )

    def test_send_update_holds_no_registry_or_retained_lock(self) -> None:
        state = client()
        delivery, registry, _runtime = service(state)
        checks: list[bool] = []

        def check_locks(_state: ClientState, _job: object) -> None:
            acquired_registry = registry.lock.acquire(blocking=False)
            if acquired_registry:
                registry.lock.release()
            acquired_retained = delivery.retained.lock.acquire(blocking=False)
            if acquired_retained:
                delivery.retained.lock.release()
            checks.append(acquired_registry and acquired_retained)

        delivery.runtime.send_job_batch = check_locks  # type: ignore[method-assign]
        delivery.send_update(state, job("one"), split_send=False)
        self.assertEqual(checks, [True])

    def test_every_final_guard_dimension_rejects_stale_delivery(self) -> None:
        mutators = {
            "connection": lambda state: setattr(state, "connection_id", 2),
            "authorization": lambda state: setattr(
                state, "authorization_generation", 1
            ),
            "difficulty": lambda state: setattr(state, "difficulty_generation", 1),
            "worker": lambda state: setattr(state, "worker", worker("miner-b")),
            "subscription": lambda state: setattr(state, "subscribed", False),
            "authorization-state": lambda state: setattr(state, "authorized", False),
            "closing": lambda state: setattr(state, "closing", True),
            "active": lambda state: setattr(state, "active_job", context("other")),
        }
        for name, mutate in mutators.items():
            with self.subTest(name=name):
                state = client()
                delivery, registry, _runtime = service(state)
                ready = context("ready")
                authority = delivery.capture_authority(
                    state, ready, expected_active_job=None
                )
                with registry.lock:
                    delivery.register_locked(
                        state, ready, clean_jobs=True, current_tip=TIP_A
                    )
                mutate(state)
                self.assertFalse(
                    delivery.record_successful_delivery(state, authority, ready, 10.0)
                )

    def test_template_and_payout_identity_are_final_guards(self) -> None:
        state = client()
        delivery, registry, _runtime = service(state)
        ready = context("ready")
        authority = delivery.capture_authority(state, ready, expected_active_job=None)
        with registry.lock:
            delivery.register_locked(state, ready, clean_jobs=True, current_tip=TIP_A)
        changed = replace(ready, payout_state_generation=8)
        state.active_job = changed
        self.assertFalse(
            delivery.record_successful_delivery(state, authority, changed, 10.0)
        )

    def test_disconnect_during_successful_blocking_send_emits_no_proof(self) -> None:
        state = client()
        delivery, registry, _runtime = service(state)
        ready = context("ready")
        authority = delivery.capture_authority(state, ready, expected_active_job=None)
        with registry.lock:
            delivery.register_locked(state, ready, clean_jobs=True, current_tip=TIP_A)
        send_started = threading.Event()
        release_send = threading.Event()

        def blocked_send(_state: ClientState, _job: object) -> None:
            send_started.set()
            release_send.wait(2)

        delivery.runtime.send_job_batch = blocked_send  # type: ignore[method-assign]
        result: list[bool] = []

        def run() -> None:
            delivery.send_update(state, ready.job, split_send=False)
            result.append(
                delivery.record_successful_delivery(state, authority, ready, 10.0)
            )

        thread = threading.Thread(target=run)
        thread.start()
        self.assertTrue(send_started.wait(1))
        with registry.lock:
            registry.begin_retirement_locked(state)
        release_send.set()
        thread.join(2)
        self.assertEqual(result, [False])
        self.assertIsNone(state._progress_delivered_context)

    def test_delivery_proof_is_exactly_once(self) -> None:
        state = client()
        delivery, registry, _runtime = service(state)
        ready = context("ready")
        authority = delivery.capture_authority(state, ready, expected_active_job=None)
        with registry.lock:
            delivery.register_locked(state, ready, clean_jobs=True, current_tip=TIP_A)
        self.assertTrue(
            delivery.record_successful_delivery(state, authority, ready, 10.0)
        )
        self.assertFalse(
            delivery.record_successful_delivery(state, authority, ready, 11.0)
        )

    def test_complete_delivery_commits_proof_before_unlocked_g1_effects(self) -> None:
        state = client()
        registry = SessionRegistry(
            lock=threading.RLock(),
            clients={state},
            connection_generation=state.connection_id,
            rejection_counts={"global": 0, "username": 0},
        )
        events: list[str] = []

        class Progress:
            def record_health_delivery(
                self,
                delivered_client: ClientState,
                delivered_context: PrismJobContext,
                _delivered_monotonic: float,
            ) -> None:
                self.assert_unlocked()
                proof = registry.eligible_snapshot()[delivered_client.connection_id]
                assert proof.delivered is not None
                self_outer.assertIs(proof.delivered.context, delivered_context)
                events.append("record")

            def reconcile_health_eligibility(self) -> None:
                self.assert_unlocked()
                events.append("reconcile")

            def assert_unlocked(self) -> None:
                is_owned = getattr(registry.lock, "_is_owned")
                self_outer.assertFalse(is_owned())

        self_outer = self
        def source_current(*_args: object, **_kwargs: object) -> bool:
            self.assertTrue(getattr(registry.lock, "_is_owned")())
            events.append("source")
            return True

        delivery = JobDeliveryService(
            registry=registry,
            runtime=Runtime(),
            jobs={},
            retained=RetainedJobIndex(),
            progress=Progress(),
            tip_authority=SimpleNamespace(
                published_current_locked=source_current,
            ),  # type: ignore[arg-type]
        )
        ready = context("ready")
        authority = delivery.capture_authority(state, ready, expected_active_job=None)
        with registry.lock:
            delivery.register_locked(state, ready, clean_jobs=True, current_tip=TIP_A)
        original_record = registry.record_delivery_locked

        def record_after_source(*args: object, **kwargs: object) -> bool:
            self.assertEqual(events, ["source"])
            events.append("proof")
            return original_record(*args, **kwargs)  # type: ignore[arg-type]

        registry.record_delivery_locked = record_after_source  # type: ignore[method-assign]
        source = DeliverySourceAuthority(
            kind="published_tip",
            payout_generation=ready.payout_state_generation,
            template_generation=ready.template_generation,
            observation_sequence=1,
            template_fingerprint=ready.template_fingerprint,
            context_parent=TIP_A,
        )

        self.assertTrue(
            delivery.complete_delivery(
                state,
                authority,
                ready,
                10.0,
                source_authorities=(source,),
            )
        )
        self.assertEqual(events, ["source", "proof", "record", "reconcile"])
        self.assertFalse(
            delivery.complete_delivery(state, authority, ready, 11.0)
        )
        self.assertEqual(events, ["source", "proof", "record", "reconcile"])

    def test_stale_source_guard_rejects_proof_and_g1_effects(self) -> None:
        state = client()
        registry = SessionRegistry(
            lock=threading.RLock(),
            clients={state},
            connection_generation=state.connection_id,
            rejection_counts={"global": 0, "username": 0},
        )
        progress = SimpleNamespace(
            record_health_delivery=Mock(),
            reconcile_health_eligibility=Mock(),
        )
        delivery = JobDeliveryService(
            registry=registry,
            runtime=Runtime(),
            jobs={},
            retained=RetainedJobIndex(),
            progress=progress,
            tip_authority=SimpleNamespace(
                published_current_locked=Mock(return_value=False),
            ),  # type: ignore[arg-type]
        )
        ready = context("ready")
        authority = delivery.capture_authority(
            state,
            ready,
            expected_active_job=None,
        )
        with registry.lock:
            delivery.register_locked(
                state,
                ready,
                clean_jobs=True,
                current_tip=TIP_A,
            )

        stale_source = DeliverySourceAuthority(
            kind="published_tip",
            payout_generation=ready.payout_state_generation,
            template_generation=ready.template_generation,
            observation_sequence=2,
            template_fingerprint=ready.template_fingerprint,
            context_parent=TIP_A,
        )

        self.assertFalse(
            delivery.complete_delivery(
                state,
                authority,
                ready,
                10.0,
                source_authorities=(stale_source,),
            )
        )
        proof = registry.eligible_snapshot()[state.connection_id]
        self.assertIsNone(proof.delivered)
        progress.record_health_delivery.assert_not_called()
        progress.reconcile_health_eligibility.assert_not_called()

    def test_artifact_source_accepts_new_same_tip_generation(self) -> None:
        server = compatibility_coordinator()
        delivery = server._ensure_job_delivery_service()
        registry = server._ensure_session_registry()
        tip = server._ensure_tip_refresh_service()
        published = tip_snapshot(TIP_A, generation=3, fingerprint="published")
        same_tip = tip_snapshot(TIP_A, generation=4, fingerprint="repository")
        artifacts = same_tip.template_artifacts
        assert artifacts is not None
        now = time.monotonic()
        tip.seed_state_for_test(latest_detected_tip=None, observation_sequence=1)
        tip.seed_published_for_test(
            first_seen=(TIP_A, now),
            observation_sequence=1,
            observed_monotonic=now,
            template=published,
        )
        ready = replace(
            context("same-tip", template_generation=artifacts.generation),
            template=artifacts.template,
            template_fingerprint=artifacts.fingerprint,
        )
        source = DeliverySourceAuthority(
            kind="artifacts",
            payout_generation=ready.payout_state_generation,
            template_generation=artifacts.generation,
            observation_sequence=0,
            template_fingerprint=artifacts.fingerprint,
            artifacts=artifacts,
        )

        with registry.lock:
            self.assertTrue(
                delivery.source_authority_current_locked(source, ready)
            )

    def test_artifact_source_rejects_arbitrary_older_published_parent(self) -> None:
        server = compatibility_coordinator()
        delivery = server._ensure_job_delivery_service()
        registry = server._ensure_session_registry()
        tip = server._ensure_tip_refresh_service()
        published = tip_snapshot(TIP_A, generation=3, fingerprint="published")
        stale = tip_snapshot(TIP_A, generation=2, fingerprint="stale")
        artifacts = stale.template_artifacts
        assert artifacts is not None
        now = time.monotonic()
        tip.seed_state_for_test(
            latest_detected_tip=(TIP_B, 2),
            observation_sequence=2,
            divergence_started_monotonic=now,
        )
        tip.seed_published_for_test(
            first_seen=(TIP_A, now),
            observation_sequence=1,
            observed_monotonic=now,
            template=published,
        )
        ready = replace(
            context("stale-parent", template_generation=artifacts.generation),
            template=artifacts.template,
            template_fingerprint=artifacts.fingerprint,
        )
        source = DeliverySourceAuthority(
            kind="artifacts",
            payout_generation=ready.payout_state_generation,
            template_generation=artifacts.generation,
            observation_sequence=0,
            template_fingerprint=artifacts.fingerprint,
            artifacts=artifacts,
        )

        with registry.lock:
            self.assertFalse(
                delivery.source_authority_current_locked(source, ready)
            )

    def test_artifact_source_accepts_exact_pinned_published_lease(self) -> None:
        server = compatibility_coordinator()
        delivery = server._ensure_job_delivery_service()
        registry = server._ensure_session_registry()
        tip = server._ensure_tip_refresh_service()
        published = tip_snapshot(TIP_A, generation=3, fingerprint="published")
        artifacts = published.template_artifacts
        assert artifacts is not None
        now = time.monotonic()
        tip.seed_state_for_test(
            latest_detected_tip=(TIP_B, 2),
            observation_sequence=2,
            divergence_started_monotonic=now,
        )
        tip.seed_published_for_test(
            first_seen=(TIP_A, now),
            observation_sequence=1,
            observed_monotonic=(
                now - tip.config.submit_tip_max_age_seconds - 1.0
            ),
            template=published,
        )
        ready = replace(
            context("pinned", template_generation=artifacts.generation),
            template=artifacts.template,
            template_fingerprint=artifacts.fingerprint,
        )
        source = DeliverySourceAuthority(
            kind="artifacts",
            payout_generation=ready.payout_state_generation,
            template_generation=artifacts.generation,
            observation_sequence=0,
            template_fingerprint=artifacts.fingerprint,
            artifacts=artifacts,
        )

        with registry.lock:
            self.assertTrue(
                delivery.source_authority_current_locked(source, ready)
            )

    def test_artifact_source_rejects_expired_pinned_published_lease(self) -> None:
        server = compatibility_coordinator()
        delivery = server._ensure_job_delivery_service()
        registry = server._ensure_session_registry()
        tip = server._ensure_tip_refresh_service()
        published = tip_snapshot(TIP_A, generation=3, fingerprint="published")
        artifacts = published.template_artifacts
        assert artifacts is not None
        now = time.monotonic()
        tip.seed_state_for_test(
            latest_detected_tip=(TIP_B, 2),
            observation_sequence=2,
            divergence_started_monotonic=(
                now - tip.config.failure_exit_seconds - 1.0
            ),
        )
        tip.seed_published_for_test(
            first_seen=(TIP_A, now),
            observation_sequence=1,
            observed_monotonic=(
                now - tip.config.submit_tip_max_age_seconds - 1.0
            ),
            template=published,
        )
        ready = replace(
            context("expired", template_generation=artifacts.generation),
            template=artifacts.template,
            template_fingerprint=artifacts.fingerprint,
        )
        source = DeliverySourceAuthority(
            kind="artifacts",
            payout_generation=ready.payout_state_generation,
            template_generation=artifacts.generation,
            observation_sequence=0,
            template_fingerprint=artifacts.fingerprint,
            artifacts=artifacts,
        )

        with registry.lock:
            self.assertFalse(
                delivery.source_authority_current_locked(source, ready)
            )

    def test_prepared_idle_delivery_carries_exact_artifact_lease(self) -> None:
        tip_hash = "00" * 32
        server = compatibility_coordinator()
        state = compatibility_client()
        prepare_idle_client(server, state, tip=tip_hash)
        bundle = install_idle_job_cache(server, tip=tip_hash)
        delivery = server._ensure_job_delivery_service()
        registry = server._ensure_session_registry()
        tip = server._ensure_tip_refresh_service()
        cache_lock = server._ensure_job_bundle_service()._cache_lock
        artifacts = (
            server._ensure_job_bundle_service()
            .template_repository.current_artifacts()
        )
        assert artifacts is not None
        tip.seed_state_for_test(
            latest_detected_tip=None,
            divergence_started_monotonic=None,
            observation_sequence=0,
        )
        tip.seed_published_for_test(
            first_seen=None,
            parent=None,
            observation_sequence=0,
            observed_monotonic=None,
            template=None,
        )
        prior_active = state.active_job
        prior_window = state.vardiff_window_started_monotonic
        state.pending_share_difficulty = Decimal("4")
        idle_authority = IdleDeliveryAuthority(
            connection_id=state.connection_id,
            worker=state.worker,
            expected_active_job=prior_active,
            expected_window_started=prior_window,
            pending_difficulty=Decimal("4"),
        )
        leases: list[AdmittedIdleBundleSource] = []
        bootstrap_events: list[str] = []
        original_rpc_call = server.rpc.call

        def observe_live_rpc(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getbestblockhash":
                self.assertFalse(bool(getattr(registry.lock, "_is_owned")()))
                acquired = cache_lock.acquire(blocking=False)
                self.assertTrue(acquired)
                if acquired:
                    cache_lock.release()
                bootstrap_events.append("live")
            return original_rpc_call(method, params)

        server.rpc.call = observe_live_rpc  # type: ignore[method-assign]
        original_observe_tip = tip.observe_tip

        def observe_tip_unlocked(*args: object, **kwargs: object) -> bool:
            self.assertFalse(bool(getattr(registry.lock, "_is_owned")()))
            acquired = cache_lock.acquire(blocking=False)
            self.assertTrue(acquired)
            if acquired:
                cache_lock.release()
            bootstrap_events.append("observe")
            return original_observe_tip(*args, **kwargs)  # type: ignore[arg-type]

        tip.observe_tip = observe_tip_unlocked  # type: ignore[method-assign]
        original_admit = server._admit_idle_bundle_source

        def observe_admission(
            admitted_client: ClientState,
            admitted_bundle: object,
            *,
            allow_uncached: bool,
        ) -> AdmittedIdleBundleSource | None:
            self.assertFalse(bool(getattr(registry.lock, "_is_owned")()))
            admitted = original_admit(
                admitted_client,
                admitted_bundle,  # type: ignore[arg-type]
                allow_uncached=allow_uncached,
            )
            if admitted is not None:
                leases.append(admitted)
            return admitted

        server._admit_idle_bundle_source = observe_admission  # type: ignore[method-assign]
        original_parent_current = tip.artifacts_parent_current_locked

        def source_current(
            source_artifacts: CachedTemplateArtifacts,
            *,
            now: float,
        ) -> bool:
            acquired = cache_lock.acquire(blocking=False)
            self.assertTrue(acquired)
            if acquired:
                cache_lock.release()
            return original_parent_current(source_artifacts, now=now)

        tip.artifacts_parent_current_locked = source_current  # type: ignore[method-assign]
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]

        with state.job_update_lock:
            delivered = delivery.maybe_send_job_locked(
                state,
                clean_jobs=True,
                raise_on_build_failure=True,
                prepared_bundle=bundle,
                idle_authority=idle_authority,
                prepared_bundle_allow_uncached=True,
            )

        self.assertTrue(delivered)
        self.assertEqual(len(leases), 1)
        self.assertEqual(bootstrap_events, ["live", "observe"])
        self.assertIs(leases[0].artifacts, artifacts)
        self.assertIs(leases[0].bundle, bundle)
        self.assertEqual(leases[0].cache_identity, bundle.key)
        self.assertIsNotNone(state.active_job)
        self.assertIs(state.active_job.template, artifacts.template)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )

    def test_prepared_idle_delivery_rejects_live_tip_mismatch_without_send(
        self,
    ) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        prepare_idle_client(server, state, tip=TIP_A)
        bundle = install_idle_job_cache(server, tip=TIP_A)
        delivery = server._ensure_job_delivery_service()
        tip = server._ensure_tip_refresh_service()
        tip.seed_state_for_test(
            latest_detected_tip=None,
            divergence_started_monotonic=None,
            observation_sequence=0,
        )
        tip.seed_published_for_test(
            first_seen=None,
            parent=None,
            observation_sequence=0,
            observed_monotonic=None,
            template=None,
        )
        self.assertIsNone(tip.newest_observed_tip())
        prior_active = state.active_job
        prior_window = state.vardiff_window_started_monotonic
        state.pending_share_difficulty = Decimal("4")
        idle_authority = IdleDeliveryAuthority(
            connection_id=state.connection_id,
            worker=state.worker,
            expected_active_job=prior_active,
            expected_window_started=prior_window,
            pending_difficulty=Decimal("4"),
        )
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]

        with state.job_update_lock:
            delivered = delivery.maybe_send_job_locked(
                state,
                clean_jobs=True,
                raise_on_build_failure=True,
                prepared_bundle=bundle,
                idle_authority=idle_authority,
                prepared_bundle_allow_uncached=True,
            )

        self.assertFalse(delivered)
        self.assertEqual(sent, [])
        self.assertEqual(tip.newest_observed_tip(), "00" * 32)
        self.assertIs(state.active_job, prior_active)

    def test_real_r1_source_flip_waits_for_atomic_s1_proof(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        state.worker = worker()
        state.username = state.worker.username
        server.clients = {state}
        delivery = server._ensure_job_delivery_service()
        registry = server._ensure_session_registry()
        tip = server._ensure_tip_refresh_service()
        self.assertIs(tip._state_lock, registry.lock)  # type: ignore[attr-defined]

        published = tip_snapshot()
        now = time.monotonic()
        tip.seed_state_for_test(latest_detected_tip=None, observation_sequence=1)
        tip.seed_published_for_test(
            first_seen=(TIP_A, now),
            observation_sequence=1,
            observed_monotonic=now,
            template=published,
        )
        ready = context("atomic-proof", owner=state.worker, payout_generation=0)
        authority = delivery.capture_authority(
            state,
            ready,
            expected_active_job=None,
        )
        with registry.lock:
            delivery.register_locked(
                state,
                ready,
                clean_jobs=True,
                current_tip=TIP_A,
            )
        source = DeliverySourceAuthority(
            kind="published_tip",
            payout_generation=ready.payout_state_generation,
            template_generation=ready.template_generation,
            observation_sequence=1,
            template_fingerprint=ready.template_fingerprint,
            context_parent=TIP_A,
        )
        assert delivery.progress is not None
        delivery.progress._record_health_delivery = lambda *_args: None  # type: ignore[attr-defined]
        delivery.progress._reconcile_health_eligibility = lambda: None  # type: ignore[attr-defined]

        proof_entered = threading.Event()
        release_proof = threading.Event()
        mutation_done = threading.Event()
        original_record = registry.record_delivery_locked

        def blocking_record(*args: object, **kwargs: object) -> bool:
            proof_entered.set()
            self.assertTrue(release_proof.wait(2))
            return original_record(*args, **kwargs)  # type: ignore[arg-type]

        registry.record_delivery_locked = blocking_record  # type: ignore[method-assign]
        results: list[bool] = []
        delivery_thread = threading.Thread(
            target=lambda: results.append(
                delivery.complete_delivery(
                    state,
                    authority,
                    ready,
                    10.0,
                    source_authorities=(source,),
                )
            )
        )

        replacement = tip_snapshot(TIP_B, generation=4, fingerprint="new")

        def flip_source() -> None:
            tip.seed_published_for_test(
                first_seen=(TIP_B, time.monotonic()),
                observation_sequence=2,
                observed_monotonic=time.monotonic(),
                template=replacement,
            )
            mutation_done.set()

        delivery_thread.start()
        self.assertTrue(proof_entered.wait(1))
        mutation_thread = threading.Thread(target=flip_source)
        mutation_thread.start()
        self.assertFalse(mutation_done.wait(0.05))
        release_proof.set()
        delivery_thread.join(2)
        mutation_thread.join(2)
        self.assertFalse(delivery_thread.is_alive())
        self.assertFalse(mutation_thread.is_alive())
        self.assertEqual(results, [True])
        self.assertTrue(mutation_done.is_set())

    def test_real_p1_mutation_waits_for_delivery_admission_not_registry(self) -> None:
        server = compatibility_coordinator()
        delivery = server._ensure_job_delivery_service()
        registry = server._ensure_session_registry()
        payout = server._ensure_payout_state_service()
        generation = payout.snapshot().generation
        admitted_registry = threading.Event()
        release_delivery = threading.Event()
        mutation_done = threading.Event()

        def hold_delivery() -> None:
            assert delivery.payout is not None
            with delivery.payout.admission(
                lambda: False,
                generation=generation,
                priority=True,
            ) as admitted:
                self.assertTrue(admitted)
                with registry.lock:
                    admitted_registry.set()
                    self.assertTrue(release_delivery.wait(2))

        def mutate() -> None:
            with payout.delivery_gate.mutation():
                mutation_done.set()

        delivery_thread = threading.Thread(target=hold_delivery)
        delivery_thread.start()
        self.assertTrue(admitted_registry.wait(1))
        mutation_thread = threading.Thread(target=mutate)
        mutation_thread.start()
        self.assertFalse(mutation_done.wait(0.05))
        release_delivery.set()
        delivery_thread.join(2)
        mutation_thread.join(2)
        self.assertFalse(delivery_thread.is_alive())
        self.assertFalse(mutation_thread.is_alive())
        self.assertTrue(mutation_done.is_set())

    def test_empty_registry_never_admits_or_records_nonmember_delivery(self) -> None:
        state = client()
        registry = SessionRegistry(
            lock=threading.RLock(),
            clients=set(),
            connection_generation=state.connection_id,
            rejection_counts={"global": 0, "username": 0},
        )
        runtime = Runtime()
        delivery = JobDeliveryService(
            registry=registry,
            runtime=runtime,
            jobs={},
            retained=RetainedJobIndex(),
        )
        ready = context("ready")
        authority = delivery.capture_authority(
            state, ready, expected_active_job=None
        )
        self.assertFalse(
            delivery.authority_current_locked(
                state, authority, expected_active_job=None
            )
        )
        state.active_job = ready
        self.assertFalse(
            delivery.record_successful_delivery(state, authority, ready, 10.0)
        )
        self.assertIsNone(state._progress_delivered_context)

    def test_collection_context_becomes_refresh_needed_after_ready_latch(self) -> None:
        state = client()
        delivery, _registry, runtime = service(state)
        state.active_job = context("collection", collection_only=True)
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash=TIP_A,
            previousblockhash=TIP_A,
            template_fingerprint="fingerprint",
            template_generation=3,
        )
        self.assertFalse(delivery.client_needs_refresh(state, snapshot))
        runtime.ready = True
        self.assertTrue(delivery.client_needs_refresh(state, snapshot))

    def test_clean_registration_retains_frozen_original_context(self) -> None:
        state = client()
        delivery, registry, _runtime = service(state)
        original_owner = state.worker
        old = context("old", owner=original_owner)
        new = context("new", owner=original_owner)
        with registry.lock:
            delivery.register_locked(state, old, clean_jobs=False, current_tip=TIP_A)
            delivery.register_locked(state, new, clean_jobs=True, current_tip=TIP_A)
        state.worker = worker("miner-b")
        state.share_difficulty = Decimal("32")
        retained = delivery.retained.lookup(
            state,
            "old",
            current_tip=TIP_A,
            current_tip_first_delivery=None,
            cached_parent=None,
            now=1.0,
        )
        self.assertIsNotNone(retained)
        assert retained is not None
        self.assertEqual(retained.context.worker, original_owner)
        self.assertEqual(retained.context.job.share_difficulty, Decimal("1"))

    def test_same_tip_capacity_is_per_connection(self) -> None:
        index = RetainedJobIndex(same_tip_per_connection=1)
        first = client(1)
        second = client(2)
        index.retain(first, "a1", context("a1"), current_tip=TIP_A, now=1)
        index.retain(second, "b1", context("b1"), current_tip=TIP_A, now=1)
        index.retain(first, "a2", context("a2"), current_tip=TIP_A, now=2)
        self.assertNotIn("a1", index.graveyard)
        self.assertIn("a2", index.graveyard)
        self.assertIn("b1", index.graveyard)

    def test_stale_grace_begins_at_first_replacement_delivery(self) -> None:
        state = client()
        index = RetainedJobIndex(stale_grace_seconds=3)
        index.retain(state, "old", context("old"), current_tip=TIP_A, now=1)
        index.prune(
            current_tip=TIP_B,
            current_tip_first_delivery=2,
            cached_parent=TIP_A,
            now=20,
        )
        self.assertIn("old", index.graveyard)
        state.tip_work_delivered = (TIP_B, 20)
        index.prune(
            current_tip=TIP_B,
            current_tip_first_delivery=2,
            cached_parent=TIP_A,
            now=22.9,
        )
        self.assertIn("old", index.graveyard)
        index.prune(
            current_tip=TIP_B,
            current_tip_first_delivery=2,
            cached_parent=TIP_A,
            now=23.1,
        )
        self.assertNotIn("old", index.graveyard)

    def test_prior_tip_does_not_consume_same_tip_capacity(self) -> None:
        state = client()
        index = RetainedJobIndex(same_tip_per_connection=1)
        index.retain(state, "prior", context("prior", parent=TIP_A), current_tip=TIP_B)
        index.retain(state, "same-1", context("same-1", parent=TIP_B), current_tip=TIP_B)
        index.retain(state, "same-2", context("same-2", parent=TIP_B), current_tip=TIP_B)
        self.assertIn("prior", index.graveyard)
        self.assertNotIn("same-1", index.graveyard)
        self.assertIn("same-2", index.graveyard)

    def test_disconnect_retires_active_and_retained_indexes(self) -> None:
        state = client()
        delivery, registry, _runtime = service(state)
        old = context("old")
        new = context("new")
        with registry.lock:
            delivery.register_locked(state, old, clean_jobs=False, current_tip=TIP_A)
            delivery.register_locked(state, new, clean_jobs=True, current_tip=TIP_A)
            delivery.retire_client_locked(state)
        self.assertEqual(delivery.jobs, {})
        self.assertEqual(delivery.retained.graveyard, {})
        self.assertEqual(state.active_job_ids, set())

    def test_map_adoption_rebuilds_indexes_and_converts_legacy_entries(self) -> None:
        state = client()
        frozen = context("legacy")
        replacement = {"legacy": (frozen, state.connection_id, 1.0)}
        index = RetainedJobIndex()
        index.adopt(
            graveyard=replacement,  # type: ignore[arg-type]
            by_connection={},
            same_tip_by_connection={},
            same_tip_job_ids=OrderedDict(),
            current_tip=TIP_A,
        )
        self.assertIsInstance(index.graveyard["legacy"], EvictedJobEntry)
        self.assertIn("legacy", index.by_connection[state.connection_id])

    def test_graveyard_only_replacement_rebuilds_disconnect_index(self) -> None:
        state = client()
        index = RetainedJobIndex()
        index.retain(state, "old", context("old"), current_tip=TIP_A)
        replacement = OrderedDict(
            [("new", EvictedJobEntry(context("new"), state.connection_id, 1, TIP_A))]
        )
        index.adopt(
            graveyard=replacement,
            by_connection=index.by_connection,
            same_tip_by_connection=index.same_tip_by_connection,
            same_tip_job_ids=index.same_tip_job_ids,
            current_tip=TIP_A,
        )
        self.assertEqual(tuple(index.by_connection[state.connection_id]), ("new",))
        self.assertEqual(index.retire_connection(state.connection_id), ("new",))
        self.assertEqual(index.graveyard, {})

    def test_difficulty_transition_clears_only_matching_pending_value(self) -> None:
        state = client()
        config = vardiff.VardiffConfig(
            enabled=True,
            target_share_interval_seconds=Decimal("15"),
            min_difficulty=Decimal("1"),
            max_difficulty=Decimal("1024"),
            retarget_interval_seconds=Decimal("90"),
            max_step_factor=Decimal("4"),
            startup_difficulty=Decimal("1"),
            max_step_down_factor=Decimal("4"),
            ewma_alpha=Decimal("0.4"),
            retarget_tolerance=Decimal("0.25"),
        )
        state.pending_share_difficulty = Decimal("2")
        JobDeliveryService.apply_job_difficulty(
            state,
            replace(job("job"), share_difficulty=Decimal("2")),
            config=config,
        )
        self.assertIsNone(state.pending_share_difficulty)

    def test_initial_request_coalesces_one_identity(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        state.worker = worker()
        state.username = state.worker.username
        server.clients = {state}
        submitted: list[PendingInitialJob] = []

        def submit(request: PendingInitialJob) -> bool:
            submitted.append(request)
            return True

        server._submit_initial_job_request = submit  # type: ignore[method-assign]
        self.assertTrue(server.request_initial_job_delivery(state))
        first = server.pending_initial_jobs[state]
        self.assertEqual(submitted, [first])
        self.assertTrue(server.request_initial_job_delivery(state))
        self.assertIs(server.pending_initial_jobs[state], first)
        self.assertEqual(submitted, [first])
        self.assertEqual(server.initial_job_coalesced_count, 1)

    def test_initial_reauthorization_replaces_cancelled_predecessor(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        state.worker = worker()
        state.username = state.worker.username
        server.clients = {state}
        server._submit_initial_job_request = lambda _request: True  # type: ignore[method-assign]
        self.assertTrue(server.request_initial_job_delivery(state))
        first = server.pending_initial_jobs[state]
        state.authorization_generation += 1
        self.assertTrue(server.request_initial_job_delivery(state))
        replacement = server.pending_initial_jobs[state]
        self.assertIsNot(replacement, first)
        self.assertTrue(first.cancelled.is_set())
        self.assertEqual(server.initial_job_superseded_count, 1)

    def test_initial_timeout_marks_closing_before_disconnect(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        state.worker = worker()
        state.username = state.worker.username
        server.clients = {state}
        server._submit_initial_job_request = lambda _request: True  # type: ignore[method-assign]
        server.stratum_initial_job_timeout_seconds = 1
        self.assertTrue(server.request_initial_job_delivery(state))
        request = server.pending_initial_jobs[state]
        observed: list[bool] = []
        server.disconnect_client = lambda candidate: observed.append(candidate.closing)  # type: ignore[method-assign]
        assert request.deadline_monotonic is not None
        self.assertEqual(
            server.sweep_initial_job_timeouts(now=request.deadline_monotonic),
            1,
        )
        self.assertEqual(observed, [True])

    def test_initial_future_cancellation_callbacks_run_outside_registry(self) -> None:
        for operation in ("cancel", "expire", "shutdown"):
            with self.subTest(operation=operation):
                state = client()
                delivery, registry, _runtime = service(state)
                future: Future[bool] = Future()
                request = PendingInitialJob(
                    client=state,
                    connection_id=state.connection_id,
                    authorization_generation=state.authorization_generation,
                    difficulty_generation=state.difficulty_generation,
                    worker=state.worker,
                    requested_monotonic=0.0,
                    deadline_monotonic=0.0,
                    future=future,
                )
                delivery.initial_state.pending[state] = request
                lock_states: list[bool] = []
                future.add_done_callback(
                    lambda _future: lock_states.append(
                        bool(getattr(registry.lock, "_is_owned")())
                    )
                )

                if operation == "cancel":
                    delivery.cancel_initial_job(state, count=True)
                elif operation == "expire":
                    delivery.initial_runtime = SimpleNamespace(
                        disconnect=lambda _client: None,
                    )  # type: ignore[assignment]
                    delivery.sweep_initial_job_timeouts(now=1.0)
                else:
                    delivery.shutdown_initial_jobs()

                self.assertTrue(future.cancelled())
                self.assertEqual(lock_states, [False])

    def test_coordinator_adopts_replaced_jobs_mapping(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        seeded: dict[str, PrismJobContext] = {"seeded": context("seeded")}
        server.jobs = seeded
        first_service = server._ensure_job_delivery_service()
        self.assertIs(first_service.jobs, seeded)
        replacement: dict[str, PrismJobContext] = {
            "replacement": context("replacement")
        }
        server.jobs = replacement
        self.assertIs(first_service.jobs, replacement)
        self.assertIs(server.jobs, replacement)

    def test_coordinator_adopts_replaced_pending_initial_mapping(self) -> None:
        server = compatibility_coordinator()
        server._ensure_initial_job_state()
        replacement: dict[ClientState, PendingInitialJob] = {}
        server.pending_initial_jobs = replacement
        server._ensure_initial_job_state()
        self.assertIs(server._initial_job_tracker.pending, replacement)

    def test_coordinator_compatibility_aliases_adopt_s2_owned_state(self) -> None:
        server = compatibility_coordinator()
        server.stratum_max_pending_initial_jobs = 3
        server.initial_job_sent_count = 4
        server.job_counter = 7
        delivery = server._ensure_job_delivery_service()
        self.assertIsInstance(delivery.initial_state, InitialJobState)
        self.assertEqual(delivery.initial_state.config.max_pending, 3)
        self.assertEqual(delivery.initial_state.sent_count, 4)
        self.assertEqual(delivery.next_job_id(), "prism-8")
        self.assertEqual(server.job_counter, 8)

        server.stratum_max_pending_initial_jobs = 5
        server.initial_job_sent_count = 6
        self.assertEqual(delivery.initial_state.config.max_pending, 5)
        self.assertEqual(delivery.initial_state.sent_count, 6)

        retained = OrderedDict()
        by_connection: dict[int, OrderedDict[str, None]] = {}
        server.evicted_job_graveyard = retained
        server.evicted_jobs_by_connection = by_connection
        self.assertIs(delivery.retained.graveyard, retained)
        self.assertIs(delivery.retained.by_connection, by_connection)

    def test_direct_build_membership_loss_never_registers_or_sends(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        state.worker = worker()
        state.username = state.worker.username
        events: list[str] = []

        def remove_membership(
            _client: ClientState,
            *,
            clean_jobs: bool,
        ) -> PrismJobContext:
            registry = server._ensure_session_registry()
            with registry.lock:
                registry._discard_client_locked(state)
            return context(
                "lost-membership",
                owner=state.worker,
                payout_generation=0,
            )

        server.build_job_for_client = remove_membership  # type: ignore[method-assign]
        server.send_difficulty = lambda *_args: events.append("difficulty")  # type: ignore[method-assign]
        server.send_job = lambda *_args: events.append("notify")  # type: ignore[method-assign]
        server.apply_job_difficulty = lambda *_args: None  # type: ignore[method-assign]

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(events, [])
        self.assertIsNone(state.active_job)
        self.assertEqual(state.active_job_ids, set())
        self.assertNotIn("lost-membership", server.jobs)
        self.assertIsNone(state._progress_delivered_context)
        self.assertNotIn(
            state.connection_id,
            server._ensure_session_registry().eligible_snapshot(),
        )

    def test_coordinator_send_monkeypatches_remain_dynamic_and_adjacent(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        events: list[str] = []
        server.send_difficulty = lambda *_args: events.append("difficulty")  # type: ignore[method-assign]
        server.send_job = lambda *_args: events.append("notify")  # type: ignore[method-assign]
        server.send_job_update(state, job("patched"))
        self.assertEqual(events, ["difficulty", "notify"])

    def test_coordinator_stamp_dependencies_remain_dynamic_after_construction(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        state.worker = worker()
        server._ensure_job_delivery_service()
        server.desired_client_share_difficulty = lambda _client: Decimal("4")  # type: ignore[method-assign]
        server.client_minimum_advertised_difficulty = lambda _client: Decimal("0")  # type: ignore[method-assign]
        server.share_weight_for_worker = lambda _worker: 17  # type: ignore[method-assign]
        bundle = SimpleNamespace(
            collection_only=False,
            collection_identity=None,
            base_job=replace(job("shared"), qbit_target=1),
            template={"previousblockhash": TIP_A},
            shares_json=[],
            prior_balances=[],
            found_block={},
            issued_at_ms=1,
            template_fingerprint="fingerprint",
            template_generation=3,
            payout_state_generation=7,
            prospective_prior_balances=None,
            payout_artifact_generation=4,
        )
        stamped = server.stamp_job_for_client(state, bundle, clean_jobs=True)
        self.assertEqual(stamped.share_weight, 17)
        self.assertEqual(stamped.job.share_difficulty, Decimal("4"))

    def test_collection_identity_override_remains_dynamic_after_construction(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        state.worker = worker()
        delivery = server._ensure_job_delivery_service()
        replacement_identity = ("replacement", "5220" + "44" * 32)
        server._collection_bundle_identity = lambda _worker: replacement_identity  # type: ignore[method-assign]
        server.share_weight_for_worker = lambda _worker: 1  # type: ignore[method-assign]
        server.desired_client_share_difficulty = lambda _client: Decimal("1")  # type: ignore[method-assign]
        server.client_minimum_advertised_difficulty = lambda _client: Decimal("0")  # type: ignore[method-assign]
        bundle = SimpleNamespace(
            collection_only=True,
            collection_identity=replacement_identity,
            base_job=replace(job("shared"), qbit_target=1),
            template={"previousblockhash": TIP_A},
            shares_json=[],
            prior_balances=[],
            found_block={},
            issued_at_ms=1,
            template_fingerprint="fingerprint",
            template_generation=3,
            payout_state_generation=0,
            prospective_prior_balances=None,
            payout_artifact_generation=4,
        )

        stamped = delivery.stamp(state, bundle, clean_jobs=True)

        self.assertTrue(stamped.collection_only)
        self.assertEqual(stamped.worker, state.worker)

    def test_coordinator_g1_adapter_never_recommits_s1_delivery_proof(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        state.worker = worker()
        state.username = state.worker.username
        server.clients = {state}
        events: list[str] = []
        delivery = server._ensure_job_delivery_service()
        assert delivery.progress is not None
        delivery.progress._record_health_delivery = (  # type: ignore[attr-defined]
            lambda *_args: events.append("health")
        )
        delivery.progress._reconcile_health_eligibility = (  # type: ignore[attr-defined]
            lambda: events.append("reconcile")
        )
        registry = server._ensure_session_registry()
        ready = context("ready", owner=state.worker, payout_generation=0)
        authority = delivery.capture_authority(state, ready, expected_active_job=None)
        with registry.lock:
            delivery.register_locked(state, ready, clean_jobs=True, current_tip=TIP_A)
        record_calls = 0
        original_record = registry.record_delivery_locked

        def counted_record(*args: object, **kwargs: object) -> bool:
            nonlocal record_calls
            record_calls += 1
            return original_record(*args, **kwargs)  # type: ignore[arg-type]

        registry.record_delivery_locked = counted_record  # type: ignore[method-assign]

        self.assertTrue(delivery.complete_delivery(state, authority, ready, 10.0))
        self.assertFalse(delivery.complete_delivery(state, authority, ready, 11.0))
        self.assertEqual(record_calls, 1)
        self.assertEqual(events, ["health", "reconcile"])
        self.assertIs(state._progress_delivered_context, ready)

    def test_coordinator_delivery_facades_execute_service_owned_state_machines(self) -> None:
        server = compatibility_coordinator()
        state = compatibility_client()
        service = server._ensure_job_delivery_service()
        schedule = Mock(return_value=True)
        maybe_send = Mock(return_value=True)
        prepared = Mock(return_value=SimpleNamespace(status="sent"))
        advertise = Mock(return_value=True)
        service.schedule_initial_job = schedule  # type: ignore[method-assign]
        service.maybe_send_job = maybe_send  # type: ignore[method-assign]
        service.send_prepared_job = prepared  # type: ignore[method-assign]
        service.advertise_client_difficulty = advertise  # type: ignore[method-assign]

        self.assertTrue(server.schedule_initial_job(state))
        self.assertTrue(server.maybe_send_job(state, clean_jobs=True))
        sent = server.send_prepared_job(
            state,
            SimpleNamespace(),  # type: ignore[arg-type]
            SimpleNamespace(),  # type: ignore[arg-type]
            SimpleNamespace(),  # type: ignore[arg-type]
            state.connection_id,
            None,
        )
        self.assertEqual(sent.status, "sent")
        self.assertTrue(server.advertise_client_difficulty(state, Decimal("4")))
        schedule.assert_called_once_with(state)
        maybe_send.assert_called_once_with(
            state,
            clean_jobs=True,
            raise_on_reorg_failure=False,
            raise_on_build_failure=False,
            tip_refresh_snapshot=None,
            tip_refresh_observation_sequence=None,
        )
        prepared.assert_called_once()
        advertise.assert_called_once_with(state, Decimal("4"))


if __name__ == "__main__":
    unittest.main()
