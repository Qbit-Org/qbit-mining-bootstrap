#!/usr/bin/env python3
"""Focused PRISM coordinator vardiff tests."""
# ruff: noqa: F403, F405

from __future__ import annotations

import unittest
from lab.prism.vardiff_service import VardiffService
from tests.prism_vardiff_test_support import *


class PrismCoordinatorVardiffTests(unittest.TestCase):
    def test_vardiff_owner_initialization_is_single_flight(self) -> None:
        server = coordinator()
        server.__dict__.pop("_vardiff_service", None)
        server.vardiff_idle_queue_depth = 7
        real_service_type = VardiffService
        constructor_entered = threading.Event()
        release_constructor = threading.Event()
        calls = 0
        calls_lock = threading.Lock()

        def construct(runtime: object) -> VardiffService:
            nonlocal calls
            with calls_lock:
                calls += 1
            constructor_entered.set()
            self.assertTrue(release_constructor.wait(1.0))
            return real_service_type(runtime)  # type: ignore[arg-type]

        services: list[VardiffService] = []
        descriptor_write_finished = threading.Event()

        def write_compatibility_field() -> None:
            server.idle_retarget_count = 9
            descriptor_write_finished.set()

        with patch(
            "lab.prism.prism_coordinator.VardiffService",
            side_effect=construct,
        ):
            threads = [
                threading.Thread(
                    target=lambda: services.append(
                        server._ensure_vardiff_service()
                    )
                )
                for _ in range(2)
            ]
            threads[0].start()
            self.assertTrue(constructor_entered.wait(1.0))
            threads[1].start()
            descriptor_writer = threading.Thread(target=write_compatibility_field)
            descriptor_writer.start()
            time.sleep(0.05)
            with calls_lock:
                self.assertEqual(calls, 1)
            self.assertFalse(descriptor_write_finished.is_set())
            release_constructor.set()
            for thread in threads:
                thread.join(1.0)
                self.assertFalse(thread.is_alive())
            descriptor_writer.join(1.0)
            self.assertFalse(descriptor_writer.is_alive())

        self.assertEqual(len(services), 2)
        self.assertIs(services[0], services[1])
        self.assertEqual(services[0].vardiff_idle_queue_depth, 7)
        self.assertEqual(services[0].idle_retarget_count, 9)
        self.assertNotIn("idle_retarget_count", server.__dict__)

    def test_load_prism_vardiff_config_defaults_to_small_miner_vardiff(self) -> None:
        names = [name for name in os.environ if name.startswith("PRISM_STRATUM_VARDIFF")]
        with patch.dict(os.environ, {}, clear=False):
            for name in names:
                os.environ.pop(name, None)
            config = load_prism_vardiff_config(Decimal("0.000000001"))

        self.assertTrue(config.enabled)
        self.assertEqual(config.target_share_interval_seconds, Decimal("15"))
        self.assertEqual(config.min_difficulty, Decimal("1E-9"))
        self.assertEqual(config.startup_difficulty, Decimal("1E-9"))
        self.assertEqual(config.max_step_factor, Decimal("4"))
        self.assertEqual(config.max_step_down_factor, Decimal("4"))
    def test_vardiff_retarget_sends_new_difficulty_and_clean_job(self) -> None:
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("1")
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        sent: dict[str, object] = {"jobs": 0}

        def fake_send_job(client: object, clean_jobs: bool) -> bool:
            sent.update({"jobs": sent["jobs"] + 1, "clean": clean_jobs})
            return True

        server.maybe_send_job = fake_send_job  # type: ignore[method-assign]

        server.note_vardiff_submitted_share(state)
        server.note_vardiff_accepted_share(state, FakeJob(Decimal("1")))  # type: ignore[arg-type]

        # Difficulty is now advertised by maybe_send_job alongside the job (gated on
        # a successful build), so the retarget commits the pending difficulty and
        # requests a single clean job.
        self.assertEqual(state.pending_share_difficulty, Decimal("4"))
        self.assertEqual(sent["jobs"], 1)
        self.assertTrue(sent["clean"])
    def test_vardiff_retarget_build_failure_keeps_consistent_difficulty_and_job(self) -> None:
        # If the job build is skipped during a retarget, the client must stay on its
        # existing job at its existing difficulty -- never advertise a new difficulty
        # for a job it never received. Otherwise its easier shares miss the old
        # target, nothing is accepted, and (since retargets only fire on accepted
        # shares) it cannot self-heal without reconnecting.
        server = coordinator()
        server.jobs = {"old-job": SimpleNamespace(job=SimpleNamespace(job_id="old-job"))}
        server.extranonce2_size = 8
        state = client()
        state.username = "miner-a"
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        state.share_difficulty = Decimal("1")
        state.active_job_ids = {"old-job"}
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        advertised: list[object] = []
        state.send = lambda payload: advertised.append(payload)  # type: ignore[method-assign]

        def failing_build(client: ClientState, *, clean_jobs: bool) -> object:
            raise ValueError("transient getblocktemplate failure")

        server.build_job_for_client = failing_build  # type: ignore[method-assign]

        server.note_vardiff_submitted_share(state)
        server.note_vardiff_accepted_share(state, FakeJob(Decimal("1")))  # type: ignore[arg-type]

        self.assertEqual(server._ensure_job_bundle_service().metrics_snapshot()["failure_count"], 1)
        self.assertIsNone(state.pending_share_difficulty)  # rolled back, not left at the new value
        self.assertEqual(state.share_difficulty, Decimal("1"))  # unchanged
        self.assertEqual(state.active_job_ids, {"old-job"})  # old job retained, still submittable
        self.assertEqual(set(server.jobs), {"old-job"})
        self.assertEqual(advertised, [])  # no set_difficulty / notify advertised for the skipped build
    def test_idle_vardiff_success_sends_paired_job_and_resets_window(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        sent: list[dict[str, object]] = []
        delivered = threading.Event()
        window_started = state.vardiff_window_started_monotonic

        def record(payload: dict[str, object]) -> None:
            sent.append(payload)
            if payload.get("method") == "mining.notify":
                delivered.set()

        state.send = record  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertTrue(delivered.wait(timeout=1))
        server.shutdown_vardiff_idle_executor()

        self.assertEqual(server.idle_retarget_count, 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(sent[0]["params"], [4.0])
        self.assertTrue(sent[1]["params"][8])
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertGreater(state.vardiff_window_started_monotonic, window_started)
        self.assertEqual(state.vardiff_window_accepted, 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(state.vardiff_window_work, Decimal("0"))
    def test_idle_vardiff_shutdown_after_delivery_keeps_committed_window(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        sent: list[dict[str, object]] = []
        delivered = threading.Event()
        window_started = state.vardiff_window_started_monotonic

        def stop_after_delivery(payload: dict[str, object]) -> None:
            sent.append(payload)
            if payload.get("method") == "mining.notify":
                server.stop_event.set()
                delivered.set()

        state.send = stop_after_delivery  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertTrue(delivered.wait(timeout=1))
        server.shutdown_vardiff_idle_executor()

        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(server.idle_retarget_count, 1)
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertGreater(state.vardiff_window_started_monotonic, window_started)
        self.assertEqual(state.vardiff_window_accepted, 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(state.vardiff_window_work, Decimal("0"))
    def test_idle_vardiff_sweep_skips_submitted_reject_storm_window(self) -> None:
        server = coordinator()
        state = client()
        state.worker = worker_identity()
        state.active_job = prism_context("job-1", "00" * 32, worker=state.worker)
        state.share_difficulty = Decimal("16")
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        state.vardiff_window_submitted = 3
        server.clients = {state}

        def fail_send_job(client: ClientState, *, clean_jobs: bool) -> bool:
            raise AssertionError("reject-storm windows must not idle-retarget")

        server.maybe_send_job = fail_send_job  # type: ignore[method-assign]

        retargeted = server.vardiff_idle_sweep_once()

        self.assertEqual(retargeted, 0)
        self.assertEqual(state.pending_share_difficulty, None)
        self.assertEqual(state.vardiff_window_submitted, 3)
    def test_idle_vardiff_share_after_snapshot_is_not_stepped_down(self) -> None:
        server = coordinator()
        blockers = [client(), client()]
        for connection_id, blocker in enumerate(blockers, start=1):
            prepare_idle_client(server, blocker, connection_id=connection_id)
        install_idle_job_cache(server)
        workers_started = threading.Barrier(3)
        release_workers = threading.Event()

        def block_delivery(payload: dict[str, object]) -> None:
            if payload.get("method") == "mining.set_difficulty":
                workers_started.wait(timeout=1)
                release_workers.wait(timeout=1)

        for blocker in blockers:
            blocker.send = block_delivery  # type: ignore[method-assign]

        target = client()
        target.connection_id = 3
        sent: list[dict[str, object]] = []
        target.send = sent.append  # type: ignore[method-assign]
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 2)
            workers_started.wait(timeout=1)
            prepare_idle_client(server, target, connection_id=3)
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)

            # The task is queued from an idle snapshot. A later submit changes
            # that exact window before any worker can commit the step-down.
            server.note_vardiff_submitted_share(target)
        finally:
            release_workers.set()
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(sent, [])
        self.assertIsNone(target.pending_share_difficulty)
        self.assertEqual(target.share_difficulty, Decimal("16"))
        self.assertEqual(target.vardiff_window_submitted, 1)
        self.assertGreaterEqual(server.vardiff_idle_skip_counts["not_idle"], 1)
    def test_idle_vardiff_failure_restores_pending_and_idle_window(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        window_state = (
            state.vardiff_window_started_monotonic,
            state.vardiff_window_accepted,
            state.vardiff_window_submitted,
            state.vardiff_window_work,
        )
        disconnected: list[ClientState] = []
        failure_finished = threading.Event()

        def failing_send(_payload: dict[str, object]) -> None:
            self.assertEqual(state.pending_share_difficulty, Decimal("4"))
            raise OSError("socket send failed")

        def fake_disconnect(client: ClientState) -> None:
            disconnected.append(client)
            server.clients.discard(client)
            failure_finished.set()

        state.send = failing_send  # type: ignore[method-assign]
        server.disconnect_client = fake_disconnect  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertTrue(failure_finished.wait(timeout=1))
        server.shutdown_vardiff_idle_executor()

        self.assertEqual(disconnected, [state])
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("16"))
        self.assertEqual(
            (
                state.vardiff_window_started_monotonic,
                state.vardiff_window_accepted,
                state.vardiff_window_submitted,
                state.vardiff_window_work,
            ),
            window_state,
        )
        self.assertEqual(server.vardiff_idle_task_failures, 1)
    def test_idle_vardiff_stamp_failure_restores_speculative_state(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        window_state = (
            state.vardiff_window_started_monotonic,
            state.vardiff_window_accepted,
            state.vardiff_window_submitted,
            state.vardiff_window_work,
        )

        def fail_stamp(*_args: object, **_kwargs: object) -> None:
            self.assertEqual(state.pending_share_difficulty, Decimal("4"))
            raise RuntimeError("cached job stamping failed")

        state.send = lambda payload: self.fail(  # type: ignore[method-assign]
            f"unexpected delivery: {payload}"
        )
        server.stamp_job_for_client = fail_stamp  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        server.shutdown_vardiff_idle_executor()

        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("16"))
        self.assertEqual(
            (
                state.vardiff_window_started_monotonic,
                state.vardiff_window_accepted,
                state.vardiff_window_submitted,
                state.vardiff_window_work,
            ),
            window_state,
        )
        self.assertEqual(server._ensure_job_bundle_service().metrics_snapshot()["failure_count"], 1)
        self.assertEqual(server.vardiff_idle_task_failures, 1)
    def test_idle_cached_bundle_requires_live_reorg_trust(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        trust_checked = threading.Event()
        sent: list[dict[str, object]] = []
        window_started = state.vardiff_window_started_monotonic

        def reject_untrusted_tip() -> bool:
            trust_checked.set()
            return False

        state.send = sent.append  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            reject_untrusted_tip
        )

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        server.shutdown_vardiff_idle_executor()

        self.assertTrue(trust_checked.is_set())
        self.assertEqual(sent, [])
        self.assertEqual(server.idle_retarget_count, 0)
        self.assertEqual(state.share_difficulty, Decimal("16"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.vardiff_window_started_monotonic, window_started)
        self.assertGreaterEqual(server.vardiff_idle_skip_counts["superseded"], 1)
    def test_idle_retarget_defers_detected_payout_source_during_tip_divergence(
        self,
    ) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        state = client()
        prepare_idle_client(server, state, tip=old_tip)
        install_idle_job_cache(server, tip=old_tip)
        repository = server._ensure_job_bundle_service().template_repository
        published_artifacts = repository.current_artifacts()
        assert published_artifacts is not None
        detected_template = gbt_template(new_tip, height=11)
        detected_artifacts = CachedTemplateArtifacts(
            template=detected_template,
            fingerprint=qbit_template_fingerprint(detected_template),
            previousblockhash=new_tip,
            transaction_hexes=(),
            witness_merkle_leaves_hex=(),
            network_difficulty=1,
            fetched_monotonic=time.monotonic(),
            generation=2,
        )
        now = time.monotonic()
        server.current_tip_first_seen = (old_tip, now)
        server.current_tip_observed_monotonic = now
        server.latest_detected_tip = (new_tip, 2)
        server.tip_refresh_divergence_started_monotonic = now
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=published_artifacts.fingerprint,
            template_generation=published_artifacts.generation,
            template_artifacts=published_artifacts,
        )
        repository.replace_for_test(detected_artifacts)
        with server._payout_state_service._lock:
            server._payout_state_service._published = dataclass_replace(
                server._payout_state_service._published,
                source_tip_hash=new_tip,
            )
        window_state = (
            state.vardiff_window_started_monotonic,
            state.vardiff_window_accepted,
            state.vardiff_window_submitted,
            state.vardiff_window_work,
        )
        sent: list[dict[str, object]] = []

        server._build_idle_job_bundle = lambda _request: self.fail(  # type: ignore[method-assign]
            "idle divergence entered the shared build scheduler"
        )
        state.send = sent.append  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 0)
        server.shutdown_vardiff_idle_executor()

        self.assertEqual(sent, [])
        self.assertEqual(state.share_difficulty, Decimal("16"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(
            (
                state.vardiff_window_started_monotonic,
                state.vardiff_window_accepted,
                state.vardiff_window_submitted,
                state.vardiff_window_work,
            ),
            window_state,
        )
        self.assertEqual(server.vardiff_idle_skip_counts["superseded"], 1)
    def test_replacement_tip_build_survives_repeated_idle_sweeps(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        state = client()
        prepare_idle_client(server, state, tip=old_tip)
        install_idle_job_cache(server, tip=old_tip)
        repository = server._ensure_job_bundle_service().template_repository
        published_artifacts = repository.current_artifacts()
        assert published_artifacts is not None
        detected_template = gbt_template(new_tip, height=11)
        detected_artifacts = CachedTemplateArtifacts(
            template=detected_template,
            fingerprint=qbit_template_fingerprint(detected_template),
            previousblockhash=new_tip,
            transaction_hexes=(),
            witness_merkle_leaves_hex=(),
            network_difficulty=1,
            fetched_monotonic=time.monotonic(),
            generation=2,
        )
        now = time.monotonic()
        server.current_tip_first_seen = (old_tip, now)
        server.current_tip_observed_monotonic = now
        server.latest_detected_tip = (new_tip, 2)
        server.tip_refresh_divergence_started_monotonic = now
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=published_artifacts.fingerprint,
            template_generation=published_artifacts.generation,
            template_artifacts=published_artifacts,
        )
        repository.replace_for_test(detected_artifacts)
        server._ensure_job_cache_state()
        replacement_cancellation = _JobBuildCancellation(timeout_seconds=60.0)
        replacement_request = SimpleNamespace(
            artifacts=detected_artifacts,
            cancellation=replacement_cancellation,
        )
        replacement_flight = SimpleNamespace(request=replacement_request)
        with server._ensure_job_bundle_service()._scheduler_lock:
            server._ensure_job_bundle_service()._active = replacement_flight
        build_called = threading.Event()
        window_started = state.vardiff_window_started_monotonic

        def unexpected_idle_build(_request: object) -> CachedJobBundle:
            build_called.set()
            raise AssertionError("idle sweep displaced the replacement build")

        server._build_idle_job_bundle = unexpected_idle_build  # type: ignore[method-assign]

        for _sweep in range(4):
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)

        # Close the race where a worker passed its first divergence check just
        # before detection. Scheduler admission must reject that idle request
        # without superseding the active replacement build.
        racing_idle_cancellation = _JobBuildCancellation(timeout_seconds=60.0)
        racing_idle_request = SimpleNamespace(
            idle_retarget=True,
            cancellation=racing_idle_cancellation,
            promise=Future(),
        )
        racing_idle_promise = server._request_job_build(racing_idle_request)  # type: ignore[arg-type]
        with self.assertRaises(JobBuildCancelled):
            racing_idle_promise.result()

        with server._ensure_job_bundle_service()._scheduler_lock:
            self.assertIs(server._ensure_job_bundle_service()._active, replacement_flight)
        self.assertFalse(replacement_cancellation.is_set())
        self.assertTrue(racing_idle_cancellation.is_set())
        self.assertFalse(build_called.is_set())
        self.assertEqual(state.vardiff_window_started_monotonic, window_started)
        self.assertEqual(server.vardiff_idle_skip_counts["superseded"], 4)
        with server._ensure_job_bundle_service()._scheduler_lock:
            server._ensure_job_bundle_service()._active = None
        server.shutdown_vardiff_idle_executor()
    def test_idle_shared_build_does_not_retry_scheduler_divergence_race(
        self,
    ) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        state = client()
        prepare_idle_client(server, state, tip=old_tip)
        install_idle_job_cache(server, tip=old_tip)
        repository = server._ensure_job_bundle_service().template_repository
        old_artifacts = repository.current_artifacts()
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._bundle_cache.clear()
        assert old_artifacts is not None
        now = time.monotonic()
        server.current_tip_first_seen = (old_tip, now)
        server.current_tip_observed_monotonic = now
        server.latest_detected_tip = (old_tip, 1)
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=old_artifacts.fingerprint,
            template_generation=old_artifacts.generation,
            template_artifacts=old_artifacts,
        )
        server._ensure_job_cache_state()
        replacement_cancellation = _JobBuildCancellation(timeout_seconds=60.0)
        replacement_flight = SimpleNamespace(
            request=SimpleNamespace(
                artifacts=SimpleNamespace(previousblockhash=new_tip),
                cancellation=replacement_cancellation,
            )
        )
        with server._ensure_job_bundle_service()._scheduler_lock:
            server._ensure_job_bundle_service()._active = replacement_flight
        request_builds = 0
        admission_attempts = 0
        idle_cancellation: _JobBuildCancellation | None = None
        service = server._ensure_job_bundle_service()
        original_request_job_build = service.request_build

        def make_idle_request(
            _artifacts: CachedTemplateArtifacts,
            _worker: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            nonlocal request_builds, idle_cancellation
            request_builds += 1
            self.assertTrue(kwargs["idle_retarget"])
            idle_cancellation = _JobBuildCancellation(timeout_seconds=60.0)
            return SimpleNamespace(
                idle_retarget=True,
                cancellation=idle_cancellation,
                promise=Future(),
            )

        def detect_before_admission(request: object) -> Future[CachedJobBundle]:
            nonlocal admission_attempts
            admission_attempts += 1
            with server.lock:
                server.latest_detected_tip = (new_tip, 2)
                server.tip_refresh_divergence_started_monotonic = time.monotonic()
            return original_request_job_build(request)  # type: ignore[arg-type]

        service.new_build_request = make_idle_request  # type: ignore[method-assign]
        service.request_build = detect_before_admission  # type: ignore[method-assign]
        server._schedule_tip_refresh_retry = lambda: None  # type: ignore[method-assign]
        assert state.worker is not None

        with self.assertRaises(JobBuildSuperseded):
            server._build_idle_job_bundle(SimpleNamespace(worker=state.worker))  # type: ignore[arg-type]

        self.assertEqual(request_builds, 1)
        self.assertEqual(admission_attempts, 1)
        self.assertIs(repository.current_artifacts(), old_artifacts)
        self.assertIsNotNone(idle_cancellation)
        assert idle_cancellation is not None
        self.assertTrue(idle_cancellation.is_set())
        self.assertFalse(replacement_cancellation.is_set())
        with server._ensure_job_bundle_service()._scheduler_lock:
            self.assertIs(server._ensure_job_bundle_service()._active, replacement_flight)
            server._ensure_job_bundle_service()._active = None
        server.shutdown_vardiff_idle_executor()
    def test_idle_cached_collection_bundle_refreshes_readiness(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        ready_bundle = install_idle_job_cache(server)
        assert state.worker is not None
        artifacts = (
            server._ensure_job_bundle_service()
            .template_repository.current_artifacts()
        )
        assert artifacts is not None
        with server._ensure_job_bundle_service()._cache_lock:
            collection_key = server._job_bundle_key(
                artifacts,
                mode="collection",
                payout_state_generation=server._payout_state_service._generation,
                payout_artifact_generation=0,
                worker=state.worker,
            )
            collection_bundle = dataclass_replace(
                ready_bundle,
                key=collection_key,
                collection_only=True,
                collection_identity=(
                    state.worker.payout_address,
                    state.worker.p2mr_program_hex,
                ),
            )
            server._ensure_job_bundle_service()._bundle_cache.clear()
            server._ensure_job_bundle_service()._bundle_cache[collection_key] = collection_bundle
        server._ensure_job_bundle_service().set_ready_for_test(False)
        server.min_ready_miners = 3
        server._ensure_job_bundle_service().set_min_ready_miners_for_test(3)
        server.accepted_share_stats = lambda: (3, 3)  # type: ignore[method-assign]
        rebuilt = threading.Event()
        sent: list[dict[str, object]] = []

        def build_ready(_request: object) -> CachedJobBundle:
            rebuilt.set()
            with server._ensure_job_bundle_service()._cache_lock:
                server._ensure_job_bundle_service()._bundle_cache[ready_bundle.key] = ready_bundle
            return ready_bundle

        state.send = sent.append  # type: ignore[method-assign]
        server._build_idle_job_bundle = build_ready  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        server.shutdown_vardiff_idle_executor()

        self.assertTrue(server._ensure_job_bundle_service().ready_latched())
        self.assertTrue(rebuilt.is_set())
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertIsNotNone(state.active_job)
        self.assertFalse(state.active_job.collection_only)
    def test_idle_cached_ready_bundle_rebinds_same_tip_observation(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        cached = install_idle_job_cache(server)
        updated_template = dict(cached.template)
        updated_template["curtime"] = int(updated_template["curtime"]) + 30
        current = server.store_template_artifacts(updated_template, generation=2)
        assert current is not None
        delivered = threading.Event()
        rebound = threading.Event()
        sent: list[dict[str, object]] = []

        def bind_current_observation(
            bundle: CachedJobBundle,
            artifacts: CachedTemplateArtifacts,
        ) -> CachedJobBundle:
            rebound.set()
            return dataclass_replace(
                bundle,
                template=artifacts.template,
                base_job=dataclass_replace(
                    bundle.base_job,
                    ntime=f'{artifacts.template["curtime"]:08x}',
                ),
                template_generation=artifacts.generation,
            )

        def record(payload: dict[str, object]) -> None:
            sent.append(payload)
            if payload.get("method") == "mining.notify":
                delivered.set()

        state.send = record  # type: ignore[method-assign]
        server._ensure_job_bundle_service().bind_cached_bundle = (  # type: ignore[method-assign]
            bind_current_observation
        )

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertTrue(delivered.wait(timeout=1))
        server.shutdown_vardiff_idle_executor()

        self.assertIsNotNone(state.active_job)
        self.assertTrue(rebound.is_set())
        self.assertIs(state.active_job.template, current.template)
        self.assertEqual(state.active_job.template_generation, current.generation)
        expected_ntime = f'{updated_template["curtime"]:08x}'
        self.assertEqual(state.active_job.job.ntime, expected_ntime)
        notify = next(
            payload
            for payload in sent
            if payload.get("method") == "mining.notify"
        )
        self.assertEqual(notify["params"][7], expected_ntime)
    def test_repeated_idle_sweeps_do_not_enqueue_duplicate_connection_work(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        delivery_started = threading.Event()
        release_delivery = threading.Event()
        sent: list[dict[str, object]] = []

        def blocking_send(payload: dict[str, object]) -> None:
            if payload.get("method") == "mining.set_difficulty":
                delivery_started.set()
                release_delivery.wait(timeout=1)
            sent.append(payload)

        state.send = blocking_send  # type: ignore[method-assign]
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)
            self.assertTrue(delivery_started.wait(timeout=1))
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)
            with server._vardiff_idle_lock:
                self.assertEqual(len(server._vardiff_idle_pending), 1)
        finally:
            release_delivery.set()
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertGreaterEqual(
            server.vardiff_idle_skip_counts["superseded"]
            + server.vardiff_idle_skip_counts["not_idle"],
            1,
        )
    def test_idle_sweep_skips_busy_client_lock_and_returns_promptly(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        lock_held = threading.Event()
        release_lock = threading.Event()

        def hold_client_lock() -> None:
            state.job_update_lock.acquire()
            try:
                lock_held.set()
                release_lock.wait(timeout=1)
            finally:
                state.job_update_lock.release()

        holder = threading.Thread(target=hold_client_lock)
        holder.start()
        self.assertTrue(lock_held.wait(timeout=1))
        started = time.monotonic()
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)
        finally:
            elapsed = time.monotonic() - started
            release_lock.set()
            holder.join(timeout=1)
            server.shutdown_vardiff_idle_executor()

        self.assertLess(elapsed, 0.25)
        self.assertEqual(server.vardiff_idle_skip_counts["busy"], 1)
    def test_stuck_bundle_builder_does_not_stale_idle_sweep_heartbeat(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        bundle = install_idle_job_cache(server)
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._bundle_cache.clear()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        build_started = threading.Event()
        release_build = threading.Event()

        def blocked_build(
            *_args: object,
            **_kwargs: object,
        ) -> CachedJobBundle:
            build_started.set()
            if not release_build.wait(timeout=1):
                raise AssertionError("idle retarget bundle build was not released")
            return bundle

        server._ensure_job_bundle_service().build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
        server._record_heartbeat("vardiff_idle_sweep")
        heartbeat_before = server._heartbeats["vardiff_idle_sweep"]
        started = time.monotonic()
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)
            elapsed = time.monotonic() - started
            self.assertTrue(build_started.wait(timeout=0.25))
            self.assertLess(elapsed, 0.25)
            self.assertGreater(
                server._heartbeats["vardiff_idle_sweep"],
                heartbeat_before,
            )
        finally:
            release_build.set()
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(server.vardiff_idle_skip_counts["cache_miss"], 1)
    def test_idle_sweep_cache_miss_builds_only_on_bounded_worker(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        bundle = install_idle_job_cache(server)
        server.job_bundle_cache_seconds = 10.0
        server._ensure_job_bundle_service().set_cache_seconds_for_test(10.0)
        expired_bundle = dataclass_replace(
            bundle,
            built_monotonic=time.monotonic() - 11.0,
        )
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._bundle_cache[bundle.key] = expired_bundle
        build_started = threading.Event()
        release_build = threading.Event()
        build_thread_ids: list[int] = []
        sent: list[dict[str, object]] = []

        def blocked_build(
            *_args: object,
            **_kwargs: object,
        ) -> CachedJobBundle:
            build_thread_ids.append(threading.get_ident())
            build_started.set()
            if not release_build.wait(timeout=1):
                raise AssertionError("idle retarget bundle build was not released")
            return bundle

        state.send = sent.append  # type: ignore[method-assign]
        server._ensure_job_bundle_service().build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
        sweep_thread_id = threading.get_ident()
        started = time.monotonic()
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)
            elapsed = time.monotonic() - started
            self.assertTrue(build_started.wait(timeout=0.25))
            self.assertLess(elapsed, 0.25)
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)
            self.assertEqual(len(build_thread_ids), 1)
            self.assertNotEqual(build_thread_ids[0], sweep_thread_id)
            with server._vardiff_idle_lock:
                self.assertEqual(len(server._vardiff_idle_pending), 1)
        finally:
            release_build.set()
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(server.vardiff_idle_skip_counts["cache_miss"], 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(server.idle_retarget_count, 1)
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertEqual(state.vardiff_window_accepted, 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(state.vardiff_window_work, Decimal("0"))
    def test_idle_retarget_delivers_fresh_bundle_when_cache_is_disabled(
        self,
    ) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        bundle = install_idle_job_cache(server)
        server.job_bundle_cache_seconds = 0.0
        server._ensure_job_bundle_service().set_cache_seconds_for_test(0.0)
        with server._ensure_job_bundle_service()._cache_lock:
            server._ensure_job_bundle_service()._bundle_cache.clear()
        built = threading.Event()
        sent: list[dict[str, object]] = []

        def build_uncached(_request: object) -> CachedJobBundle:
            built.set()
            return bundle

        server._build_idle_job_bundle = build_uncached  # type: ignore[method-assign]
        state.send = sent.append  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        server.shutdown_vardiff_idle_executor()

        self.assertTrue(built.is_set())
        self.assertEqual(server.vardiff_idle_skip_counts["cache_miss"], 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(server.idle_retarget_count, 1)
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertIsNone(state.pending_share_difficulty)
        with server._ensure_job_bundle_service()._cache_lock:
            self.assertEqual(server._ensure_job_bundle_service()._bundle_cache, {})
    def test_idle_preparation_oserror_keeps_client_connected(self) -> None:
        for failure_phase in ("bundle", "reorg"):
            with self.subTest(failure_phase=failure_phase):
                server = coordinator()
                state = client()
                prepare_idle_client(server, state)
                install_idle_job_cache(server)
                disconnected: list[ClientState] = []
                window_started = state.vardiff_window_started_monotonic

                def fail_bundle(_request: object) -> CachedJobBundle:
                    raise OSError("qbit RPC transport unavailable")

                def fail_reorg() -> bool:
                    raise OSError("qbit trust RPC transport unavailable")

                def unexpected_send(payload: dict[str, object]) -> None:
                    self.fail(f"unexpected idle delivery: {payload}")

                def record_disconnect(client_state: ClientState) -> None:
                    disconnected.append(client_state)

                if failure_phase == "bundle":
                    server._build_idle_job_bundle = (  # type: ignore[method-assign]
                        fail_bundle
                    )
                else:
                    server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
                        fail_reorg
                    )
                state.send = unexpected_send  # type: ignore[method-assign]
                server.disconnect_client = record_disconnect  # type: ignore[method-assign]

                self.assertEqual(server.vardiff_idle_sweep_once(), 1)
                server.shutdown_vardiff_idle_executor()

                self.assertEqual(disconnected, [])
                self.assertIn(state, server.clients)
                self.assertFalse(state.closing)
                self.assertEqual(server.vardiff_idle_task_failures, 1)
                self.assertIsNone(state.pending_share_difficulty)
                self.assertEqual(state.share_difficulty, Decimal("16"))
                self.assertEqual(
                    state.vardiff_window_started_monotonic,
                    window_started,
                )
    def test_disconnect_while_idle_retarget_pending_prevents_delivery(self) -> None:
        server = coordinator()
        blockers = [client(), client()]
        for connection_id, blocker in enumerate(blockers, start=1):
            prepare_idle_client(server, blocker, connection_id=connection_id)
        install_idle_job_cache(server)
        workers_started = threading.Barrier(3)
        release_workers = threading.Event()

        def block_delivery(payload: dict[str, object]) -> None:
            if payload.get("method") == "mining.set_difficulty":
                workers_started.wait(timeout=1)
                release_workers.wait(timeout=1)

        for blocker in blockers:
            blocker.send = block_delivery  # type: ignore[method-assign]

        target = client()
        target_sent: list[dict[str, object]] = []
        target.send = target_sent.append  # type: ignore[method-assign]
        target.close = lambda: None  # type: ignore[method-assign]
        disconnected_skip = threading.Event()
        record_skip = server._record_vardiff_idle_skip

        def record_and_signal(reason: str) -> None:
            record_skip(reason)
            if reason == "disconnected":
                disconnected_skip.set()

        server._record_vardiff_idle_skip = record_and_signal  # type: ignore[method-assign]
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 2)
            workers_started.wait(timeout=1)
            prepare_idle_client(server, target, connection_id=3)
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)
            server.disconnect_client(target)
        finally:
            release_workers.set()
        try:
            self.assertTrue(disconnected_skip.wait(timeout=1))
        finally:
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(target_sent, [])
        self.assertTrue(target.closing)
        self.assertNotIn(target, server.clients)
        self.assertGreaterEqual(server.vardiff_idle_skip_counts["disconnected"], 1)
    def test_hundreds_of_busy_and_dead_clients_cannot_stall_idle_sweep(self) -> None:
        server = coordinator()

        class BusyLock:
            def acquire(self, blocking: bool = True) -> bool:
                self.assert_nonblocking = blocking
                return False

            def release(self) -> None:
                raise AssertionError("unacquired busy lock released")

        for connection_id in range(1, 201):
            state = client()
            prepare_idle_client(server, state, connection_id=connection_id)
            state.job_update_lock = BusyLock()  # type: ignore[assignment]
        for connection_id in range(201, 401):
            state = client()
            prepare_idle_client(server, state, connection_id=connection_id)
            state.closing = True

        server.vardiff_idle_sweep_seconds = 0.5
        started = time.monotonic()
        self.assertEqual(server.vardiff_idle_sweep_once(), 0)
        elapsed = time.monotonic() - started
        server.shutdown_vardiff_idle_executor()

        self.assertLess(elapsed, server.vardiff_idle_sweep_seconds)
        self.assertEqual(server.vardiff_idle_clients_inspected, 400)
        self.assertEqual(server.vardiff_idle_skip_counts["busy"], 200)
        self.assertEqual(server.vardiff_idle_skip_counts["disconnected"], 200)
    def test_idle_retarget_queue_is_globally_bounded(self) -> None:
        server = coordinator()
        states = [client() for _ in range(9)]
        for connection_id, state in enumerate(states, start=1):
            prepare_idle_client(server, state, connection_id=connection_id)
        install_idle_job_cache(server)
        release_workers = threading.Event()
        two_workers_started = threading.Event()
        started_lock = threading.Lock()
        started_count = 0

        def block_delivery(payload: dict[str, object]) -> None:
            nonlocal started_count
            if payload.get("method") != "mining.set_difficulty":
                return
            with started_lock:
                started_count += 1
                if started_count == 2:
                    two_workers_started.set()
            release_workers.wait(timeout=1)

        for state in states:
            state.send = block_delivery  # type: ignore[method-assign]

        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 8)
            self.assertTrue(two_workers_started.wait(timeout=1))
            with server._vardiff_idle_lock:
                self.assertEqual(len(server._vardiff_idle_pending), 8)
                self.assertEqual(server.vardiff_idle_inflight, 2)
                self.assertEqual(server.vardiff_idle_queue_depth, 6)
            self.assertEqual(server.vardiff_idle_skip_counts["queue_full"], 1)
        finally:
            release_workers.set()
            server.shutdown_vardiff_idle_executor()
    def test_maybe_send_job_isolates_build_failure_and_keeps_client_connected(self) -> None:
        server = coordinator()
        server.jobs = {}
        server.extranonce2_size = 8
        state = client()
        state.username = "miner-a"
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def boom(client: ClientState, *, clean_jobs: bool) -> None:
            raise ValueError(
                "full coinbase transaction does not end its coinbase scriptSig "
                "with the extranonce placeholder"
            )

        server.build_job_for_client = boom  # type: ignore[method-assign]

        # The bug: this used to propagate out of handle_client and drop the miner.
        # It must now be swallowed so the client thread survives a single bad template.
        server.maybe_send_job(state, clean_jobs=True)

        self.assertEqual(server._ensure_job_bundle_service().metrics_snapshot()["failure_count"], 1)
        self.assertEqual(state.active_job_ids, set())
        self.assertEqual(server.jobs, {})
        self.assertEqual(sent, [])  # no difficulty / mining.notify pushed for the failed build

        # A subsequent good template still issues a job (skip, do not permanently break).
        server.build_job_for_client = lambda client, *, clean_jobs: SimpleNamespace(  # type: ignore[method-assign]
            job=SimpleNamespace(
                job_id="job-ok",
                share_difficulty=Decimal("1"),
                share_target=target_from_compact("207fffff"),
            ),
            template={"previousblockhash": "00" * 32},
            collection_only=False,
        )
        server.send_difficulty = lambda client, job: None  # type: ignore[method-assign]
        server.send_job = lambda client, job: sent.append("notify")  # type: ignore[method-assign]
        server.apply_job_difficulty = lambda client, job: None  # type: ignore[method-assign]

        server.maybe_send_job(state, clean_jobs=True)

        self.assertEqual(server._ensure_job_bundle_service().metrics_snapshot()["failure_count"], 1)
        self.assertEqual(state.active_job_ids, {"job-ok"})
        self.assertEqual(sent, ["notify"])
    def test_maybe_send_job_does_not_swallow_send_failures_as_build_failures(self) -> None:
        # Only the job build is isolated. A Stratum send failure (a dead socket)
        # must propagate so handle_client disconnects and cleans up, rather than
        # being miscounted as a build failure or leaving the client wedged.
        server = coordinator()
        server.jobs = {}
        server.extranonce2_size = 8
        state = client()
        state.username = "miner-a"
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )

        server.build_job_for_client = lambda client, *, clean_jobs: SimpleNamespace(  # type: ignore[method-assign]
            job=SimpleNamespace(
                job_id="job-dead",
                share_difficulty=Decimal("1"),
                share_target=target_from_compact("207fffff"),
            ),
            collection_only=False,
        )
        server.send_difficulty = lambda client, job: None  # type: ignore[method-assign]

        def dead_socket(client: ClientState, job: object) -> None:
            raise OSError("broken pipe")

        server.send_job = dead_socket  # type: ignore[method-assign]

        with self.assertRaises(OSError):
            server.maybe_send_job(state, clean_jobs=True)

        # The send failure is not a build failure, and handle_client (not us) owns
        # the disconnect/cleanup of the registered job for the dead connection.
        self.assertEqual(server._ensure_job_bundle_service().metrics_snapshot()["failure_count"], 0)
    def _pending_append(self, tag: str, accepted_at_ms: int = 2) -> PendingShareAppend:
        from lab.prism.share_ledger import PendingShare

        return PendingShareAppend(
            pending_share=PendingShare(
                share_id=f"miner-a:{tag}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=10,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1_700_000_000,
            ),
            username="miner-a",
            job_id="job-1",
            block_hash_hex=tag * 32,
            collection_only=False,
            credit_policy=None,
        )

class PrismStampedJobFloorTests(unittest.TestCase):
    """The listener floor must hold on the wire, not just in vardiff policy.

    Stamped jobs are the single choke point for every mining.set_difficulty
    the coordinator sends, and marketplace verification judges the first one.
    The regression here is a young chain: qbit network difficulty below the
    high-diff floor used to drag the advertised difficulty down with it.
    """
    def stamp_coordinator(self) -> PrismCoordinator:
        server = coordinator()
        server.job_counter = 0
        server.share_weights_by_username = {}
        server.default_share_weight = 1
        return server
    def cached_bundle(self) -> CachedJobBundle:
        # bits 207fffff: regtest-grade network difficulty (~4.7e-10), far
        # below the 500k marketplace floor.
        qbit_target = target_from_compact("207fffff")
        base_job = direct_stratum.DirectQbitStratumJob(
            job_id="prism-template-base",
            previousblockhash_display="00" * 32,
            prevhash="00" * 32,
            coinb1="",
            coinb2="",
            full_coinbase_prefix="",
            full_coinbase_suffix="",
            merkle_branch=(),
            transaction_hexes=(),
            version="20000000",
            nbits="207fffff",
            ntime="6553f100",
            qbit_target=qbit_target,
            share_target=qbit_target,
            share_difficulty=Decimal("1"),
            extranonce1_hex="ffffffff",
            extranonce2_size=8,
            clean_jobs=True,
        )
        return CachedJobBundle(
            key=("test",),
            template=gbt_template("00" * 32),
            template_fingerprint="fp",
            coinbase_manifest={},
            shares_json=[],
            prior_balances=[],
            found_block={"network_difficulty": 1},
            collection_only=False,
            issued_at_ms=12345,
            base_job=base_job,
            built_monotonic=time.monotonic(),
        )
    def highdiff_client(self) -> ClientState:
        state = client()
        state.worker = worker_identity()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.minimum_advertised_difficulty = Decimal("500000")
        state.share_difficulty = Decimal("500000")
        return state
    def test_stamped_job_enforces_floor_below_network_difficulty(self) -> None:
        server = self.stamp_coordinator()
        state = self.highdiff_client()

        context = server.stamp_job_for_client(state, self.cached_bundle(), clean_jobs=True)

        self.assertEqual(
            context.job.share_target,
            direct_stratum.difficulty_target(Decimal("500000")),
        )
        # Decimal round-tripping can land within 1e-27 of the floor; the wire
        # value is float(difficulty), which is what marketplaces judge.
        self.assertGreaterEqual(float(context.job.share_difficulty), 500000.0)
    def test_stamped_job_keeps_network_cap_without_listener_floor(self) -> None:
        server = self.stamp_coordinator()
        state = client()
        state.worker = worker_identity()
        # Even an absurd desired difficulty stays capped at the network
        # target on the default listener: shares are never required to be
        # harder than blocks there.
        state.share_difficulty = Decimal("500000")

        context = server.stamp_job_for_client(state, self.cached_bundle(), clean_jobs=True)

        self.assertEqual(context.job.share_target, target_from_compact("207fffff"))
        self.assertLess(context.job.share_difficulty, Decimal("1"))
    def test_stamped_job_honors_md_raised_floor_on_highdiff_listener(self) -> None:
        server = self.stamp_coordinator()
        state = self.highdiff_client()
        state.requested_min_difficulty = Decimal("2000000")
        server.apply_client_difficulty_requests(state)

        context = server.stamp_job_for_client(state, self.cached_bundle(), clean_jobs=True)

        self.assertEqual(
            context.job.share_target,
            direct_stratum.difficulty_target(Decimal("2000000")),
        )
        self.assertGreaterEqual(float(context.job.share_difficulty), 2000000.0)
