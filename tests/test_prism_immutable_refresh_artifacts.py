#!/usr/bin/env python3
"""Expected behavior for immutable PRISM tip-refresh artifacts."""

from __future__ import annotations

import threading
import unittest
from dataclasses import replace as dataclass_replace

from lab.prism.prism_coordinator import (
    CachedJobBundle,
    CachedTemplateArtifacts,
    ClientState,
    TemplateRefreshBlocked,
    WorkerIdentity,
    qbit_template_fingerprint,
)
from tests.test_prism_coordinator_job_cache import (
    base_template,
    client,
    coordinator,
    install_fake_bundle_builder,
)


class ImmutableRefreshArtifactTests(unittest.TestCase):
    def test_fetched_snapshot_owns_exact_derived_artifacts(self) -> None:
        server, _ = coordinator()
        original_store = server.store_template_artifacts
        stored: list[CachedTemplateArtifacts | None] = []

        def recording_store(
            template: dict[str, object],
            *,
            generation: int | None = None,
        ) -> CachedTemplateArtifacts | None:
            artifacts = original_store(template, generation=generation)
            stored.append(artifacts)
            return artifacts

        server.store_template_artifacts = recording_store  # type: ignore[method-assign]

        snapshot = server.fetch_qbit_tip_template_snapshot()

        self.assertEqual(len(stored), 1)
        self.assertIsNotNone(stored[0])
        self.assertIs(snapshot.template_artifacts, stored[0])
        assert snapshot.template_artifacts is not None
        self.assertEqual(
            snapshot.template_fingerprint,
            snapshot.template_artifacts.fingerprint,
        )
        self.assertEqual(
            snapshot.template_generation,
            snapshot.template_artifacts.generation,
        )
        self.assertEqual(
            snapshot.previousblockhash,
            snapshot.template_artifacts.previousblockhash,
        )
        self.assertEqual(
            snapshot.bestblockhash,
            snapshot.template_artifacts.previousblockhash,
        )

    def test_fetch_fails_closed_when_exact_artifacts_cannot_be_derived(self) -> None:
        server, _ = coordinator()
        server.store_template_artifacts = (  # type: ignore[method-assign]
            lambda _template, *, generation=None: None
        )

        with self.assertRaises(TemplateRefreshBlocked):
            server.fetch_qbit_tip_template_snapshot()

        self.assertIsNone(server.tip_template_snapshot)
        self.assertIsNone(server._template_artifacts)

    def test_prepare_rejects_mismatched_snapshot_artifact_invariants(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        server.clients = {state}
        snapshot = server.fetch_qbit_tip_template_snapshot()
        artifacts = snapshot.template_artifacts
        self.assertIsNotNone(artifacts)
        assert artifacts is not None

        with self.assertRaises(TemplateRefreshBlocked):
            server.prepare_tip_refresh_bundle(
                dataclass_replace(snapshot, template_artifacts=None),
            )
        with self.assertRaises(TemplateRefreshBlocked):
            server.prepare_tip_refresh_bundle(
                dataclass_replace(snapshot, bestblockhash="55" * 32),
            )

        changed_template = dict(artifacts.template)
        changed_template["coinbasevalue"] = int(changed_template["coinbasevalue"]) + 1
        wrong_parent_template = dict(artifacts.template)
        wrong_parent_template["previousblockhash"] = "44" * 32
        cases = {
            "fingerprint": dataclass_replace(artifacts, fingerprint="ff" * 32),
            "generation": dataclass_replace(
                artifacts,
                generation=artifacts.generation + 1,
            ),
            "previousblockhash": dataclass_replace(
                artifacts,
                previousblockhash="33" * 32,
            ),
            "template_fingerprint": dataclass_replace(
                artifacts,
                template=changed_template,
            ),
            "template_previousblockhash": dataclass_replace(
                artifacts,
                template=wrong_parent_template,
            ),
        }

        for name, mismatched in cases.items():
            with self.subTest(name=name):
                mismatched_snapshot = dataclass_replace(
                    snapshot,
                    template_artifacts=mismatched,
                )
                with self.assertRaises(TemplateRefreshBlocked):
                    server.prepare_tip_refresh_bundle(mismatched_snapshot)

        self.assertEqual(server.job_counter, 0)
        self.assertIsNone(state.active_job)

    def test_concurrent_global_cache_replacement_does_not_abort_snapshot_artifacts(
        self,
    ) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        states = [client(1), client(2)]
        server.clients = set(states)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        artifacts_a = snapshot.template_artifacts
        self.assertIsNotNone(artifacts_a)
        assert artifacts_a is not None

        build_entered = threading.Event()
        release_build = threading.Event()
        original_shared_job_bundle = server.shared_job_bundle
        bundles: list[CachedJobBundle] = []
        errors: list[BaseException] = []

        def blocked_shared_job_bundle(
            artifacts: CachedTemplateArtifacts,
            identity: WorkerIdentity | None = None,
            **kwargs: object,
        ) -> CachedJobBundle:
            self.assertIs(artifacts, artifacts_a)
            build_entered.set()
            if not release_build.wait(5):
                raise AssertionError("immutable artifact build was not released")
            return original_shared_job_bundle(artifacts, identity, **kwargs)

        def prepare() -> None:
            try:
                bundles.append(server.prepare_tip_refresh_bundle(snapshot))
            except BaseException as exc:  # noqa: BLE001 - surfaced by the test
                errors.append(exc)

        server.shared_job_bundle = blocked_shared_job_bundle  # type: ignore[method-assign]
        thread = threading.Thread(target=prepare)
        thread.start()
        try:
            self.assertTrue(build_entered.wait(5))
            template_b = dict(rpc.template)
            template_b["coinbasevalue"] = int(template_b["coinbasevalue"]) + 1
            artifacts_b = server.store_template_artifacts(template_b)
            self.assertIsNotNone(artifacts_b)
            assert artifacts_b is not None
            self.assertNotEqual(artifacts_a.fingerprint, artifacts_b.fingerprint)
            self.assertIs(server._template_artifacts, artifacts_b)
        finally:
            release_build.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(bundles), 1)
        bundle = bundles[0]
        self.assertEqual(bundle.template_fingerprint, artifacts_a.fingerprint)
        self.assertEqual(bundle.template_generation, artifacts_a.generation)
        self.assertEqual(
            bundle.template["previousblockhash"],
            artifacts_a.previousblockhash,
        )
        self.assertEqual(
            qbit_template_fingerprint(bundle.template),
            artifacts_a.fingerprint,
        )
        self.assertEqual(
            bundle.found_block["coinbase_value_sats"],
            artifacts_a.template["coinbasevalue"],
        )
        self.assertEqual(recorded["calls"], 1)

    def test_newer_observation_supersedes_old_build_before_send_and_runs_promptly(
        self,
    ) -> None:
        tip_a = "11" * 32
        tip_b = "22" * 32
        template_a = base_template(height=10, prevhash=tip_a)
        template_b = base_template(height=11, prevhash=tip_b)
        server, rpc = coordinator(template=template_a)
        install_fake_bundle_builder(server)
        server._pool_ready_latched = True
        states = [client(1), client(2)]
        server.clients = set(states)
        sent_fingerprints: list[str] = []
        sent_lock = threading.Lock()

        def record_send(state: ClientState, payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            active_job = state.active_job
            self.assertIsNotNone(active_job)
            with sent_lock:
                sent_fingerprints.append(active_job.template_fingerprint)

        for state in states:
            state.send = (  # type: ignore[method-assign]
                lambda payload, state=state: record_send(state, payload)
            )

        first_build_entered = threading.Event()
        release_first_build = threading.Event()
        original_shared_job_bundle = server.shared_job_bundle
        build_count = 0
        build_count_lock = threading.Lock()

        def block_first_shared_job_bundle(
            artifacts: CachedTemplateArtifacts,
            identity: WorkerIdentity | None = None,
            **kwargs: object,
        ) -> CachedJobBundle:
            nonlocal build_count
            with build_count_lock:
                build_count += 1
                current_build = build_count
            if current_build == 1:
                first_build_entered.set()
                if not release_first_build.wait(5):
                    raise AssertionError("superseded artifact build was not released")
            return original_shared_job_bundle(artifacts, identity, **kwargs)

        server.shared_job_bundle = block_first_shared_job_bundle  # type: ignore[method-assign]
        old_results: list[int] = []
        old_errors: list[BaseException] = []
        new_results: list[int] = []
        new_errors: list[BaseException] = []

        def poll(results: list[int], errors: list[BaseException]) -> None:
            try:
                results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surfaced by the test
                errors.append(exc)

        old_thread = threading.Thread(target=poll, args=(old_results, old_errors))
        new_thread = threading.Thread(target=poll, args=(new_results, new_errors))
        old_thread.start()
        try:
            self.assertTrue(first_build_entered.wait(5))
            rpc.tip = tip_b
            rpc.template = template_b
            self.assertTrue(server.observe_tip_for_refresh(tip_b))
            # A competing producer observes/cancels immediately, but cannot
            # enter the heavy lane while the obsolete owner unwinds.
            new_thread.start()
        finally:
            release_first_build.set()
            old_thread.join(5)
            new_thread.join(5)

        self.assertFalse(old_thread.is_alive())
        self.assertFalse(new_thread.is_alive())
        self.assertEqual(old_results, [])
        self.assertEqual(len(old_errors), 1)
        self.assertIsInstance(old_errors[0], TemplateRefreshBlocked)
        self.assertEqual(new_errors, [])
        self.assertEqual(new_results, [0])
        try:
            self.assertEqual(
                server.poll_qbit_tip_template_once(),
                len(states),
            )
        finally:
            server.shutdown_tip_refresh_executor()
        self.assertEqual(
            sent_fingerprints,
            [qbit_template_fingerprint(template_b)] * len(states),
        )
        self.assertNotIn(qbit_template_fingerprint(template_a), sent_fingerprints)


if __name__ == "__main__":
    unittest.main()
