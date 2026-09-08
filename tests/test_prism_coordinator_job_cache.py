#!/usr/bin/env python3
"""Direct ownership tests for the extracted PRISM template and job services.

These cases target the J1 owner boundary itself: symbol identity, descriptor
routing, template-artifact generation ordering, and bundle cache-admission
fencing through the real ``JobBundleService`` /
``TemplateArtifactRepository`` APIs. The publication-priority and scheduler
behavior itself is asserted by the upstream suites in
``tests/test_prism_job_builder.py`` and ``tests/test_prism_job_build_promises.py``.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace as dataclass_replace
import time
from types import SimpleNamespace
import unittest

from lab.prism import job_bundle, prism_coordinator, template_artifacts
from lab.prism.job_bundle import CachedJobBundle, JobBuildKey
from lab.prism.prism_coordinator import PayoutStateArtifact
from lab.prism.template_artifacts import CachedTemplateArtifacts
from tests.prism_coordinator_test_support import base_template, coordinator


def cached_bundle(
    artifacts: CachedTemplateArtifacts,
    *,
    payout_generation: int,
    payout_artifact_sha256: str,
    key: tuple[object, ...] = ("candidate",),
    collection_only: bool = True,
    payout_append_invalidation_epoch: int = 0,
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
        payout_append_invalidation_epoch=payout_append_invalidation_epoch,
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
        built_monotonic=time.monotonic(),
        template_generation=artifacts.generation,
        payout_state_generation=payout_generation,
        collection_identity=build_key.collection_identity,
        build_key=build_key,
    )


def seed_published_payout_artifact(server: object, sha256: str) -> int:
    """Publish a payout artifact through the P1 owner and return the generation."""
    server._ensure_payout_state_service()  # type: ignore[attr-defined]
    generation = int(server._payout_state_generation)  # type: ignore[attr-defined]
    server._published_payout_state = dataclass_replace(  # type: ignore[attr-defined]
        server._published_payout_state,  # type: ignore[attr-defined]
        artifact=PayoutStateArtifact(
            generation=generation,
            source_generation=0,
            prior_balances_json="[]",
            prior_balances_sha256=sha256,
            prepared_monotonic=time.monotonic(),
        ),
    )
    return generation


class MovedSymbolIdentityTests(unittest.TestCase):
    def test_moved_symbol_reexports_preserve_identity(self) -> None:
        self.assertIs(prism_coordinator.CachedJobBundle, job_bundle.CachedJobBundle)
        # PR 80 removed the JobBuildKey compatibility re-export.
        self.assertFalse(hasattr(prism_coordinator, "JobBuildKey"))
        self.assertIs(
            prism_coordinator.JobBuildSuperseded, job_bundle.JobBuildSuperseded
        )
        self.assertIs(prism_coordinator._JobBuildRequest, job_bundle.JobBuildRequest)
        self.assertIs(prism_coordinator._JobBuildFlight, job_bundle.JobBuildFlight)
        self.assertIs(
            prism_coordinator._JobBuildCancellation, job_bundle.JobBuildCancellation
        )
        self.assertIs(
            prism_coordinator._JobBundleBuildControl,
            job_bundle.JobBundleBuildControl,
        )
        self.assertIs(
            prism_coordinator.CachedTemplateArtifacts,
            template_artifacts.CachedTemplateArtifacts,
        )
        self.assertIs(
            prism_coordinator.QbitTipTemplateSnapshot,
            template_artifacts.QbitTipTemplateSnapshot,
        )
        # qbit_template_fingerprint intentionally remains a local coordinator
        # compatibility definition at this layer; the owner carries its own
        # copy with identical semantics (no reverse import).
        template = base_template()
        self.assertEqual(
            prism_coordinator.qbit_template_fingerprint(template),
            template_artifacts.qbit_template_fingerprint(template),
        )

    def test_service_owns_job_cache_state_and_descriptors_route(self) -> None:
        server, _rpc = coordinator()
        service = server._ensure_job_bundle_service()

        self.assertIs(server._job_cache_lock, service._job_cache_lock)
        self.assertIs(server._job_bundle_cache, service._job_bundle_cache)
        self.assertIs(
            server._job_build_scheduler_lock, service._job_build_scheduler_lock
        )
        self.assertIs(server.job_cache_hit_counts, service.job_cache_hit_counts)
        # The repository deliberately shares the historical combined lock.
        self.assertIs(
            service.template_repository._job_cache_lock,
            service._job_cache_lock,
        )

        replacement: OrderedDict[tuple[object, ...], CachedJobBundle] = OrderedDict()
        server._job_bundle_cache = replacement
        self.assertIs(service._job_bundle_cache, replacement)


class TemplateArtifactRepositoryTests(unittest.TestCase):
    def test_repository_derive_reuses_expensive_fields_and_orders_stores(self) -> None:
        server, _rpc = coordinator()
        repository = server._ensure_job_bundle_service().template_repository

        first = repository.derive(
            base_template(), generation=repository.reserve_generation()
        )
        self.assertTrue(repository.store_artifacts(first))

        second = repository.derive(
            first.template, generation=repository.reserve_generation()
        )

        self.assertEqual(second.template, first.template)
        # A same-fingerprint derivation reuses the expensive derived fields
        # of the stored observation instead of recomputing them.
        self.assertIs(second.transaction_hexes, first.transaction_hexes)
        self.assertIs(
            second.witness_merkle_leaves_hex, first.witness_merkle_leaves_hex
        )
        self.assertTrue(repository.store_artifacts(second))
        self.assertFalse(repository.store_artifacts(first))
        self.assertIs(repository.current_artifacts(), second)

    def test_store_artifacts_prunes_stale_fingerprints_and_cancels_builds(
        self,
    ) -> None:
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        repository = service.template_repository
        old = server.store_template_artifacts(dict(rpc.template))
        assert old is not None
        cancellations: list[tuple[str, bool]] = []
        server._cancel_obsolete_job_builds = (  # type: ignore[method-assign]
            lambda reason, *, keep_published_snapshot=False: cancellations.append(
                (reason, keep_published_snapshot)
            )
        )
        with service._job_cache_lock:
            service._job_bundle_cache = OrderedDict(
                (
                    (("old", 0), SimpleNamespace(template_fingerprint=old.fingerprint)),
                    (("other", 0), SimpleNamespace(template_fingerprint="ff" * 32)),
                )
            )

        replacement = repository.derive(
            base_template(height=11, prevhash="22" * 32),
            generation=repository.reserve_generation(),
        )
        self.assertNotEqual(replacement.fingerprint, old.fingerprint)
        self.assertTrue(repository.store_artifacts(replacement))

        self.assertEqual(
            cancellations,
            [("template fingerprint superseded", True)],
        )
        with service._job_cache_lock:
            self.assertEqual(dict(service._job_bundle_cache), {})
        self.assertIs(repository.current_artifacts(), replacement)

    def test_repository_adoption_seeds_generation_like_first_touch(self) -> None:
        server, _rpc = coordinator()
        repository = server._ensure_job_bundle_service().template_repository
        seeded = repository.derive(base_template(), generation=5)

        repository.adopt_template_artifacts(seeded)

        self.assertIs(repository.current_artifacts(), seeded)
        self.assertEqual(repository.reserve_generation(), 6)

        repository.adopt_template_artifact_generation(10)
        # An explicitly seeded counter is authoritative: later artifact
        # adoption may replace the artifacts but not rewind the counter.
        repository.adopt_template_artifacts(
            repository.derive(base_template(), generation=3)
        )
        self.assertEqual(repository.reserve_generation(), 11)


class JobBundleCacheAdmissionTests(unittest.TestCase):
    def admission_fixture(self):  # type: ignore[no-untyped-def]
        server, rpc = coordinator()
        service = server._ensure_job_bundle_service()
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        generation = seed_published_payout_artifact(server, "payout-sha")
        server._artifacts_buildable_locked = lambda _artifacts: True  # type: ignore[method-assign]
        server._published_snapshot_artifacts_locked = (  # type: ignore[method-assign]
            lambda _artifacts: False
        )
        return server, service, artifacts, generation

    def test_cache_admission_requires_current_payout_state(self) -> None:
        server, service, artifacts, generation = self.admission_fixture()

        exact = cached_bundle(
            artifacts,
            payout_generation=generation,
            payout_artifact_sha256="payout-sha",
            key=("exact",),
        )
        self.assertTrue(service._cache_job_bundle_if_current(exact, artifacts))
        with service._job_cache_lock:
            self.assertIn(("exact",), service._job_bundle_cache)

        stale_generation = cached_bundle(
            artifacts,
            payout_generation=generation + 1,
            payout_artifact_sha256="payout-sha",
            key=("stale-generation",),
        )
        self.assertFalse(
            service._cache_job_bundle_if_current(stale_generation, artifacts)
        )

        stale_artifact = cached_bundle(
            artifacts,
            payout_generation=generation,
            payout_artifact_sha256="different-sha",
            key=("stale-artifact",),
        )
        self.assertFalse(
            service._cache_job_bundle_if_current(stale_artifact, artifacts)
        )

        stale_epoch = cached_bundle(
            artifacts,
            payout_generation=generation,
            payout_artifact_sha256="payout-sha",
            key=("stale-epoch",),
            collection_only=False,
            payout_append_invalidation_epoch=7,
        )
        self.assertFalse(
            service._cache_job_bundle_if_current(stale_epoch, artifacts)
        )

        with service._job_cache_lock:
            self.assertEqual(list(service._job_bundle_cache), [("exact",)])

    def test_cache_admission_requires_current_template_or_pinned_snapshot(
        self,
    ) -> None:
        server, service, old, generation = self.admission_fixture()
        repository = service.template_repository
        stale_bundle = cached_bundle(
            old,
            payout_generation=generation,
            payout_artifact_sha256="payout-sha",
            key=("old-after-publication",),
        )
        server._cancel_obsolete_job_builds = (  # type: ignore[method-assign]
            lambda _reason, **_kwargs: None
        )
        replacement = repository.derive(
            base_template(height=11, prevhash="22" * 32),
            generation=repository.reserve_generation(),
        )
        self.assertTrue(repository.store_artifacts(replacement))

        # An old completion cannot cache after a newer fingerprint published.
        self.assertFalse(
            service._cache_job_bundle_if_current(stale_bundle, old)
        )
        with service._job_cache_lock:
            self.assertNotIn(("old-after-publication",), service._job_bundle_cache)

        # The sole exception is a pinned rebuild of exactly the published
        # snapshot, which direct issuance would otherwise rebuild all window.
        server._published_snapshot_artifacts_locked = (  # type: ignore[method-assign]
            lambda candidate: candidate.fingerprint == old.fingerprint
        )
        self.assertTrue(service._cache_job_bundle_if_current(stale_bundle, old))
        with service._job_cache_lock:
            self.assertIn(("old-after-publication",), service._job_bundle_cache)

    def test_collection_admission_requires_exact_generation_unlike_ready(
        self,
    ) -> None:
        server, service, artifacts, generation = self.admission_fixture()
        repository = service.template_repository
        # A same-fingerprint, newer-generation observation of the same
        # template supersedes the stored one without a fingerprint change.
        newer = repository.derive(
            artifacts.template, generation=repository.reserve_generation()
        )
        self.assertTrue(repository.store_artifacts(newer))

        old_collection = cached_bundle(
            artifacts,
            payout_generation=generation,
            payout_artifact_sha256="payout-sha",
            key=("old-collection",),
            collection_only=True,
        )
        old_ready = cached_bundle(
            artifacts,
            payout_generation=generation,
            payout_artifact_sha256="payout-sha",
            key=("old-ready",),
            collection_only=False,
        )

        self.assertFalse(
            service._cache_job_bundle_if_current(old_collection, artifacts)
        )
        self.assertTrue(service._cache_job_bundle_if_current(old_ready, artifacts))
        with service._job_cache_lock:
            self.assertNotIn(("old-collection",), service._job_bundle_cache)
            self.assertIn(("old-ready",), service._job_bundle_cache)


class JobBuildExecutorOwnershipTests(unittest.TestCase):
    def test_executor_is_single_flight_named_and_shutdown_is_terminal(self) -> None:
        server, _rpc = coordinator()
        service = server._ensure_job_bundle_service()

        executor = service._job_build_executor_locked()
        self.assertIs(service._job_build_executor_locked(), executor)
        self.assertEqual(executor._thread_name_prefix, "prism-job-build")

        service.shutdown_job_build_executor()

        self.assertTrue(service._job_build_executor_shutdown)
        with self.assertRaisesRegex(RuntimeError, "shut down"):
            service._job_build_executor_locked()
        # The coordinator facade delegates to the same owner state.
        with self.assertRaisesRegex(RuntimeError, "shut down"):
            server._job_build_executor_locked()


if __name__ == "__main__":
    unittest.main()
