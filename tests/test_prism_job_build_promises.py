#!/usr/bin/env python3
"""Single-flight job-build promise lifecycle regressions.

A cancelled or superseded flight must never leave joiners parked on an
unresolved promise: cancellation resolves the shared promise exceptionally at
the moment of termination, and dedup admission refuses (and eventually
evicts) flights whose completion never released their scheduler slot, so the
next requester becomes the new owner instead of orbiting an abandoned
flight.
"""

from __future__ import annotations

import threading
import unittest
from concurrent.futures import Future

from lab.prism.prism_coordinator import (
    JobBuildCancelled,
    JobBuildSuperseded,
    PrismCoordinator,
    WorkerIdentity,
    _JobBuildFlight,
    _JobBuildRequest,
)
from tests.test_prism_coordinator_job_cache import (
    coordinator,
    install_fake_bundle_builder,
    synthetic_manifest_coinbase_hex,
    worker,
)


def install_gated_bundle_builder(
    server: PrismCoordinator,
) -> tuple[threading.Event, threading.Event, dict[str, object]]:
    """A counting fake audit-bundle builder that parks until released.

    Returns (entered, release, recorded): ``entered`` is set when a build has
    reached the builder, ``release`` lets it finish. Waiter-wakeup assertions
    run while the build is provably still executing.
    """

    entered = threading.Event()
    release = threading.Event()
    recorded: dict[str, object] = {"calls": 0}

    def gated_build_audit_bundle(**kwargs: object) -> dict[str, object]:
        recorded["calls"] = int(recorded["calls"]) + 1
        entered.set()
        if not release.wait(timeout=30.0):
            raise AssertionError("bundle builder gate never released")
        suffix_hex = str(kwargs["coinbase_script_sig_suffix_hex"])
        return {
            "found_block": dict(kwargs["found_block"]),  # type: ignore[call-overload]
            "payout_policy_manifest": {"accounts": []},
            "signed_coinbase_manifest": {
                "manifest": {
                    "coinbase_tx_hex": synthetic_manifest_coinbase_hex(
                        suffix_hex
                    ),
                }
            },
        }

    server.build_audit_bundle = gated_build_audit_bundle  # type: ignore[method-assign]
    return entered, release, recorded


class _PromiseHarness(unittest.TestCase):
    def setUp(self) -> None:
        self.server, self.rpc = coordinator()
        self.identity = worker()
        self.artifacts = self.server.store_template_artifacts(
            dict(self.rpc.template)
        )
        assert self.artifacts is not None
        self.cache_key = self.server._job_bundle_key(
            self.artifacts,
            mode="ready",
            payout_state_generation=0,
            worker=self.identity,
        )

    def tearDown(self) -> None:
        self.server.shutdown_job_build_executor()

    def new_request(self) -> _JobBuildRequest:
        return self.server._new_job_build_request(
            self.artifacts,
            self.identity,
            mode="ready",
            payout_state_generation=0,
            cache_key=self.cache_key,
        )


class CancelledFlightWakesJoinersTests(_PromiseHarness):
    def test_cancelling_running_build_delivers_at_drain(self) -> None:
        # Owner starts a build that parks inside the builder; a joiner shares
        # the same flight. Cancelling a flight whose build is still executing
        # keeps today's ordering: the promise resolves when the drain
        # completes, never earlier -- and, with the completion path hardened,
        # never later.
        entered, release, recorded = install_gated_bundle_builder(self.server)
        owner = self.new_request()
        owner_promise = self.server._request_job_build(owner)
        self.assertTrue(entered.wait(timeout=5.0))

        joiner = self.new_request()
        joiner_promise = self.server._request_job_build(joiner)
        self.assertIs(joiner_promise, owner_promise)

        with self.server._job_build_scheduler_lock:
            flight = self.server._job_build_active
            assert flight is not None
            self.assertTrue(
                self.server._cancel_job_build_flight_locked(
                    flight,
                    "superseded",
                )
            )
        self.assertFalse(owner_promise.done())

        release.set()
        error = owner_promise.exception(timeout=5.0)
        self.assertIsInstance(error, JobBuildSuperseded)

        # The retry after the drain becomes a fresh owner and builds.
        rebuild = self.new_request()
        rebuild_promise = self.server._request_job_build(rebuild)
        self.assertIsNot(rebuild_promise, owner_promise)
        rebuild_promise.result(timeout=10.0)
        self.assertEqual(recorded["calls"], 2)

    def test_cancelling_never_started_flight_wakes_waiters_immediately(
        self,
    ) -> None:
        # Without a live executor future nothing else can ever resolve the
        # promise: cancellation itself must wake the waiters instead of
        # leaving them to burn their full wait deadline.
        owner = self.new_request()
        flight = _JobBuildFlight(request=owner)
        self.server._job_build_active = flight

        with self.server._job_build_scheduler_lock:
            self.assertTrue(
                self.server._cancel_job_build_flight_locked(
                    flight,
                    "superseded",
                )
            )

        error = owner.promise.exception(timeout=1.0)
        self.assertIsInstance(error, JobBuildSuperseded)
        self.server._job_build_active = None

    def test_timeout_cancellation_maps_to_job_build_cancelled(self) -> None:
        owner = self.new_request()
        flight = _JobBuildFlight(request=owner)
        self.server._job_build_active = flight

        with self.server._job_build_scheduler_lock:
            self.server._cancel_job_build_flight_locked(flight, "timeout")

        error = owner.promise.exception(timeout=1.0)
        self.assertIsInstance(error, JobBuildCancelled)
        self.assertNotIsInstance(error, JobBuildSuperseded)
        self.server._job_build_active = None

    def test_cancel_obsolete_job_builds_resolves_orphaned_promises(self) -> None:
        # The detection-sweep termination path shares the same guarantee: a
        # flight it cancels without a live future resolves immediately, and a
        # draining build keeps today's resolve-at-drain ordering.
        entered, release, _recorded = install_gated_bundle_builder(self.server)
        owner = self.new_request()
        owner_promise = self.server._request_job_build(owner)
        self.assertTrue(entered.wait(timeout=5.0))

        self.server._cancel_obsolete_job_builds(
            "superseded by a newer template observation"
        )
        self.assertFalse(owner_promise.done())

        release.set()
        error = owner_promise.exception(timeout=5.0)
        self.assertIsInstance(error, JobBuildSuperseded)


class OrphanedFlightEvictionTests(_PromiseHarness):
    def test_orphaned_flight_is_evicted_and_not_reserved(self) -> None:
        # Manufacture the wedge a died done-callback leaves behind: a flight
        # whose executor future finished but whose promise was never resolved
        # and whose slot was never released. Repeated requests must not orbit
        # it: the sweep resolves the promise from the finished future, frees
        # the slot, and the next requester becomes the new owner. The gated
        # builder keeps the new owner's build parked so its slot ownership
        # stays observable.
        entered, release, _recorded = install_gated_bundle_builder(self.server)
        self.server.job_build_orphan_sweep_grace_seconds = 0.0

        corpse_request = self.new_request()
        corpse_future: Future = Future()
        corpse_future.set_exception(RuntimeError("builder thread died"))
        corpse = _JobBuildFlight(
            request=corpse_request,
            future=corpse_future,
        )
        self.server._job_build_active = corpse

        # First admission cannot yet distinguish the corpse from a completion
        # whose callback is still in flight; it only marks the observation.
        first = self.new_request()
        first_promise = self.server._request_job_build(first)
        self.assertIs(first_promise, corpse_request.promise)
        self.assertFalse(corpse_request.promise.done())

        # Past the grace, the next admission evicts: the abandoned promise
        # resolves (waking the first requester) and the corpse never owns the
        # slot again.
        second = self.new_request()
        second_promise = self.server._request_job_build(second)
        self.assertIsNot(second_promise, corpse_request.promise)
        self.assertIsInstance(
            corpse_request.promise.exception(timeout=1.0),
            RuntimeError,
        )
        self.assertTrue(entered.wait(timeout=5.0))
        with self.server._job_build_scheduler_lock:
            active = self.server._job_build_active
            assert active is not None
            self.assertIs(active.request, second)
        self.assertEqual(
            self.server.job_build_scheduler_counts["orphan_evicted"],
            1,
        )
        release.set()
        second_promise.result(timeout=10.0)

    def test_running_flight_is_never_swept(self) -> None:
        # The sweep must not touch a flight whose future is still executing:
        # evicting it would let a replacement start and break the bounded
        # two-build concurrency invariant.
        entered, release, _recorded = install_gated_bundle_builder(self.server)
        self.server.job_build_orphan_sweep_grace_seconds = 0.0
        owner = self.new_request()
        owner_promise = self.server._request_job_build(owner)
        self.assertTrue(entered.wait(timeout=5.0))

        with self.server._job_build_scheduler_lock:
            self.server._evict_orphaned_job_build_flights_locked()
            self.server._evict_orphaned_job_build_flights_locked()
            flight = self.server._job_build_active
            assert flight is not None
            self.assertIs(flight.request, owner)
        self.assertEqual(
            self.server.job_build_scheduler_counts["orphan_evicted"],
            0,
        )
        release.set()
        owner_promise.result(timeout=10.0)


class StructuralResolutionFaultInjectionTests(_PromiseHarness):
    def test_poisoned_completion_bookkeeping_still_resolves_promise(self) -> None:
        # The completion callback's promise resolution must survive a raise
        # anywhere in its bookkeeping (done callbacks swallow exceptions, so
        # without the try/finally the promise would strand until the sweep).
        install_fake_bundle_builder(self.server)

        def poisoned_promote() -> None:
            raise RuntimeError("bookkeeping poisoned by test")

        self.server._promote_pending_job_build_locked = (  # type: ignore[method-assign]
            poisoned_promote
        )
        try:
            owner = self.new_request()
            promise = self.server._request_job_build(owner)
            self.assertIsNotNone(promise.result(timeout=10.0))
            # The sweep played no part: the callback itself resolved it.
            self.assertEqual(
                self.server.job_build_scheduler_counts["orphan_evicted"],
                0,
            )
        finally:
            del self.server._promote_pending_job_build_locked

    def test_promote_start_failure_resolves_consumed_pending_promise(
        self,
    ) -> None:
        # A pending request consumed by promotion whose executor start fails
        # has no flight and therefore no completion callback left; promotion
        # itself must settle the promise for its waiters.
        pending = self.new_request()
        self.server._job_build_pending = pending

        def failing_start(request: _JobBuildRequest) -> _JobBuildFlight:
            raise RuntimeError("executor unavailable")

        self.server._start_job_build_locked = (  # type: ignore[method-assign]
            failing_start
        )
        try:
            with self.server._job_build_scheduler_lock:
                with self.assertRaises(RuntimeError):
                    self.server._promote_pending_job_build_locked()
        finally:
            del self.server._start_job_build_locked
        self.assertIsNone(self.server._job_build_pending)
        self.assertIsInstance(
            pending.promise.exception(timeout=1.0),
            JobBuildSuperseded,
        )

    def test_resolved_priority_corpse_does_not_defer_routine_requesters(
        self,
    ) -> None:
        # A resolved-promise publication-critical corpse is not live priority
        # work: deferring on its already-done promise would wake instantly
        # and hot-spin until the sweep's grace elapses. The routine requester
        # must take ownership through the normal supersession path instead.
        entered, release, _recorded = install_gated_bundle_builder(self.server)
        corpse_request = self.server._new_job_build_request(
            self.artifacts,
            self.identity,
            mode="ready",
            payout_state_generation=0,
            cache_key=self.cache_key,
            publication_critical=True,
        )
        corpse_request.promise.set_exception(
            JobBuildSuperseded("resolved before the slot was released")
        )
        corpse_future: Future = Future()
        corpse_future.set_exception(RuntimeError("builder thread died"))
        self.server._job_build_active = _JobBuildFlight(
            request=corpse_request,
            future=corpse_future,
        )

        routine = self.new_request()
        promise = self.server._request_job_build(routine)
        self.assertIs(promise, routine.promise)
        self.assertTrue(entered.wait(timeout=5.0))
        release.set()
        promise.result(timeout=10.0)

    def test_shutdown_settles_terminal_corpse_waiters(self) -> None:
        # Executor shutdown re-fires callbacks for queued and running
        # futures, but a terminal future's callback never fires again:
        # shutdown is the last chance to settle its waiters.
        corpse = self.new_request()
        corpse_future: Future = Future()
        corpse_future.set_exception(RuntimeError("builder thread died"))
        self.server._job_build_active = _JobBuildFlight(
            request=corpse,
            future=corpse_future,
        )
        self.server.shutdown_job_build_executor()
        self.assertIsInstance(
            corpse.promise.exception(timeout=1.0),
            JobBuildSuperseded,
        )


class HealthySingleFlightTests(_PromiseHarness):
    def test_equivalent_requests_share_one_build(self) -> None:
        entered, release, recorded = install_gated_bundle_builder(self.server)
        owner = self.new_request()
        owner_promise = self.server._request_job_build(owner)
        self.assertTrue(entered.wait(timeout=5.0))

        joiner = self.new_request()
        joiner_promise = self.server._request_job_build(joiner)
        self.assertIs(joiner_promise, owner_promise)

        release.set()
        owner_bundle = owner_promise.result(timeout=10.0)
        self.assertIs(joiner_promise.result(timeout=10.0), owner_bundle)
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(
            self.server.job_build_scheduler_counts["orphan_evicted"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
