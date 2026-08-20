#!/usr/bin/env python3
"""Admission-path lock isolation for the reconnect herd (issue #159).

The coordinator lock used to serialize three unrelated things that a
reconnect herd hits at once: per-connection stale-grace anchoring, first-job
queue bookkeeping, and an O(N) fleet coverage census run once per delivered
job. These tests pin each of them to its own owner, and pin the lock order
that keeps the split deadlock-free.
"""

from __future__ import annotations

import threading
import time
import unittest
from decimal import Decimal
from types import SimpleNamespace

from lab.prism.prism_coordinator import (
    ClientState,
    PendingInitialJob,
    PrismCoordinator,
    WorkerIdentity,
)


HERD = 1024
TIP_A = "aa" * 32
TIP_B = "bb" * 32


class FakeSocket:
    def __init__(self) -> None:
        self.closed = False

    def settimeout(self, _timeout: object) -> None:
        return

    def setsockopt(self, *_args: object) -> None:
        return

    def shutdown(self, _how: object) -> None:
        return

    def close(self) -> None:
        self.closed = True


class TrackedLock:
    """Plain lock that remembers its current owner thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._owner: int | None = None

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        acquired = self._lock.acquire(blocking, timeout)
        if acquired:
            self._owner = threading.get_ident()
        return acquired

    def release(self) -> None:
        self._owner = None
        self._lock.release()

    def __enter__(self) -> "TrackedLock":
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()

    def held_by_current_thread(self) -> bool:
        return self._owner == threading.get_ident()


class CountingLock:
    """Coordinator-lock double that counts acquisitions and pins lock order.

    Acquisitions are counted while the lock is held, so the count is exact
    under any amount of contention. ``forbidden`` names a lock that must never
    be held when this one is taken; a violation there is the reverse nesting
    that would make the admission split deadlock-prone.
    """

    def __init__(self, forbidden: TrackedLock | None = None) -> None:
        self._lock = threading.RLock()
        self._forbidden = forbidden
        self.acquisitions = 0
        self.order_violations = 0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        forbidden = self._forbidden
        if forbidden is not None and forbidden.held_by_current_thread():
            self.order_violations += 1
        acquired = self._lock.acquire(blocking, timeout)
        if acquired:
            self.acquisitions += 1
        return acquired

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> "CountingLock":
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


class AnchorRecordingClient(ClientState):
    """ClientState that counts every commit of the stale-grace anchor."""

    @property
    def tip_work_delivered(self) -> tuple[str, float] | None:
        return self.__dict__.get("_anchor")

    @tip_work_delivered.setter
    def tip_work_delivered(self, value: tuple[str, float] | None) -> None:
        self.__dict__["_anchor"] = value
        if value is not None:
            self.__dict__["_anchor_writes"] = (
                int(self.__dict__.get("_anchor_writes", 0)) + 1
            )

    @property
    def anchor_writes(self) -> int:
        return int(self.__dict__.get("_anchor_writes", 0))


def worker(name: str) -> WorkerIdentity:
    return WorkerIdentity(
        username=name,
        payout_address=name,
        worker_name=None,
        script_pubkey_hex="5220" + "11" * 32,
        p2mr_program_hex="11" * 32,
    )


def coordinator(
    *,
    connection_limit: int = HERD + 8,
    pending_limit: int = HERD,
) -> PrismCoordinator:
    server = PrismCoordinator.__new__(PrismCoordinator)
    server.stop_event = threading.Event()
    server.clients = set()
    server.jobs = {}
    server.connection_counter = 0
    server.job_counter = 0
    server.stratum_max_connections = connection_limit
    server.stratum_max_connections_per_username = 0
    server.stratum_max_pending_initial_jobs = pending_limit
    server.stratum_initial_job_timeout_seconds = 30.0
    server.mining_health_startup_grace_seconds = 30.0
    server.initial_job_max_workers = 1
    server.tip_refresh_max_workers = 1
    server.tip_template_snapshot = None
    server.started_monotonic = 0.0
    server.submitted_share_count = 0
    server.rejection_counts_by_reason = {}
    server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
    server.apply_stratum_send_timeout = lambda _sock: None  # type: ignore[method-assign]
    server.client_startup_difficulty = lambda _profile: Decimal("1")  # type: ignore[method-assign]
    # The S2 admission lock is a private implementation detail; the lock-order
    # assertions below are the reason this test reaches for it.
    admission = TrackedLock()
    server.lock = CountingLock(forbidden=admission)
    server._ensure_job_cache_state()
    server._ensure_tip_refresh_state()
    server._ensure_initial_job_state()
    service = server._ensure_job_delivery_service()
    service._initial_job_admission_lock = admission
    return server


def client(
    server: PrismCoordinator,
    connection_id: int,
    *,
    with_job: bool = False,
    recording: bool = False,
) -> ClientState:
    factory = AnchorRecordingClient if recording else ClientState
    state = factory(
        sock=FakeSocket(),
        address=("127.0.0.1", 40_000 + connection_id),
        connection_id=connection_id,
        extranonce1_hex=f"{connection_id:08x}",
    )
    state.subscribed = True
    state.authorized = True
    state.authorization_generation = 1
    state.worker = worker(f"miner-{connection_id}")
    state.username = state.worker.username
    if with_job:
        state.active_job = SimpleNamespace(
            template={"previousblockhash": TIP_A},
            payout_state_generation=0,
        )
    server.clients.add(state)
    return state


def run_concurrently(targets: list, *, timeout: float = 60.0) -> None:
    """Start every target behind one barrier and join them all."""
    barrier = threading.Barrier(len(targets), timeout=timeout)

    def entry(target) -> None:  # type: ignore[no-untyped-def]
        barrier.wait()
        target()

    threads = [threading.Thread(target=entry, args=(target,)) for target in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout)
    for thread in threads:
        assert not thread.is_alive(), "worker thread did not finish"


class DeliveryAnchorIsolationTests(unittest.TestCase):
    def test_contended_coordinator_lock_cannot_delay_a_delivery_anchor(self) -> None:
        server = coordinator()
        blocked = client(server, 1)
        delivering = client(server, 2)
        held = threading.Event()
        release = threading.Event()

        def hold_coordinator_lock() -> None:
            with server.lock:
                held.set()
                release.wait(30)

        holder = threading.Thread(target=hold_coordinator_lock)
        holder.start()
        try:
            self.assertTrue(held.wait(10))
            before = server.lock.acquisitions
            started = time.monotonic()
            server.note_tip_work_delivered(delivering, TIP_A)
            elapsed = time.monotonic() - started
        finally:
            release.set()
            holder.join(10)

        # A different connection's first delivery completed while the whole
        # control plane was blocked: it neither took nor waited for the
        # coordinator lock.
        self.assertEqual(server.lock.acquisitions, before)
        self.assertLess(elapsed, 5.0)
        self.assertIsNotNone(delivering.tip_work_delivered)
        assert delivering.tip_work_delivered is not None
        self.assertEqual(delivering.tip_work_delivered[0], TIP_A)
        self.assertIsNone(blocked.tip_work_delivered)
        self.assertEqual(server.lock.order_violations, 0)

    def test_same_tip_refresh_race_cannot_slide_an_existing_anchor(self) -> None:
        server = coordinator()
        state = client(server, 1)

        server.note_tip_work_delivered(state, TIP_A)
        first = state.tip_work_delivered
        self.assertIsNotNone(first)

        # However many same-tip refreshes race, the stale-grace anchor stays
        # on the first delivery for that tip.
        run_concurrently(
            [lambda: server.note_tip_work_delivered(state, TIP_A) for _ in range(16)]
        )
        self.assertEqual(state.tip_work_delivered, first)

        # A new tip is a new anchor.
        server.note_tip_work_delivered(state, TIP_B)
        anchored = state.tip_work_delivered
        assert anchored is not None and first is not None
        self.assertEqual(anchored[0], TIP_B)
        self.assertGreaterEqual(anchored[1], first[1])

    def test_reconnect_herd_first_deliveries_anchor_each_connection_once(self) -> None:
        server = coordinator()
        clients = [
            client(server, index + 1, recording=True) for index in range(HERD)
        ]
        before = server.lock.acquisitions

        def deliver(state: ClientState) -> None:
            server.note_tip_work_delivered(state, TIP_A)

        # Four threads race the first delivery of every connection at once.
        run_concurrently(
            [
                (lambda state=state: deliver(state))
                for state in clients
                for _ in range(4)
            ]
        )

        self.assertEqual(server.lock.acquisitions, before)
        self.assertEqual(server.lock.order_violations, 0)
        for state in clients:
            anchor = state.tip_work_delivered
            self.assertIsNotNone(anchor)
            assert anchor is not None
            self.assertEqual(anchor[0], TIP_A)
            self.assertEqual(state.anchor_writes, 1)


class InitialJobAdmissionLockTests(unittest.TestCase):
    def test_queue_bookkeeping_never_reaches_the_coordinator_lock(self) -> None:
        server = coordinator(pending_limit=HERD)
        started = threading.Event()
        release = threading.Event()

        def blocked(request: PendingInitialJob) -> bool:
            started.set()
            release.wait(30)
            return not server._initial_request_cancelled(request)

        server._run_initial_job = blocked  # type: ignore[method-assign]
        clients = [client(server, index + 1) for index in range(HERD)]
        try:
            # Fresh admission asks the coordinator exactly one constant-time
            # question -- does this connection already hold current-tip work --
            # and nothing else.
            before = server.lock.acquisitions
            for state in clients:
                self.assertTrue(server.schedule_initial_job(state))
            self.assertTrue(started.wait(10))
            self.assertEqual(server.lock.acquisitions - before, HERD)
            self.assertEqual(len(server.pending_initial_jobs), HERD)

            # Coalescing a repeat request is pure queue bookkeeping.
            before = server.lock.acquisitions
            for state in clients:
                self.assertTrue(server.schedule_initial_job(state))
            self.assertEqual(server.lock.acquisitions, before)
            self.assertEqual(server.initial_job_coalesced_count, HERD)
            self.assertEqual(len(server.pending_initial_jobs), HERD)

            # So is releasing every one of them again, including the physical
            # queue reclamation its cancellation callbacks perform.
            before = server.lock.acquisitions
            for state in clients:
                server.cancel_initial_job_delivery(state)
            self.assertEqual(server.lock.acquisitions, before)
            self.assertEqual(len(server.pending_initial_jobs), 0)
            self.assertEqual(server.initial_job_cancelled_count, HERD)
            self.assertEqual(
                server.initial_job_queue_capacity_reclaimed_count,
                HERD - 1,
            )
            self.assertEqual(server.lock.order_violations, 0)
        finally:
            release.set()
            server.shutdown_initial_job_executor()

    def test_queue_bookkeeping_runs_while_the_coordinator_lock_is_held(self) -> None:
        server = coordinator(pending_limit=8)
        submitted: list[PendingInitialJob] = []
        server._submit_initial_job_request = (  # type: ignore[method-assign]
            lambda request: bool(submitted.append(request)) or True
        )
        state = client(server, 1)
        self.assertTrue(server.schedule_initial_job(state))
        self.assertEqual(len(submitted), 1)

        held = threading.Event()
        release = threading.Event()

        def hold_coordinator_lock() -> None:
            with server.lock:
                held.set()
                release.wait(30)

        finished = threading.Event()

        def bookkeep() -> None:
            # Coalescing and cancellation are admission-domain only, so a
            # blocked control plane cannot stall a reconnecting miner's queue
            # bookkeeping.
            server.schedule_initial_job(state)
            server.cancel_initial_job_delivery(state)
            finished.set()

        holder = threading.Thread(target=hold_coordinator_lock)
        bookkeeper = threading.Thread(target=bookkeep)
        holder.start()
        try:
            self.assertTrue(held.wait(10))
            bookkeeper.start()
            self.assertTrue(
                finished.wait(5),
                "queue bookkeeping waited on the contended coordinator lock",
            )
            # Still held: the bookkeeping above ran through, not after, the
            # control plane being blocked.
            self.assertFalse(release.is_set())
            self.assertEqual(server.initial_job_coalesced_count, 1)
            self.assertEqual(len(server.pending_initial_jobs), 0)
            self.assertEqual(server.initial_job_cancelled_count, 1)
        finally:
            release.set()
            holder.join(10)
            bookkeeper.join(10)
        self.assertEqual(server.lock.order_violations, 0)

    def test_concurrent_admission_enforces_the_exact_pending_capacity(self) -> None:
        capacity = 64
        server = coordinator(pending_limit=capacity)
        submitted: list[PendingInitialJob] = []
        submit_lock = threading.Lock()

        def record(request: PendingInitialJob) -> bool:
            with submit_lock:
                submitted.append(request)
            return True

        server._submit_initial_job_request = record  # type: ignore[method-assign]
        clients = [client(server, index + 1) for index in range(HERD)]
        admitted: list[bool] = [False] * HERD

        def admit(index: int) -> None:
            admitted[index] = bool(server.schedule_initial_job(clients[index]))

        run_concurrently(
            [(lambda index=index: admit(index)) for index in range(HERD)]
        )

        self.assertEqual(sum(admitted), capacity)
        self.assertEqual(len(server.pending_initial_jobs), capacity)
        self.assertEqual(len(submitted), capacity)
        self.assertEqual(server.initial_job_queue_rejection_count, HERD - capacity)
        # Every rejected connection was retired exactly once, and every
        # admitted one still owns its slot.
        self.assertEqual(len(server.clients), capacity)
        for index, state in enumerate(clients):
            self.assertEqual(server.pending_initial_jobs.get(state) is not None,
                             admitted[index])
            self.assertEqual(state.sock.closed, not admitted[index])
        self.assertEqual(server.lock.order_violations, 0)


class PendingQueueCompatibilitySurfaceTests(unittest.TestCase):
    """The coordinator-facing queue surface is owned by the admission lock."""

    def test_capacity_read_is_taken_under_the_admission_lock(self) -> None:
        server = coordinator(pending_limit=8)
        server._submit_initial_job_request = lambda _request: True  # type: ignore[method-assign]
        state = client(server, 1)
        self.assertTrue(server.schedule_initial_job(state))

        admission = server._ensure_job_delivery_service()._initial_job_admission_lock
        started = threading.Event()
        finished = threading.Event()
        observed: list[object] = []

        def capacity_read() -> None:
            started.set()
            observed.append(len(server.pending_initial_jobs))
            observed.append(state in server.pending_initial_jobs)
            finished.set()

        reader = threading.Thread(target=capacity_read)
        self.assertTrue(admission.acquire(timeout=5))
        try:
            reader.start()
            self.assertTrue(started.wait(5))
            # The external capacity read is the one the session owner's
            # reauthorization pre-check performs. It must serialize with
            # admission rather than run beside it.
            self.assertFalse(
                finished.wait(0.5),
                "capacity read completed without the admission lock",
            )
        finally:
            admission.release()
        reader.join(10)

        self.assertTrue(finished.is_set())
        self.assertEqual(observed, [1, True])

    def test_external_reads_cannot_observe_a_partly_swept_queue(self) -> None:
        cohort = 64
        server = coordinator(pending_limit=HERD)
        clients = [client(server, index + 1) for index in range(cohort)]
        observed: set[int] = set()
        failures: list[BaseException] = []
        stop = threading.Event()
        empty_observed = threading.Event()
        cohort_observed = threading.Event()

        def reader() -> None:
            try:
                while not stop.is_set():
                    size = len(server.pending_initial_jobs)
                    observed.add(size)
                    if size == 0:
                        empty_observed.set()
                    elif size == cohort:
                        cohort_observed.set()
            except BaseException as exc:  # pragma: no cover - failure path
                failures.append(exc)

        watcher = threading.Thread(target=reader)
        watcher.start()
        try:
            self.assertTrue(
                empty_observed.wait(5),
                "reader did not observe the empty queue before seeding",
            )
            for _ in range(24):
                expired = time.monotonic() - 1.0
                for state in clients:
                    state.closing = False
                    server.clients.add(state)
                server.pending_initial_jobs = {
                    state: PendingInitialJob(
                        client=state,
                        connection_id=state.connection_id,
                        authorization_generation=1,
                        difficulty_generation=0,
                        worker=state.worker,
                        requested_monotonic=expired,
                        deadline_monotonic=expired,
                    )
                    for state in clients
                }
                self.assertTrue(
                    cohort_observed.wait(5),
                    "reader did not observe the fully seeded queue",
                )
                self.assertEqual(server.sweep_initial_job_timeouts(), cohort)
        finally:
            stop.set()
            watcher.join(10)

        self.assertEqual(failures, [])
        # The sweep releases the whole cohort inside one critical section and
        # seeding installs it inside another, so an external reader sees the
        # queue wholly pending or wholly released -- never mid-cancellation.
        self.assertTrue(
            observed.issubset({0, cohort}),
            f"external read observed a partly swept queue: {sorted(observed)}",
        )
        self.assertIn(cohort, observed)
        self.assertIn(0, observed)
        self.assertEqual(server.lock.order_violations, 0)

    def test_surface_is_strictly_immutable(self) -> None:
        server = coordinator(pending_limit=4)
        state = client(server, 1)
        view = server.pending_initial_jobs

        # No write reaches admission state through the compatibility surface:
        # every mutator is absent, not merely guarded.
        for mutator in (
            "__setitem__",
            "__delitem__",
            "pop",
            "popitem",
            "clear",
            "update",
            "setdefault",
        ):
            self.assertFalse(
                hasattr(view, mutator),
                f"compatibility surface exposes {mutator}",
            )
        request = PendingInitialJob(
            client=state,
            connection_id=state.connection_id,
            authorization_generation=1,
            difficulty_generation=0,
            worker=state.worker,
            requested_monotonic=0.0,
            deadline_monotonic=None,
        )
        with self.assertRaises(TypeError):
            view[state] = request
        with self.assertRaises(TypeError):
            del view[state]

        # Seeding goes through the setter, which adopts the mapping's contents
        # under the admission lock instead of exposing a mutable map.
        server.pending_initial_jobs = {state: request}
        self.assertEqual(len(server.pending_initial_jobs), 1)
        self.assertIs(server.pending_initial_jobs[state], request)

        # The owner's mutable map is private and is never the object handed to
        # a compatibility reader.
        service = server._ensure_job_delivery_service()
        self.assertIsNot(view, service._pending_initial_jobs)
        self.assertIsInstance(service._pending_initial_jobs, dict)


class CoverageCensusOwnershipTests(unittest.TestCase):
    def test_observability_refresh_not_delivery_resets_the_coverage_failure(
        self,
    ) -> None:
        server = coordinator(connection_limit=4)
        state = client(server, 1, with_job=True)
        server.current_tip_first_seen = (TIP_A, 100.0)
        observability = server._ensure_observability_service()
        observability.set_delivery_failure_started_monotonic_for_test(50.0)

        # Delivery anchors this connection and does nothing else: no fleet
        # census, no coordinator lock, no health-policy write.
        before = server.lock.acquisitions
        server.note_tip_work_delivered(state, TIP_A)
        self.assertEqual(server.lock.acquisitions, before)
        self.assertEqual(
            observability.state().mining_delivery_failure_started_monotonic,
            50.0,
        )

        # The refresh census -- the one that already walks authorized clients
        # and current-work coverage -- performs the reset.
        inputs = observability.port.mining_delivery_inputs(120.0)

        self.assertEqual(inputs.authorized_connections, 1)
        self.assertEqual(inputs.clients_with_current_tip_jobs, 1)
        self.assertIsNone(
            observability.state().mining_delivery_failure_started_monotonic
        )

    def test_refresh_below_the_coverage_threshold_leaves_the_failure_running(
        self,
    ) -> None:
        server = coordinator(connection_limit=HERD)
        covered = [client(server, index + 1, with_job=True) for index in range(90)]
        for index in range(10):
            client(server, 1_000 + index)
        server.current_tip_first_seen = (TIP_A, 100.0)
        observability = server._ensure_observability_service()
        observability.set_delivery_failure_started_monotonic_for_test(50.0)

        inputs = observability.port.mining_delivery_inputs(120.0)

        self.assertEqual(inputs.authorized_connections, 100)
        self.assertEqual(inputs.clients_with_current_tip_jobs, len(covered))
        self.assertEqual(
            observability.state().mining_delivery_failure_started_monotonic,
            50.0,
        )


if __name__ == "__main__":
    unittest.main()
