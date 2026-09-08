"""Failed singleflight owners must not pin their flight until cyclic GC (#251).

``PublicResponseCache`` and ``P2mrAddressValidator`` record an owner's failure
on the in-flight entry so coalesced waiters can re-raise it. The recorded
exception carries a traceback, the traceback holds the owner's frame, and a
frame that still names the flight closes the cycle
``flight -> exception -> traceback -> frame -> flight``. Popping the flight
from its map removes the only external reference, but nothing in that cycle
is freed by reference counting: the failed origin/RPC frame and every local it
holds stay allocated until a cyclic collection happens to run.

These tests exercise the real owner methods with automatic collection
disabled, so an object that is still alive when asserted is being held by a
cycle, not by a pending collection. Weak references are the only handles
kept: no ``f_locals`` or referrer inspection, which would itself retain the
frames under test. Threads are ordered with events and barriers, never
sleeps, and every wait is bounded. Test-only GC policy is restored on exit.
"""

from __future__ import annotations

import gc
import threading
import unittest
import weakref
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from lab.prism.public_api import PublicResponseCache, _PublicInflight
from lab.prism.stratum_session import P2mrAddressValidator, StratumError

CACHE_KEY: tuple[str, tuple[tuple[str, tuple[str, ...]], ...]] = ("/issue-251", ())
ADDRESS = "qbit1issue251payoutaddress"
P2MR_SCRIPT = "5220" + "ab" * 32
WAITER_COUNT = 3
WAIT_SECONDS = 5.0


class Payload:
    """Weak-referenceable stand-in for a failed origin call's locals."""


@contextmanager
def _without_cyclic_gc() -> Iterator[None]:
    """Leave only reference counting to free objects inside the block.

    Assertions inside run before any ``gc.collect()``; whatever is still alive
    there is held by a reference cycle. The prior policy is restored on exit,
    after one full collection reclaims anything a failing case left behind.
    """
    gc.collect()
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        yield
    finally:
        gc.collect()
        if was_enabled:
            gc.enable()


def _traceback_names(error: BaseException) -> list[str]:
    """Function names along ``error.__traceback__``, outermost first.

    Reads code objects only, never ``f_locals``, so checking that the
    diagnostics survived does not itself keep the frames alive.
    """
    names: list[str] = []
    traceback = error.__traceback__
    while traceback is not None:
        names.append(traceback.tb_frame.f_code.co_name)
        traceback = traceback.tb_next
    return names


class _Origin:
    """A ``compute()`` / ``rpc_call`` that creates a weak-referenceable local.

    With ``park=True`` the call blocks until ``release`` is set, so concurrent
    requests can register as waiters while the owner is in flight. Failing
    calls raise after creating the local; successful calls return ``success``.
    """

    def __init__(self, *, park: bool = False, success: object | None = None) -> None:
        self.park = park
        self.success = success
        self.calls = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        self.payloads: list[weakref.ReferenceType[Payload]] = []
        self._lock = threading.Lock()

    def __call__(self, *args: object) -> object:
        payload = Payload()
        with self._lock:
            self.calls += 1
            self.payloads.append(weakref.ref(payload))
        self.entered.set()
        if self.park and not self.release.wait(timeout=WAIT_SECONDS):
            raise TimeoutError("test did not release the parked origin call")
        if self.success is not None:
            return self.success
        raise RuntimeError("injected origin failure")


class _GatedEvent(threading.Event):
    """Flight event whose waiters announce themselves and can be held.

    A waiter reaching the singleflight's ``event.wait()`` joins ``arrived``
    (the test thread is the extra party) and then blocks on ``proceed`` before
    entering the real wait. The test therefore knows when every waiter is
    inside the owner method and decides whether the owner finishes before or
    after they observe the recorded result.

    The real wait is this class's own condition variable rather than the
    inherited one, so the guarantee below depends on nothing private to the
    standard library. Each waiter counts itself in ``all_inside`` while
    holding the condition's lock and only releases that lock by entering
    ``Condition.wait``; ``set()`` needs the same lock. Once ``all_inside`` is
    set, every waiter is therefore blocked in the real wait before any
    ``set()`` can complete. Waits are bounded: a flight that is never set
    fails the test instead of hanging it.
    """

    def __init__(self, *, waiters: int) -> None:
        super().__init__()
        self.arrived = threading.Barrier(waiters + 1)
        self.proceed = threading.Event()
        self.all_inside = threading.Event()
        self._expected_waiters = waiters
        self._waiting = 0
        self._set = False
        self._condition = threading.Condition()

    def is_set(self) -> bool:
        with self._condition:
            return self._set

    def set(self) -> None:
        with self._condition:
            self._set = True
            self._condition.notify_all()

    def clear(self) -> None:
        with self._condition:
            self._set = False

    def wait(self, timeout: float | None = None) -> bool:
        # The owner methods wait without a timeout; bound it here so a
        # regression that forgets to wake waiters fails this call instead of
        # leaving non-daemon test threads blocked after the join times out.
        bound = WAIT_SECONDS if timeout is None else min(timeout, WAIT_SECONDS)
        self.arrived.wait(timeout=WAIT_SECONDS)
        if not self.proceed.wait(timeout=WAIT_SECONDS):
            raise TimeoutError("test did not release the gated waiters")
        with self._condition:
            self._waiting += 1
            if self._waiting == self._expected_waiters:
                self.all_inside.set()
            if not self._set and not self._condition.wait(timeout=bound):
                raise TimeoutError(f"the flight was not set within {bound}s")
            return True


class _InflightMap(dict[Any, Any]):
    """Inflight map that weakly records each registered flight.

    When ``event`` is given, the flight's event is swapped for it at
    registration, which is the only point where the owner method exposes the
    flight it just created.
    """

    def __init__(self, *, event: threading.Event | None = None) -> None:
        super().__init__()
        self.event = event
        self.flights: list[weakref.ReferenceType[Any]] = []

    def __setitem__(self, key: Any, flight: Any) -> None:
        if self.event is not None:
            flight.event = self.event
        self.flights.append(weakref.ref(flight))
        super().__setitem__(key, flight)


class _ReleaseAssertions(unittest.TestCase):
    def assert_released(self, refs: list[weakref.ReferenceType[Any]], what: str) -> None:
        self.assertTrue(refs, f"no {what} was ever registered")
        self.assertEqual(
            [None] * len(refs),
            [ref() for ref in refs],
            f"{what} still alive without a cyclic collection",
        )

    def observe_failure(self, call: Callable[[], object]) -> list[str]:
        """Run ``call``, require the injected failure, and return its frames.

        The exception is dropped before returning, exactly as a request
        handler that logged and answered the error would drop it.
        """
        try:
            call()
        except RuntimeError as error:
            self.assertEqual(str(error), "injected origin failure")
            return _traceback_names(error)
        self.fail("expected the injected origin failure")

    def assert_shared_failure(
        self,
        outcomes: list[tuple[str, BaseException | None]],
        *,
        chained: bool,
    ) -> None:
        """Check every thread saw the injected failure with diagnostics intact.

        Runs in its own frame so no local keeps an exception, and with it the
        failed origin frame, alive once the caller clears ``outcomes``.
        """
        leader_errors = [error for name, error in outcomes if name == "owner"]
        waiter_errors = [error for name, error in outcomes if name != "owner"]
        self.assertEqual(len(leader_errors), 1)
        self.assertEqual(len(waiter_errors), WAITER_COUNT)
        leader_error = leader_errors[0]
        self.assertIsInstance(leader_error, RuntimeError)
        self.assertEqual(str(leader_error), "injected origin failure")
        self.assertEqual(_traceback_names(leader_error)[-1], "__call__")
        for waiter_error in waiter_errors:
            self.assertIsInstance(waiter_error, RuntimeError)
            self.assertEqual(str(waiter_error), "injected origin failure")
            if chained:
                # The documented wrapper: a fresh error chained to the leader's
                # original, which still carries the RPC frame.
                self.assertIs(waiter_error.__cause__, leader_error)
            else:
                # The owner's own exception object, re-raised by each waiter;
                # its traceback grew by the waiter frames and kept the origin.
                self.assertIs(waiter_error, leader_error)

    def run_concurrently(
        self,
        request: Callable[[], object],
        *,
        origin: _Origin,
        gate: _GatedEvent,
        owner_finishes_first: bool,
        flight_released: Callable[[], None],
        waiters_arrived: Callable[[], None] | None = None,
    ) -> list[tuple[str, BaseException | None]]:
        """Run one owner and WAITER_COUNT coalesced waiters through ``request``.

        Returns ``(thread name, error)`` per thread, ``None`` for a normal
        return. The owner is parked inside the origin call until every waiter
        has entered the flight's ``event.wait()``, when ``waiters_arrived``
        runs. With ``owner_finishes_first`` the owner completes, drops out of
        the inflight map (checked by ``flight_released``), and unwinds
        entirely before any waiter reads the recorded outcome. Otherwise the
        owner is released only after every waiter is blocked in the real
        wait, which the gate guarantees rather than merely makes likely.
        """
        outcomes: list[tuple[str, BaseException | None]] = []
        outcomes_lock = threading.Lock()

        def run() -> None:
            error: BaseException | None = None
            try:
                request()
            except BaseException as exc:
                error = exc
            with outcomes_lock:
                outcomes.append((threading.current_thread().name, error))
            del error

        owner = threading.Thread(target=run, name="owner")
        waiters = [
            threading.Thread(target=run, name=f"waiter-{index}") for index in range(WAITER_COUNT)
        ]
        started: list[threading.Thread] = []
        try:
            owner.start()
            started.append(owner)
            self.assertTrue(origin.entered.wait(timeout=WAIT_SECONDS))
            for waiter in waiters:
                waiter.start()
                started.append(waiter)
            gate.arrived.wait(timeout=WAIT_SECONDS)
            if waiters_arrived is not None:
                waiters_arrived()
            if owner_finishes_first:
                origin.release.set()
                owner.join(timeout=WAIT_SECONDS)
                self.assertFalse(owner.is_alive())
                flight_released()
                gate.proceed.set()
            else:
                gate.proceed.set()
                self.assertTrue(gate.all_inside.wait(timeout=WAIT_SECONDS))
                origin.release.set()
        finally:
            # Unblock whatever is still parked, then join only the threads
            # that were actually started so an earlier assertion failure is
            # reported as itself rather than masked by joining unstarted ones.
            origin.release.set()
            gate.proceed.set()
            for thread in started:
                thread.join(timeout=WAIT_SECONDS)
        self.assertEqual(len(started), 1 + WAITER_COUNT)
        self.assertFalse(any(thread.is_alive() for thread in started))
        self.assertEqual(origin.calls, 1)
        self.assertEqual(len(outcomes), 1 + WAITER_COUNT)
        return outcomes


class PublicResponseCacheFailedFlightTests(_ReleaseAssertions):
    """Foreground owners, background refreshes, and waiters release the flight."""

    def lookup(self, cache: PublicResponseCache, compute: Callable[[], Any]) -> Any:
        return cache.get_or_compute(key=CACHE_KEY, ttl_seconds=60, compute=compute)

    def test_foreground_owner_failure_releases_flight_and_compute_frame(self) -> None:
        origin = _Origin()
        with _without_cyclic_gc():
            cache = PublicResponseCache()
            flights = _InflightMap()
            cache._inflight = flights

            frames = self.observe_failure(lambda: self.lookup(cache, origin))

            self.assertNotIn(CACHE_KEY, cache._inflight)
            self.assertEqual(origin.calls, 1)
            # The owner re-raised its own exception with the origin frame in
            # the traceback; the diagnostics were not trimmed to break the cycle.
            self.assertEqual(frames[-2:], ["get_or_compute", "__call__"])
            self.assert_released(flights.flights, "foreground flight")
            self.assert_released(origin.payloads, "failed compute frame local")

    def test_background_refresh_failure_releases_flight_and_compute_frame(self) -> None:
        origin = _Origin()
        with _without_cyclic_gc():
            cache = PublicResponseCache()
            flights = _InflightMap()
            cache._inflight = flights
            flight = _PublicInflight(event=threading.Event())
            cache._inflight[CACHE_KEY] = flight

            cache._refresh_entry(CACHE_KEY, 60, 30, origin, flight)

            self.assertNotIn(CACHE_KEY, cache._inflight)
            self.assertTrue(flight.event.is_set())
            self.assertIsInstance(flight.exception, RuntimeError)
            self.assertIsNone(flight.result)
            # The swallowed error keeps its origin frame for whoever coalesced.
            self.assertEqual(
                _traceback_names(flight.exception)[-2:], ["_refresh_entry", "__call__"]
            )
            del flight
            self.assert_released(flights.flights, "background refresh flight")
            self.assert_released(origin.payloads, "failed refresh compute frame local")

    def test_stale_hit_refresh_thread_failure_releases_flight_and_keeps_entry(self) -> None:
        origin = _Origin()
        with _without_cyclic_gc():
            cache = PublicResponseCache()
            flights = _InflightMap()
            cache._inflight = flights
            status, payload, state, _age = cache.get_or_compute(
                key=CACHE_KEY, ttl_seconds=60, compute=lambda: (200, {"v": "stale"})
            )
            self.assertEqual((200, {"v": "stale"}, "MISS"), (status, payload, state))
            entry = cache._entries[CACHE_KEY]
            entry.expires_at = entry.stored_at - 1  # expired, inside the window below
            entry.stale_until = entry.expires_at + 30
            del entry

            status, payload, state, _age = cache.get_or_compute(
                key=CACHE_KEY,
                ttl_seconds=60,
                compute=origin,
                stale_while_revalidate_seconds=30,
            )

            self.assertEqual((200, {"v": "stale"}, "STALE"), (status, payload, state))
            self.assertTrue(origin.entered.wait(timeout=WAIT_SECONDS))
            refreshers = [
                thread
                for thread in threading.enumerate()
                if thread.name == "prism-public-cache-refresh"
            ]
            for thread in refreshers:
                thread.join(timeout=WAIT_SECONDS)
            self.assertFalse(any(thread.is_alive() for thread in refreshers))
            self.assertNotIn(CACHE_KEY, cache._inflight)
            self.assertIn(CACHE_KEY, cache._entries)  # the failure deleted nothing
            self.assertEqual(origin.calls, 1)
            self.assert_released(flights.flights, "stale refresh flight")
            self.assert_released(origin.payloads, "failed refresh compute frame local")

    def assert_waiters_release_failed_flight(self, *, owner_finishes_first: bool) -> None:
        origin = _Origin(park=True)
        gate = _GatedEvent(waiters=WAITER_COUNT)
        with _without_cyclic_gc():
            cache = PublicResponseCache()
            flights = _InflightMap(event=gate)
            cache._inflight = flights

            outcomes = self.run_concurrently(
                lambda: self.lookup(cache, origin),
                origin=origin,
                gate=gate,
                owner_finishes_first=owner_finishes_first,
                flight_released=lambda: self.assertNotIn(CACHE_KEY, cache._inflight),
            )

            self.assertNotIn(CACHE_KEY, cache._inflight)
            self.assert_shared_failure(outcomes, chained=False)
            outcomes.clear()
            self.assert_released(flights.flights, "shared flight")
            self.assert_released(origin.payloads, "failed compute frame local")

    def test_waiters_release_flight_when_owner_fails_while_they_wait(self) -> None:
        self.assert_waiters_release_failed_flight(owner_finishes_first=False)

    def test_waiters_release_flight_when_owner_failed_before_they_observe(self) -> None:
        self.assert_waiters_release_failed_flight(owner_finishes_first=True)

    def test_successful_flight_is_released_after_the_last_observer(self) -> None:
        origin = _Origin(park=True, success=(200, {"v": "fresh"}))
        gate = _GatedEvent(waiters=WAITER_COUNT)
        with _without_cyclic_gc():
            cache = PublicResponseCache()
            flights = _InflightMap(event=gate)
            cache._inflight = flights
            results: list[Any] = []
            results_lock = threading.Lock()

            def request() -> None:
                result = self.lookup(cache, origin)
                with results_lock:
                    results.append(result)

            outcomes = self.run_concurrently(
                request,
                origin=origin,
                gate=gate,
                owner_finishes_first=True,
                flight_released=lambda: self.assertNotIn(CACHE_KEY, cache._inflight),
            )

            self.assertEqual([error for _name, error in outcomes], [None] * (1 + WAITER_COUNT))
            self.assertEqual(len(results), 1 + WAITER_COUNT)
            self.assertEqual(
                {(status, state) for status, _payload, state, _age in results}, {(200, "MISS")}
            )
            self.assertEqual(cache._entries[CACHE_KEY].payload, {"v": "fresh"})
            self.assert_released(flights.flights, "successful flight")


class P2mrAddressValidatorFailedFlightTests(_ReleaseAssertions):
    """The validation leader and its waiters release the pending flight."""

    def validator(self, rpc_call: Callable[..., object], flights: _InflightMap) -> P2mrAddressValidator:
        return P2mrAddressValidator(
            rpc_call=rpc_call,
            max_entries=lambda: 4,
            ttl_seconds=lambda: 60.0,
            inflight=flights,
        )

    def test_leader_rpc_failure_releases_flight_and_rpc_frame(self) -> None:
        origin = _Origin()
        with _without_cyclic_gc():
            flights = _InflightMap()
            validator = self.validator(origin, flights)

            frames = self.observe_failure(lambda: validator.validate(ADDRESS, label="test"))

            self.assertEqual(validator.inflight, {})
            self.assertEqual(validator.cache, {})
            self.assertEqual(origin.calls, 1)
            self.assertEqual(frames[-2:], ["validate", "__call__"])
            self.assert_released(flights.flights, "validation flight")
            self.assert_released(origin.payloads, "failed RPC frame local")

    def test_leader_validation_failure_releases_flight(self) -> None:
        with _without_cyclic_gc():
            flights = _InflightMap()
            validator = self.validator(lambda *_args: {"isvalid": False}, flights)

            try:
                validator.validate(ADDRESS, label="payout")
            except StratumError as error:
                self.assertEqual(error.code, 20)
                self.assertIn("payout is not a valid qbit address", error.message)
            else:
                self.fail("expected the address to be rejected")

            self.assertEqual(validator.inflight, {})
            self.assert_released(flights.flights, "rejected validation flight")

    def assert_waiters_release_failed_flight(self, *, owner_finishes_first: bool) -> None:
        origin = _Origin(park=True)
        gate = _GatedEvent(waiters=WAITER_COUNT)
        with _without_cyclic_gc():
            flights = _InflightMap(event=gate)
            validator = self.validator(origin, flights)

            def flight_released() -> None:
                with validator.cache_lock:
                    self.assertNotIn(ADDRESS, validator.inflight)

            def waiters_counted() -> None:
                # The existing accounting contract: every coalesced request
                # is counted on the leader's flight while it is in flight.
                with validator.cache_lock:
                    self.assertEqual(validator.inflight[ADDRESS].waiters, WAITER_COUNT)

            outcomes = self.run_concurrently(
                lambda: validator.validate(ADDRESS, label="test"),
                origin=origin,
                gate=gate,
                owner_finishes_first=owner_finishes_first,
                flight_released=flight_released,
                waiters_arrived=waiters_counted,
            )

            self.assertEqual(validator.inflight, {})
            self.assertEqual(validator.cache, {})
            self.assert_shared_failure(outcomes, chained=True)
            outcomes.clear()
            self.assert_released(flights.flights, "shared validation flight")
            self.assert_released(origin.payloads, "failed RPC frame local")

    def test_waiters_release_flight_when_leader_fails_while_they_wait(self) -> None:
        self.assert_waiters_release_failed_flight(owner_finishes_first=False)

    def test_waiters_release_flight_when_leader_failed_before_they_observe(self) -> None:
        self.assert_waiters_release_failed_flight(owner_finishes_first=True)

    def test_successful_flight_is_released_after_the_last_observer(self) -> None:
        origin = _Origin(park=True, success={"isvalid": True, "scriptPubKey": P2MR_SCRIPT})
        gate = _GatedEvent(waiters=WAITER_COUNT)
        with _without_cyclic_gc():
            flights = _InflightMap(event=gate)
            validator = self.validator(origin, flights)
            results: list[tuple[str, str]] = []
            results_lock = threading.Lock()

            def request() -> None:
                result = validator.validate(ADDRESS, label="test")
                with results_lock:
                    results.append(result)

            def flight_released() -> None:
                with validator.cache_lock:
                    self.assertNotIn(ADDRESS, validator.inflight)

            outcomes = self.run_concurrently(
                request,
                origin=origin,
                gate=gate,
                owner_finishes_first=True,
                flight_released=flight_released,
            )

            self.assertEqual([error for _name, error in outcomes], [None] * (1 + WAITER_COUNT))
            self.assertEqual(results, [(P2MR_SCRIPT, P2MR_SCRIPT[4:])] * (1 + WAITER_COUNT))
            self.assertIn(ADDRESS, validator.cache)
            self.assert_released(flights.flights, "successful validation flight")


if __name__ == "__main__":
    unittest.main()
