#!/usr/bin/env python3
"""Deterministic concurrency harness for PRISM's stateful owners.

Why this exists
---------------
The concurrency defects this line keeps shipping (tip-refresh livelock,
publication starvation, lease heartbeat self-fencing, the found-block landing
livelock, the orphaned lease-tuple lock in #123) share a shape: two owners
interleave around a PostgreSQL row, and the bad schedule is rare enough that
running the coordinator and hoping never produces it. Reviews caught them;
tests could not express them.

This module makes the schedule an input rather than an accident. Four pieces:

``DeterministicScheduler``
    Real threads, one baton. Exactly one actor runs at a time, and it only
    stops at named checkpoints the harness places at PostgreSQL statement
    boundaries. The controlling test decides who runs next, so an
    interleaving is *reproducible and order-stable*, not merely likely. The
    scheduler records a trace of every checkpoint, which is what determinism
    is asserted against.

``VirtualClock``
    All time the lease lifecycle measures about itself — adoption silence
    from advisory-guard acquisition, caller deadlines, TTL expiry, retry
    sleeps — reads this clock. Nothing sleeps in wall-clock time. Tests
    advance it explicitly, or call ``advance_to_next_deadline`` to jump to
    the earliest pending timeout.

``FakePostgres``
    An in-memory model of the tables PRISM's owners contend on, with the
    PostgreSQL semantics those owners actually depend on: exclusive tuple
    locks held for the lifetime of a transaction, ``FOR NO KEY UPDATE SKIP
    LOCKED`` declining to queue, per-statement READ COMMITTED snapshots,
    ``lock_timeout``/``statement_timeout``, session-scoped advisory locks,
    and ``pg_stat_activity`` locker attribution against a snapshot-visible
    ``xmax``. Explicit transactions are modelled as BEGIN / statement /
    COMMIT with a checkpoint in between, because that gap is the whole of
    #123.

``HarnessLock`` / ``HarnessRLock`` / ``HarnessCondition``
    Process-local synchronisation the scheduler can see. A contended acquire
    parks at a named checkpoint instead of wedging the baton, and a timed
    wait measures the virtual clock. The lease scenarios never needed these:
    they interleave two *processes* around one row, so no two actors ever
    contended a lock inside one coordinator. The landing topology does —
    the payout-balance serializer, the publication order guard and the
    per-hash disposition lease are exactly what its two tails share.

Substitution goes through ports, not monkeypatching
---------------------------------------------------
``PsqlShareLedger`` takes ``sql_backend_factory``, ``lease_guard_factory``,
``monotonic`` and ``lease_retry_sleep``. The fakes here implement
``LedgerSqlPort`` and ``LeaseGuardPort``. No bound method is reassigned and
no ``time`` module is patched, so the code under test is the shipped code:
the real retry loop, the real CAS SQL, the real adoption arithmetic, the real
fail-closed branches.

The synchronisation primitives are the one deliberate exception, and they are
a different kind of substitution: a harness lock preserves the semantics of
the lock it replaces exactly and changes only how a waiter blocks, so the
decision logic under test is untouched. Landing scenarios install them on the
coordinator's lock attributes at construction, the same way the coordinator
itself installs a ``threading.RLock`` there.

What is modelled and what is not
--------------------------------
Modelled: the writer-lease statements ``PsqlShareLedger`` emits — startup
acquisition, same-identity adoption, renewal, release, and the guard-session
verification probe — plus the landing statements it emits against
``qbit_pool_blocks`` and ``qbit_block_candidate_outbox``: prepared-row
persistence, the ``qbit_confirm_pool_block`` publication-ordinal allocator
and its read-back, the durable ordinal floor, pool-block state, prepared
rejection and the outbox terminal update.

Not modelled: everything else. ``FakePostgres`` classifies each statement and
raises ``UnsupportedStatement`` on anything it does not recognise, including
the share-ledger statements, which belong to the follow-up state machines
named in issue #128. That is deliberate: a fake that silently answers an
unrecognised statement drifts away from production without anyone noticing,
so this one fails loudly instead. The classifier is anchored on fragments of
the production SQL, so a change to that SQL breaks classification rather than
quietly changing meaning.

Where a modelled statement is a large CTE — ``persist_accepted_block`` writes
pool-block, bundle, payout-entry and carry-forward rows in one statement —
the model reproduces the effects the landing decisions read back, not the
whole statement. The classifier still pins the statement's identity on its
production fragments, so the day persist grows an effect a landing consults,
that fragment set is where the model is extended.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sys
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import Self

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.share_ledger import (  # noqa: E402
    WRITER_LEASE_HEARTBEAT_SESSION_PREFIX,
    LedgerOperationTimeout,
    PsqlShareLedger,
    parse_single_json_value,
)
from lab.prism.writer_lease_timing import (  # noqa: E402
    WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS,
)

# Real seconds the controller will wait for an actor to hand the baton back
# before declaring the harness itself broken. It is a failure detector, not
# an ordering input: no schedule depends on it, so it cannot change what a
# run does. An actor that blocks on a resource the harness does not model (a
# real threading.Lock, a real socket) trips it instead of hanging the suite.
# It is not free of consequence — an actor that genuinely took this long to
# reach its next checkpoint would fail a run that ought to pass — but the
# margin against microseconds of work is enormous. The env override exists
# for heavily oversubscribed CI hosts where 20s of scheduler stall is a real
# false red; it widens the failure detector only and never affects ordering.
BATON_TIMEOUT_SECONDS = float(
    os.environ.get("PRISM_HARNESS_BATON_TIMEOUT_SECONDS", "20.0")
)

# Virtual epoch origin for clock_timestamp(). Fixed so timestamp text is
# byte-identical across runs, which matters: the adoption CAS compares
# updated_at as text.
CLOCK_ORIGIN = datetime(2026, 6, 26, 19, 49, 22, 233718, tzinfo=timezone.utc)

# The guard session's server-side statement_timeout, taken from the shipped
# timing policy rather than repeated, so a change to the bound the heartbeat
# envelope is sized against also changes what this model enforces.
GUARD_STATEMENT_TIMEOUT_SECONDS = WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS


class HarnessError(AssertionError):
    """The harness could not model what the code under test asked for."""


class UnsupportedStatement(HarnessError):
    """FakePostgres was handed SQL it does not model."""


class SchedulerStall(HarnessError):
    """An actor did not reach a checkpoint within the baton timeout."""


class NotRunnable(HarnessError):
    """The controller tried to step an actor that cannot make progress."""


class _ActorAbort(BaseException):
    """Unwinds a parked actor thread at harness teardown."""


# --------------------------------------------------------------------------
# Virtual clock
# --------------------------------------------------------------------------


class VirtualClock:
    """Monotonic and wall clock that only moves when a test says so."""

    def __init__(self, *, monotonic_origin: float = 10_000.0) -> None:
        self._monotonic = float(monotonic_origin)
        self._origin_monotonic = float(monotonic_origin)

    def monotonic(self) -> float:
        return self._monotonic

    def now(self) -> datetime:
        """``clock_timestamp()``: wall time, advancing with the monotonic clock."""
        return CLOCK_ORIGIN + timedelta(
            seconds=self._monotonic - self._origin_monotonic
        )

    def advance(self, seconds: float) -> float:
        seconds = float(seconds)
        if seconds < 0:
            raise HarnessError("virtual time cannot move backwards")
        self._monotonic += seconds
        return self._monotonic

    def advance_to(self, monotonic_target: float) -> float:
        if monotonic_target < self._monotonic:
            raise HarnessError("virtual time cannot move backwards")
        self._monotonic = float(monotonic_target)
        return self._monotonic

    @staticmethod
    def timestamp_text(moment: datetime) -> str:
        """Render a timestamptz the way psql/psycopg hand it back as text."""
        return f"{moment:%Y-%m-%d %H:%M:%S}.{moment.microsecond:06d}+00"


# --------------------------------------------------------------------------
# Scheduler
# --------------------------------------------------------------------------


@dataclass
class Call:
    """One unit of work handed to an actor."""

    label: str
    fn: Callable[[], Any]
    done: bool = False
    result: Any = None
    error: BaseException | None = None

    def value(self) -> Any:
        """Return the result, re-raising whatever the actor raised."""
        if not self.done:
            raise HarnessError(f"call {self.label!r} has not finished")
        if self.error is not None:
            raise self.error
        return self.result


class Actor:
    """One logical thread of the system under test.

    An actor owns a work queue. It parks at the ``idle`` checkpoint between
    items, so a scenario reads as a sequence of ``call``/``start`` steps
    rather than as thread plumbing.
    """

    def __init__(self, scheduler: DeterministicScheduler, name: str) -> None:
        self.scheduler = scheduler
        self.name = name
        self.queue: list[Call] = []
        self.calls: list[Call] = []
        self.stop: str | None = None
        self.finished = False
        self.parked_forever = False
        self._ready: Callable[[], bool] | None = None
        self._wake_at: float | None = None
        self._blocked = False
        self._thread = threading.Thread(
            target=self._run,
            name=f"harness-{name}",
            daemon=True,
        )
        self._started = False

    # -- controller side ---------------------------------------------------

    def submit(self, fn: Callable[[], Any], *, label: str | None = None) -> Call:
        call = Call(label=label or getattr(fn, "__name__", "call"), fn=fn)
        self.queue.append(call)
        self.calls.append(call)
        return call

    @property
    def blocked(self) -> bool:
        return self._blocked

    @property
    def block_reason(self) -> str | None:
        return self.stop if self._blocked else None

    @property
    def wake_at(self) -> float | None:
        return self._wake_at if self._blocked else None

    def runnable(self) -> bool:
        if self.finished or self.parked_forever:
            return False
        if not self._blocked:
            return True
        if self._ready is not None and self._ready():
            return True
        return (
            self._wake_at is not None
            and self.scheduler.clock.monotonic() >= self._wake_at
        )

    # -- actor side --------------------------------------------------------

    def _run(self) -> None:
        try:
            self.scheduler._await_baton(self)
            while True:
                while not self.queue:
                    # Blocked rather than checkpointed: an actor with nothing
                    # to do is not runnable, so it neither consumes a drain
                    # round nor masks the fact that the system has quiesced.
                    self.scheduler.block("idle", ready=lambda: bool(self.queue))
                call = self.queue.pop(0)
                try:
                    call.result = call.fn()
                except BaseException as exc:  # recorded, not swallowed
                    if isinstance(exc, _ActorAbort):
                        raise
                    call.error = exc
                finally:
                    call.done = True
                self.scheduler.checkpoint(f"done:{call.label}")
        except _ActorAbort:
            pass
        finally:
            self.finished = True
            self.stop = "finished"
            self.scheduler._release_baton(self)


class DeterministicScheduler:
    """Baton-passing scheduler: one runnable actor, chosen by the test."""

    def __init__(
        self,
        clock: VirtualClock,
        *,
        baton_timeout_seconds: float = BATON_TIMEOUT_SECONDS,
    ) -> None:
        self.clock = clock
        self.baton_timeout_seconds = float(baton_timeout_seconds)
        self.actors: dict[str, Actor] = {}
        self.trace: list[str] = []
        self._condition = threading.Condition()
        self._current: Actor | None = None
        self._local = threading.local()
        self._closed = False

    # -- construction ------------------------------------------------------

    def actor(self, name: str) -> Actor:
        if name in self.actors:
            raise HarnessError(f"actor {name!r} already exists")
        actor = Actor(self, name)
        self.actors[name] = actor
        return actor

    # -- actor-side primitives --------------------------------------------

    def current(self) -> Actor:
        actor = getattr(self._local, "actor", None)
        if actor is None:
            raise HarnessError(
                "this call must run on an actor thread; drive it through "
                "Actor.submit/LeaseHarness.run rather than from the test body"
            )
        return actor

    def checkpoint(self, label: str) -> None:
        """Announce a stop and hand the baton back to the controller."""
        actor = self.current()
        actor.stop = label
        actor._blocked = False
        actor._ready = None
        actor._wake_at = None
        self.trace.append(f"{actor.name}@{label}")
        self._yield(actor)

    def block(
        self,
        label: str,
        *,
        ready: Callable[[], bool] | None = None,
        wake_at: float | None = None,
    ) -> None:
        """Park at ``label`` until ``ready()`` holds or the clock reaches ``wake_at``.

        Returns once the controller schedules this actor again; the caller
        re-checks its own condition, so a spurious resume is harmless.
        """
        actor = self.current()
        actor.stop = label
        actor._blocked = True
        actor._ready = ready
        actor._wake_at = wake_at
        self.trace.append(f"{actor.name}@{label}")
        self._yield(actor)
        actor._blocked = False
        actor._ready = None
        actor._wake_at = None

    def park_forever(self, label: str) -> None:
        """Abandon this actor at ``label``; it never runs again.

        Models a client that vanished: the process is gone, but nothing it
        held server-side has been released.
        """
        actor = self.current()
        actor.stop = label
        actor.parked_forever = True
        actor._blocked = True
        actor._ready = lambda: False
        actor._wake_at = None
        self.trace.append(f"{actor.name}@{label}")
        self._yield(actor)
        raise HarnessError("a parked actor must never be resumed")

    # -- baton mechanics ---------------------------------------------------

    def _yield(self, actor: Actor) -> None:
        with self._condition:
            self._current = None
            self._condition.notify_all()
            while self._current is not actor:
                if self._closed:
                    raise _ActorAbort()
                self._condition.wait(0.05)

    def _await_baton(self, actor: Actor) -> None:
        self._local.actor = actor
        with self._condition:
            while self._current is not actor:
                if self._closed:
                    raise _ActorAbort()
                self._condition.wait(0.05)

    def _release_baton(self, actor: Actor) -> None:
        with self._condition:
            if self._current is actor:
                self._current = None
            self._condition.notify_all()

    # -- controller-side stepping -----------------------------------------

    def step(self, actor: Actor) -> str:
        """Run ``actor`` until its next checkpoint. Returns the stop label."""
        if actor.finished:
            raise NotRunnable(f"actor {actor.name!r} has finished")
        if not actor.runnable():
            raise NotRunnable(
                f"actor {actor.name!r} is blocked at {actor.stop!r} and cannot "
                "make progress; advance the clock or let another actor commit"
            )
        if not actor._started:
            actor._started = True
            actor._thread.start()
        with self._condition:
            self._current = actor
            self._condition.notify_all()
            waited = self._condition.wait_for(
                lambda: self._current is not actor,
                timeout=self.baton_timeout_seconds,
            )
        if not waited:
            raise SchedulerStall(
                f"actor {actor.name!r} did not reach a checkpoint within "
                f"{self.baton_timeout_seconds:g}s (last stop {actor.stop!r}). It is "
                "probably blocked on a resource the harness does not model."
            )
        return actor.stop or "finished"

    def run_until(self, actor: Actor, label: str, *, limit: int = 200) -> str:
        """Step ``actor`` until it stops at ``label``."""
        for _ in range(limit):
            stop = self.step(actor)
            if stop == label:
                return stop
        raise HarnessError(
            f"actor {actor.name!r} did not reach {label!r} within {limit} steps; "
            f"last stop {actor.stop!r}"
        )

    def runnable_actors(self) -> list[Actor]:
        return [actor for actor in self.actors.values() if actor.runnable()]

    def next_deadline(self) -> float | None:
        """Earliest virtual monotonic time at which a blocked actor wakes."""
        deadlines = [
            actor.wake_at
            for actor in self.actors.values()
            if actor.blocked
            and not actor.parked_forever
            and actor.wake_at is not None
        ]
        return min(deadlines) if deadlines else None

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._current = None
            self._condition.notify_all()
        for actor in self.actors.values():
            if actor._started:
                actor._thread.join(timeout=2.0)


# --------------------------------------------------------------------------
# Scheduler-aware synchronisation
# --------------------------------------------------------------------------


class HarnessLock:
    """A mutex the scheduler can see, drop-in for ``threading.Lock``.

    The code under test acquires and releases exactly as it would a real
    lock; the only difference is what happens when the lock is already held.
    A real lock would block the actor thread outside the baton, so the
    controller would time out and report a stall on a wait that is in fact
    legitimate. This one parks the waiter at ``lock:<name>`` and lets the
    controller schedule the holder, which is what makes a contended
    process-local lock an *interleaving point* rather than a harness limit.

    Ownership is per actor, not per OS thread: actors are threads here, but
    naming the actor is what lets the trace read as a schedule.

    Grants are FIFO. PostgreSQL's tuple-lock queue is modelled that way for
    the same reason (see ``_await_lease_lock``): without a queue the model
    could express a grant order that neither a real futex-backed lock nor
    the scheduler would produce, and the schedule is an input here.
    """

    reentrant = False

    def __init__(self, scheduler: DeterministicScheduler, name: str) -> None:
        self.scheduler = scheduler
        self.name = name
        self._owner: Actor | None = None
        self._depth = 0
        self._waiters: list[Actor] = []

    # -- observation -------------------------------------------------------

    @property
    def owner(self) -> Actor | None:
        return self._owner

    def locked(self) -> bool:
        return self._owner is not None

    def held_by_current(self) -> bool:
        return self._owner is self.scheduler.current()

    # -- acquisition -------------------------------------------------------

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        actor = self.scheduler.current()
        if self._owner is actor:
            if not self.reentrant:
                raise HarnessError(
                    f"actor {actor.name!r} re-acquired non-reentrant lock "
                    f"{self.name!r}; production would deadlock here"
                )
            self._depth += 1
            return True
        deadline = (
            None
            if timeout is None or timeout < 0
            else self.scheduler.clock.monotonic() + max(0.0, float(timeout))
        )

        def granted() -> bool:
            if self._owner is not None:
                return False
            return not self._waiters or self._waiters[0] is actor

        if granted():
            self._take(actor)
            return True
        if not blocking:
            return False
        self._waiters.append(actor)
        try:
            while not granted():
                if (
                    deadline is not None
                    and self.scheduler.clock.monotonic() >= deadline
                ):
                    return False
                self.scheduler.block(
                    f"lock:{self.name}",
                    ready=granted,
                    wake_at=deadline,
                )
        finally:
            self._waiters.remove(actor)
        self._take(actor)
        return True

    def _take(self, actor: Actor) -> None:
        self._owner = actor
        self._depth = 1

    def release(self) -> None:
        actor = self.scheduler.current()
        if self._owner is None:
            raise HarnessError(f"lock {self.name!r} released while unheld")
        if self._owner is not actor:
            raise HarnessError(
                f"lock {self.name!r} released by {actor.name!r} but held by "
                f"{self._owner.name!r}"
            )
        self._depth -= 1
        if self._depth == 0:
            self._owner = None

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc_info: object) -> None:
        self.release()


class HarnessRLock(HarnessLock):
    """``threading.RLock`` for actors: the owner may re-enter."""

    reentrant = True


class HarnessSemaphore:
    """``threading.BoundedSemaphore`` for actors.

    The ledger's read slots are a semaphore, and ``_operation_gate`` treats
    it exactly like the writer lock — same ``acquire(timeout=)`` contract,
    same ``LedgerOperationTimeout`` on expiry. Modelling it means a scenario
    can exhaust read concurrency on purpose rather than discovering that the
    harness quietly never blocks there.
    """

    def __init__(
        self,
        scheduler: DeterministicScheduler,
        name: str,
        *,
        value: int = 1,
    ) -> None:
        self.scheduler = scheduler
        self.name = name
        self.initial = int(value)
        self._value = int(value)
        self._waiters: list[Actor] = []

    @property
    def value(self) -> int:
        return self._value

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        actor = self.scheduler.current()
        deadline = (
            None
            if timeout is None or timeout < 0
            else self.scheduler.clock.monotonic() + max(0.0, float(timeout))
        )

        def granted() -> bool:
            if self._value <= 0:
                return False
            return not self._waiters or self._waiters[0] is actor

        if granted():
            self._value -= 1
            return True
        if not blocking:
            return False
        self._waiters.append(actor)
        try:
            while not granted():
                if (
                    deadline is not None
                    and self.scheduler.clock.monotonic() >= deadline
                ):
                    return False
                self.scheduler.block(
                    f"semaphore:{self.name}",
                    ready=granted,
                    wake_at=deadline,
                )
        finally:
            self._waiters.remove(actor)
        self._value -= 1
        return True

    def release(self) -> None:
        if self._value >= self.initial:
            raise HarnessError(f"semaphore {self.name!r} released too many times")
        self._value += 1

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc_info: object) -> None:
        self.release()


class HarnessCondition:
    """``threading.Condition`` over a harness lock and the virtual clock.

    ``wait`` releases the lock, parks, and re-acquires before returning, so a
    predicate loop written against a real condition behaves identically. A
    timed wait measures the virtual clock, which is what lets a scenario put
    a heartbeat-slice wait (``_await_unfenced_appends_predating_anchor``, the
    ledger admission slices) under a watchdog without anything sleeping.
    """

    def __init__(
        self,
        lock: HarnessLock | None = None,
        *,
        scheduler: DeterministicScheduler | None = None,
        name: str = "condition",
    ) -> None:
        if lock is None:
            if scheduler is None:
                raise HarnessError("a condition needs a lock or a scheduler")
            lock = HarnessRLock(scheduler, name)
        self._lock = lock
        self.scheduler = lock.scheduler
        self.name = name
        # Monotonic notify counter. A waiter records the value it parked on,
        # so a notify that arrives while the controller has the baton is
        # never lost the way a missed edge would be.
        self._generation = 0

    def __enter__(self) -> bool:
        return self._lock.acquire()

    def __exit__(self, *exc_info: object) -> None:
        self._lock.release()

    def acquire(self, *args: Any, **kwargs: Any) -> bool:
        return self._lock.acquire(*args, **kwargs)

    def release(self) -> None:
        self._lock.release()

    def wait(self, timeout: float | None = None) -> bool:
        if not self._lock.held_by_current():
            raise HarnessError(
                f"cannot wait on condition {self.name!r} without holding its lock"
            )
        parked_generation = self._generation
        depth = self._lock._depth
        self._lock._depth = 1
        self._lock.release()
        clock = self.scheduler.clock
        wake_at = (
            None if timeout is None else clock.monotonic() + max(0.0, float(timeout))
        )
        notified = False
        while True:
            if self._generation != parked_generation:
                notified = True
                break
            if wake_at is not None and clock.monotonic() >= wake_at:
                break
            self.scheduler.block(
                f"cond:{self.name}",
                ready=lambda: self._generation != parked_generation,
                wake_at=wake_at,
            )
        self._lock.acquire()
        self._lock._depth = depth
        return notified

    def wait_for(
        self,
        predicate: Callable[[], bool],
        timeout: float | None = None,
    ) -> bool:
        clock = self.scheduler.clock
        deadline = (
            None if timeout is None else clock.monotonic() + max(0.0, float(timeout))
        )
        while not predicate():
            if deadline is None:
                self.wait()
                continue
            remaining = deadline - clock.monotonic()
            if remaining <= 0:
                break
            self.wait(remaining)
        return predicate()

    def notify(self, n: int = 1) -> None:
        self.notify_all()

    def notify_all(self) -> None:
        self._generation += 1


class HarnessEvent:
    """``threading.Event`` for actors, with a clock-backed ``wait``."""

    def __init__(self, scheduler: DeterministicScheduler, name: str = "event") -> None:
        self.scheduler = scheduler
        self.name = name
        self._set = False

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True

    def clear(self) -> None:
        self._set = False

    def wait(self, timeout: float | None = None) -> bool:
        clock = self.scheduler.clock
        wake_at = (
            None if timeout is None else clock.monotonic() + max(0.0, float(timeout))
        )
        while not self._set:
            if wake_at is not None and clock.monotonic() >= wake_at:
                break
            self.scheduler.block(
                f"event:{self.name}",
                ready=lambda: self._set,
                wake_at=wake_at,
            )
        return self._set


# --------------------------------------------------------------------------
# Statement classification
# --------------------------------------------------------------------------


class LeaseOp(str, Enum):
    ACQUIRE = "acquire"
    ADOPT = "adopt"
    RENEW = "renew"
    RELEASE = "release"
    VERIFY = "verify"
    # The cheap read-only ownership proof the heartbeat runs on most beats
    # (issue #212). Deliberately a distinct trace step from VERIFY: a
    # scenario asserting the split has to be able to see which question the
    # coordinator actually asked PostgreSQL on each beat.
    PROVE = "prove"


class LandingOp(str, Enum):
    """Statements the block-candidate landing tails emit.

    Names follow the ``PsqlShareLedger`` method that emits them rather than
    the SQL verb, because a scenario reads the trace as landing steps.
    """

    PERSIST_BLOCK = "persist_block"
    CONFIRM_BLOCK = "confirm_block"
    CONFIRMED_SEQUENCE = "confirmed_sequence"
    PUBLICATION_FLOOR = "publication_floor"
    POOL_BLOCK_STATE = "pool_block_state"
    REJECT_PREPARED = "reject_prepared"
    OUTBOX_RECORD = "outbox_record"
    OUTBOX_ATTEMPT = "outbox_attempt"
    OUTBOX_FINISH = "outbox_finish"
    OUTBOX_PENDING_PAGE = "outbox_pending_page"
    OUTBOX_BATCH_ABANDON = "outbox_batch_abandon"
    ALL_SHARES = "all_shares"
    PRIOR_BALANCES = "prior_balances"
    PRIOR_BALANCES_AS_OF = "prior_balances_as_of"
    SHARE_STATS = "share_stats"


StatementKind = LeaseOp | LandingOp


@dataclass(frozen=True)
class Statement:
    kind: StatementKind
    payload: dict[str, Any]
    lease_ttl_seconds: float | None
    authority_margin_seconds: float | None
    advisory_lock: tuple[int, int] | None
    sql: str

    @property
    def is_landing(self) -> bool:
        return isinstance(self.kind, LandingOp)


_PAYLOAD_RE = re.compile(
    r"\$(qbit_prism_json(?:_x)*)\$(.*?)\$\1\$::jsonb",
    re.DOTALL,
)
_INTERVAL_RE = re.compile(r"make_interval\(secs => ([0-9eE\.\+\-]+)\)")
_CLASSID_RE = re.compile(r"classid = (\d+)::oid")
_OBJID_RE = re.compile(r"objid = (\d+)::oid")

# Each entry is (op, required fragments). Fragments are lifted verbatim from
# the production SQL in lab/prism/share_ledger.py, so a change there breaks
# classification loudly instead of silently changing what the fake models.
_SIGNATURES: tuple[tuple[LeaseOp, tuple[str, ...]], ...] = (
    (
        LeaseOp.PROVE,
        (
            "'guard_advisory_lock_held'",
            "'writer_session_token_current'",
            "'lease_renewal_due'",
        ),
    ),
    (
        LeaseOp.VERIFY,
        (
            "FOR NO KEY UPDATE SKIP LOCKED",
            "'guard_advisory_lock_held'",
            "'lease_renewed_count'",
            "'lease_locked_by_this_process'",
        ),
    ),
    (
        LeaseOp.ACQUIRE,
        (
            "INSERT INTO qbit_ledger_writer_lease",
            "ON CONFLICT (singleton) DO UPDATE",
            "qbit_ledger_writer_lease.lease_expires_at <= clock_timestamp()",
        ),
    ),
    (
        LeaseOp.ADOPT,
        (
            "UPDATE qbit_ledger_writer_lease",
            "data->>'observed_writer_session_token'",
            "data->>'observed_lease_updated_at'",
        ),
    ),
    (
        LeaseOp.RELEASE,
        (
            "UPDATE qbit_ledger_writer_lease",
            "lease_expires_at = clock_timestamp() - interval '1 second'",
            "json_build_object('released'",
        ),
    ),
    (
        LeaseOp.RENEW,
        (
            "UPDATE qbit_ledger_writer_lease",
            "'renewed_count', (SELECT count(*) FROM lease)",
        ),
    ),
)

# The landing statements, same discipline: every fragment is lifted verbatim
# from lab/prism/share_ledger.py. Ordered most specific first, because two
# pool-block reads differ only by which columns they project.
_LANDING_SIGNATURES: tuple[tuple[LandingOp, tuple[str, ...]], ...] = (
    (
        LandingOp.PERSIST_BLOCK,
        (
            "INSERT INTO qbit_pool_blocks (",
            "existing_block AS (",
            "INSERT INTO qbit_pool_audit_bundles (",
        ),
    ),
    (
        LandingOp.CONFIRM_BLOCK,
        ("'confirmed_count', qbit_confirm_pool_block(",),
    ),
    (
        LandingOp.REJECT_PREPARED,
        ("'rejected_count', qbit_reject_prepared_pool_block(",),
    ),
    (
        LandingOp.POOL_BLOCK_STATE,
        (
            "'state', (",
            "'maturity_state', maturity_state,",
            "FROM qbit_pool_blocks",
        ),
    ),
    (
        LandingOp.CONFIRMED_SEQUENCE,
        (
            "'audit_publication_sequence', (",
            "FROM qbit_pool_blocks",
            "AND chain_state = 'confirmed'",
        ),
    ),
    (
        LandingOp.PUBLICATION_FLOOR,
        (
            "'audit_publication_sequence_floor',",
            "COALESCE(MAX(audit_publication_sequence), 0)",
        ),
    ),
    (
        LandingOp.OUTBOX_PENDING_PAGE,
        (
            # The read-only replay enumeration (issue #211). Its fragments
            # name the three facts one page must answer from one snapshot:
            # the row, whether the candidate already landed, and the keyset
            # cursor the next page resumes from.
            "'pool_block_exists', EXISTS (",
            "FROM qbit_pool_blocks pool",
            "FROM qbit_block_candidate_outbox",
            "WHERE state = 'pending'",
        ),
    ),
    (
        LandingOp.OUTBOX_RECORD,
        (
            "INSERT INTO qbit_block_candidate_outbox (",
            "'block candidate payload mismatch'",
            # Both fragments above also occur in the share-submission append,
            # which opens the same outbox insert and raises the same mismatch
            # error, so on their own they name a share append as the outbox
            # state machine. Only `persist_block_candidate_intent` inserts the
            # candidate with no share to attach it to and therefore leaves a
            # colliding row alone; the share append claims that row with
            # `ON CONFLICT (block_hash) DO UPDATE`. That difference in conflict
            # action is the discriminator, and it is load-bearing rather than
            # incidental: it is the SQL-level statement of which caller owns
            # the row.
            "ON CONFLICT (block_hash) DO NOTHING",
        ),
    ),
    (
        LandingOp.OUTBOX_BATCH_ABANDON,
        (
            # The page-oriented terminal write (issue #196), listed before
            # OUTBOX_FINISH because it satisfies that signature too: the
            # discriminators are the set predicate and the pool-block
            # re-check, which is the clause that makes acting on a stale
            # enumeration safe.
            "UPDATE qbit_block_candidate_outbox",
            "SET state = 'abandoned',",
            "WHERE block_hash = ANY(ARRAY[",
            "FROM qbit_pool_blocks pool",
        ),
    ),
    (
        LandingOp.OUTBOX_FINISH,
        (
            "UPDATE qbit_block_candidate_outbox",
            "completed_at = clock_timestamp(),",
            "candidate = NULL",
        ),
    ),
    (
        LandingOp.OUTBOX_ATTEMPT,
        (
            "UPDATE qbit_block_candidate_outbox",
            "SET attempt_count = attempt_count + 1,",
        ),
    ),
    (
        LandingOp.ALL_SHARES,
        (
            "'share_seq', share_seq,",
            "FROM qbit_share_ledger",
            "WHERE accepted;",
        ),
    ),
    (
        LandingOp.SHARE_STATS,
        (
            "'accepted_share_count', count(*),",
            "FROM qbit_share_ledger",
        ),
    ),
    (
        LandingOp.PRIOR_BALANCES_AS_OF,
        (
            "'recipient_id', miner_id,",
            "FROM qbit_payout_carry_forward carry",
            "AND block.block_height <= target.block_height",
        ),
    ),
    (
        LandingOp.PRIOR_BALANCES,
        (
            "'recipient_id', miner_id,",
            "FROM qbit_current_carry_forward_balances();",
        ),
    ),
)

# Statements that belong to the state machines issue #128 still defers.
# Recognised only so the failure names the right follow-up instead of reading
# as an unexplained harness gap.
_DEFERRED_SIGNATURES: tuple[tuple[str, str], ...] = (
    ("INSERT INTO qbit_share_ledger", "share submission and dedupe"),
    ("qbit_ctv_fanouts", "CTV fanout broadcasting"),
)

# Landing statement arguments. The ledger inlines them as SQL literals, so
# the model reads them back out of the statement rather than being told.
_POOL_FUNCTION_RE = re.compile(
    r"qbit_(?:confirm|reject_prepared)_pool_block\(\s*"
    r"'([0-9a-f]+)',\s*(-?\d+),\s*"
    r"'((?:[^']|'')*)',\s*(\d+),\s*'((?:[^']|'')*)'",
    re.DOTALL,
)
# The lease CTE every outbox statement opens with, which inlines the writer
# identity rather than carrying it in a jsonb payload.
_LEASE_IDENTITY_RE = re.compile(
    r"writer_id = '((?:[^']|'')*)'\s*"
    r"AND writer_epoch = (\d+)\s*"
    r"AND writer_session_token = '((?:[^']|'')*)'",
    re.DOTALL,
)
_POOL_BLOCK_HASH_RE = re.compile(r"WHERE block_hash = '([0-9a-f]+)'")
# The pending page's own two arguments: the bounded window and the optional
# keyset cursor. Both are inlined as literals by pending_block_candidate_rows.
_PENDING_PAGE_LIMIT_RE = re.compile(r"LIMIT (\d+)\n\) pending;")
_PENDING_PAGE_CURSOR_RE = re.compile(
    r"AND \(created_at, block_hash\) > "
    r"\('((?:[^']|'')*)'::timestamptz, '((?:[^']|'')*)'\)"
)
_OUTBOX_STATE_RE = re.compile(r"SET state = '(submitted|abandoned)'")
_OUTBOX_BATCH_HASHES_RE = re.compile(
    r"WHERE block_hash = ANY\(ARRAY\[(.*?)\]::text\[\]\)",
    re.DOTALL,
)
_OUTBOX_ERROR_RE = re.compile(r"last_error = (NULL|'(?:[^']|'')*')")
_OUTBOX_SHA_RE = re.compile(r"candidate_sha256 <> '([0-9a-f]+)'")


#: Terminal-arm keys that name a lease column. Nothing is visible to report a
#: holder from -- the arm exists precisely for that case -- so they answer
#: null.
_ACQUIRE_TERMINAL_NULL_KEYS = frozenset(
    {
        "writer_id",
        "writer_epoch",
        "writer_session_token",
        "lease_expires_at",
        "lease_updated_at",
        "lease_age_seconds",
        "lease_wait_seconds",
    }
)

#: Terminal-arm keys whose value is a SQL literal, taken from the statement
#: rather than restated here. ``acquired`` is false on this arm;
#: ``lease_snapshot_retry`` is the flag ``_try_acquire_writer_lease`` retries
#: on; ``lease`` and ``retry_reason`` carry the operator-facing subject and
#: explanation that replace the old "postgres query returned no JSON".
#:
#: Reading the value out of the SQL matters more than it looks. An earlier
#: draft keyed on the *name* -- anything containing "retry" answered true --
#: which got ``lease_snapshot_retry`` right by luck and would have answered
#: ``retry_reason`` with a boolean where production emits a sentence. The
#: model now answers what the statement says, so it cannot drift from the
#: wording the ledger PR chose.
_ACQUIRE_TERMINAL_LITERAL_KEYS = frozenset(
    {
        "acquired",
        "lease_snapshot_retry",
        "lease",
        "retry_reason",
    }
)


def _split_sql_arguments(body: str) -> list[str]:
    """Split a call's argument text on its top-level commas.

    Parenthesised sub-expressions and single-quoted literals are held
    together, because both occur in this statement: the holder arm nests
    ``GREATEST(0, EXTRACT(...))``, and the terminal arm's retry reason is a
    sentence that must survive as one argument.
    """
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    quoted = False
    index = 0
    while index < len(body):
        char = body[index]
        if quoted:
            current.append(char)
            if char == "'":
                if index + 1 < len(body) and body[index + 1] == "'":
                    current.append("'")
                    index += 1
                else:
                    quoted = False
        elif char == "'":
            quoted = True
            current.append(char)
        elif char in "([":
            depth += 1
            current.append(char)
        elif char in ")]":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        index += 1
    remainder = "".join(current).strip()
    if remainder:
        parts.append(remainder)
    return parts


def _balanced_call_body(text: str, open_index: int) -> str:
    """Return the argument text of a call whose ``(`` is at ``open_index``."""
    depth = 0
    for index in range(open_index, len(text)):
        char = text[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[open_index + 1 : index]
    raise UnsupportedStatement("acquire statement has unbalanced parentheses")


def acquire_result_arms(sql: str) -> int:
    """Count the result arms of the acquire statement's final COALESCE.

    The acquire statement ends in ``SELECT COALESCE(<arm>, <arm>, ...)``. At
    this baseline there are exactly two: the ``RETURNING`` row of the upsert
    (acquired) and the committed holder row (not acquired). Both can be empty
    at once -- two coordinators racing the very first acquire -- and the
    statement then evaluates to SQL NULL, which the ports turn into
    production's "postgres query returned no JSON".

    Issue #140 adds an explicit not-acquired/retry arm so that race reports a
    lease outcome instead of what reads as a driver fault. That is a change to
    the shipped statement, so this model keys off the statement rather than
    off a version flag or a local constant: a statement carrying a terminal
    arm cannot evaluate to NULL, and continuing to model it as if it could
    would let a scenario assert an outcome the server can no longer produce --
    the exact class of harness lie this suite exists to prevent.

    Counting arms rather than matching a chosen literal is deliberate: the arm
    has to exist for the fix to work, whatever its author names its keys.
    """
    _head, separator, tail = sql.partition("SELECT COALESCE(")
    if not separator:
        raise UnsupportedStatement(
            "acquire statement does not end in the SELECT COALESCE this model "
            "recognises; its result shape changed and the lease evaluator has "
            "to be re-read against it rather than assumed:\n" + sql.strip()[:400]
        )
    arms = tail.count("'acquired',")
    if arms < 2:
        raise UnsupportedStatement(
            f"acquire statement's COALESCE names {arms} result arm(s); this "
            "model knows the two-arm shape (acquired / holder) and the "
            "three-or-more-arm shape issue #140 adds. Fewer than two is a "
            "shape it has never seen."
        )
    return arms


def acquire_terminal_arm_fields(sql: str) -> tuple[tuple[str, str], ...]:
    """Return ``(key, raw value text)`` for the last ``json_build_object`` arm.

    ``json_build_object`` takes alternating key and value arguments, so the
    pairing is the call's own structure rather than a convention this model
    imposes.
    """
    _head, _separator, tail = sql.partition("SELECT COALESCE(")
    marker = "json_build_object("
    index = tail.rfind(marker)
    if index < 0:
        raise UnsupportedStatement("acquire statement's terminal arm builds no JSON object")
    body = _balanced_call_body(tail, index + len(marker) - 1)
    arguments = _split_sql_arguments(body)
    if len(arguments) % 2:
        raise UnsupportedStatement(
            "the acquire statement's terminal arm names an odd number of "
            f"json_build_object arguments ({len(arguments)}); it cannot be read "
            "as key/value pairs"
        )
    fields: list[tuple[str, str]] = []
    for key_text, value_text in zip(arguments[::2], arguments[1::2], strict=True):
        if len(key_text) < 2 or not key_text.startswith("'") or not key_text.endswith("'"):
            raise UnsupportedStatement(
                f"the acquire statement's terminal arm names {key_text!r} where a "
                "quoted key was expected"
            )
        fields.append((_unquote(key_text[1:-1]), value_text))
    return tuple(fields)


def acquire_terminal_arm_keys(sql: str) -> tuple[str, ...]:
    """Return the keys named by the last ``json_build_object`` arm."""
    return tuple(key for key, _value in acquire_terminal_arm_fields(sql))


def classify(sql: str) -> Statement:
    """Map one production statement onto the operation it performs."""
    for op, fragments in _SIGNATURES:
        if all(fragment in sql for fragment in fragments):
            if op is LeaseOp.PROVE:
                # The proof renews nothing, so its only interval literal is
                # the renewal-due (own-write authority) margin.
                lease_ttl_seconds = None
                authority_margin_seconds = _extract_interval(sql, first=True)
            else:
                lease_ttl_seconds = _extract_interval(sql, first=True)
                authority_margin_seconds = (
                    _extract_interval(sql, first=False)
                    if op is LeaseOp.VERIFY
                    else None
                )
            return Statement(
                kind=op,
                payload=_extract_payload(sql),
                lease_ttl_seconds=lease_ttl_seconds,
                authority_margin_seconds=authority_margin_seconds,
                advisory_lock=_extract_advisory_lock(sql),
                sql=sql,
            )
    for landing_op, fragments in _LANDING_SIGNATURES:
        if all(fragment in sql for fragment in fragments):
            return Statement(
                kind=landing_op,
                payload=_landing_payload(landing_op, sql),
                lease_ttl_seconds=_extract_interval(sql, first=True),
                authority_margin_seconds=None,
                advisory_lock=None,
                sql=sql,
            )
    for marker, state_machine in _DEFERRED_SIGNATURES:
        if marker in sql:
            raise UnsupportedStatement(
                f"{marker} belongs to the {state_machine} state machine, which "
                "issue #128 defers to a follow-up PR; this harness models the "
                "writer-lease lifecycle and block candidate landing"
            )
    raise UnsupportedStatement(
        "FakePostgres does not model this statement. Either the production "
        "SQL changed shape (update _SIGNATURES or _LANDING_SIGNATURES) or the "
        "scenario reached a state machine this harness does not cover:\n"
        f"{sql.strip()[:400]}"
    )


def _unquote(literal: str) -> str:
    """Undo ``PsqlShareLedger._text_literal`` quoting."""
    return literal.replace("''", "'")


def _lease_identity(op: LandingOp, sql: str) -> dict[str, Any]:
    match = _LEASE_IDENTITY_RE.search(sql)
    if match is None:
        raise UnsupportedStatement(
            f"{op.value} statement carried no writer lease identity"
        )
    return {
        "writer_id": _unquote(match.group(1)),
        "writer_epoch": int(match.group(2)),
        "writer_session_token": _unquote(match.group(3)),
    }


def _landing_payload(op: LandingOp, sql: str) -> dict[str, Any]:
    """Recover the arguments a landing statement inlined as SQL literals."""
    if op is LandingOp.PERSIST_BLOCK:
        return _extract_payload(sql)
    if op in {LandingOp.CONFIRM_BLOCK, LandingOp.REJECT_PREPARED}:
        match = _POOL_FUNCTION_RE.search(sql)
        if match is None:
            raise UnsupportedStatement(
                f"{op.value} statement did not name a block hash and height"
            )
        return {
            "block_hash": match.group(1),
            "active_tip_height": int(match.group(2)),
            "writer_id": _unquote(match.group(3)),
            "writer_epoch": int(match.group(4)),
            "writer_session_token": _unquote(match.group(5)),
        }
    if op in {LandingOp.POOL_BLOCK_STATE, LandingOp.CONFIRMED_SEQUENCE}:
        match = _POOL_BLOCK_HASH_RE.search(sql)
        if match is None:
            raise UnsupportedStatement(f"{op.value} statement named no block hash")
        return {"block_hash": match.group(1)}
    if op is LandingOp.OUTBOX_RECORD:
        hash_match = _POOL_BLOCK_HASH_RE.search(sql)
        sha_match = _OUTBOX_SHA_RE.search(sql)
        if hash_match is None or sha_match is None:
            raise UnsupportedStatement(
                "outbox record statement named no block hash or payload digest"
            )
        return {
            "block_hash": hash_match.group(1),
            "candidate_sha256": sha_match.group(1),
            **_lease_identity(op, sql),
        }
    if op is LandingOp.OUTBOX_BATCH_ABANDON:
        hashes_match = _OUTBOX_BATCH_HASHES_RE.search(sql)
        error_match = _OUTBOX_ERROR_RE.search(sql)
        if hashes_match is None:
            raise UnsupportedStatement(
                "outbox batch abandon statement named no block hash set"
            )
        raw_error = None if error_match is None else error_match.group(1)
        return {
            "block_hashes": tuple(
                _unquote(element.strip()[1:-1])
                for element in _split_sql_arguments(hashes_match.group(1))
            ),
            "last_error": (
                None
                if raw_error in (None, "NULL")
                else str(raw_error)[1:-1].replace("''", "'")
            ),
            **_lease_identity(op, sql),
        }
    if op is LandingOp.OUTBOX_PENDING_PAGE:
        limit_match = _PENDING_PAGE_LIMIT_RE.search(sql)
        if limit_match is None:
            raise UnsupportedStatement(
                "pending candidate page statement carried no bounded LIMIT"
            )
        cursor_match = _PENDING_PAGE_CURSOR_RE.search(sql)
        return {
            "limit": int(limit_match.group(1)),
            "after_cursor": (
                None
                if cursor_match is None
                else (
                    _unquote(cursor_match.group(1)),
                    _unquote(cursor_match.group(2)),
                )
            ),
        }
    if op in {LandingOp.OUTBOX_ATTEMPT, LandingOp.OUTBOX_FINISH}:
        hash_match = _POOL_BLOCK_HASH_RE.search(sql)
        if hash_match is None:
            raise UnsupportedStatement(f"{op.value} statement named no block hash")
        payload: dict[str, Any] = {
            "block_hash": hash_match.group(1),
            **_lease_identity(op, sql),
        }
        if op is LandingOp.OUTBOX_FINISH:
            state_match = _OUTBOX_STATE_RE.search(sql)
            if state_match is None:
                raise UnsupportedStatement(
                    "outbox finish statement named no terminal state"
                )
            payload["state"] = state_match.group(1)
            error_match = _OUTBOX_ERROR_RE.search(sql)
            raw_error = None if error_match is None else error_match.group(1)
            payload["last_error"] = (
                None
                if raw_error in (None, "NULL")
                else str(raw_error)[1:-1].replace("''", "'")
            )
        return payload
    return {}


def _extract_payload(sql: str) -> dict[str, Any]:
    match = _PAYLOAD_RE.search(sql)
    if match is None:
        raise UnsupportedStatement("statement carried no jsonb payload")
    payload = json.loads(match.group(2))
    if not isinstance(payload, dict):
        raise UnsupportedStatement("statement payload was not an object")
    return payload


def _extract_interval(sql: str, *, first: bool) -> float | None:
    matches = _INTERVAL_RE.findall(sql)
    if not matches:
        return None
    if first:
        return float(matches[0])
    return float(matches[1]) if len(matches) > 1 else None


def _extract_advisory_lock(sql: str) -> tuple[int, int] | None:
    classid = _CLASSID_RE.search(sql)
    objid = _OBJID_RE.search(sql)
    if classid is None or objid is None:
        return None
    return (int(classid.group(1)), int(objid.group(1)))


def advisory_lock_pair(key: int) -> tuple[int, int]:
    """Split a 64-bit advisory key the way pg_locks reports it."""
    return ((key >> 32) & 0xFFFFFFFF, key & 0xFFFFFFFF)


# --------------------------------------------------------------------------
# The PostgreSQL model
# --------------------------------------------------------------------------


@dataclass
class LeaseRow:
    writer_id: str
    writer_epoch: int
    writer_session_token: str
    lease_expires_at: datetime
    updated_at: datetime
    # The locking transaction's xid, exactly as PostgreSQL stamps xmax on a
    # row-locked tuple. Visible to a snapshot taken while the lock is held.
    xmax: str | None = None


@dataclass
class PoolBlockRow:
    """One ``qbit_pool_blocks`` row.

    ``audit_publication_sequence`` is the durable publication ordinal the
    audit store gates on. It is assigned once, by the confirmation that first
    moves the row to ``confirmed``, and survives every later disposition —
    which is why an exact replay must not burn a second ordinal and why the
    floor is a MAX over this column rather than a counter.
    """

    block_hash: str
    block_height: int
    parent_hash: str
    chain_state: str = "prepared"
    maturity_state: str = "immature"
    audit_publication_sequence: int | None = None


@dataclass
class OutboxRow:
    """One ``qbit_block_candidate_outbox`` row: the durable replay source."""

    block_hash: str
    candidate_sha256: str
    # ``created_at DEFAULT clock_timestamp()``. The pending page orders and
    # keysets on ``(created_at, block_hash)``, so the model has to carry the
    # stamp the real column would: an ordering that came from insertion order
    # would answer the cursor questions the enumeration actually asks with a
    # property production does not have.
    created_at: datetime = CLOCK_ORIGIN
    state: str = "pending"
    attempt_count: int = 0
    last_error: str | None = None


@dataclass
class Transaction:
    xid: str
    backend: Backend
    explicit: bool
    holds_lease_lock: bool = False
    staged_lease: LeaseRow | None = None
    # Landing writes are staged for the same reason lease writes are: a
    # deadline-scoped statement sends COMMIT as a separate message, so there
    # is a window in which this transaction can see its own pool-block row
    # and nobody else can. #133 lives inside exactly such a window, so a
    # model that published the write at statement time could hide it.
    staged_pool_blocks: dict[str, PoolBlockRow] = field(default_factory=dict)
    staged_outbox: dict[str, OutboxRow] = field(default_factory=dict)
    orphaned: bool = False


@dataclass
class Backend:
    """One server-side session."""

    pid: str
    application_name: str
    server: FakePostgres
    transaction: Transaction | None = None
    closed: bool = False
    advisory_locks: set[tuple[int, int]] = field(default_factory=set)
    # Guard sessions carry a server-side statement_timeout; pool sessions
    # take their deadline from the caller instead.
    statement_timeout_seconds: float | None = None

    @property
    def backend_xid(self) -> str | None:
        if self.closed or self.transaction is None:
            return None
        return self.transaction.xid


class LockTimeout(RuntimeError):
    """SQLSTATE 55P03 / 57014 as the ledger's callers observe them."""


class FakePostgres:
    """In-memory model of the writer-lease table and its locking behaviour."""

    def __init__(self, scheduler: DeterministicScheduler, clock: VirtualClock) -> None:
        self.scheduler = scheduler
        self.clock = clock
        self.lease: LeaseRow | None = None
        self.statements: list[Statement] = []
        # Optional per-statement server time, in virtual seconds. Scenarios
        # that model database latency (the rapid-block tail behind issue
        # #212) install a callable here; the default None keeps every
        # existing scenario's statements instantaneous.
        self.statement_latency_seconds: (
            Callable[[Statement], float] | None
        ) = None
        # Landing tables. Insertion-ordered dicts, because the trace and every
        # assertion made against them has to be order-stable across runs.
        self.pool_blocks: dict[str, PoolBlockRow] = {}
        self.outbox: dict[str, OutboxRow] = {}
        self.shares: list[dict[str, Any]] = []
        self.carry_forward_balances: list[dict[str, Any]] = []
        self._lease_lock_holder: Transaction | None = None
        self._lease_lock_waiters: list[Transaction] = []
        self._backends: list[Backend] = []
        self._advisory_owners: dict[tuple[int, int], Backend] = {}
        self._next_pid = 1
        self._next_xid = 1
        # qbit_audit_publication_sequence_seq. A sequence, not MAX + 1: the
        # allocator burns a value per fresh confirmation and never reuses
        # one, which is why the floor read below is a separate MAX.
        self._audit_publication_sequence = 0

    # -- sessions ----------------------------------------------------------

    def connect(
        self,
        *,
        application_name: str,
        statement_timeout_seconds: float | None = None,
    ) -> Backend:
        backend = Backend(
            pid=f"pid-{self._next_pid}",
            application_name=application_name,
            server=self,
            statement_timeout_seconds=statement_timeout_seconds,
        )
        self._next_pid += 1
        self._backends.append(backend)
        return backend

    def disconnect(self, backend: Backend) -> None:
        """Close a session cleanly: PostgreSQL rolls back and releases everything."""
        if backend.closed:
            return
        backend.closed = True
        if backend.transaction is not None:
            self._rollback(backend.transaction)
        for key in list(backend.advisory_locks):
            if self._advisory_owners.get(key) is backend:
                del self._advisory_owners[key]
        backend.advisory_locks.clear()

    def try_advisory_lock(self, backend: Backend, key: int) -> bool:
        pair = advisory_lock_pair(key)
        owner = self._advisory_owners.get(pair)
        if owner is not None and not owner.closed:
            return owner is backend
        self._advisory_owners[pair] = backend
        backend.advisory_locks.add(pair)
        return True

    def live_backends(self) -> list[Backend]:
        return [backend for backend in self._backends if not backend.closed]

    # -- transactions ------------------------------------------------------

    def _begin(self, backend: Backend, *, explicit: bool) -> Transaction:
        if backend.transaction is not None:
            raise HarnessError(
                f"backend {backend.pid} already has an open transaction"
            )
        transaction = Transaction(
            xid=f"xid-{self._next_xid}",
            backend=backend,
            explicit=explicit,
        )
        self._next_xid += 1
        backend.transaction = transaction
        return transaction

    def _commit(self, transaction: Transaction) -> None:
        if transaction.staged_lease is not None:
            self.lease = replace(transaction.staged_lease, xmax=None)
            transaction.staged_lease = None
        self.pool_blocks.update(transaction.staged_pool_blocks)
        self.outbox.update(transaction.staged_outbox)
        transaction.staged_pool_blocks.clear()
        transaction.staged_outbox.clear()
        self._release_lease_lock(transaction)
        transaction.backend.transaction = None

    def _rollback(self, transaction: Transaction) -> None:
        transaction.staged_lease = None
        # The publication ordinal is deliberately not rolled back: nextval is
        # non-transactional, so an aborted confirmation burns its value and
        # leaves a gap. The floor read is a MAX precisely so a gap is
        # harmless.
        transaction.staged_pool_blocks.clear()
        transaction.staged_outbox.clear()
        self._release_lease_lock(transaction)
        transaction.backend.transaction = None

    def _release_lease_lock(self, transaction: Transaction) -> None:
        if self._lease_lock_holder is transaction:
            self._lease_lock_holder = None
            if self.lease is not None and self.lease.xmax == transaction.xid:
                self.lease = replace(self.lease, xmax=None)

    def orphan(self, transaction: Transaction) -> None:
        """The client vanished without RST: the transaction stays open.

        PostgreSQL finished executing the statement and is waiting for a
        COMMIT that will never arrive, so it keeps the tuple lock.

        This model does not implement a reaper. Production now sets
        ``idle_in_transaction_session_timeout``
        (``PRISM_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS``, 15s by
        default) on every coordinator session, so a real orphan of *this*
        deployment's making is aborted and its locks released. An orphan here
        is therefore the worst case rather than the only case: the foreign
        session the guard cannot reach, or the window before it fires.
        Scenarios that want the reap must model it explicitly; nothing in the
        harness ends an orphan on its own.
        """
        transaction.orphaned = True

    @property
    def lease_lock_holder(self) -> Transaction | None:
        return self._lease_lock_holder

    def lease_lock_held_by_other(self, transaction: Transaction | None) -> bool:
        holder = self._lease_lock_holder
        return holder is not None and holder is not transaction

    # -- statement execution ----------------------------------------------

    def execute(
        self,
        backend: Backend,
        sql: str,
        *,
        transaction: Transaction | None,
        timeout_seconds: float | None,
        tag: str,
    ) -> Any:
        """Run one statement, stopping at every interleaving point it offers."""
        if backend.closed:
            raise RuntimeError("connection is closed")
        statement = classify(sql)
        self.statements.append(statement)
        autocommit = transaction is None
        if autocommit:
            transaction = self._begin(backend, explicit=False)

        self.scheduler.checkpoint(f"{tag}.begin:{statement.kind.value}")

        # A caller deadline arrives as SET LOCAL lock_timeout; the guard
        # session's bound is a connection-level statement_timeout. PostgreSQL
        # reports them with different messages and SQLSTATEs, and the whole
        # point of the SKIP LOCKED heartbeat was that the guard's bound is the
        # fatal one, so the model must not blur them.
        effective_timeout = timeout_seconds
        timeout_kind = "lock"
        session_bound = backend.statement_timeout_seconds
        if session_bound is not None and (
            effective_timeout is None or session_bound <= effective_timeout
        ):
            effective_timeout = session_bound
            timeout_kind = "statement"
        try:
            self._charge_execution_latency(
                statement,
                effective_timeout,
                timeout_kind,
            )
            result = self._evaluate(
                statement,
                transaction,
                effective_timeout,
                tag,
                timeout_kind,
            )
        except BaseException:
            if autocommit:
                self._rollback(transaction)
            raise

        self.scheduler.checkpoint(f"{tag}.done:{statement.kind.value}")
        if autocommit:
            self._commit(transaction)
        return result

    def _charge_execution_latency(
        self,
        statement: Statement,
        timeout_seconds: float | None,
        timeout_kind: str,
    ) -> None:
        """Spend modelled server time for this statement, honouring its bound.

        Lock waits were already modelled (``_await_lease_lock``); this is the
        other half — a statement that is simply *slow*, which is what the
        rapid-block bursts behind issue #212 produce and what the heartbeat's
        staleness envelope has to absorb. Advancing the virtual clock inside
        the statement is exact under the baton scheduler: no other actor is
        running, so the time is charged to this statement alone and every
        other actor's deadline sees it on its next turn.

        A statement whose modelled cost exceeds the session bound is
        cancelled at the bound, exactly as PostgreSQL's statement_timeout
        cancels it — the guard session's 0.5s ceiling is the term the
        heartbeat policy is sized against, so a scenario must not be able to
        model a guarded round trip that quietly outlives it.
        """
        latency_for = self.statement_latency_seconds
        if latency_for is None:
            return
        latency = float(latency_for(statement) or 0.0)
        if latency <= 0.0:
            return
        if timeout_seconds is not None and latency > timeout_seconds:
            self.clock.advance(max(0.0, timeout_seconds))
            raise LockTimeout(
                f"canceling statement due to {timeout_kind} timeout"
            )
        self.clock.advance(latency)

    def _evaluate(
        self,
        statement: Statement,
        transaction: Transaction,
        timeout_seconds: float | None,
        tag: str,
        timeout_kind: str = "lock",
    ) -> Any:
        if statement.is_landing:
            return self._evaluate_landing(
                statement,
                transaction,
                timeout_seconds,
                tag,
                timeout_kind,
            )
        if statement.kind is LeaseOp.VERIFY:
            return self._evaluate_verify(statement, transaction, tag)
        if statement.kind is LeaseOp.PROVE:
            return self._evaluate_prove(statement, transaction)

        # READ COMMITTED takes one snapshot per command, *before* any lock
        # wait. This is the distinction that makes a queued statement report
        # stale data: after the wait, only the conflicting tuple is re-read
        # (EvalPlanQual) to re-evaluate the write predicate, while every other
        # scan in the statement — including the arm that builds the whole
        # not-acquired result — still reads `snapshot`. A model that re-read
        # committed state everywhere would report a fresh row and a small
        # lease_age_seconds where the real server reports the pre-wait row and
        # an age that has silently absorbed the entire wait.
        snapshot = self._visible_lease(transaction)

        if statement.kind is LeaseOp.ACQUIRE:
            # INSERT ... ON CONFLICT DO UPDATE locks the conflicting tuple
            # before evaluating its WHERE, so it queues even when the
            # predicate would exclude the row.
            self._await_lease_lock(
                statement,
                transaction,
                timeout_seconds,
                tag,
                timeout_kind,
            )
            return self._evaluate_acquire(statement, transaction, snapshot)

        # A plain UPDATE evaluates its WHERE against the snapshot version
        # first and never locks a row the predicate already excludes: it
        # returns zero rows immediately rather than queueing. Getting this
        # wrong matters to any fencing argument, where "the predecessor is
        # blocked" and "the predecessor already knows it lost" are different
        # states.
        evaluators = {
            LeaseOp.ADOPT: (self._adopt_matches, self._evaluate_adopt),
            LeaseOp.RENEW: (self._renew_matches, self._evaluate_renew),
            LeaseOp.RELEASE: (self._release_matches, self._evaluate_release),
        }
        try:
            matches, evaluate = evaluators[statement.kind]
        except KeyError:
            raise UnsupportedStatement(
                f"unhandled lease operation {statement.kind}"
            ) from None
        if snapshot is None or not matches(statement, snapshot):
            return evaluate(statement, transaction, None)
        self._await_lease_lock(
            statement,
            transaction,
            timeout_seconds,
            tag,
            timeout_kind,
        )
        # EvalPlanQual: the qual is re-checked against the version this
        # statement just locked, which a concurrent commit may have moved.
        current = self._visible_lease(transaction)
        if current is None or not matches(statement, current):
            return evaluate(statement, transaction, None)
        return evaluate(statement, transaction, current)

    def _await_lease_lock(
        self,
        statement: Statement,
        transaction: Transaction,
        timeout_seconds: float | None,
        tag: str,
        timeout_kind: str = "lock",
    ) -> None:
        # Waiting is not conditional on a *committed* row existing. Two
        # concurrent INSERTs conflict on the singleton unique index, and the
        # second one queues behind the first exactly as an ON CONFLICT
        # DO UPDATE queues behind an uncommitted update.
        deadline = (
            None
            if timeout_seconds is None
            else self.clock.monotonic() + max(0.0, timeout_seconds)
        )

        def granted() -> bool:
            # PostgreSQL serializes tuple-lock waiters through a heavyweight
            # lock and grants in arrival order, so a later arrival cannot
            # overtake a queued waiter. Without the queue the model could
            # express a grant order the server never produces — which matters
            # precisely because the schedule is an input here.
            if self.lease_lock_held_by_other(transaction):
                return False
            return (
                not self._lease_lock_waiters
                or self._lease_lock_waiters[0] is transaction
            )

        if granted():
            self._take_lease_lock(transaction)
            return
        self._lease_lock_waiters.append(transaction)
        try:
            while not granted():
                if deadline is not None and self.clock.monotonic() >= deadline:
                    raise LockTimeout(
                        f"canceling statement due to {timeout_kind} timeout\n"
                        "CONTEXT:  while updating tuple (0,1) in relation "
                        '"qbit_ledger_writer_lease"'
                    )
                self.scheduler.block(
                    f"{tag}.lockwait:{statement.kind.value}",
                    ready=granted,
                    wake_at=deadline,
                )
        finally:
            self._lease_lock_waiters.remove(transaction)
        self._take_lease_lock(transaction)

    def _take_lease_lock(self, transaction: Transaction) -> None:
        if self.lease_lock_held_by_other(transaction):
            raise HarnessError(
                "the lease tuple lock was taken while another transaction "
                "held it; the model must queue, never steal"
            )
        self._lease_lock_holder = transaction
        transaction.holds_lease_lock = True
        if self.lease is not None:
            self.lease = replace(self.lease, xmax=transaction.xid)

    def _visible_lease(self, transaction: Transaction) -> LeaseRow | None:
        """READ COMMITTED: the statement sees committed state plus its own writes."""
        if transaction.staged_lease is not None:
            return transaction.staged_lease
        return self.lease

    def _stage(self, transaction: Transaction, row: LeaseRow) -> None:
        transaction.staged_lease = row
        if not transaction.explicit:
            return
        # An explicit transaction's write is invisible to everyone else until
        # COMMIT, but the tuple lock is already taken; reflect the locker in
        # the committed row's xmax so pg_stat_activity attribution works.
        if self.lease is not None:
            self.lease = replace(self.lease, xmax=transaction.xid)

    # -- individual operations --------------------------------------------

    def _evaluate_acquire(
        self,
        statement: Statement,
        transaction: Transaction,
        snapshot: LeaseRow | None,
    ) -> dict[str, Any] | None:
        """INSERT ... ON CONFLICT DO UPDATE, with EPQ on the locked tuple.

        Returns None when the statement evaluates to SQL NULL, which the
        ports turn into production's "postgres query returned no JSON" the
        same way `parse_single_json_value` does. That is a reachable
        production outcome, not a harness edge case: when two coordinators
        race the very first acquire, the loser's DO UPDATE affects zero rows
        and its COALESCE fallback reads a snapshot in which no row exists
        yet, so the whole statement returns NULL.
        """
        payload = statement.payload
        now = self.clock.now()
        ttl = self._ttl(statement)
        acquired = {
            "acquired": True,
            "writer_id": payload["writer_id"],
            "writer_epoch": int(payload["writer_epoch"]),
            "writer_session_token": payload["writer_session_token"],
        }

        def claim() -> None:
            self._stage(
                transaction,
                LeaseRow(
                    writer_id=str(payload["writer_id"]),
                    writer_epoch=int(payload["writer_epoch"]),
                    writer_session_token=str(payload["writer_session_token"]),
                    lease_expires_at=now + timedelta(seconds=ttl),
                    updated_at=now,
                ),
            )

        # The conflict is resolved against the tuple this statement locked,
        # not against its snapshot.
        current = self._visible_lease(transaction)
        if current is None:
            claim()
            return acquired
        if self._is_exact_session(current, payload) or (
            current.lease_expires_at <= now
        ):
            claim()
            return acquired
        if snapshot is None:
            return self._unobserved_not_acquired(statement)
        return self._observed(snapshot, acquired=False)

    def _unobserved_not_acquired(self, statement: Statement) -> dict[str, Any] | None:
        """Answer a race whose loser can see neither the upsert nor a holder.

        Which answer is correct is a property of the shipped statement, not of
        this harness, so it is read off the statement every time. The two-arm
        shape evaluates to SQL NULL here; the shape issue #140 adds carries a
        terminal arm and therefore cannot.
        """
        if acquire_result_arms(statement.sql) == 2:
            return None
        payload: dict[str, Any] = {}
        for key, value_text in acquire_terminal_arm_fields(statement.sql):
            payload[key] = self._terminal_arm_value(key, value_text)
        if "acquired" not in payload:
            raise UnsupportedStatement(
                "the acquire statement's terminal arm does not name 'acquired'; "
                "the lease evaluator cannot report an outcome from it"
            )
        return payload

    @staticmethod
    def _terminal_arm_value(key: str, value_text: str) -> Any:
        """Answer one terminal-arm key from what the statement says it emits.

        The key set stays an allowlist -- an unknown key is refused even when
        its value is a perfectly readable literal, because a key this model
        has never seen is a shape nobody has reasoned about. What is *not*
        guessed is the value: for the literal keys it is parsed out of the
        SQL, so the model reports the wording the ledger PR chose rather than
        a restatement of it that can drift.
        """
        text = value_text.strip()
        if key in _ACQUIRE_TERMINAL_LITERAL_KEYS:
            lowered = text.lower()
            if lowered == "true":
                return True
            if lowered == "false":
                return False
            if lowered == "null":
                return None
            if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
                return _unquote(text[1:-1])
            raise UnsupportedStatement(
                f"the acquire statement's terminal arm gives {key!r} the "
                f"non-literal value {text!r}; this model can only report a "
                "value the statement states outright"
            )
        if key in _ACQUIRE_TERMINAL_NULL_KEYS:
            # A lease column with no visible row behind it.
            return None
        raise UnsupportedStatement(
            f"the acquire statement's terminal arm names {key!r}, which this "
            "model does not know how to answer. Teach it that key rather than "
            "letting a scenario assert against a guess."
        )

    @staticmethod
    def _adopt_matches(statement: Statement, row: LeaseRow) -> bool:
        payload = statement.payload
        return (
            row.writer_id == payload["writer_id"]
            and row.writer_epoch == int(payload["writer_epoch"])
            and row.writer_session_token
            == payload["observed_writer_session_token"]
            and VirtualClock.timestamp_text(row.updated_at)
            == payload["observed_lease_updated_at"]
        )

    @staticmethod
    def _renew_matches(statement: Statement, row: LeaseRow) -> bool:
        return FakePostgres._is_exact_session(row, statement.payload)

    _release_matches = _renew_matches

    def _evaluate_adopt(
        self,
        statement: Statement,
        transaction: Transaction,
        matched: LeaseRow | None,
    ) -> dict[str, Any] | None:
        payload = statement.payload
        now = self.clock.now()
        ttl = self._ttl(statement)
        # `lease_expires_at > clock_timestamp()` is evaluated with the rest of
        # the predicate but depends on live time rather than on the row
        # version, so it is checked here rather than in _adopt_matches.
        if matched is not None and matched.lease_expires_at > now:
            self._stage(
                transaction,
                replace(
                    matched,
                    writer_session_token=str(payload["writer_session_token"]),
                    lease_expires_at=now + timedelta(seconds=ttl),
                    updated_at=now,
                    xmax=None,
                ),
            )
            return {
                "acquired": True,
                "adopted": True,
                "writer_id": payload["writer_id"],
                "writer_epoch": int(payload["writer_epoch"]),
                "writer_session_token": payload["writer_session_token"],
            }
        # The CAS affected no rows, so the COALESCE fallback reports whatever
        # this statement's snapshot holds.
        snapshot = self._visible_lease(transaction)
        if snapshot is None:
            return None
        observed = self._observed(snapshot, acquired=False)
        observed["adopted"] = False
        return observed

    def _evaluate_renew(
        self,
        statement: Statement,
        transaction: Transaction,
        matched: LeaseRow | None,
    ) -> dict[str, Any]:
        now = self.clock.now()
        ttl = self._ttl(statement)
        if matched is None:
            return {"error": "writer lease is not active"}
        self._stage(
            transaction,
            replace(
                matched,
                lease_expires_at=now + timedelta(seconds=ttl),
                updated_at=now,
                xmax=None,
            ),
        )
        return {"backend": "postgres-psql", "renewed_count": 1}

    def _evaluate_release(
        self,
        statement: Statement,
        transaction: Transaction,
        matched: LeaseRow | None,
    ) -> dict[str, Any]:
        now = self.clock.now()
        if matched is None:
            return {"released": 0}
        self._stage(
            transaction,
            replace(
                matched,
                lease_expires_at=now - timedelta(seconds=1),
                updated_at=now,
                xmax=None,
            ),
        )
        return {"released": 1}

    def _evaluate_prove(
        self,
        statement: Statement,
        transaction: Transaction,
    ) -> dict[str, Any]:
        """The read-only ownership proof: no row lock, no locker attribution.

        Everything it reads comes from one MVCC snapshot plus this backend's
        own advisory-lock state, so unlike VERIFY there is no
        snapshot-versus-live-state window and no interleaving point inside
        the statement. That is the whole point of the split: the frequent
        statement cannot queue, cannot misattribute, and cannot need a
        second round trip.
        """
        payload = statement.payload
        now = self.clock.now()
        margin = statement.authority_margin_seconds or 0.0
        snapshot = self._visible_lease(transaction)
        exact = snapshot is not None and self._is_exact_session(snapshot, payload)
        guard_backend = transaction.backend
        advisory_held = (
            statement.advisory_lock is not None
            and statement.advisory_lock in guard_backend.advisory_locks
            and self._advisory_owners.get(statement.advisory_lock) is guard_backend
            and not guard_backend.closed
        )
        return {
            "backend": "postgres-psql",
            "guard_advisory_lock_held": bool(advisory_held),
            "writer_session_token_current": bool(exact),
            "lease_expired": bool(
                exact and snapshot.lease_expires_at <= now
            ),
            "lease_renewal_due": bool(
                exact
                and snapshot.lease_expires_at <= now + timedelta(seconds=margin)
            ),
        }

    def _evaluate_verify(
        self,
        statement: Statement,
        transaction: Transaction,
        tag: str,
    ) -> dict[str, Any]:
        payload = statement.payload
        now = self.clock.now()
        ttl = self._ttl(statement)
        margin = statement.authority_margin_seconds or 0.0
        # One MVCC snapshot for every lease-row read in this statement.
        snapshot = self._visible_lease(transaction)
        exact = snapshot is not None and self._is_exact_session(snapshot, payload)

        # `renewable ... FOR NO KEY UPDATE SKIP LOCKED` never queues.
        renewed = 0
        if exact and not self.lease_lock_held_by_other(transaction):
            assert snapshot is not None
            self._take_lease_lock(transaction)
            self._stage(
                transaction,
                replace(
                    snapshot,
                    lease_expires_at=now + timedelta(seconds=ttl),
                    updated_at=now,
                    xmax=None,
                ),
            )
            renewed = 1

        # The documented misfire window: the statement's row reads come from
        # `snapshot`, while pg_stat_activity is read live. A checkpoint here
        # lets a scenario commit an own fenced write between the two.
        self.scheduler.checkpoint(f"{tag}.snapshot:verify")

        guard_backend = transaction.backend
        advisory_held = (
            statement.advisory_lock is not None
            and statement.advisory_lock in guard_backend.advisory_locks
            and self._advisory_owners.get(statement.advisory_lock) is guard_backend
            and not guard_backend.closed
        )
        locked_by_this_process = any(
            backend.application_name == payload.get("pool_application_name")
            and backend.backend_xid is not None
            and snapshot is not None
            and backend.backend_xid == snapshot.xmax
            for backend in self.live_backends()
        )
        return {
            "backend": "postgres-psql",
            "guard_advisory_lock_held": bool(advisory_held),
            "writer_session_token_current": bool(exact),
            "lease_renewed_count": renewed,
            "lease_expired": bool(exact and snapshot.lease_expires_at <= now),
            "lease_expiring_within_authority_margin": bool(
                exact
                and snapshot.lease_expires_at <= now + timedelta(seconds=margin)
            ),
            "lease_locked_by_this_process": bool(locked_by_this_process),
        }

    # -- block candidate landing -------------------------------------------

    def _visible_pool_block(
        self,
        transaction: Transaction,
        block_hash: str,
    ) -> PoolBlockRow | None:
        """READ COMMITTED: committed rows plus this transaction's own writes."""
        staged = transaction.staged_pool_blocks.get(block_hash)
        if staged is not None:
            return staged
        return self.pool_blocks.get(block_hash)

    def _visible_pool_blocks(
        self,
        transaction: Transaction,
    ) -> dict[str, PoolBlockRow]:
        merged = dict(self.pool_blocks)
        merged.update(transaction.staged_pool_blocks)
        return merged

    def _stage_pool_block(
        self,
        transaction: Transaction,
        row: PoolBlockRow,
    ) -> PoolBlockRow:
        staged = replace(row)
        transaction.staged_pool_blocks[staged.block_hash] = staged
        return staged

    def _visible_outbox_row(
        self,
        transaction: Transaction,
        block_hash: str,
    ) -> OutboxRow | None:
        staged = transaction.staged_outbox.get(block_hash)
        if staged is not None:
            return staged
        return self.outbox.get(block_hash)

    def _renew_lease_for_landing(
        self,
        statement: Statement,
        transaction: Transaction,
        timeout_seconds: float | None,
        tag: str,
        timeout_kind: str,
    ) -> bool:
        """The ``lease AS (UPDATE ...)`` CTE every landing statement opens with.

        Every landing statement fences itself on the writer lease and renews
        it in the same round trip. Modelling that is not decoration: a
        deposed writer's landing statement fails here, which is the only
        thing standing between a fenced-out process and a durable pool-block
        write. The identity comes out of the statement itself — inlined as
        literals by the ledger — so the model never has to be told which
        writer a session belongs to.

        The renewal is an ordinary UPDATE on the lease tuple, so it obeys the
        same rules as every other one: a predicate the snapshot already
        excludes returns zero rows without queueing, and a matching predicate
        takes the tuple lock and waits behind whoever holds it. A landing
        stalled behind an orphaned lease transaction is a real outage shape,
        and the model has to be able to express it.
        """
        payload = statement.payload
        writer_id = payload.get("writer_id")
        writer_epoch = payload.get("writer_epoch")
        session_token = payload.get("writer_session_token")
        if writer_id is None or session_token is None:
            # A statement whose identity the classifier could not recover is
            # a model gap, not a lease failure; fail loudly rather than
            # inventing a fence outcome.
            raise UnsupportedStatement(
                f"{statement.kind.value} statement carried no writer identity"
            )
        identity = {
            "writer_id": writer_id,
            "writer_epoch": writer_epoch,
            "writer_session_token": session_token,
        }

        def matches(row: LeaseRow | None) -> bool:
            return row is not None and self._is_exact_session(row, identity)

        if not matches(self._visible_lease(transaction)):
            return False
        self._await_lease_lock(
            statement,
            transaction,
            timeout_seconds,
            tag,
            timeout_kind,
        )
        current = self._visible_lease(transaction)
        if not matches(current):
            return False
        assert current is not None
        now = self.clock.now()
        ttl = statement.lease_ttl_seconds
        if ttl is not None:
            self._stage(
                transaction,
                replace(
                    current,
                    lease_expires_at=now + timedelta(seconds=ttl),
                    updated_at=now,
                    xmax=None,
                ),
            )
        return True

    def _evaluate_landing(
        self,
        statement: Statement,
        transaction: Transaction,
        timeout_seconds: float | None,
        tag: str,
        timeout_kind: str,
    ) -> Any:
        kind = statement.kind
        assert isinstance(kind, LandingOp)
        payload = statement.payload
        # Reads that open no lease CTE at all.
        if kind is LandingOp.PUBLICATION_FLOOR:
            return {
                "audit_publication_sequence_floor": max(
                    [
                        row.audit_publication_sequence or 0
                        for row in self._visible_pool_blocks(transaction).values()
                    ],
                    default=0,
                )
            }
        if kind is LandingOp.ALL_SHARES:
            return [dict(share) for share in self.shares]
        if kind is LandingOp.SHARE_STATS:
            # The evidence annotations, not an accounting input: the audit
            # envelope carries them and the publication replay path reuses
            # the originally durable values rather than re-reading.
            return {
                "accepted_share_count": len(self.shares),
                "distinct_miner_count": len(
                    {str(share.get("miner_id")) for share in self.shares}
                ),
                "max_share_seq": max(
                    (int(share.get("share_seq", 0)) for share in self.shares),
                    default=0,
                ),
            }
        if kind in {LandingOp.PRIOR_BALANCES, LandingOp.PRIOR_BALANCES_AS_OF}:
            # The durable carry-forward view. A landing reads it to prove the
            # payout base under its coinbase has not moved since the job was
            # issued, so a scenario that wants to fail that check moves this
            # list rather than reaching into the coordinator. The as-of
            # variant restricts the same view to blocks at or below one
            # confirmed height; with no payout entries modelled the two
            # coincide, and a scenario that needs them to differ is asking
            # for the settlement machine, which #128 lists separately.
            return [dict(balance) for balance in self.carry_forward_balances]
        if kind is LandingOp.POOL_BLOCK_STATE:
            row = self._visible_pool_block(transaction, str(payload["block_hash"]))
            return {
                "state": None
                if row is None
                else {
                    "block_hash": row.block_hash,
                    "block_height": row.block_height,
                    "parent_hash": row.parent_hash,
                    "chain_state": row.chain_state,
                    "maturity_state": row.maturity_state,
                    "audit_publication_sequence": row.audit_publication_sequence,
                }
            }
        if kind is LandingOp.OUTBOX_PENDING_PAGE:
            # Deliberately grouped with the reads above, before
            # _renew_lease_for_landing: the production statement opens no
            # lease CTE, so a model that renewed here would report a
            # lease-touching read the ledger does not perform -- and would
            # hide the very property issue #211 turns on, that enumeration
            # neither takes the writer gate nor touches the lease row.
            return self._pending_candidate_page(transaction, payload)
        if kind is LandingOp.CONFIRMED_SEQUENCE:
            row = self._visible_pool_block(transaction, str(payload["block_hash"]))
            confirmed = (
                row is not None
                and row.chain_state == "confirmed"
                and row.maturity_state != "reversed"
            )
            return {
                "audit_publication_sequence": (
                    row.audit_publication_sequence if confirmed else None
                )
            }

        lease_held = self._renew_lease_for_landing(
            statement,
            transaction,
            timeout_seconds,
            tag,
            timeout_kind,
        )
        if kind in {LandingOp.CONFIRM_BLOCK, LandingOp.REJECT_PREPARED}:
            if not lease_held:
                # The PL/pgSQL function RAISEs rather than returning a value,
                # so the caller sees an error, not a zero count.
                raise RuntimeError("writer lease is not active")
            if kind is LandingOp.CONFIRM_BLOCK:
                return {
                    "backend": "postgres-psql",
                    "confirmed_count": self._confirm_pool_block(
                        transaction,
                        str(payload["block_hash"]),
                        int(payload["active_tip_height"]),
                    ),
                }
            return {
                "backend": "postgres-psql",
                "rejected_count": self._reject_prepared_pool_block(
                    transaction,
                    str(payload["block_hash"]),
                ),
            }

        if not lease_held:
            return {"error": "writer lease is not active"}
        if kind is LandingOp.PERSIST_BLOCK:
            return self._persist_pool_block(statement, transaction)
        if kind is LandingOp.OUTBOX_RECORD:
            return self._record_outbox_row(statement, transaction)
        if kind is LandingOp.OUTBOX_BATCH_ABANDON:
            # The pool-block clause lives *inside* this fenced UPDATE for the
            # reason issue #211 depends on: the caller's page fact is advisory
            # by the time it gets here, and a candidate that landed in between
            # must be absent from the returned set rather than abandoned.
            pool_blocks = self._visible_pool_blocks(transaction)
            abandoned: list[str] = []
            for block_hash in payload["block_hashes"]:
                row = self._visible_outbox_row(transaction, str(block_hash))
                if row is None or row.state != "pending":
                    continue
                if row.block_hash in pool_blocks:
                    continue
                staged = replace(row)
                staged.state = "abandoned"
                staged.last_error = payload.get("last_error")
                transaction.staged_outbox[staged.block_hash] = staged
                abandoned.append(staged.block_hash)
            return {"abandoned": sorted(abandoned)}
        if kind in {LandingOp.OUTBOX_ATTEMPT, LandingOp.OUTBOX_FINISH}:
            row = self._visible_outbox_row(transaction, str(payload["block_hash"]))
            if row is None or row.state != "pending":
                return {"updated": 0}
            staged = replace(row)
            if kind is LandingOp.OUTBOX_ATTEMPT:
                staged.attempt_count += 1
            else:
                staged.state = str(payload["state"])
                staged.last_error = payload.get("last_error")
            transaction.staged_outbox[staged.block_hash] = staged
            return {"updated": 1}
        raise UnsupportedStatement(f"unhandled landing operation {kind}")

    def _confirm_pool_block(
        self,
        transaction: Transaction,
        block_hash: str,
        active_tip_height: int,
    ) -> int:
        """``qbit_confirm_pool_block``: allocate the publication ordinal.

        The ordinal comes from a sequence, not from ``MAX + 1``. That
        distinction is the whole of the allocator's contract: an exact replay
        of an already-confirmed row matches zero rows in the UPDATE and so
        burns nothing, while a terminally disposed row reports the superseded
        disposition (-1) instead of a plain miss. The durable floor read is a
        separate MAX, which is what lets a *later* ordinal publish first and
        leave an earlier one behind the floor — the state #133 turns on.
        """
        row = self._visible_pool_block(transaction, block_hash)
        if (
            row is not None
            and row.block_height == active_tip_height
            and row.chain_state == "prepared"
            and row.maturity_state == "immature"
        ):
            self._audit_publication_sequence += 1
            staged = self._stage_pool_block(transaction, row)
            staged.chain_state = "confirmed"
            staged.audit_publication_sequence = self._audit_publication_sequence
            return 1
        if (
            row is not None
            and row.block_height == active_tip_height
            and row.chain_state == "confirmed"
            and row.maturity_state != "reversed"
        ):
            return 1
        if row is not None and (
            row.chain_state in {"inactive", "rejected", "reversed"}
            or row.maturity_state == "reversed"
        ):
            return -1
        return 0

    def _reject_prepared_pool_block(
        self,
        transaction: Transaction,
        block_hash: str,
    ) -> int:
        row = self._visible_pool_block(transaction, block_hash)
        if (
            row is None
            or row.chain_state != "prepared"
            or row.maturity_state != "immature"
        ):
            return 0
        staged = self._stage_pool_block(transaction, row)
        staged.chain_state = "rejected"
        staged.maturity_state = "reversed"
        return 1

    def _persist_pool_block(
        self,
        statement: Statement,
        transaction: Transaction,
    ) -> dict[str, Any]:
        """``persist_accepted_block``, reduced to what a landing reads back.

        The production statement writes the pool block, its audit bundle, the
        payout entries and the carry-forward deltas in one CTE. The landing
        decisions downstream read the prepared row (through
        ``pool_block_state`` and the confirmation) and the returned counts;
        the payout rows are the reorg reconciler's and the settlement
        machine's business, neither of which this harness drives yet. The
        counts below are therefore reported from what is modelled, and the
        row shape stays honest: a re-persist of an existing block reports the
        existing row rather than inserting a second one.
        """
        payload = statement.payload
        block_hash = str(payload["block_hash"])
        existing = self._visible_pool_block(transaction, block_hash)
        if existing is None:
            transaction.staged_pool_blocks[block_hash] = PoolBlockRow(
                block_hash=block_hash,
                block_height=int(payload["block_height"]),
                parent_hash=str(payload["parent_hash"]),
            )
        elif existing.block_height != int(payload["block_height"]):
            raise RuntimeError("pool block height conflicts")
        accounts = payload.get("accounts") or []
        return {
            "backend": "postgres-psql",
            "share_count": len(self.shares),
            "block_count": 1,
            "bundle_count": 1,
            "payout_entry_count": len(accounts),
            "carry_forward_count": 0,
            "onchain_output_count": 0,
        }

    def _pending_candidate_page(
        self,
        transaction: Transaction,
        payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """One snapshot answering row, landed-block fact and cursor together.

        The three facts come from one visible set here for the same reason
        they come from one statement in production: a caller that read the
        rows and their pool-block facts at different instants could see a
        candidate as pending *and* unlanded when it was neither.

        The order and the keyset predicate are ``(created_at, block_hash)``,
        the total order the production index provides. The block-hash
        tiebreak is not decoration -- creation stamps collide under one
        transaction's ``clock_timestamp()``, and a cursor on the stamp alone
        would replay or skip a whole colliding group.
        """
        visible = dict(self.outbox)
        visible.update(transaction.staged_outbox)
        pool_blocks = self._visible_pool_blocks(transaction)
        after = payload.get("after_cursor")
        rows = sorted(
            (
                row
                for row in visible.values()
                if row.state == "pending"
            ),
            key=lambda row: (row.created_at, row.block_hash),
        )
        page: list[dict[str, Any]] = []
        for row in rows:
            cursor_stamp = self._pending_cursor_stamp(row.created_at)
            if after is not None and (cursor_stamp, row.block_hash) <= (
                str(after[0]),
                str(after[1]),
            ):
                continue
            page.append(
                {
                    "block_hash": row.block_hash,
                    "candidate": {"block_hash_hex": row.block_hash},
                    "pool_block_exists": row.block_hash in pool_blocks,
                    "cursor": [cursor_stamp, row.block_hash],
                }
            )
            if len(page) >= int(payload["limit"]):
                break
        return page

    @staticmethod
    def _pending_cursor_stamp(moment: datetime) -> str:
        """``to_char(created_at AT TIME ZONE 'UTC', ...US"Z"')``, exactly.

        Microsecond precision with an explicit UTC marker, because the cursor
        is compared as text and a truncated stamp would re-emit or skip whole
        sub-second groups.
        """
        return (
            f"{moment.astimezone(timezone.utc):%Y-%m-%dT%H:%M:%S}"
            f".{moment.microsecond:06d}Z"
        )

    def _record_outbox_row(
        self,
        statement: Statement,
        transaction: Transaction,
    ) -> dict[str, Any]:
        payload = statement.payload
        block_hash = str(payload["block_hash"])
        digest = str(payload["candidate_sha256"])
        existing = self._visible_outbox_row(transaction, block_hash)
        if existing is None:
            transaction.staged_outbox[block_hash] = OutboxRow(
                block_hash=block_hash,
                candidate_sha256=digest,
                created_at=self.clock.now(),
            )
            return {"inserted": 1, "state": "pending"}
        if existing.candidate_sha256 != digest:
            return {"error": "block candidate payload mismatch"}
        return {"inserted": 0, "state": existing.state}

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _is_exact_session(row: LeaseRow, payload: dict[str, Any]) -> bool:
        return (
            row.writer_id == payload["writer_id"]
            and row.writer_epoch == int(payload["writer_epoch"])
            and row.writer_session_token == payload["writer_session_token"]
        )

    @staticmethod
    def _ttl(statement: Statement) -> float:
        if statement.lease_ttl_seconds is None:
            raise UnsupportedStatement(
                f"{statement.kind.value} statement carried no make_interval TTL"
            )
        return statement.lease_ttl_seconds

    def _observed(self, row: LeaseRow, *, acquired: bool) -> dict[str, Any]:
        now = self.clock.now()
        return {
            "acquired": acquired,
            "writer_id": row.writer_id,
            "writer_epoch": row.writer_epoch,
            "writer_session_token": row.writer_session_token,
            "lease_expires_at": VirtualClock.timestamp_text(row.lease_expires_at),
            "lease_updated_at": VirtualClock.timestamp_text(row.updated_at),
            "lease_age_seconds": max(
                0.0, (now - row.updated_at).total_seconds()
            ),
            "lease_wait_seconds": max(
                0.0, (row.lease_expires_at - now).total_seconds()
            ),
        }


# --------------------------------------------------------------------------
# Ports
# --------------------------------------------------------------------------


class FakeSqlBackend:
    """``LedgerSqlPort`` over ``FakePostgres``.

    Mirrors ``_NativePostgresClient.run_json``'s transaction shape exactly,
    because that shape is what #123 turns on: with no caller deadline the
    statement runs in autocommit and commits with it, while a deadline-scoped
    statement runs BEGIN / SET LOCAL / statement / COMMIT, leaving a window in
    which the server holds the tuple lock and is waiting for a COMMIT the
    client may never send.
    """

    def __init__(
        self,
        server: FakePostgres,
        *,
        pool_size: int,
        application_name: str,
        tag: str,
    ) -> None:
        self.server = server
        self._pool_size = max(1, int(pool_size))
        self.application_name = application_name
        self.tag = tag
        self._free: list[Backend] = [
            server.connect(application_name=application_name)
            for _ in range(self._pool_size)
        ]
        self._in_use: list[Backend] = []
        self.closed = False

    @property
    def pool_size(self) -> int:
        return self._pool_size

    def _acquire(self, timeout_seconds: float | None = None) -> Backend:
        deadline = (
            None
            if timeout_seconds is None
            else self.server.clock.monotonic() + max(0.0, timeout_seconds)
        )
        while not self._free:
            if deadline is not None and self.server.clock.monotonic() >= deadline:
                # _NativePostgresClient.connection raises exactly this rather
                # than waiting past the caller's budget.
                raise LedgerOperationTimeout(
                    "timed out waiting for a postgres pool slot"
                )
            self.server.scheduler.block(
                f"{self.tag}.pool:exhausted",
                ready=lambda: bool(self._free),
                wake_at=deadline,
            )
        backend = self._free.pop(0)
        self._in_use.append(backend)
        return backend

    def _release(self, backend: Backend) -> None:
        if backend in self._in_use:
            self._in_use.remove(backend)
        if not backend.closed and backend.transaction is None:
            self._free.append(backend)

    def run_json(
        self,
        sql: str,
        *,
        retry_safe: bool = False,
        timeout_seconds: float | None = None,
        on_statement_start: Callable[[], None] | None = None,
    ) -> Any:
        if self.closed:
            raise RuntimeError("connection pool is closed")
        backend = self._acquire(timeout_seconds)
        transaction: Transaction | None = None
        try:
            if on_statement_start is not None:
                on_statement_start()
            if timeout_seconds is not None:
                # `with conn.transaction():` — SET LOCAL statement_timeout and
                # lock_timeout, run the statement, then send COMMIT.
                transaction = self.server._begin(backend, explicit=True)
            result = self.server.execute(
                backend,
                sql,
                transaction=transaction,
                timeout_seconds=timeout_seconds,
                tag=self.tag,
            )
            if transaction is not None:
                self.server.scheduler.checkpoint(f"{self.tag}.precommit")
                self.server._commit(transaction)
            # Production's own NULL handling, called rather than re-described:
            # a statement that evaluates to SQL NULL raises here, exactly as
            # it does for a real client.
            return parse_single_json_value(result)
        except LockTimeout as exc:
            if transaction is not None and backend.transaction is transaction:
                self.server._rollback(transaction)
            if timeout_seconds is None:
                raise
            # Both shipped `LedgerSqlPort` implementations translate a
            # cancellation caused by the caller's own deadline into
            # `LedgerOperationTimeout`, and only when that deadline was armed:
            # `_NativePostgresClient.run_json` gates on
            # `timeout_seconds is not None and _is_postgres_deadline_error`,
            # and the psql backend applies the same test to stderr. That
            # translation is the contract callers retry against, so a model of
            # this seam that skipped it would let a caller-side retry loop look
            # dead here while working in production. Nothing exercised it
            # before a deadline reached a pooled lease statement.
            raise LedgerOperationTimeout(
                f"postgres operation exceeded {timeout_seconds:g}s"
            ) from exc
        except BaseException:
            if transaction is not None and backend.transaction is transaction:
                self.server._rollback(transaction)
            raise
        finally:
            self._release(backend)

    def run_script(self, sql: str) -> None:
        raise UnsupportedStatement(
            "schema initialization is not modelled; construct the ledger with "
            "initialize_schema=False"
        )

    def close(self) -> None:
        self.closed = True
        for backend in [*self._free, *self._in_use]:
            if backend.transaction is not None and backend.transaction.orphaned:
                # A vanished client's backend outlives the object that owned
                # it; closing the pool locally does not reach the server.
                continue
            self.server.disconnect(backend)
        self._free.clear()


class FakeLeaseGuard:
    """``LeaseGuardPort`` over ``FakePostgres``.

    The guard session is autocommit, carries a server-side
    ``statement_timeout``, and serializes callers through one query slot.
    That slot is modelled with a scheduler-aware lock so a second caller
    parks at a named checkpoint instead of deadlocking the baton.
    """

    def __init__(
        self,
        server: FakePostgres,
        *,
        advisory_lock_key: int,
        application_name: str,
        tag: str,
    ) -> None:
        self.server = server
        self.advisory_lock_key = advisory_lock_key
        self.tag = tag
        self.backend = server.connect(
            application_name=application_name,
            statement_timeout_seconds=GUARD_STATEMENT_TIMEOUT_SECONDS,
        )
        self._held = False
        self._closed = False
        self._slot_owner: Actor | None = None

    def try_acquire(self) -> bool:
        self._held = self.server.try_advisory_lock(
            self.backend,
            self.advisory_lock_key,
        )
        return self._held

    @property
    def held(self) -> bool:
        return self._held and not self._closed and not self.backend.closed

    def run_json(
        self,
        sql: str,
        *,
        on_query_start: Callable[[], None] | None = None,
        on_statement_end: Callable[[], None] | None = None,
        followup: Callable[[Any], str | None] | None = None,
    ) -> Any:
        actor = self.server.scheduler.current()
        while self._slot_owner is not None and self._slot_owner is not actor:
            self.server.scheduler.block(
                f"{self.tag}.slot:wait",
                ready=lambda: self._slot_owner is None,
            )
        self._slot_owner = actor
        try:
            if on_query_start is not None:
                on_query_start()
            if not self.held:
                raise RuntimeError("postgres writer lease guard is not held")
            result = parse_single_json_value(
                self.server.execute(
                    self.backend,
                    sql,
                    transaction=None,
                    timeout_seconds=None,
                    tag=self.tag,
                )
            )
            if on_statement_end is not None:
                on_statement_end()
            while followup is not None:
                next_sql = followup(result)
                if next_sql is None:
                    break
                result = parse_single_json_value(
                    self.server.execute(
                        self.backend,
                        next_sql,
                        transaction=None,
                        timeout_seconds=None,
                        tag=self.tag,
                    )
                )
                if on_statement_end is not None:
                    on_statement_end()
            return result
        finally:
            self._slot_owner = None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._held = False
        self.server.disconnect(self.backend)


# --------------------------------------------------------------------------
# Harness front end
# --------------------------------------------------------------------------


class Coordinator:
    """One PRISM coordinator process: an actor plus the ledger it builds."""

    def __init__(
        self,
        harness: LeaseHarness,
        name: str,
        *,
        writer_id: str,
        writer_epoch: int,
        session_token: str,
        ledger_kwargs: dict[str, Any],
    ) -> None:
        self.harness = harness
        self.name = name
        self.writer_id = writer_id
        self.writer_epoch = writer_epoch
        self.session_token = session_token
        self.actor = harness.scheduler.actor(name)
        self._ledger: PsqlShareLedger | None = None
        self._ledger_kwargs = ledger_kwargs
        self.sql_backend: FakeSqlBackend | None = None
        self.guard: FakeLeaseGuard | None = None

    # -- construction ------------------------------------------------------

    def _sql_backend_factory(
        self,
        conninfo: str,
        *,
        pool_size: int,
        application_name: str,
    ) -> FakeSqlBackend:
        self.sql_backend = FakeSqlBackend(
            self.harness.server,
            pool_size=pool_size,
            application_name=application_name,
            tag=self.name,
        )
        return self.sql_backend

    def _lease_guard_factory(
        self,
        conninfo: str,
        *,
        advisory_lock_key: int,
    ) -> FakeLeaseGuard:
        self.guard = FakeLeaseGuard(
            self.harness.server,
            advisory_lock_key=advisory_lock_key,
            application_name=f"{self.name}-guard",
            tag=f"{self.name}.guard",
        )
        return self.guard

    def _build(self) -> PsqlShareLedger:
        ledger = PsqlShareLedger(
            psql_command="psql postgres://harness/qbit",
            database_url="postgres://harness/qbit",
            writer_id=self.writer_id,
            writer_epoch=self.writer_epoch,
            writer_session_token=self.session_token,
            initialize_schema=False,
            monotonic=self.harness.clock.monotonic,
            lease_retry_sleep=self.harness.sleep,
            sql_backend_factory=self._sql_backend_factory,
            lease_guard_factory=self._lease_guard_factory,
            **self._ledger_kwargs,
        )
        self._ledger = ledger
        return ledger

    def start(self) -> Call:
        """Queue this coordinator's startup (construction acquires the lease)."""
        return self.actor.submit(self._build, label="startup")

    @property
    def ledger(self) -> PsqlShareLedger:
        if self._ledger is None:
            raise HarnessError(
                f"coordinator {self.name!r} has not finished startup yet"
            )
        return self._ledger

    @property
    def started(self) -> bool:
        return self._ledger is not None

    # -- driving -----------------------------------------------------------

    def submit(self, fn: Callable[[], Any], *, label: str | None = None) -> Call:
        return self.actor.submit(fn, label=label)

    def call(self, fn: Callable[[], Any], *, label: str | None = None) -> Any:
        """Run ``fn`` on this actor to completion and return its result."""
        call = self.actor.submit(fn, label=label)
        self.harness.run_until(self, f"done:{call.label}")
        return call.value()

    def vanish(self) -> None:
        """Abandon this coordinator where it stands, without releasing anything.

        A partition, SIGSTOP or VM pause: the process stops responding and no
        RST reaches the server.

        What survives server-side depends on the transaction shape, and the
        distinction is the whole of #123 rather than a detail. A
        deadline-scoped statement runs inside an explicit transaction whose
        COMMIT is a separate client message, so the server is left holding an
        open transaction and its locks indefinitely. A plain autocommit
        statement has no such message: PostgreSQL finishes executing it and
        commits it on its own, and the vanished client merely never reads the
        answer. Modelling both as orphans would let a scenario construct the
        outage shape on a code path that provably cannot produce it — #123 is
        explicit that the shape did not exist before the deadline plumbing
        arrived.
        """
        actor = self.actor
        if not actor.blocked and actor.stop is None:
            raise HarnessError(f"coordinator {self.name!r} has not run yet")
        for backend in [
            *(self.sql_backend._in_use if self.sql_backend else []),
        ]:
            transaction = backend.transaction
            if transaction is None:
                continue
            if transaction.explicit:
                self.harness.server.orphan(transaction)
            else:
                self.harness.server._commit(transaction)
        actor.parked_forever = True
        actor._blocked = True
        actor._ready = lambda: False
        actor._wake_at = None
        self.harness.scheduler.trace.append(f"{self.name}@vanished")


class HarnessBase:
    """What every state machine's harness owns, whichever one it drives.

    Issue #128 names four owners and this covers the second of them, so the
    boundary matters more than it would for a one-off: the clock, the
    scheduler, the PostgreSQL model, the stepping vocabulary and the output
    capture are the same for landing as for the lease lifecycle, and will be
    the same again for tip refresh and share submission. What differs is only
    what a harness *builds* — coordinators here, a pool with two landing
    tails there — so that is all a subclass supplies.

    ``run_scenario`` and ``assert_deterministic`` are typed against this
    class rather than against any one harness, which is what stops the
    determinism check from being re-derived per state machine.
    """

    def __init__(
        self,
        *,
        capture_output: bool = True,
        baton_timeout_seconds: float = BATON_TIMEOUT_SECONDS,
    ) -> None:
        self.clock = VirtualClock()
        self.scheduler = DeterministicScheduler(
            self.clock,
            baton_timeout_seconds=baton_timeout_seconds,
        )
        self.server = FakePostgres(self.scheduler, self.clock)
        self.sleeps: list[tuple[str, float]] = []
        self._output = io.StringIO()
        self._errors = io.StringIO()
        self._redirects: list[contextlib.AbstractContextManager[Any]] = []
        if capture_output:
            # Kept apart rather than merged. The coordinator reserves stdout
            # for a strict JSON-lines protocol in worker processes and routes
            # audit-path diagnostics to stderr, so which stream a message
            # arrives on is itself an assertion a scenario may want to make.
            self._redirects = [
                contextlib.redirect_stdout(self._output),
                contextlib.redirect_stderr(self._errors),
            ]
            for redirect in self._redirects:
                redirect.__enter__()

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self.scheduler.close()
        for redirect in reversed(self._redirects):
            redirect.__exit__(None, None, None)
        self._redirects = []

    @property
    def output(self) -> str:
        return self._output.getvalue()

    @property
    def errors(self) -> str:
        return self._errors.getvalue()

    @property
    def trace(self) -> list[str]:
        return list(self.scheduler.trace)

    # -- synchronisation ---------------------------------------------------

    def lock(self, name: str) -> HarnessLock:
        return HarnessLock(self.scheduler, name)

    def rlock(self, name: str) -> HarnessRLock:
        return HarnessRLock(self.scheduler, name)

    def condition(self, name: str) -> HarnessCondition:
        return HarnessCondition(self.rlock(name), name=name)

    def event(self, name: str) -> HarnessEvent:
        return HarnessEvent(self.scheduler, name)

    def semaphore(self, name: str, value: int) -> HarnessSemaphore:
        return HarnessSemaphore(self.scheduler, name, value=value)

    # -- clock -------------------------------------------------------------

    def sleep(self, seconds: float) -> None:
        """Park until the virtual clock reaches the wake time."""
        actor = self.scheduler.current()
        self.sleeps.append((actor.name, float(seconds)))
        wake_at = self.clock.monotonic() + float(seconds)
        while self.clock.monotonic() < wake_at:
            self.scheduler.block(f"sleep:{seconds:g}", wake_at=wake_at)

    def advance(self, seconds: float) -> float:
        return self.clock.advance(seconds)

    def advance_to_next_deadline(self) -> float | None:
        """Jump to the earliest pending sleep or timeout.

        Returns the new virtual monotonic time, or None when no actor is
        waiting on time at all — which is itself a finding: an actor blocked
        with no deadline can only be released by another actor, and if none
        can run, nothing bounds the wait.
        """
        deadline = self.scheduler.next_deadline()
        if deadline is None:
            return None
        return self.clock.advance_to(max(deadline, self.clock.monotonic()))

    # -- stepping ----------------------------------------------------------

    def _actor(self, who: Any) -> Actor:
        if isinstance(who, Actor):
            return who
        actor = getattr(who, "actor", None)
        if isinstance(actor, Actor):
            return actor
        raise HarnessError(f"{who!r} is not an actor and does not own one")

    def step(self, who: Any) -> str:
        return self.scheduler.step(self._actor(who))

    def run_until(self, who: Any, label: str, *, limit: int = 200) -> str:
        return self.scheduler.run_until(self._actor(who), label, limit=limit)

    def run_until_blocked(self, who: Any, label: str, *, limit: int = 200) -> str:
        """Step until the actor parks at ``label`` and is genuinely stuck there."""
        actor = self._actor(who)
        stop = self.scheduler.run_until(actor, label, limit=limit)
        if not actor.blocked:
            raise HarnessError(
                f"actor {actor.name!r} reached {label!r} but is not blocked"
            )
        return stop

    def drain(self, order: list[Any] | None = None, *, limit: int = 500) -> None:
        """Step runnable actors in a fixed order until none can progress.

        Order is explicit and never depends on thread timing, so a drained
        run is as reproducible as a hand-written schedule.
        """
        actors = (
            [self._actor(who) for who in order]
            if order is not None
            else list(self.scheduler.actors.values())
        )
        for _ in range(limit):
            progressed = False
            for actor in actors:
                if actor.runnable():
                    self.scheduler.step(actor)
                    progressed = True
            if not progressed:
                return
        raise HarnessError(f"drain did not quiesce within {limit} rounds")

    # -- observation -------------------------------------------------------

    def statement_kinds(self) -> list[str]:
        return [statement.kind.value for statement in self.server.statements]


class LeaseHarness(HarnessBase):
    """Front end: build coordinators, step them, advance time, read the trace."""

    def __init__(
        self,
        *,
        lease_ttl_seconds: float = 60.0,
        capture_output: bool = True,
        baton_timeout_seconds: float = BATON_TIMEOUT_SECONDS,
        **default_ledger_kwargs: Any,
    ) -> None:
        super().__init__(
            capture_output=capture_output,
            baton_timeout_seconds=baton_timeout_seconds,
        )
        self.coordinators: dict[str, Coordinator] = {}
        self._default_ledger_kwargs = {
            "lease_ttl_seconds": lease_ttl_seconds,
            **default_ledger_kwargs,
        }
        self._session_counter = 0

    # -- construction ------------------------------------------------------

    def coordinator(
        self,
        name: str,
        *,
        writer_id: str = "prism-coordinator",
        writer_epoch: int = 1,
        session_token: str | None = None,
        heartbeat_capable: bool = True,
        **ledger_kwargs: Any,
    ) -> Coordinator:
        self._session_counter += 1
        if session_token is None:
            suffix = f"{name}-{self._session_counter}"
            session_token = (
                f"{WRITER_LEASE_HEARTBEAT_SESSION_PREFIX}{suffix}"
                if heartbeat_capable
                else uuid.uuid5(uuid.NAMESPACE_OID, suffix).hex
            )
        coordinator = Coordinator(
            self,
            name,
            writer_id=writer_id,
            writer_epoch=writer_epoch,
            session_token=session_token,
            ledger_kwargs={**self._default_ledger_kwargs, **ledger_kwargs},
        )
        self.coordinators[name] = coordinator
        return coordinator

    # -- observation -------------------------------------------------------

    def lease_row(self) -> LeaseRow | None:
        return self.server.lease

    def lease_holder_session(self) -> str | None:
        row = self.server.lease
        return None if row is None else row.writer_session_token


# --------------------------------------------------------------------------
# Determinism checking
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ScenarioRun:
    """What one execution of a scenario produced."""

    trace: tuple[str, ...]
    outcome: Any

    def digest(self) -> str:
        return json.dumps(
            {"trace": list(self.trace), "outcome": self.outcome},
            sort_keys=True,
            default=repr,
        )


def run_scenario(
    scenario: Callable[[Any], Any],
    *,
    harness_factory: Callable[..., HarnessBase] = LeaseHarness,
    **harness_kwargs: Any,
) -> ScenarioRun:
    """Execute ``scenario`` against a fresh harness and record what happened.

    ``harness_factory`` names which state machine's harness to build. It
    defaults to the lease lifecycle only because that is what landed first;
    nothing here knows anything about leases, and a scenario for any owner
    deriving from ``HarnessBase`` runs unchanged.
    """
    harness = harness_factory(**harness_kwargs)
    try:
        outcome = scenario(harness)
        return ScenarioRun(trace=tuple(harness.trace), outcome=outcome)
    finally:
        harness.close()


def assert_deterministic(
    test: Any,
    scenario: Callable[[Any], Any],
    *,
    repeats: int = 25,
    harness_factory: Callable[..., HarnessBase] = LeaseHarness,
    **harness_kwargs: Any,
) -> ScenarioRun:
    """Run ``scenario`` repeatedly and require an identical trace and outcome.

    A concurrency test that is merely usually right is worse than no test, so
    every scenario in this suite is required to prove order-stability rather
    than to assert it once. The checkpoint trace is the stronger half of the
    check: two runs can agree on the answer while disagreeing about the
    schedule that produced it, and that is exactly the flakiness this harness
    exists to rule out.
    """
    if repeats < 2:
        raise HarnessError("determinism needs at least two runs to mean anything")
    runs = [
        run_scenario(
            scenario,
            harness_factory=harness_factory,
            **harness_kwargs,
        )
        for _ in range(repeats)
    ]
    first = runs[0]
    for index, run in enumerate(runs[1:], start=2):
        if run.trace != first.trace:
            test.fail(
                f"run {index} produced a different schedule than run 1.\n"
                f"run 1: {list(first.trace)}\n"
                f"run {index}: {list(run.trace)}"
            )
        if run.digest() != first.digest():
            test.fail(
                f"run {index} produced a different outcome than run 1.\n"
                f"run 1: {first.outcome!r}\n"
                f"run {index}: {run.outcome!r}"
            )
    return first


__all__ = [
    "Actor",
    "Backend",
    "Call",
    "Coordinator",
    "DeterministicScheduler",
    "FakeLeaseGuard",
    "FakePostgres",
    "FakeSqlBackend",
    "HarnessBase",
    "HarnessCondition",
    "HarnessError",
    "HarnessEvent",
    "HarnessLock",
    "HarnessRLock",
    "HarnessSemaphore",
    "LandingOp",
    "LeaseHarness",
    "LeaseOp",
    "LeaseRow",
    "LockTimeout",
    "NotRunnable",
    "OutboxRow",
    "PoolBlockRow",
    "ScenarioRun",
    "SchedulerStall",
    "Statement",
    "StatementKind",
    "Transaction",
    "UnsupportedStatement",
    "VirtualClock",
    "acquire_result_arms",
    "acquire_terminal_arm_fields",
    "acquire_terminal_arm_keys",
    "advisory_lock_pair",
    "assert_deterministic",
    "classify",
    "run_scenario",
]
