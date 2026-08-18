#!/usr/bin/env python3
"""Deterministic concurrency harness for PRISM's writer-lease lifecycle.

Why this exists
---------------
The concurrency defects this line keeps shipping (tip-refresh livelock,
publication starvation, lease heartbeat self-fencing, the found-block landing
livelock, the orphaned lease-tuple lock in #123) share a shape: two owners
interleave around a PostgreSQL row, and the bad schedule is rare enough that
running the coordinator and hoping never produces it. Reviews caught them;
tests could not express them.

This module makes the schedule an input rather than an accident. Three pieces:

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
    An in-memory model of the one table the lease lifecycle uses, with the
    PostgreSQL semantics that lifecycle actually depends on: exclusive tuple
    locks held for the lifetime of a transaction, ``FOR NO KEY UPDATE SKIP
    LOCKED`` declining to queue, per-statement READ COMMITTED snapshots,
    ``lock_timeout``/``statement_timeout``, session-scoped advisory locks,
    and ``pg_stat_activity`` locker attribution against a snapshot-visible
    ``xmax``. Explicit transactions are modelled as BEGIN / statement /
    COMMIT with a checkpoint in between, because that gap is the whole of
    #123.

Substitution goes through ports, not monkeypatching
---------------------------------------------------
``PsqlShareLedger`` takes ``sql_backend_factory``, ``lease_guard_factory``,
``monotonic`` and ``lease_retry_sleep``. The fakes here implement
``LedgerSqlPort`` and ``LeaseGuardPort``. No bound method is reassigned and
no ``time`` module is patched, so the code under test is the shipped code:
the real retry loop, the real CAS SQL, the real adoption arithmetic, the real
fail-closed branches.

What is modelled and what is not
--------------------------------
Modelled: the writer-lease statements ``PsqlShareLedger`` emits — startup
acquisition, same-identity adoption, renewal, release, and the guard-session
verification probe.

Not modelled: everything else. ``FakePostgres`` classifies each statement and
raises ``UnsupportedStatement`` on anything it does not recognise, including
the share-ledger and block-candidate statements, which belong to the
follow-up state machines named in issue #128. That is deliberate: a fake that
silently answers an unrecognised statement drifts away from production
without anyone noticing, so this one fails loudly instead. The classifier is
anchored on fragments of the production SQL, so a change to that SQL breaks
classification rather than quietly changing meaning.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import sys
import threading
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lab.prism.share_ledger import (  # noqa: E402
    WRITER_LEASE_HEARTBEAT_SESSION_PREFIX,
    PsqlShareLedger,
)

# Real seconds the controller will wait for an actor to hand the baton back
# before declaring the harness itself broken. This is a failure detector
# only: it never orders anything, so it cannot make a passing run flaky. An
# actor that blocks on a resource the harness does not know about (a real
# threading.Lock, a real socket) trips it instead of hanging the suite.
BATON_TIMEOUT_SECONDS = 20.0

# Virtual epoch origin for clock_timestamp(). Fixed so timestamp text is
# byte-identical across runs, which matters: the adoption CAS compares
# updated_at as text.
CLOCK_ORIGIN = datetime(2026, 6, 26, 19, 49, 22, 233718, tzinfo=timezone.utc)

# The guard session connects with `-c statement_timeout=500`.
GUARD_STATEMENT_TIMEOUT_SECONDS = 0.5


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
        if (
            self._wake_at is not None
            and self.scheduler.clock.monotonic() >= self._wake_at
        ):
            return True
        return False

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
                except BaseException as exc:  # noqa: BLE001 - recorded, not swallowed
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
# Statement classification
# --------------------------------------------------------------------------


class LeaseOp(str, Enum):
    ACQUIRE = "acquire"
    ADOPT = "adopt"
    RENEW = "renew"
    RELEASE = "release"
    VERIFY = "verify"


@dataclass(frozen=True)
class Statement:
    kind: LeaseOp
    payload: dict[str, Any]
    lease_ttl_seconds: float | None
    authority_margin_seconds: float | None
    advisory_lock: tuple[int, int] | None
    sql: str


_PAYLOAD_RE = re.compile(r"\$(qbit_prism_json(?:_x)*)\$(.*?)\$\1\$::jsonb", re.S)
_INTERVAL_RE = re.compile(r"make_interval\(secs => ([0-9eE\.\+\-]+)\)")
_CLASSID_RE = re.compile(r"classid = (\d+)::oid")
_OBJID_RE = re.compile(r"objid = (\d+)::oid")

# Each entry is (op, required fragments). Fragments are lifted verbatim from
# the production SQL in lab/prism/share_ledger.py, so a change there breaks
# classification loudly instead of silently changing what the fake models.
_SIGNATURES: tuple[tuple[LeaseOp, tuple[str, ...]], ...] = (
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

# Statements that belong to the state machines issue #128 defers to follow-up
# PRs. Recognised only so the failure names the right follow-up instead of
# reading as an unexplained harness gap.
_DEFERRED_SIGNATURES: tuple[tuple[str, str], ...] = (
    ("INSERT INTO qbit_share_ledger", "share submission and dedupe"),
    ("qbit_pool_blocks", "block candidate landing"),
    ("qbit_block_candidate_outbox", "block candidate landing"),
    ("qbit_ctv_fanouts", "CTV fanout broadcasting"),
)


def classify(sql: str) -> Statement:
    """Map one production statement onto the lease operation it performs."""
    for op, fragments in _SIGNATURES:
        if all(fragment in sql for fragment in fragments):
            return Statement(
                kind=op,
                payload=_extract_payload(sql),
                lease_ttl_seconds=_extract_interval(sql, first=True),
                authority_margin_seconds=(
                    _extract_interval(sql, first=False)
                    if op is LeaseOp.VERIFY
                    else None
                ),
                advisory_lock=_extract_advisory_lock(sql),
                sql=sql,
            )
    for marker, state_machine in _DEFERRED_SIGNATURES:
        if marker in sql:
            raise UnsupportedStatement(
                f"{marker} belongs to the {state_machine} state machine, which "
                "issue #128 defers to a follow-up PR; this harness models the "
                "writer-lease lifecycle only"
            )
    raise UnsupportedStatement(
        "FakePostgres does not model this statement. Either the production "
        "lease SQL changed shape (update _SIGNATURES) or the scenario reached "
        "a state machine this harness does not cover:\n"
        f"{sql.strip()[:400]}"
    )


def _extract_payload(sql: str) -> dict[str, Any]:
    match = _PAYLOAD_RE.search(sql)
    if match is None:
        raise UnsupportedStatement("lease statement carried no jsonb payload")
    payload = json.loads(match.group(2))
    if not isinstance(payload, dict):
        raise UnsupportedStatement("lease statement payload was not an object")
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
class Transaction:
    xid: str
    backend: Backend
    explicit: bool
    holds_lease_lock: bool = False
    staged_lease: LeaseRow | None = None
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
        self._lease_lock_holder: Transaction | None = None
        self._backends: list[Backend] = []
        self._advisory_owners: dict[tuple[int, int], Backend] = {}
        self._next_pid = 1
        self._next_xid = 1

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
        self._release_lease_lock(transaction)
        transaction.backend.transaction = None

    def _rollback(self, transaction: Transaction) -> None:
        transaction.staged_lease = None
        self._release_lease_lock(transaction)
        transaction.backend.transaction = None

    def _release_lease_lock(self, transaction: Transaction) -> None:
        if self._lease_lock_holder is transaction:
            self._lease_lock_holder = None
            if self.lease is not None and self.lease.xmax == transaction.xid:
                self.lease = replace(self.lease, xmax=None)

    def orphan(self, transaction: Transaction) -> None:
        """The client vanished without RST: the transaction stays open forever.

        PostgreSQL finished executing the statement and is waiting for a
        COMMIT that will never arrive. Nothing releases the tuple lock until
        TCP keepalive teardown, and nothing in this repository sets
        ``idle_in_transaction_session_timeout``.
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

    def _evaluate(
        self,
        statement: Statement,
        transaction: Transaction,
        timeout_seconds: float | None,
        tag: str,
        timeout_kind: str = "lock",
    ) -> Any:
        if statement.kind is LeaseOp.VERIFY:
            return self._evaluate_verify(statement, transaction, tag)
        # Every other lease statement writes the singleton row, so it needs
        # the exclusive tuple lock and queues for it. That queue is the whole
        # of #123: nothing bounds it unless the caller armed a deadline.
        self._await_lease_lock(
            statement,
            transaction,
            timeout_seconds,
            tag,
            timeout_kind,
        )
        if statement.kind is LeaseOp.ACQUIRE:
            return self._evaluate_acquire(statement, transaction)
        if statement.kind is LeaseOp.ADOPT:
            return self._evaluate_adopt(statement, transaction)
        if statement.kind is LeaseOp.RENEW:
            return self._evaluate_renew(statement, transaction)
        if statement.kind is LeaseOp.RELEASE:
            return self._evaluate_release(statement, transaction)
        raise UnsupportedStatement(f"unhandled lease operation {statement.kind}")

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
        while self.lease_lock_held_by_other(transaction):
            if deadline is not None and self.clock.monotonic() >= deadline:
                raise LockTimeout(
                    f"canceling statement due to {timeout_kind} timeout\n"
                    "CONTEXT:  while updating tuple (0,1) in relation "
                    '"qbit_ledger_writer_lease"'
                )
            self.scheduler.block(
                f"{tag}.lockwait:{statement.kind.value}",
                ready=lambda: not self.lease_lock_held_by_other(transaction),
                wake_at=deadline,
            )
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
    ) -> dict[str, Any]:
        payload = statement.payload
        now = self.clock.now()
        ttl = self._ttl(statement)
        row = self._visible_lease(transaction)
        if row is None:
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
            return {
                "acquired": True,
                "writer_id": payload["writer_id"],
                "writer_epoch": int(payload["writer_epoch"]),
                "writer_session_token": payload["writer_session_token"],
            }
        same_session = (
            row.writer_id == payload["writer_id"]
            and row.writer_epoch == int(payload["writer_epoch"])
            and row.writer_session_token == payload["writer_session_token"]
        )
        if same_session or row.lease_expires_at <= now:
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
            return {
                "acquired": True,
                "writer_id": payload["writer_id"],
                "writer_epoch": int(payload["writer_epoch"]),
                "writer_session_token": payload["writer_session_token"],
            }
        return self._observed(row, acquired=False)

    def _evaluate_adopt(
        self,
        statement: Statement,
        transaction: Transaction,
    ) -> dict[str, Any]:
        payload = statement.payload
        now = self.clock.now()
        ttl = self._ttl(statement)
        row = self._visible_lease(transaction)
        if row is None:
            raise HarnessError("adoption ran against a missing lease row")
        matches = (
            row.writer_id == payload["writer_id"]
            and row.writer_epoch == int(payload["writer_epoch"])
            and row.writer_session_token == payload["observed_writer_session_token"]
            and VirtualClock.timestamp_text(row.updated_at)
            == payload["observed_lease_updated_at"]
            and row.lease_expires_at > now
        )
        if not matches:
            observed = self._observed(row, acquired=False)
            observed["adopted"] = False
            return observed
        self._stage(
            transaction,
            replace(
                row,
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

    def _evaluate_renew(
        self,
        statement: Statement,
        transaction: Transaction,
    ) -> dict[str, Any]:
        payload = statement.payload
        now = self.clock.now()
        ttl = self._ttl(statement)
        row = self._visible_lease(transaction)
        if row is None or not self._is_exact_session(row, payload):
            return {"error": "writer lease is not active"}
        self._stage(
            transaction,
            replace(
                row,
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
    ) -> dict[str, Any]:
        payload = statement.payload
        now = self.clock.now()
        row = self._visible_lease(transaction)
        if row is None or not self._is_exact_session(row, payload):
            return {"released": 0}
        self._stage(
            transaction,
            replace(
                row,
                lease_expires_at=now - timedelta(seconds=1),
                updated_at=now,
                xmax=None,
            ),
        )
        return {"released": 1}

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

    def _acquire(self) -> Backend:
        while not self._free:
            self.server.scheduler.block(
                f"{self.tag}.pool:exhausted",
                ready=lambda: bool(self._free),
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
    ) -> Any:
        if self.closed:
            raise RuntimeError("connection pool is closed")
        backend = self._acquire()
        transaction: Transaction | None = None
        try:
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
            return result
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
            result = self.server.execute(
                self.backend,
                sql,
                transaction=None,
                timeout_seconds=None,
                tag=self.tag,
            )
            while followup is not None:
                next_sql = followup(result)
                if next_sql is None:
                    break
                result = self.server.execute(
                    self.backend,
                    next_sql,
                    transaction=None,
                    timeout_seconds=None,
                    tag=self.tag,
                )
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


class LeaseHarness:
    """Front end: build coordinators, step them, advance time, read the trace."""

    def __init__(
        self,
        *,
        lease_ttl_seconds: float = 60.0,
        capture_output: bool = True,
        baton_timeout_seconds: float = BATON_TIMEOUT_SECONDS,
        **default_ledger_kwargs: Any,
    ) -> None:
        self.clock = VirtualClock()
        self.scheduler = DeterministicScheduler(
            self.clock,
            baton_timeout_seconds=baton_timeout_seconds,
        )
        self.server = FakePostgres(self.scheduler, self.clock)
        self.coordinators: dict[str, Coordinator] = {}
        self.sleeps: list[tuple[str, float]] = []
        self._default_ledger_kwargs = {
            "lease_ttl_seconds": lease_ttl_seconds,
            **default_ledger_kwargs,
        }
        self._session_counter = 0
        self._output = io.StringIO()
        self._redirect: contextlib.AbstractContextManager[Any] | None = None
        if capture_output:
            self._redirect = contextlib.redirect_stdout(self._output)
            self._redirect.__enter__()

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self) -> LeaseHarness:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self.scheduler.close()
        if self._redirect is not None:
            self._redirect.__exit__(None, None, None)
            self._redirect = None

    @property
    def output(self) -> str:
        return self._output.getvalue()

    @property
    def trace(self) -> list[str]:
        return list(self.scheduler.trace)

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

    # -- clock -------------------------------------------------------------

    def sleep(self, seconds: float) -> None:
        """``lease_retry_sleep``: park until the virtual clock reaches the wake time."""
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

    def _actor(self, who: Coordinator | Actor) -> Actor:
        return who.actor if isinstance(who, Coordinator) else who

    def step(self, who: Coordinator | Actor) -> str:
        return self.scheduler.step(self._actor(who))

    def run_until(
        self,
        who: Coordinator | Actor,
        label: str,
        *,
        limit: int = 200,
    ) -> str:
        return self.scheduler.run_until(self._actor(who), label, limit=limit)

    def run_until_blocked(
        self,
        who: Coordinator | Actor,
        label: str,
        *,
        limit: int = 200,
    ) -> str:
        """Step until the actor parks at ``label`` and is genuinely stuck there."""
        actor = self._actor(who)
        stop = self.scheduler.run_until(actor, label, limit=limit)
        if not actor.blocked:
            raise HarnessError(
                f"actor {actor.name!r} reached {label!r} but is not blocked"
            )
        return stop

    def drain(
        self,
        order: list[Coordinator | Actor] | None = None,
        *,
        limit: int = 500,
    ) -> None:
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

    def lease_row(self) -> LeaseRow | None:
        return self.server.lease

    def lease_holder_session(self) -> str | None:
        row = self.server.lease
        return None if row is None else row.writer_session_token

    def statement_kinds(self) -> list[str]:
        return [statement.kind.value for statement in self.server.statements]


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


def run_scenario(scenario: Callable[[LeaseHarness], Any], **harness_kwargs: Any) -> ScenarioRun:
    """Execute ``scenario`` against a fresh harness and record what happened."""
    harness = LeaseHarness(**harness_kwargs)
    try:
        outcome = scenario(harness)
        return ScenarioRun(trace=tuple(harness.trace), outcome=outcome)
    finally:
        harness.close()


def assert_deterministic(
    test: Any,
    scenario: Callable[[LeaseHarness], Any],
    *,
    repeats: int = 25,
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
    runs = [run_scenario(scenario, **harness_kwargs) for _ in range(repeats)]
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
    "HarnessError",
    "LeaseHarness",
    "LeaseOp",
    "LeaseRow",
    "LockTimeout",
    "NotRunnable",
    "SchedulerStall",
    "ScenarioRun",
    "Statement",
    "Transaction",
    "UnsupportedStatement",
    "VirtualClock",
    "advisory_lock_pair",
    "assert_deterministic",
    "classify",
    "run_scenario",
]
