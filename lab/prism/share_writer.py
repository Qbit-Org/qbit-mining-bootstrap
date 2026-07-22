"""Durable accepted-share writer and legacy recovery journal.

This module owns the mutable share-persistence boundary.  It deliberately has
no dependency on :mod:`lab.prism.prism_coordinator`; the coordinator remains a
construction root and temporary compatibility facade.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
import dataclasses
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import queue
import threading
from typing import Any, Callable, Protocol

from lab.prism.coordinator_shutdown import ShutdownInProgress, _WriterOperationToken
from lab.prism.share_ledger import (
    PendingShare,
    ShareReplayConflict,
    ShareReplayResult,
)


MAX_PENDING_SHARE_APPENDS = 4_096
PENDING_SHARE_COMMIT_WARN_SECONDS = 30.0
_STARTUP_RECOVERY_WAIT_POLL_SECONDS = 0.01
_WRITER_EXIT_COMPONENTS = frozenset(
    {"share_submission", "share_persistence", "accepted_block_handling"}
)


class ShareWriterError(RuntimeError):
    """A persistence operation failed before it could be acknowledged."""


class ShareWriterQueueFull(ShareWriterError):
    """The bounded share group-commit queue rejected an entry."""


class ShareLedgerPort(Protocol):
    def append(self, pending: PendingShare) -> Any: ...

    def append_batch(
        self,
        entries: list[tuple[PendingShare, dict[str, Any] | None]],
    ) -> list[Any]: ...

    def append_recovered_share(self, pending: PendingShare) -> ShareReplayResult: ...


@dataclass(frozen=True)
class ShareWriterPorts:
    """Narrow, call-time capabilities used by :class:`ShareWriter`."""

    ledger: Callable[[], ShareLedgerPort]
    writer_operation: Callable[[str], AbstractContextManager[object]]
    reserve_writer: Callable[[str], _WriterOperationToken]
    writer_admission_closed: Callable[[], bool]
    has_active_writer: Callable[[set[str]], bool]
    heartbeat: Callable[[str], None]
    monotonic: Callable[[], float]
    wall_time_ms: Callable[[], int]
    stop_is_set: Callable[[], bool]
    stop_wait: Callable[[float], bool]
    log: Callable[[str], None]
    log_exception: Callable[[], None]
    hot_path_log_enabled: Callable[[], bool]


@dataclass
class ShareWriterConfig:
    batch_size: int = 64
    linger_seconds: float = 0.005
    enqueue_timeout_seconds: float = 15.0
    pending_floor_warn_seconds: float = PENDING_SHARE_COMMIT_WARN_SECONDS
    recovery_path: Path | None = None


@dataclass(frozen=True)
class PendingShareInput:
    share_id: str
    miner_id: str
    order_key: str
    p2mr_program_hex: str
    share_difficulty: int
    network_difficulty: int
    template_height: int
    job_id: str
    job_issued_at_ms: int
    ntime: int
    credit_policy: str | None = None


@dataclass
class PendingShareAppend:
    """A share waiting for the ledger group-commit writer.

    The client thread does not count or acknowledge this share until
    ``committed`` is set successfully.  A block candidate intent, when
    present, is inserted in the same transaction as the share.
    """

    pending_share: PendingShare
    username: str
    job_id: str
    block_hash_hex: str
    collection_only: bool
    credit_policy: str | None
    candidate_intent: dict[str, Any] | None = None
    committed: threading.Event = field(default_factory=threading.Event)
    record: Any | None = None
    error: BaseException | None = None
    writer_token: _WriterOperationToken | None = None


@dataclass(frozen=True)
class ShareWriterMetricsSnapshot:
    queue_depth: int
    active: bool
    append_failures: int
    recovered_to_disk: int
    replayed: int
    replay_exact_existing: int
    replay_conflicts: int


@dataclass
class _PendingFloorHolder:
    pending: PendingShare
    registered_monotonic: float
    anchor_ms: int
    warned: bool = False


class ShareWriter:
    """Own group commit, the pending snapshot floor, and legacy recovery."""

    def __init__(
        self,
        config: ShareWriterConfig,
        ports: ShareWriterPorts,
        *,
        append_queue: queue.Queue[PendingShareAppend] | None = None,
        floor_lock: threading.Lock | threading.RLock | None = None,
        floor: dict[object, list[object]] | None = None,
        recovery_lock: threading.Lock | threading.RLock | None = None,
        active: bool = False,
        append_failures: int = 0,
        recovered_to_disk: int = 0,
        replayed: int = 0,
    ):
        self.config = config
        self.ports = ports
        self._queue = append_queue or queue.Queue(maxsize=MAX_PENDING_SHARE_APPENDS)
        self._floor_lock = floor_lock or threading.Lock()
        self._floor = floor if floor is not None else {}
        self._floor_anchors: dict[str, int] = {
            str(entry[0].share_id): int(entry[0].accepted_at_ms)
            for entry in self._floor.values()
            if entry and hasattr(entry[0], "share_id")
        }
        # A stamped submission, durable credit-bearing outbox, and active
        # candidate actor are independent reasons to hold the same logical
        # snapshot floor. Attempts and actors use object identity; durable
        # candidates use share_id so restart reconstruction reaches the holder.
        self._attempt_holders: dict[int, _PendingFloorHolder] = {}
        self._candidate_holders: dict[str, _PendingFloorHolder] = {}
        self._candidate_actor_holders: dict[int, _PendingFloorHolder] = {}
        self._recovery_lock = recovery_lock or threading.Lock()
        self._state_lock = threading.Lock()
        self._active = bool(active)
        self._running = False
        self._recovery_started = False
        self._startup_recovery_complete = threading.Event()
        self._startup_recovery_complete.set()
        self._startup_recovery_cancelled = threading.Event()
        self._append_failures = int(append_failures)
        self._recovered_to_disk = int(recovered_to_disk)
        self._replayed = int(replayed)
        self._replay_exact_existing = 0
        self._replay_conflicts = 0

    # Compatibility state is adopted by identity.  Replacement while the loop
    # is live would split one logical queue/floor across two owners, so reject
    # it rather than silently stranding work.
    @property
    def append_queue(self) -> queue.Queue[PendingShareAppend]:
        return self._queue

    def adopt_queue(self, value: queue.Queue[PendingShareAppend]) -> None:
        with self._state_lock:
            if self._running and value is not self._queue:
                raise RuntimeError("cannot replace the share queue while the writer is running")
            self._queue = value

    @property
    def floor_lock(self) -> threading.Lock | threading.RLock:
        return self._floor_lock

    @property
    def floor(self) -> dict[object, list[object]]:
        return self._floor

    def adopt_floor_lock(self, value: threading.Lock | threading.RLock) -> None:
        with self._state_lock:
            if self._running and value is not self._floor_lock:
                raise RuntimeError("cannot replace the pending-share floor lock while running")
            self._floor_lock = value

    def adopt_floor(self, value: dict[object, list[object]]) -> None:
        with self._state_lock:
            if self._running and value is not self._floor:
                raise RuntimeError("cannot replace the pending-share floor while running")
            self._floor = value
            self._floor_anchors = {
                str(entry[0].share_id): int(entry[0].accepted_at_ms)
                for entry in value.values()
                if entry and hasattr(entry[0], "share_id")
            }
            self._attempt_holders = {}
            self._candidate_holders = {}
            self._candidate_actor_holders = {}

    @property
    def recovery_lock(self) -> threading.Lock | threading.RLock:
        with self._state_lock:
            return self._recovery_lock

    @property
    def recovery_path(self) -> Path | None:
        with self._state_lock:
            return self.config.recovery_path

    def adopt_recovery_lock(self, value: threading.Lock | threading.RLock) -> None:
        with self._state_lock:
            if self._recovery_started and value is not self._recovery_lock:
                raise RuntimeError("cannot replace the share recovery lock after replay starts")
            self._recovery_lock = value

    def set_recovery_path(self, value: Path | None) -> None:
        with self._state_lock:
            if self._recovery_started and value != self.config.recovery_path:
                raise RuntimeError("cannot replace the share recovery path after replay starts")
            self.config.recovery_path = value

    @property
    def active(self) -> bool:
        with self._state_lock:
            return self._active

    @active.setter
    def active(self, value: bool) -> None:
        with self._state_lock:
            self._active = bool(value)

    @property
    def append_failures(self) -> int:
        with self._state_lock:
            return self._append_failures

    @append_failures.setter
    def append_failures(self, value: int) -> None:
        with self._state_lock:
            self._append_failures = int(value)

    @property
    def recovered_to_disk(self) -> int:
        with self._state_lock:
            return self._recovered_to_disk

    @recovered_to_disk.setter
    def recovered_to_disk(self, value: int) -> None:
        with self._state_lock:
            self._recovered_to_disk = int(value)

    @property
    def replayed(self) -> int:
        with self._state_lock:
            return self._replayed

    @replayed.setter
    def replayed(self, value: int) -> None:
        with self._state_lock:
            self._replayed = int(value)

    def metrics_snapshot(self) -> ShareWriterMetricsSnapshot:
        with self._state_lock:
            return ShareWriterMetricsSnapshot(
                queue_depth=self._queue.qsize(),
                active=self._active,
                append_failures=self._append_failures,
                recovered_to_disk=self._recovered_to_disk,
                replayed=self._replayed,
                replay_exact_existing=self._replay_exact_existing,
                replay_conflicts=self._replay_conflicts,
            )

    def begin_startup_recovery(self) -> None:
        """Fence contingent candidate credit behind legacy ACK recovery."""
        # A new epoch must clear the prior outcome before it exposes a closed
        # gate. Startup calls this before the candidate submitter is started.
        self._startup_recovery_cancelled.clear()
        self._startup_recovery_complete.clear()

    def finish_startup_recovery(self) -> None:
        """Open a successfully completed recovery epoch."""
        self._startup_recovery_complete.set()

    def cancel_startup_recovery(self) -> None:
        """Abort gated candidate credit before shutdown waits for writers."""
        # Publish cancellation first: every waiter woken by the open gate, and
        # every later caller that observes it open, must abort without append.
        self._startup_recovery_cancelled.set()
        self._startup_recovery_complete.set()

    def _wait_for_startup_recovery(self) -> None:
        """Wait for normal recovery completion or abort promptly on closure."""
        if self._startup_recovery_complete.is_set():
            # This cancellation check is the fast-path linearization point.
            # cancel_startup_recovery publishes cancellation before opening
            # the gate, so a cancellation interleaved with is_set() cannot be
            # mistaken for an already-normally-open recovery epoch.
            if self._startup_recovery_cancelled.is_set():
                raise ShutdownInProgress("PRISM startup share recovery was cancelled")
            return
        while True:
            if (
                self._startup_recovery_cancelled.is_set()
                or self.ports.stop_is_set()
                or self.ports.writer_admission_closed()
            ):
                raise ShutdownInProgress(
                    "PRISM startup share recovery was interrupted by shutdown"
                )
            if self._startup_recovery_complete.wait(
                _STARTUP_RECOVERY_WAIT_POLL_SECONDS
            ):
                if (
                    self._startup_recovery_cancelled.is_set()
                    or self.ports.stop_is_set()
                    or self.ports.writer_admission_closed()
                ):
                    raise ShutdownInProgress(
                        "PRISM startup share recovery was interrupted by shutdown"
                    )
                return

    def make_pending_share(self, value: PendingShareInput) -> PendingShare:
        """Stamp and register a pending share under one floor-lock hold."""
        with self._floor_lock:
            pending = PendingShare(
                share_id=value.share_id,
                miner_id=value.miner_id,
                order_key=value.order_key,
                p2mr_program_hex=value.p2mr_program_hex,
                share_difficulty=value.share_difficulty,
                network_difficulty=value.network_difficulty,
                template_height=value.template_height,
                job_id=value.job_id,
                job_issued_at_ms=value.job_issued_at_ms,
                accepted_at_ms=self.ports.wall_time_ms(),
                ntime=value.ntime,
                credit_policy=value.credit_policy,
            )
            self._migrate_legacy_holders_locked(str(pending.share_id))
            self._attempt_holders[id(pending)] = _PendingFloorHolder(
                pending=pending,
                registered_monotonic=self.ports.monotonic(),
                anchor_ms=int(pending.accepted_at_ms),
            )
            self._rebuild_floor_locked(str(pending.share_id), preferred=pending)
            return pending

    def adopt_pending_share(self, pending: PendingShare) -> None:
        """Atomically promote/register a durable candidate by stable share ID.

        If ``pending`` is a live stamped attempt, promotion removes that exact
        attempt and acquires the durable holder in the same floor-lock hold.
        Startup replay has no attempt holder and simply adds/rebinds the durable
        holder. Same-ID retries retain the minimum stamp without letting a
        failed newer attempt release the older durable source.
        """
        with self._floor_lock:
            self._adopt_pending_share_locked(pending)

    def _adopt_pending_share_locked(self, pending: PendingShare) -> None:
        logical_key = str(pending.share_id)
        self._migrate_legacy_holders_locked(logical_key)
        promoted = self._attempt_holders.pop(id(pending), None)
        self._ensure_candidate_holder_locked(pending, source=promoted)
        self._rebuild_floor_locked(logical_key, preferred=pending)

    def _ensure_candidate_holder_locked(
        self,
        pending: PendingShare,
        *,
        source: _PendingFloorHolder | None = None,
    ) -> None:
        """Create/rebind the stable durable-outbox holder under the floor lock."""
        logical_key = str(pending.share_id)
        existing = self._candidate_holders.get(logical_key)
        if existing is None:
            registered_monotonic = (
                source.registered_monotonic
                if source is not None
                else self.ports.monotonic()
            )
            anchor_ms = min(
                int(pending.accepted_at_ms),
                source.anchor_ms if source is not None else int(pending.accepted_at_ms),
            )
            self._candidate_holders[logical_key] = _PendingFloorHolder(
                pending=pending,
                registered_monotonic=registered_monotonic,
                anchor_ms=anchor_ms,
                warned=source.warned if source is not None else False,
            )
        else:
            existing.pending = pending
            existing.anchor_ms = min(
                existing.anchor_ms,
                int(pending.accepted_at_ms),
                source.anchor_ms if source is not None else int(pending.accepted_at_ms),
            )
            if source is not None:
                existing.registered_monotonic = min(
                    existing.registered_monotonic,
                    source.registered_monotonic,
                )
                existing.warned = existing.warned or source.warned

    def begin_candidate_actor(self, pending: PendingShare) -> None:
        """Acquire one active credit-candidate actor and stable retry holder.

        The exact stamped attempt moves to actor ownership atomically, so share
        append cleanup cannot erase it before the actor decides whether credit
        committed or a durable retry/terminal transition now owns the floor.
        """
        with self._floor_lock:
            logical_key = str(pending.share_id)
            self._migrate_legacy_holders_locked(logical_key)
            promoted = self._attempt_holders.pop(id(pending), None)
            actor = self._candidate_actor_holders.get(id(pending))
            if actor is None:
                actor = promoted or _PendingFloorHolder(
                    pending=pending,
                    registered_monotonic=self.ports.monotonic(),
                    anchor_ms=int(pending.accepted_at_ms),
                )
                self._candidate_actor_holders[id(pending)] = actor
            else:
                actor.pending = pending
                actor.anchor_ms = min(actor.anchor_ms, int(pending.accepted_at_ms))
                if promoted is not None:
                    actor.anchor_ms = min(actor.anchor_ms, promoted.anchor_ms)
                    actor.registered_monotonic = min(
                        actor.registered_monotonic,
                        promoted.registered_monotonic,
                    )
                    actor.warned = actor.warned or promoted.warned
            self._ensure_candidate_holder_locked(pending, source=actor)
            self._rebuild_floor_locked(logical_key, preferred=pending)

    def finish_candidate_actor(self, pending: PendingShare) -> None:
        """Release only this active candidate object's floor authority."""
        with self._floor_lock:
            logical_key = str(getattr(pending, "share_id", ""))
            self._migrate_legacy_holders_locked(logical_key)
            self._candidate_actor_holders.pop(id(pending), None)
            self._rebuild_floor_locked(logical_key)

    def _migrate_legacy_holders_locked(self, logical_key: str) -> None:
        """Adopt a directly inserted compatibility floor entry as an attempt."""
        represented_ids = set(self._attempt_holders)
        candidate_holder = self._candidate_holders.get(logical_key)
        if candidate_holder is not None:
            represented_ids.add(id(candidate_holder.pending))
        represented_ids.update(
            holder_id
            for holder_id, holder in self._candidate_actor_holders.items()
            if str(holder.pending.share_id) == logical_key
        )
        for key, entry in list(self._floor.items()):
            pending = entry[0] if entry else None
            if (
                str(getattr(pending, "share_id", "")) != logical_key
                or id(pending) in represented_ids
            ):
                continue
            self._attempt_holders[id(pending)] = _PendingFloorHolder(
                pending=pending,
                registered_monotonic=float(entry[1]),
                anchor_ms=int(
                    self._floor_anchors.get(
                        logical_key,
                        getattr(pending, "accepted_at_ms", 0),
                    )
                ),
                warned=bool(entry[2]),
            )
            represented_ids.add(id(pending))
            if key != logical_key:
                migrated_entry = self._floor.pop(key)
                self._floor.setdefault(logical_key, migrated_entry)

    def _holders_for_locked(self, logical_key: str) -> list[_PendingFloorHolder]:
        holders = [
            holder
            for holder in self._attempt_holders.values()
            if str(holder.pending.share_id) == logical_key
        ]
        candidate_holder = self._candidate_holders.get(logical_key)
        if candidate_holder is not None:
            holders.append(candidate_holder)
        holders.extend(
            holder
            for holder in self._candidate_actor_holders.values()
            if str(holder.pending.share_id) == logical_key
        )
        return holders

    def _rebuild_floor_locked(
        self,
        logical_key: str,
        *,
        preferred: PendingShare | None = None,
    ) -> None:
        holders = self._holders_for_locked(logical_key)
        if not holders:
            self._floor.pop(logical_key, None)
            self._floor_anchors.pop(logical_key, None)
            return
        representative = None
        if preferred is not None:
            representative = next(
                (holder for holder in holders if holder.pending is preferred),
                None,
            )
        if representative is None:
            representative = self._candidate_holders.get(logical_key) or holders[-1]
        values = [
            representative.pending,
            min(holder.registered_monotonic for holder in holders),
            any(holder.warned for holder in holders),
        ]
        existing = self._floor.get(logical_key)
        if isinstance(existing, list) and len(existing) == 3:
            existing[:] = values
        else:
            self._floor[logical_key] = values
        self._floor_anchors[logical_key] = min(holder.anchor_ms for holder in holders)

    def finish_pending_attempt(self, pending: PendingShare) -> None:
        """Release only one stamped submission attempt holder."""
        with self._floor_lock:
            logical_key = str(getattr(pending, "share_id", ""))
            self._migrate_legacy_holders_locked(logical_key)
            self._attempt_holders.pop(id(pending), None)
            self._floor.pop(id(pending), None)
            self._rebuild_floor_locked(logical_key)

    def finish_pending_candidate(self, pending: PendingShare) -> None:
        """Release only the durable credit-candidate holder for ``share_id``."""
        with self._floor_lock:
            logical_key = str(getattr(pending, "share_id", ""))
            self._migrate_legacy_holders_locked(logical_key)
            self._candidate_holders.pop(logical_key, None)
            self._rebuild_floor_locked(logical_key)

    def finish_pending_share(self, pending: PendingShare) -> None:
        """Compatibility terminal: remove all holders for one durable identity.

        Product paths use the attempt, durable-candidate, or actor-specific
        terminal so one owner cannot release another.
        """
        with self._floor_lock:
            share_id = getattr(pending, "share_id", None)
            if share_id is None:
                self._floor.pop(id(pending), None)
                self._attempt_holders.pop(id(pending), None)
                return
            logical_key = str(share_id)
            self._attempt_holders = {
                holder_id: holder
                for holder_id, holder in self._attempt_holders.items()
                if str(holder.pending.share_id) != logical_key
            }
            self._candidate_holders.pop(logical_key, None)
            self._candidate_actor_holders = {
                holder_id: holder
                for holder_id, holder in self._candidate_actor_holders.items()
                if str(holder.pending.share_id) != logical_key
            }
            self._floor.pop(logical_key, None)
            self._floor_anchors.pop(logical_key, None)
            self._floor.pop(id(pending), None)
            # Compatibility tests and old embeddings may have inserted an
            # id-keyed entry for an earlier object with the same durable ID.
            for key, entry in list(self._floor.items()):
                candidate = entry[0] if entry else None
                if str(getattr(candidate, "share_id", "")) == logical_key:
                    self._floor.pop(key, None)

    def transfer_pending_floor(self, old: PendingShare, new: PendingShare) -> None:
        """Merge retry attempts without orphaning durable candidate leases.

        Attempts with one durable share ID become one logical lease and retain
        the minimum acceptance stamp. Distinct parent/descendant IDs remain
        independent because either outbox row may later credit its own share;
        both can be released by a reconstructed object carrying that ID.
        """
        if old is new:
            return
        with self._floor_lock:
            old_key = str(old.share_id)
            new_key = str(new.share_id)
            if old_key != new_key:
                # Parent/descendant candidates are separate durable outbox
                # identities and may each still credit a share.  Keep both
                # stable leases; later reconstructed candidates release them
                # by share ID even when neither Python object remains queued.
                self._adopt_pending_share_locked(old)
                self._adopt_pending_share_locked(new)
                return
            self._adopt_pending_share_locked(old)
            self._adopt_pending_share_locked(new)

    def snapshot_anchor_ms(self, issued_at_ms: int) -> int:
        stale_share_ids: list[str] = []
        floor_ms: int | None = None
        current = self.ports.monotonic()
        with self._floor_lock:
            for entry in self._floor.values():
                pending = entry[0]
                accepted_at_ms = self._floor_anchors.get(
                    str(pending.share_id),
                    int(pending.accepted_at_ms),
                )
                floor_ms = (
                    accepted_at_ms
                    if floor_ms is None
                    else min(floor_ms, accepted_at_ms)
                )
                if (
                    not bool(entry[2])
                    and current - float(entry[1]) > self.config.pending_floor_warn_seconds
                ):
                    entry[2] = True
                    logical_key = str(pending.share_id)
                    for holder in self._holders_for_locked(logical_key):
                        holder.warned = True
                    stale_share_ids.append(str(pending.share_id))
        for share_id in stale_share_ids:
            self.ports.log(
                "prism coordinator: pending share commit is holding the job "
                f"snapshot anchor floor share_id={share_id}"
            )
        return issued_at_ms if floor_ms is None else min(issued_at_ms, floor_ms - 1)

    def append_and_wait(self, entry: PendingShareAppend) -> Any:
        """Persist an entry under admission with one lower terminal owner.

        In normal submit flow this operation inherits the already-admitted
        ``share_submission`` token.  A direct service caller instead owns a
        fresh ``share_persistence`` admission. Once admission enters, enqueue
        rollback, the queue-visible writer, or synchronous append owns attempt
        cleanup. This wrapper cleans only refusal before that ownership handoff,
        so an interrupted waiter cannot drop a still-queued floor.
        """
        admission_entered = False
        try:
            # O1 starts the durable candidate submitter before legacy share
            # replay. During that narrow startup window, candidate credit is
            # contingent on landing while the journal contains shares already
            # acknowledged by an older process. Serialize the two sources so
            # scheduling cannot assign nondeterministic cross-source sequence.
            self._wait_for_startup_recovery()
            with self.ports.writer_operation("share_persistence"):
                admission_entered = True
                if self.active:
                    self.enqueue(entry, wait=True)
                else:
                    self.append_entry(entry)
            return entry.record
        except BaseException:
            if not admission_entered:
                self.finish_pending_attempt(entry.pending_share)
            raise

    def enqueue(self, entry: PendingShareAppend, *, wait: bool = False) -> None:
        try:
            if entry.writer_token is None:
                entry.writer_token = self.ports.reserve_writer("share_persistence")
            if wait:
                self._queue.put(entry, timeout=self.config.enqueue_timeout_seconds)
            else:
                self._queue.put_nowait(entry)
        except queue.Full as exc:
            self._rollback_invisible_enqueue(entry)
            raise ShareWriterQueueFull("share ledger commit queue is full") from exc
        except BaseException:
            self._rollback_invisible_enqueue(entry)
            raise
        if not wait:
            return
        entry.committed.wait()
        if entry.error is not None:
            raise ShareWriterError(f"share ledger commit failed: {entry.error}")

    def _rollback_invisible_enqueue(self, entry: PendingShareAppend) -> None:
        """Release all ownership when an entry never became queue-visible."""
        if entry.writer_token is not None:
            entry.writer_token.finish()
            entry.writer_token = None
        self.finish_pending_attempt(entry.pending_share)

    def run(self) -> None:
        with self._state_lock:
            if self._running:
                raise RuntimeError("share writer loop is already running")
            self._running = True
        try:
            while True:
                self.ports.heartbeat("share_writer")
                stopping = self.ports.stop_is_set()
                try:
                    entry = self._queue.get(timeout=0.2 if stopping else 1.0)
                except queue.Empty:
                    if (
                        stopping
                        and self.ports.writer_admission_closed()
                        and not self.ports.has_active_writer(set(_WRITER_EXIT_COMPONENTS))
                    ):
                        return
                    continue
                batch = [entry]
                batch_size = max(1, int(self.config.batch_size))
                deadline = self.ports.monotonic() + max(
                    0.0, float(self.config.linger_seconds)
                )
                if entry.candidate_intent is not None:
                    deadline = self.ports.monotonic()
                while len(batch) < batch_size:
                    remaining = deadline - self.ports.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        next_entry = self._queue.get(timeout=remaining)
                    except queue.Empty:
                        break
                    batch.append(next_entry)
                    if next_entry.candidate_intent is not None:
                        break
                self.append_batch(batch)
        finally:
            with self._state_lock:
                self._running = False

    def append_batch(self, batch: list[PendingShareAppend]) -> bool:
        """Commit one writer batch and release every waiting submitter."""
        try:
            ledger = self.ports.ledger()
            append_batch = getattr(ledger, "append_batch", None)
            if callable(append_batch):
                records = append_batch(
                    [(entry.pending_share, entry.candidate_intent) for entry in batch]
                )
            else:
                records = [ledger.append(entry.pending_share) for entry in batch]
            if len(records) != len(batch):
                raise RuntimeError("share ledger returned an incomplete commit batch")
            hot_path_log = self.ports.hot_path_log_enabled()
            for entry, record in zip(batch, records, strict=True):
                entry.record = record
                if hot_path_log:
                    self._log_committed(entry, record)
            return True
        except Exception as exc:
            with self._state_lock:
                self._append_failures += len(batch)
            for entry in batch:
                entry.error = exc
            self.ports.log(
                f"prism coordinator: share ledger group commit failed count={len(batch)}"
            )
            self.ports.log_exception()
            return False
        finally:
            for entry in batch:
                self.finish_pending_attempt(entry.pending_share)
                entry.committed.set()
                if entry.writer_token is not None:
                    entry.writer_token.finish()
                    entry.writer_token = None

    def append_entry(
        self,
        entry: PendingShareAppend,
        *,
        retry_until_stopped: bool = False,
    ) -> bool:
        """Synchronously append one accepted share, preserving retry order."""
        backoff_seconds = 0.5
        try:
            while True:
                try:
                    ledger = self.ports.ledger()
                    append_batch = getattr(ledger, "append_batch", None)
                    if callable(append_batch):
                        record = append_batch(
                            [(entry.pending_share, entry.candidate_intent)]
                        )[0]
                    else:
                        record = ledger.append(entry.pending_share)
                    entry.record = record
                    break
                except Exception:
                    if not retry_until_stopped:
                        raise
                    with self._state_lock:
                        self._append_failures += 1
                    self.ports.log(
                        "prism coordinator: ledger share append failed; retrying "
                        f"share_id={entry.pending_share.share_id}"
                    )
                    self.ports.log_exception()
                    if self.ports.stop_wait(backoff_seconds):
                        self.recover_to_disk(
                            entry,
                            "ledger unavailable at shutdown",
                        )
                        return False
                    backoff_seconds = min(backoff_seconds * 2, 5.0)
                    self.ports.heartbeat("share_writer")
            if self.ports.hot_path_log_enabled():
                self._log_committed(entry, record)
            entry.committed.set()
            return True
        finally:
            self.finish_pending_attempt(entry.pending_share)

    def _log_committed(self, entry: PendingShareAppend, record: Any) -> None:
        self.ports.log(
            "prism coordinator: accepted share "
            f"seq={record.share_seq} miner={entry.username} job={entry.job_id} "
            f"hash={entry.block_hash_hex} collection={entry.collection_only} "
            f"credit_policy={entry.credit_policy or 'normal'}"
        )

    def recover_to_disk(self, entry: PendingShareAppend, reason: str) -> None:
        with self._state_lock:
            self._recovery_started = True
            path = self.config.recovery_path
            recovery_lock = self._recovery_lock
        if path is None:
            self.ports.log(
                "prism coordinator: WOULD LOSE acked share (no recovery path) "
                f"share_id={entry.pending_share.share_id} reason={reason}"
            )
            return
        try:
            payload = json.dumps(
                dataclasses.asdict(entry.pending_share),
                separators=(",", ":"),
            )
        except Exception:
            payload = None
        with recovery_lock:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                if payload is None:
                    raise ValueError("pending share is not serializable")
                with open(path, "a", encoding="utf-8") as handle:
                    handle.write(payload + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                with self._state_lock:
                    self._recovered_to_disk += 1
                self.ports.log(
                    "prism coordinator: recovered unpersisted acked share to disk "
                    f"share_id={entry.pending_share.share_id} reason={reason}"
                )
            except Exception:
                self.ports.log(
                    "prism coordinator: FAILED to recover acked share to disk; "
                    f"share may be lost share_id={entry.pending_share.share_id} "
                    f"reason={reason}"
                )
                self.ports.log_exception()

    def replay_recovery_file(self) -> int:
        """Replay the recovery journal with typed exact/conflict outcomes."""
        with self.ports.writer_operation("share_recovery_replay"):
            return self._replay_recovery_file_admitted()

    def _replay_recovery_file_admitted(self) -> int:
        with self._state_lock:
            self._recovery_started = True
            path = self.config.recovery_path
        if path is None:
            return 0
        with self._recovery_lock:
            if not path.exists():
                return 0
            try:
                lines = [
                    line
                    for line in path.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]
            except Exception:
                self.ports.log("prism coordinator: could not read share recovery file")
                self.ports.log_exception()
                return 0
            pendings: list[PendingShare] = []
            parse_failed = False
            for line in lines:
                try:
                    pendings.append(PendingShare(**json.loads(line)))
                except Exception:
                    parse_failed = True
                    self.ports.log(
                        "prism coordinator: skipping an unparseable recovered share line"
                    )
                    self.ports.log_exception()
            pendings.sort(key=lambda pending: pending.accepted_at_ms)
            replayed = 0
            exact_existing = 0
            conflict = False
            for pending in pendings:
                ledger = self.ports.ledger()
                append_recovered = getattr(ledger, "append_recovered_share", None)
                if not callable(append_recovered):
                    self.ports.log(
                        "prism coordinator: recovery ledger lacks typed replay support; "
                        "keeping the file"
                    )
                    break
                try:
                    result = append_recovered(pending)
                except ShareReplayConflict:
                    conflict = True
                    with self._state_lock:
                        self._replay_conflicts += 1
                    self.ports.log(
                        "prism coordinator: recovered share conflicts with durable "
                        f"payload; keeping the file share_id={pending.share_id}"
                    )
                    break
                except Exception:
                    self.ports.log(
                        "prism coordinator: failed to replay a recovered share; "
                        "keeping the file"
                    )
                    self.ports.log_exception()
                    break
                disposition = getattr(result, "disposition", None)
                if disposition == "inserted":
                    replayed += 1
                elif disposition == "exact_existing":
                    exact_existing += 1
                else:
                    self.ports.log(
                        "prism coordinator: recovery ledger returned an unsupported "
                        f"replay disposition {disposition!r}; keeping the file"
                    )
                    break
            completed = replayed + exact_existing == len(pendings)
            with self._state_lock:
                self._replayed += replayed
                self._replay_exact_existing += exact_existing
            if exact_existing:
                self.ports.log(
                    f"prism coordinator: skipped {exact_existing} already-committed "
                    "recovered share(s) during replay"
                )
            if replayed:
                self.ports.log(
                    f"prism coordinator: replayed {replayed} recovered share(s) "
                    "into the ledger"
                )
            if not parse_failed and not conflict and completed:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
            return replayed


class ShareWriterCompatibilityField:
    """Descriptor routing a temporary coordinator attribute to S3 ownership."""

    def __init__(self, name: str, default: Any):
        self.name = name
        self.default = default

    def __get__(self, instance: Any, owner: type[Any]) -> Any:
        if instance is None:
            return self
        service = instance.__dict__.get("_share_writer_service")
        if service is None:
            value = instance.__dict__.get(self.name, self.default)
            if callable(value) and getattr(value, "__share_writer_default_factory__", False):
                value = value()
                instance.__dict__[self.name] = value
            return value
        return _compat_get(service, self.name)

    def __set__(self, instance: Any, value: Any) -> None:
        service = instance.__dict__.get("_share_writer_service")
        if service is None:
            instance.__dict__[self.name] = value
            return
        _compat_set(service, self.name, value)


def compatibility_default(factory: Callable[[], Any]) -> Callable[[], Any]:
    setattr(factory, "__share_writer_default_factory__", True)
    return factory


def _compat_get(service: ShareWriter, name: str) -> Any:
    if name == "share_append_queue":
        return service.append_queue
    if name == "share_commit_batch_size":
        return service.config.batch_size
    if name == "share_commit_linger_seconds":
        return service.config.linger_seconds
    if name == "share_commit_timeout_seconds":
        return service.config.enqueue_timeout_seconds
    if name == "share_writer_active":
        return service.active
    if name == "share_append_failure_count":
        return service.append_failures
    if name == "share_recovery_path":
        return service.recovery_path
    if name == "share_recovery_lock":
        return service.recovery_lock
    if name == "shares_recovered_to_disk":
        return service.recovered_to_disk
    if name == "shares_replayed":
        return service.replayed
    if name == "_pending_share_commit_lock":
        return service.floor_lock
    if name == "_pending_share_commit_floor":
        return service.floor
    raise AttributeError(name)


def _compat_set(service: ShareWriter, name: str, value: Any) -> None:
    if name == "share_append_queue":
        service.adopt_queue(value)
    elif name == "share_commit_batch_size":
        service.config.batch_size = int(value)
    elif name == "share_commit_linger_seconds":
        service.config.linger_seconds = float(value)
    elif name == "share_commit_timeout_seconds":
        service.config.enqueue_timeout_seconds = float(value)
    elif name == "share_writer_active":
        service.active = bool(value)
    elif name == "share_append_failure_count":
        service.append_failures = int(value)
    elif name == "share_recovery_path":
        service.set_recovery_path(Path(value) if value is not None else None)
    elif name == "share_recovery_lock":
        service.adopt_recovery_lock(value)
    elif name == "shares_recovered_to_disk":
        service.recovered_to_disk = int(value)
    elif name == "shares_replayed":
        service.replayed = int(value)
    elif name == "_pending_share_commit_lock":
        service.adopt_floor_lock(value)
    elif name == "_pending_share_commit_floor":
        service.adopt_floor(value)
    else:
        raise AttributeError(name)
