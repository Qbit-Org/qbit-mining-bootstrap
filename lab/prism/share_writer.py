#!/usr/bin/env python3
"""Durable accepted-share writer, pending-commit floor, and legacy recovery.

This module owns the S3 domain: the bounded group-commit queue and writer
loop, the pending-share snapshot-anchor floor, disk recovery of acked shares
the ledger could not persist, and startup replay of that recovery journal.

It never imports ``prism_coordinator``.  The ledger handle, shutdown
controller, watchdog heartbeats, the P1 append/anchor fencing entrypoints,
and live configuration attributes are reached through the
:class:`ShareWriterRuntime` typed port, resolved at call time so the
historical coordinator monkeypatch seams (including the instance-level facade
patches used by the current test suite) keep intercepting exactly as before
the extraction.  Replay semantics deliberately keep the current tree's
behavior; the typed exact/conflict recovery consumption from the share
ledger arrives with a later chunk through the ``replay_recovered_shares``
seam below.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
import json
import os
import queue
import threading
import time
import traceback
from typing import Any, Protocol

from lab.prism.share_ledger import PendingShare, ShareReplayConflict
from lab.prism.stratum_session import StratumError


MAX_PENDING_SHARE_APPENDS = 4_096
# After this long on the pending-commit floor a share is assumed wedged (not
# normal group-commit latency) and the anchor clamp logs it once.
PENDING_SHARE_COMMIT_WARN_SECONDS = 30.0
_WRITER_EXIT_COMPONENTS = frozenset(
    {"share_submission", "share_persistence", "accepted_block_handling"}
)


class ShareWriterError(RuntimeError):
    """A persistence operation failed before it could be acknowledged."""


class ShareWriterQueueFull(ShareWriterError):
    """The bounded share group-commit queue rejected an entry."""


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
    writer_token: "_WriterOperationToken | None" = None


class ShareWriterRuntime(Protocol):
    """Typed port over the coordinator, resolved at call time.

    Every member is looked up on the live coordinator object when used, so
    instance monkeypatches (``server._finish_pending_share_commit = ...``,
    ``server._append_share_batch = ...`` and friends) and coordinator-owned
    live state (the ledger handle, the P1 append/anchor fencing) keep working
    exactly as before the extraction.  Owner facades route back into this
    service; that round trip is deliberate.  The legacy S3 field names
    (``share_append_queue`` through ``_pending_share_commit_floor``) also
    resolve here -- coordinator class descriptors route them to this
    service's single mutable copy.
    """

    # Cross-domain objects and live configuration attributes.
    hot_path_log_enabled: Any
    ledger: Any
    stop_event: Any

    def _append_share_batch(self, *args: Any, **kwargs: Any) -> Any: ...

    def _append_share_entry(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_share_hot_path_state(self, *args: Any, **kwargs: Any) -> Any: ...

    def _ensure_shutdown_controller(self, *args: Any, **kwargs: Any) -> Any: ...

    def _finish_pending_share_commit(self, *args: Any, **kwargs: Any) -> Any: ...

    def _landing_fence_for_predating_append(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_heartbeat(self, *args: Any, **kwargs: Any) -> Any: ...

    def _record_late_visible_payout_append(self, *args: Any, **kwargs: Any) -> Any: ...

    def _recover_share_to_disk(self, *args: Any, **kwargs: Any) -> Any: ...

    def _retire_payout_windows_for_late_append(self, *args: Any, **kwargs: Any) -> Any: ...

    def accepted_share_difficulty(self, *args: Any, **kwargs: Any) -> Any: ...

    def enqueue_share_append(self, *args: Any, **kwargs: Any) -> Any: ...

    def note_vardiff_accepted_share(self, *args: Any, **kwargs: Any) -> Any: ...

    def note_worker_accepted_share(self, *args: Any, **kwargs: Any) -> Any: ...


class ShareWriter:
    """Sole owner of S3 group commit, the pending floor, and recovery."""

    def __init__(
        self,
        runtime: ShareWriterRuntime,
        *,
        internal_error_reason: str,
        now_ms: Any,
    ) -> None:
        self._runtime = runtime
        # The rejection-reason id remains a coordinator registry constant and
        # the wall-clock stamp resolves the coordinator module's ``now_ms``
        # global at call time (tests patch it there); both are injected so
        # this leaf module never imports prism_coordinator.
        self._internal_error_reason = internal_error_reason
        self._now_ms = now_ms
        # Accepted shares drain through a bounded group-commit writer.  A
        # submitting client waits on its entry's completion event, making the
        # database commit the acknowledgement boundary without paying one
        # process/transaction round trip per share during bursts.
        self.share_append_queue: queue.Queue[PendingShareAppend] = queue.Queue(
            maxsize=MAX_PENDING_SHARE_APPENDS
        )
        self.share_commit_batch_size = 64
        self.share_commit_linger_seconds = 0.005
        self.share_commit_timeout_seconds = 15.0
        self.share_writer_active = False
        self.share_append_failure_count = 0
        # Retain the historical recovery-file reader for clean upgrades from a
        # release that could acknowledge before Postgres commit.  New shares
        # are never written here: an unavailable ledger produces no success
        # acknowledgement and an exact retry is idempotent.
        self.share_recovery_path = None
        self.share_recovery_lock = threading.Lock()
        self.shares_recovered_to_disk = 0
        self.shares_replayed = 0
        self.share_replay_conflicts = 0
        self._pending_share_commit_lock = threading.Lock()
        # Shares whose accepted_at_ms has been assigned but whose ledger
        # row has not reached a terminal outcome in this process. Job and
        # payout-artifact snapshot anchors clamp below every entry so a
        # bundle's eligible-share set stays exactly reproducible from the
        # durable ledger after those rows commit.
        self._pending_share_commit_floor: dict[int, list[object]] = {}

    # -- pending-commit floor ----------------------------------------------

    def finish_pending_share(self, pending_share: PendingShare) -> None:
        """Drop a share from the snapshot anchor floor.

        Called once the share's ledger row reached a terminal outcome in this
        process: durably committed, rejected back to the miner, recovered to
        the on-disk replay file, or its block candidate terminally abandoned.
        Idempotent, and a no-op for shares this process never registered
        (intent/journal replays re-create PendingShare objects from JSON).
        """
        with self._pending_share_commit_lock:
            self._pending_share_commit_floor.pop(id(pending_share), None)

    def finish_pending_attempt(self, pending_share: PendingShare) -> None:
        """Release one stamped submission attempt's floor authority.

        Layer-original seam: at this layer the floor keeps the current
        tree's single object-identity holder per stamped share, so attempt
        release and candidate release act on the same entry.  A later chunk
        introduces independent attempt/durable-candidate/actor holders behind
        these two adapters without touching their callers.
        """
        self.finish_pending_share(pending_share)

    def finish_pending_candidate(self, pending_share: PendingShare) -> None:
        """Release the durable block-candidate actor's floor authority.

        See :meth:`finish_pending_attempt` for the staged holder split.
        """
        self.finish_pending_share(pending_share)

    def snapshot_anchor_ms(self, issued_at_ms: int) -> int:
        """Clamp a share-snapshot anchor below every coverable share stamp.

        The reward-window contract lets an auditor replay
        qbit_audit_share_window(anchor) against the durable ledger and expect
        exactly the shares the published bundle counted. Two hazards would
        violate that. A share whose accepted_at_ms is already assigned but
        whose row has not committed yet (group-commit queue, in-flight batch,
        or a block-candidate credit linked after landing) is invisible to the
        MVCC snapshot now but joins later replays at any anchor at or above
        its accepted_at_ms. And a share stamped right after this clamp
        returns can land in the same millisecond as the clamp instant --
        never protected by the pending floor -- so an anchor equal to that
        instant would cover it too (the window predicate is
        anchor-inclusive). Anchoring strictly below every pending share and
        strictly below the clamp-time millisecond keeps the issued snapshot
        reproducible without making job builds wait behind the writer
        connection: any stamp assigned after this call is at or above the
        clamp instant, hence above the anchor.
        """
        stale_share_ids: list[str] = []
        floor_ms: int | None = None
        now_monotonic = time.monotonic()
        with self._pending_share_commit_lock:
            for entry in self._pending_share_commit_floor.values():
                share = entry[0]
                accepted_at_ms = int(share.accepted_at_ms)
                if floor_ms is None or accepted_at_ms < floor_ms:
                    floor_ms = accepted_at_ms
                if (
                    not entry[2]
                    and now_monotonic - float(entry[1])
                    > PENDING_SHARE_COMMIT_WARN_SECONDS
                ):
                    entry[2] = True
                    stale_share_ids.append(str(share.share_id))
        for share_id in stale_share_ids:
            # A long-held floor entry is a wedged writer or a leaked release
            # path, not normal group-commit latency. The share-commit liveness
            # watchdog owns recovery; this log makes the anchor clamp visible.
            print(
                "prism coordinator: pending share commit is holding the job "
                f"snapshot anchor floor share_id={share_id}",
                flush=True,
            )
        if floor_ms is None:
            return issued_at_ms - 1
        return min(issued_at_ms - 1, floor_ms - 1)

    def pending_share_from_submission(
        self,
        *,
        context: Any,
        submission: Any,
        ntime_hex: str,
        credit_policy: str | None = None,
    ) -> PendingShare:
        runtime = self._runtime
        share_difficulty = runtime.accepted_share_difficulty(context)
        # Assign accepted_at_ms and register the commit-floor entry under one
        # lock hold: a snapshot anchored between assignment and registration
        # could otherwise anchor at or above this share and miss its later
        # commit. Released via _finish_pending_share_commit.
        with self._pending_share_commit_lock:
            pending = PendingShare(
                share_id=f"{context.worker.username}:{submission.block_hash_hex}",
                miner_id=context.worker.payout_address,
                order_key=context.worker.payout_address,
                p2mr_program_hex=context.worker.p2mr_program_hex,
                share_difficulty=share_difficulty,
                network_difficulty=max(1, int(context.found_block["network_difficulty"])),
                template_height=int(context.template["height"]) - 1,
                job_id=context.job.job_id,
                job_issued_at_ms=context.issued_at_ms,
                accepted_at_ms=self._now_ms(),
                ntime=int(ntime_hex, 16),
                credit_policy=credit_policy,
            )
            self._pending_share_commit_floor[id(pending)] = [
                pending,
                time.monotonic(),
                False,
            ]
        return pending

    # -- append paths ------------------------------------------------------

    def append_accepted_share(
        self,
        client: Any,
        context: Any,
        submission: Any,
        pending_share: PendingShare,
        *,
        credit_policy: str | None = None,
        candidate_intent: dict[str, Any] | None = None,
    ) -> str | None:
        runtime = self._runtime
        entry = PendingShareAppend(
            pending_share=pending_share,
            username=context.worker.username,
            job_id=context.job.job_id,
            block_hash_hex=submission.block_hash_hex,
            collection_only=bool(context.collection_only),
            credit_policy=credit_policy,
            candidate_intent=candidate_intent,
        )
        try:
            if getattr(self, "share_writer_active", False):
                runtime.enqueue_share_append(entry, wait=True)
            else:
                runtime._append_share_entry(entry)
        finally:
            # The append reached a terminal outcome for this process: durably
            # committed, or surfaced an error the miner will retry with a fresh
            # share. Either way the stamped share no longer holds the snapshot
            # anchor floor. Idempotent with the group-commit writer's release.
            runtime._finish_pending_share_commit(pending_share)
        record = entry.record
        # Exact ledger replays return the original record. Their durable share
        # must not increment process-local worker/vardiff counters again.
        if record is None or bool(getattr(record, "newly_inserted", True)):
            runtime.note_worker_accepted_share(context.worker.username, credit_policy)
            runtime.note_vardiff_accepted_share(client, context.job)
        if candidate_intent is None or record is None:
            return None
        candidate_state = getattr(record, "candidate_outbox_state", None)
        return str(candidate_state) if candidate_state is not None else None

    def enqueue_share_append(
        self, entry: PendingShareAppend, *, wait: bool = False
    ) -> None:
        runtime = self._runtime
        queue_obj = getattr(self, "share_append_queue", None)
        if queue_obj is None:
            queue_obj = queue.Queue(maxsize=MAX_PENDING_SHARE_APPENDS)
            self.share_append_queue = queue_obj
        if entry.writer_token is None:
            entry.writer_token = runtime._ensure_shutdown_controller().reserve_writer(
                "share_persistence"
            )
        try:
            if wait:
                queue_obj.put(
                    entry,
                    timeout=getattr(self, "share_commit_timeout_seconds", 15.0),
                )
            else:
                queue_obj.put_nowait(entry)
        except queue.Full:
            entry.writer_token.finish()
            entry.writer_token = None
            raise StratumError(
                20,
                "share ledger commit queue is full",
                reason=self._internal_error_reason,
            )
        if not wait:
            return
        # Once admitted, wait for a definite transaction outcome. A local
        # timeout is ambiguous because Postgres may commit immediately after
        # it; the liveness watchdog owns recovery from a wedged writer.
        entry.committed.wait()
        if entry.error is not None:
            raise StratumError(
                20,
                f"share ledger commit failed: {entry.error}",
                reason=self._internal_error_reason,
            )

    def share_append_loop(self) -> None:
        runtime = self._runtime
        while True:
            runtime._record_heartbeat("share_writer")
            queue_obj = getattr(self, "share_append_queue", None)
            if queue_obj is None:
                queue_obj = queue.Queue(maxsize=MAX_PENDING_SHARE_APPENDS)
                self.share_append_queue = queue_obj
            stopping = runtime.stop_event.is_set()
            try:
                entry = queue_obj.get(timeout=0.2 if stopping else 1.0)
            except queue.Empty:
                controller = runtime._ensure_shutdown_controller()
                if (
                    stopping
                    and controller.writer_admission_closed()
                    and not controller.has_active_writer(set(_WRITER_EXIT_COMPONENTS))
                ):
                    return
                continue
            batch = [entry]
            batch_size = max(1, int(getattr(self, "share_commit_batch_size", 64)))
            deadline = time.monotonic() + max(
                0.0, float(getattr(self, "share_commit_linger_seconds", 0.005))
            )
            if entry.candidate_intent is not None:
                deadline = time.monotonic()
            while len(batch) < batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    next_entry = queue_obj.get(timeout=remaining)
                    batch.append(next_entry)
                    if next_entry.candidate_intent is not None:
                        break
                except queue.Empty:
                    break
            runtime._append_share_batch(batch)

    def append_share_batch(self, batch: list[PendingShareAppend]) -> bool:
        """Commit a writer batch, then release every waiting submitter."""
        runtime = self._runtime
        try:
            invalidations: list[tuple[PendingShare, int]] = []
            # A batch holding a replay-shaped row commits under the landing
            # fence: the durable append and its epoch bump must land on the
            # same side of a landing's fence-guarded submit, or the landing
            # can verify the pre-bump epoch after the row is already durable
            # (see _landing_fence_for_predating_append).
            with runtime._landing_fence_for_predating_append(
                [entry.pending_share for entry in batch]
            ) as fence_owned:
                append_batch = getattr(runtime.ledger, "append_batch", None)
                if callable(append_batch):
                    records = append_batch(
                        [(entry.pending_share, entry.candidate_intent) for entry in batch]
                    )
                else:
                    # Compatibility for lightweight test/tool ledgers. Production's
                    # Postgres ledger always supplies the atomic batch method.
                    records = [runtime.ledger.append(entry.pending_share) for entry in batch]
                if len(records) != len(batch):
                    raise RuntimeError("share ledger returned an incomplete commit batch")
                for entry, record in zip(batch, records, strict=True):
                    entry.record = record
                    invalidation_epoch = runtime._record_late_visible_payout_append(
                        entry.pending_share,
                        landing_fence_owned=fence_owned,
                    )
                    if invalidation_epoch is not None:
                        invalidations.append(
                            (entry.pending_share, invalidation_epoch)
                        )
            for pending_share, invalidation_epoch in invalidations:
                runtime._retire_payout_windows_for_late_append(
                    pending_share, invalidation_epoch
                )
            if getattr(runtime, "hot_path_log_enabled", False):
                for entry in batch:
                    record = entry.record
                    print(
                        "prism coordinator: accepted share "
                        f"seq={record.share_seq} miner={entry.username} job={entry.job_id} "
                        f"hash={entry.block_hash_hex} collection={entry.collection_only} "
                        f"credit_policy={entry.credit_policy or 'normal'}",
                        flush=True,
                    )
            return True
        except Exception as exc:
            runtime._ensure_share_hot_path_state()
            with runtime._share_accounting_lock:
                self.share_append_failure_count = (
                    int(getattr(self, "share_append_failure_count", 0)) + len(batch)
                )
            for entry in batch:
                entry.error = exc
            print(
                f"prism coordinator: share ledger group commit failed count={len(batch)}",
                flush=True,
            )
            traceback.print_exc()
            return False
        finally:
            for entry in batch:
                runtime._finish_pending_share_commit(entry.pending_share)
                entry.committed.set()
                if entry.writer_token is not None:
                    entry.writer_token.finish()
                    entry.writer_token = None

    def recover_share_to_disk(self, entry: PendingShareAppend, reason: str) -> None:
        """Durably capture an acked share the writer could not persist.

        Appends the canonical pending-share JSON to the recovery file (fsynced)
        so a ledger outage or shutdown never silently loses a share the miner
        was told was accepted; replayed on the next start. Best-effort: if even
        the recovery write fails, log loudly rather than raise on the writer.
        """
        path = getattr(self, "share_recovery_path", None)
        if path is None:
            print(
                "prism coordinator: WOULD LOSE acked share (no recovery path) "
                f"share_id={entry.pending_share.share_id} reason={reason}",
                flush=True,
            )
            return
        try:
            payload = json.dumps(dataclasses.asdict(entry.pending_share), separators=(",", ":"))
        except Exception:
            payload = None
        with getattr(self, "share_recovery_lock", threading.Lock()):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                if payload is None:
                    raise ValueError("pending share is not serializable")
                with open(path, "a", encoding="utf-8") as handle:
                    handle.write(payload + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                self.shares_recovered_to_disk = (
                    int(getattr(self, "shares_recovered_to_disk", 0)) + 1
                )
                print(
                    "prism coordinator: recovered unpersisted acked share to disk "
                    f"share_id={entry.pending_share.share_id} reason={reason}",
                    flush=True,
                )
            except Exception:
                print(
                    "prism coordinator: FAILED to recover acked share to disk; "
                    f"share may be lost share_id={entry.pending_share.share_id} reason={reason}",
                    flush=True,
                )
                traceback.print_exc()

    def replay_recovered_shares(self) -> int:
        """Replay any recovery-file shares into the ledger at startup.

        Ledgers with typed recovery support report each row's disposition
        through ``share_ledger.append_recovered_share``: an ``inserted`` row
        counts as replayed, an ``exact_existing`` row is an already-committed
        duplicate from an earlier partial replay (skipped, not
        double-counted). A :class:`ShareReplayConflict` is keyed to one
        share_id, so the conflicting row is quarantined -- counted, logged,
        and preserved in the retained journal -- while the remaining rows
        keep replaying instead of stranding behind it; an unknown disposition
        conservatively stops and keeps the journal file for inspection. Only
        a clean pass may remove the journal, so a transient failure here
        never drops shares. Embedder ledgers without ``append_recovered_share`` keep the
        historical ``append`` path whose duplicate-share_id error is treated
        as an exact replay. Writer admission stays with the coordinator
        facade's ``ledger_writer_operation`` decorator.
        """
        runtime = self._runtime
        path = getattr(self, "share_recovery_path", None)
        if path is None or not path.exists():
            return 0
        try:
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        except Exception:
            print("prism coordinator: could not read share recovery file", flush=True)
            traceback.print_exc()
            return 0
        # Parse line-by-line and skip any single unparseable line rather than
        # aborting the whole replay: a crash mid-append can leave the last line
        # torn, and one torn line must not block the intact shares before it.
        pendings: list[PendingShare] = []
        parse_failed = False
        for line in lines:
            try:
                pendings.append(PendingShare(**json.loads(line)))
            except Exception:
                parse_failed = True
                print("prism coordinator: skipping an unparseable recovered share line", flush=True)
                traceback.print_exc()
        # Replay in acceptance order. A share recovered out of FIFO order (a
        # ledger flap during the shutdown drain, or an overflow-recovered newest
        # share) otherwise sorts by file order; ordering by accepted_at_ms lands
        # each share with a share_seq consistent with when it was accepted, so
        # the reward window stays correctly ordered.
        pendings.sort(key=lambda pending: pending.accepted_at_ms)
        replayed = 0
        skipped_duplicates = 0
        replay_conflicts = 0
        for pending in pendings:
            append_recovered = getattr(
                runtime.ledger, "append_recovered_share", None
            )
            try:
                # A recovered row reconstructs its pre-crash timestamps, so
                # it routinely predates a live anchor; each row commits under
                # the landing fence with its epoch bump so a concurrent
                # landing can never verify a pre-bump epoch against an
                # already-durable replayed share. The window retirement runs
                # after the fence releases, per row, so replay never holds
                # the fence across the prepare-lock wait.
                invalidation_epoch: int | None = None
                with runtime._landing_fence_for_predating_append(
                    [pending]
                ) as fence_owned:
                    if callable(append_recovered):
                        result = append_recovered(pending)
                        disposition = getattr(result, "disposition", None)
                        if disposition == "exact_existing":
                            # Already committed by an earlier (partial)
                            # replay; treat it as done so a retry never
                            # stops on the first committed row and strands
                            # every share after it. The durable row already
                            # bumped visibility on its original commit, so
                            # no new epoch bump happens here.
                            skipped_duplicates += 1
                            continue
                        if disposition != "inserted":
                            print(
                                "prism coordinator: recovery ledger returned an "
                                f"unsupported replay disposition {disposition!r}; "
                                "keeping the file",
                                flush=True,
                            )
                            self.shares_replayed = (
                                int(getattr(self, "shares_replayed", 0)) + replayed
                            )
                            return replayed
                    else:
                        runtime.ledger.append(pending)
                    invalidation_epoch = runtime._record_late_visible_payout_append(
                        pending,
                        landing_fence_owned=fence_owned,
                    )
                if invalidation_epoch is not None:
                    runtime._retire_payout_windows_for_late_append(
                        pending, invalidation_epoch
                    )
                replayed += 1
            except ShareReplayConflict:
                # The durable row disagrees with the journal payload. The
                # conflict is keyed to this one share_id and says nothing
                # about the rows after it, so quarantine the row -- count it
                # and keep the file for inspection -- and continue replaying
                # the rest of the journal instead of stranding valid shares
                # behind it. Exact duplicates replay idempotently, so a
                # re-run of the retained file never double-credits.
                replay_conflicts += 1
                self.share_replay_conflicts = (
                    int(getattr(self, "share_replay_conflicts", 0)) + 1
                )
                print(
                    "prism coordinator: recovered share conflicts with durable "
                    "payload; quarantining the row and keeping the file "
                    f"share_id={pending.share_id}",
                    flush=True,
                )
                continue
            except Exception as exc:
                if not callable(append_recovered) and "duplicate share_id" in str(exc):
                    # Historical string-matched idempotence for embedder
                    # ledgers without typed replay support.
                    skipped_duplicates += 1
                    continue
                print("prism coordinator: failed to replay a recovered share; keeping the file", flush=True)
                traceback.print_exc()
                self.shares_replayed = int(getattr(self, "shares_replayed", 0)) + replayed
                return replayed
        if skipped_duplicates:
            print(
                f"prism coordinator: skipped {skipped_duplicates} already-committed "
                "recovered share(s) during replay",
                flush=True,
            )
        if replay_conflicts:
            print(
                f"prism coordinator: quarantined {replay_conflicts} conflicting "
                "recovered share(s) during replay; keeping the file",
                flush=True,
            )
        if parse_failed or replay_conflicts:
            # Keep the file (with its intact-but-already-replayed lines, which
            # the ledger dedups on a re-run) so the torn or conflicting line is
            # preserved for inspection rather than silently discarded.
            self.shares_replayed = int(getattr(self, "shares_replayed", 0)) + replayed
            if replayed:
                print(f"prism coordinator: replayed {replayed} recovered share(s) into the ledger", flush=True)
            return replayed
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        self.shares_replayed = int(getattr(self, "shares_replayed", 0)) + replayed
        if replayed:
            print(f"prism coordinator: replayed {replayed} recovered share(s) into the ledger", flush=True)
        return replayed

    def append_share_entry(
        self, entry: PendingShareAppend, *, retry_until_stopped: bool = False
    ) -> bool:
        """Synchronously append one accepted share.

        On the writer thread a transient ledger failure retries with capped
        backoff so ordering is preserved and nothing is silently lost; the
        synchronous path (no writer) propagates the exception exactly as the
        pre-async code did.

        Returns True when the share was persisted to the ledger, or False when
        it was recovered to disk instead (ledger still down at shutdown). The
        caller uses that to keep the shutdown drain in order.
        """
        runtime = self._runtime
        backoff_seconds = 0.5
        invalidation_epoch: int | None = None
        while True:
            try:
                # A replay-shaped row commits under the landing fence with
                # its epoch bump (see _landing_fence_for_predating_append).
                # The fence wraps one attempt only: the backoff sleep below
                # runs outside it, so a retrying ledger outage never holds
                # up a landing.
                with runtime._landing_fence_for_predating_append(
                    [entry.pending_share]
                ) as fence_owned:
                    append_batch = getattr(runtime.ledger, "append_batch", None)
                    if callable(append_batch):
                        record = append_batch(
                            [(entry.pending_share, entry.candidate_intent)]
                        )[0]
                    else:
                        record = runtime.ledger.append(entry.pending_share)
                    entry.record = record
                    invalidation_epoch = runtime._record_late_visible_payout_append(
                        entry.pending_share,
                        landing_fence_owned=fence_owned,
                    )
                break
            except Exception:
                if not retry_until_stopped:
                    raise
                runtime._ensure_share_hot_path_state()
                with runtime._share_accounting_lock:
                    self.share_append_failure_count = (
                        int(getattr(self, "share_append_failure_count", 0)) + 1
                    )
                print(
                    "prism coordinator: ledger share append failed; retrying "
                    f"share_id={entry.pending_share.share_id}",
                    flush=True,
                )
                traceback.print_exc()
                if runtime.stop_event.wait(backoff_seconds):
                    # Shutting down mid-outage: do not silently drop this
                    # already-acked, already-counted share -- recover it to
                    # disk for replay on the next start.
                    runtime._recover_share_to_disk(entry, "ledger unavailable at shutdown")
                    return False
                backoff_seconds = min(backoff_seconds * 2, 5.0)
                runtime._record_heartbeat("share_writer")
        if invalidation_epoch is not None:
            runtime._retire_payout_windows_for_late_append(
                entry.pending_share, invalidation_epoch
            )
        if getattr(runtime, "hot_path_log_enabled", False):
            print(
                "prism coordinator: accepted share "
                f"seq={record.share_seq} miner={entry.username} job={entry.job_id} "
                f"hash={entry.block_hash_hex} collection={entry.collection_only} "
                f"credit_policy={entry.credit_policy or 'normal'}",
                flush=True,
            )
        entry.committed.set()
        return True


class ShareWriterCompatibilityField:
    """Route one legacy S3 coordinator field to its owner service.

    The descriptor keeps the historical attribute name readable and writable
    on the coordinator while :class:`ShareWriter` owns the single mutable
    copy, mirroring the S1/S2/J1/G1 extraction pattern. Direct test
    assignment through the legacy name is thereby adopted by the owner.
    """

    def __init__(self, attribute: str) -> None:
        self._attribute = attribute

    def __get__(
        self,
        instance: Any,
        owner: type | None = None,
    ) -> Any:
        if instance is None:
            return self
        return getattr(
            instance._ensure_share_writer_service(),
            self._attribute,
        )

    def __set__(self, instance: Any, value: Any) -> None:
        setattr(
            instance._ensure_share_writer_service(),
            self._attribute,
            value,
        )


__all__ = [
    "MAX_PENDING_SHARE_APPENDS",
    "PENDING_SHARE_COMMIT_WARN_SECONDS",
    "PendingShareAppend",
    "ShareWriter",
    "ShareWriterCompatibilityField",
    "ShareWriterError",
    "ShareWriterQueueFull",
    "ShareWriterRuntime",
]
