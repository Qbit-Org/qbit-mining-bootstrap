#!/usr/bin/env python3
"""Direct tests for the coordinator-free S3 share writer service.

The service resolves the coordinator through a call-time ``ShareWriterRuntime``
typed port, so these tests drive it with a small recording runtime plus the
real shutdown controller and (mostly) the real in-memory ledger.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import queue
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from lab.prism.coordinator_shutdown import (
    CoordinatorShutdownController,
    ShutdownInProgress,
)
from lab.prism.share_ledger import (
    PendingShare,
    ShareReplayResult,
    SingleWriterShareLedger,
)
from lab.prism.share_writer import (
    PENDING_SHARE_COMMIT_WARN_SECONDS,
    PendingShareAppend,
    ShareWriter,
)
from lab.prism.stratum_session import StratumError


class RecordingRuntime:
    """Minimal call-time runtime port mirroring the coordinator facades."""

    def __init__(
        self,
        *,
        ledger: object,
        controller: CoordinatorShutdownController,
        stop_event: threading.Event,
    ) -> None:
        self.ledger = ledger
        self.hot_path_log_enabled = False
        self.stop_event = stop_event
        self._controller = controller
        self._share_accounting_lock = threading.Lock()
        self.writer: ShareWriter | None = None
        self.worker_notes: list[tuple[str, str | None]] = []
        self.vardiff_notes: list[tuple[object, object]] = []
        self.retired: list[tuple[PendingShare, int]] = []

    def _ensure_shutdown_controller(self) -> CoordinatorShutdownController:
        return self._controller

    def _record_heartbeat(self, _name: str, *, phase: str | None = None) -> None:
        return None

    def _ensure_share_hot_path_state(self) -> None:
        return None

    def _finish_pending_share_commit(self, pending: PendingShare) -> None:
        assert self.writer is not None
        self.writer.finish_pending_share(pending)

    @contextmanager
    def _landing_fence_for_predating_append(self, _pendings: list[PendingShare]):
        yield False

    def _record_late_visible_payout_append(
        self,
        _pending: PendingShare,
        *,
        landing_fence_owned: bool,
    ) -> int | None:
        return None

    def _retire_payout_windows_for_late_append(
        self, pending: PendingShare, epoch: int
    ) -> None:
        self.retired.append((pending, epoch))

    def _recover_share_to_disk(self, entry: PendingShareAppend, reason: str) -> None:
        assert self.writer is not None
        self.writer.recover_share_to_disk(entry, reason)

    def accepted_share_difficulty(self, _context: object) -> int:
        return 1

    def enqueue_share_append(
        self, entry: PendingShareAppend, *, wait: bool = False
    ) -> None:
        assert self.writer is not None
        self.writer.enqueue_share_append(entry, wait=wait)

    def _append_share_entry(self, entry: PendingShareAppend) -> bool:
        assert self.writer is not None
        return self.writer.append_share_entry(entry)

    def note_worker_accepted_share(
        self, username: str, credit_policy: str | None
    ) -> None:
        self.worker_notes.append((username, credit_policy))

    def note_vardiff_accepted_share(self, client: object, job: object) -> None:
        self.vardiff_notes.append((client, job))


class ShareWriterServiceTests(unittest.TestCase):
    def _service(
        self,
        *,
        ledger: object | None = None,
        wall_times: list[int] | None = None,
        controller: CoordinatorShutdownController | None = None,
    ) -> tuple[ShareWriter, object, CoordinatorShutdownController, RecordingRuntime]:
        ledger = ledger if ledger is not None else SingleWriterShareLedger()
        controller = controller or CoordinatorShutdownController(1.0)
        stop = threading.Event()
        times = list(wall_times or [100])
        last = times[-1]

        def wall_time_ms() -> int:
            return times.pop(0) if times else last

        runtime = RecordingRuntime(
            ledger=ledger,
            controller=controller,
            stop_event=stop,
        )
        service = ShareWriter(
            runtime,
            internal_error_reason="internal_error",
            now_ms=wall_time_ms,
        )
        runtime.writer = service
        return service, ledger, controller, runtime

    @staticmethod
    def _context(username: str = "miner-a") -> SimpleNamespace:
        return SimpleNamespace(
            worker=SimpleNamespace(
                username=username,
                payout_address="miner-a",
                p2mr_program_hex="11" * 32,
            ),
            found_block={"network_difficulty": 1},
            template={"height": 10},
            job=SimpleNamespace(job_id="job-1"),
            issued_at_ms=1,
            collection_only=False,
        )

    @staticmethod
    def _submission(block_hash_hex: str = "ab" * 32) -> SimpleNamespace:
        return SimpleNamespace(block_hash_hex=block_hash_hex)

    def _stamp(self, service: ShareWriter, block_hash_hex: str = "ab" * 32) -> PendingShare:
        return service.pending_share_from_submission(
            context=self._context(),
            submission=self._submission(block_hash_hex),
            ntime_hex="65000000",
        )

    @staticmethod
    def _entry(pending: PendingShare) -> PendingShareAppend:
        return PendingShareAppend(
            pending_share=pending,
            username="miner-a",
            job_id="job-1",
            block_hash_hex=pending.share_id.rsplit(":", 1)[-1],
            collection_only=False,
            credit_policy=None,
        )

    @staticmethod
    def _pending(share_id: str, *, accepted_at_ms: int = 100, ntime: int = 1_700_000_000) -> PendingShare:
        return PendingShare(
            share_id=share_id,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=accepted_at_ms,
            ntime=ntime,
        )

    # -- compatibility identity and descriptor routing ---------------------

    def test_coordinator_reexports_exact_pending_append_identity(self) -> None:
        from lab.prism.prism_coordinator import PendingShareAppend as compatibility

        self.assertIs(compatibility, PendingShareAppend)

    def test_legacy_coordinator_fields_route_to_the_service_owner(self) -> None:
        from lab.prism.prism_coordinator import PrismCoordinator

        server = PrismCoordinator.__new__(PrismCoordinator)
        service = server._ensure_share_writer_service()

        self.assertIs(server.share_append_queue, service.share_append_queue)
        self.assertIs(
            server._pending_share_commit_floor,
            service._pending_share_commit_floor,
        )

        server.share_commit_batch_size = 7
        self.assertEqual(service.share_commit_batch_size, 7)

        adopted: queue.Queue[PendingShareAppend] = queue.Queue(maxsize=3)
        server.share_append_queue = adopted
        self.assertIs(service.share_append_queue, adopted)

        server.share_writer_active = True
        self.assertTrue(service.share_writer_active)

    # -- pending-commit floor ----------------------------------------------

    def test_submission_stamp_registers_floor_and_anchor_clamps_below(self) -> None:
        service, _ledger, _controller, _runtime = self._service(wall_times=[100])

        pending = self._stamp(service)

        self.assertEqual(pending.accepted_at_ms, 100)
        entry = service._pending_share_commit_floor[id(pending)]
        self.assertIs(entry[0], pending)
        self.assertEqual(len(entry), 3)
        self.assertFalse(entry[2])
        self.assertEqual(service.snapshot_anchor_ms(1_000), 99)

        service.finish_pending_share(pending)
        self.assertEqual(service._pending_share_commit_floor, {})
        self.assertEqual(service.snapshot_anchor_ms(1_000), 999)

    def test_floor_release_is_idempotent_and_keyed_by_object_identity(self) -> None:
        # Layer note: the current tree keeps the id-keyed holder (the old
        # stack's durable share-id lease was deliberately not built), so a
        # reconstructed object with the same durable identity does not
        # release the original stamp's floor entry.
        service, _ledger, _controller, _runtime = self._service(wall_times=[100])
        pending = self._stamp(service)

        reconstructed = PendingShare(**pending.__dict__)
        service.finish_pending_share(reconstructed)
        self.assertIn(id(pending), service._pending_share_commit_floor)
        self.assertEqual(service.snapshot_anchor_ms(1_000), 99)

        service.finish_pending_share(pending)
        self.assertEqual(service._pending_share_commit_floor, {})
        service.finish_pending_share(pending)
        self.assertEqual(service._pending_share_commit_floor, {})

    def test_attempt_and_candidate_release_share_one_current_holder(self) -> None:
        # finish_pending_attempt / finish_pending_candidate are the staged
        # future split seams; at this layer both release the single holder.
        service, _ledger, _controller, _runtime = self._service(wall_times=[100, 200])

        attempt = self._stamp(service)
        service.finish_pending_attempt(attempt)
        self.assertEqual(service._pending_share_commit_floor, {})

        candidate = self._stamp(service, "cd" * 32)
        service.finish_pending_candidate(candidate)
        self.assertEqual(service._pending_share_commit_floor, {})

    def test_stale_floor_entry_warns_once_and_keeps_holding_the_anchor(self) -> None:
        service, _ledger, _controller, _runtime = self._service()
        pending = self._pending("miner-a:stale", accepted_at_ms=100)
        service._pending_share_commit_floor[id(pending)] = [
            pending,
            time.monotonic() - PENDING_SHARE_COMMIT_WARN_SECONDS - 1.0,
            False,
        ]

        with patch("builtins.print") as first_pass:
            self.assertEqual(service.snapshot_anchor_ms(1_000), 99)
        warned = [
            call
            for call in first_pass.call_args_list
            if "holding the job snapshot anchor floor" in str(call)
        ]
        self.assertEqual(len(warned), 1)
        self.assertIn("miner-a:stale", str(warned[0]))
        self.assertTrue(service._pending_share_commit_floor[id(pending)][2])

        with patch("builtins.print") as second_pass:
            self.assertEqual(service.snapshot_anchor_ms(1_000), 99)
        self.assertEqual(second_pass.call_args_list, [])

    # -- append admission and floor release --------------------------------

    def test_full_queue_admission_refusal_releases_token_and_floor(self) -> None:
        service, _ledger, controller, _runtime = self._service()
        service.share_writer_active = True
        service.share_commit_timeout_seconds = 0.05
        service.share_append_queue = queue.Queue(maxsize=1)
        blocker = self._entry(self._pending("miner-a:blocker"))
        blocker.writer_token = controller.reserve_writer("share_persistence")
        service.share_append_queue.put_nowait(blocker)
        pending = self._stamp(service)

        with self.assertRaisesRegex(StratumError, "commit queue is full"):
            service.append_accepted_share(
                object(),
                self._context(),
                self._submission(),
                pending,
            )

        self.assertEqual(service._pending_share_commit_floor, {})
        self.assertEqual(
            controller.snapshot()["active_writers"],
            {"share_persistence": 1},
        )
        blocker.writer_token.finish()
        self.assertEqual(controller.snapshot()["active_writers"], {})

    def test_closed_writer_admission_refusal_releases_unhanded_attempt(self) -> None:
        controller = CoordinatorShutdownController(1.0)
        service, ledger, _controller, runtime = self._service(controller=controller)
        service.share_writer_active = True
        pending = self._stamp(service)
        controller.begin_shutdown("test shutdown")

        with self.assertRaises(ShutdownInProgress):
            service.append_accepted_share(
                object(),
                self._context(),
                self._submission(),
                pending,
            )

        self.assertEqual(service._pending_share_commit_floor, {})
        self.assertEqual(len(ledger), 0)
        self.assertEqual(runtime.worker_notes, [])
        self.assertEqual(controller.snapshot()["active_writers"], {})

    def test_nested_append_inherits_outer_admission_after_shutdown_closes(self) -> None:
        controller = CoordinatorShutdownController(1.0)
        service, ledger, _controller, _runtime = self._service(controller=controller)
        service.share_writer_active = True
        outer = controller.enter_writer("share_submission")
        try:
            pending = self._stamp(service)
            controller.begin_shutdown("test shutdown")
            entry = self._entry(pending)
            service.enqueue_share_append(entry)
        finally:
            controller.exit_writer(outer)

        self.assertIsNotNone(entry.writer_token)
        queued = service.share_append_queue.get_nowait()
        self.assertIs(queued, entry)
        self.assertTrue(service.append_share_batch([queued]))
        self.assertEqual(len(ledger), 1)
        self.assertTrue(entry.committed.is_set())
        self.assertIsNone(entry.writer_token)
        self.assertEqual(service._pending_share_commit_floor, {})
        self.assertEqual(controller.snapshot()["active_writers"], {})

    def test_synchronous_append_releases_floor_and_never_recounts_replays(self) -> None:
        service, ledger, _controller, runtime = self._service()
        service.share_writer_active = False
        pending = self._stamp(service)

        service.append_accepted_share(
            object(),
            self._context(),
            self._submission(),
            pending,
        )

        self.assertEqual(len(ledger), 1)
        self.assertEqual(service._pending_share_commit_floor, {})
        self.assertEqual(runtime.worker_notes, [("miner-a", None)])
        self.assertEqual(len(runtime.vardiff_notes), 1)

        # An exact ledger replay returns the original record; process-local
        # worker/vardiff counters must not increment again.
        service.append_accepted_share(
            object(),
            self._context(),
            self._submission(),
            pending,
        )
        self.assertEqual(len(ledger), 1)
        self.assertEqual(runtime.worker_notes, [("miner-a", None)])
        self.assertEqual(len(runtime.vardiff_notes), 1)

    def test_batch_success_sets_records_and_releases_every_waiter(self) -> None:
        service, ledger, controller, _runtime = self._service(wall_times=[100, 200])
        first = self._entry(self._stamp(service, "aa" * 32))
        second = self._entry(self._stamp(service, "bb" * 32))
        for entry in (first, second):
            entry.writer_token = controller.reserve_writer("share_persistence")

        self.assertTrue(service.append_share_batch([first, second]))

        self.assertEqual(len(ledger), 2)
        for entry in (first, second):
            self.assertTrue(entry.committed.is_set())
            self.assertIsNone(entry.error)
            self.assertIsNotNone(entry.record)
            self.assertIsNone(entry.writer_token)
        self.assertEqual(service._pending_share_commit_floor, {})
        self.assertEqual(controller.snapshot()["active_writers"], {})

    def test_batch_failure_marks_entries_and_still_releases_everything(self) -> None:
        class FailingLedger:
            def append_batch(self, _entries: object) -> list[object]:
                raise RuntimeError("ledger unavailable")

        service, _ledger, controller, _runtime = self._service(
            ledger=FailingLedger(),
            wall_times=[100, 200],
        )
        first = self._entry(self._stamp(service, "aa" * 32))
        second = self._entry(self._stamp(service, "bb" * 32))
        for entry in (first, second):
            entry.writer_token = controller.reserve_writer("share_persistence")

        with patch("builtins.print"), patch("traceback.print_exc"):
            self.assertFalse(service.append_share_batch([first, second]))

        self.assertEqual(service.share_append_failure_count, 2)
        for entry in (first, second):
            self.assertTrue(entry.committed.is_set())
            self.assertIsInstance(entry.error, RuntimeError)
            self.assertIsNone(entry.writer_token)
        self.assertEqual(service._pending_share_commit_floor, {})
        self.assertEqual(controller.snapshot()["active_writers"], {})

    def test_append_loop_exits_only_when_admission_closed_and_writers_drained(
        self,
    ) -> None:
        service, _ledger, controller, runtime = self._service()
        runtime.stop_event.set()
        exited = threading.Event()

        def run_loop() -> None:
            service.share_append_loop()
            exited.set()

        loop = threading.Thread(target=run_loop, daemon=True)
        loop.start()
        # Admission still open: the stopping loop keeps polling for late
        # writer handoffs instead of abandoning queued shares.
        self.assertFalse(exited.wait(0.4))

        token = controller.reserve_writer("share_submission")
        controller.begin_shutdown("test shutdown")
        # Admission is closed but a submitting writer is still active.
        self.assertFalse(exited.wait(0.4))

        token.finish()
        self.assertTrue(exited.wait(2.0))
        loop.join(timeout=2.0)
        self.assertFalse(loop.is_alive())

    # -- recovery journal and typed replay ---------------------------------

    def test_recovered_share_replays_inserted_and_unlinks_clean_journal(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, ledger, _controller, _runtime = self._service()
            service.share_recovery_path = path
            pending = self._stamp(service)
            with patch("builtins.print"):
                service.recover_share_to_disk(self._entry(pending), "test")
            self.assertEqual(service.shares_recovered_to_disk, 1)

            with patch("builtins.print"):
                self.assertEqual(service.replay_recovered_shares(), 1)

            self.assertFalse(path.exists())
            self.assertEqual(service.shares_replayed, 1)
            self.assertEqual(len(ledger), 1)
            self.assertEqual(
                [record.share_id for record in ledger.all_shares()],
                [pending.share_id],
            )

    def test_recovery_exact_existing_is_typed_skip_and_clears_clean_journal(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, ledger, _controller, _runtime = self._service()
            service.share_recovery_path = path
            pending = self._stamp(service)
            with patch("builtins.print"):
                service.recover_share_to_disk(self._entry(pending), "test")
            replay = ledger.append_recovered_share(pending)
            self.assertEqual(replay.disposition, "inserted")

            with patch("builtins.print"):
                self.assertEqual(service.replay_recovered_shares(), 0)

            self.assertFalse(path.exists())
            self.assertEqual(len(ledger), 1)

    def test_recovery_payload_conflict_is_typed_and_retains_journal(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, ledger, _controller, _runtime = self._service()
            service.share_recovery_path = path
            pending = self._stamp(service)
            with patch("builtins.print"):
                service.recover_share_to_disk(self._entry(pending), "test")
            ledger.append_recovered_share(
                PendingShare(**{**pending.__dict__, "ntime": pending.ntime + 1})
            )

            with patch("builtins.print"):
                self.assertEqual(service.replay_recovered_shares(), 0)

            self.assertTrue(path.exists())
            self.assertEqual(service.shares_replayed, 0)
            self.assertEqual(service.share_replay_conflicts, 1)

    def test_conflicting_row_is_quarantined_and_rest_of_journal_replays(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, ledger, _controller, _runtime = self._service()
            service.share_recovery_path = path
            conflicting = self._stamp(service, "ab" * 32)
            later_a = self._stamp(service, "ac" * 32)
            later_b = self._stamp(service, "ad" * 32)
            with patch("builtins.print"):
                service.recover_share_to_disk(self._entry(conflicting), "test")
                service.recover_share_to_disk(self._entry(later_a), "test")
                service.recover_share_to_disk(self._entry(later_b), "test")
            # The durable copy of the first row disagrees with the journal
            # payload, so its replay raises ShareReplayConflict.
            ledger.append_recovered_share(
                PendingShare(
                    **{**conflicting.__dict__, "ntime": conflicting.ntime + 1}
                )
            )

            with patch("builtins.print"):
                self.assertEqual(service.replay_recovered_shares(), 2)

            # The rows behind the conflict are credited; the conflicting row
            # is quarantined -- counted and preserved in the retained file --
            # instead of stranding the rest of the journal.
            self.assertTrue(path.exists())
            self.assertEqual(service.shares_replayed, 2)
            self.assertEqual(service.share_replay_conflicts, 1)
            self.assertEqual(len(ledger), 3)
            replayed_ids = {record.share_id for record in ledger.all_shares()}
            self.assertIn(later_a.share_id, replayed_ids)
            self.assertIn(later_b.share_id, replayed_ids)

            # Re-running the retained journal double-credits nothing: the
            # replayed rows are exact duplicates now and the conflicting row
            # conflicts again.
            with patch("builtins.print"):
                self.assertEqual(service.replay_recovered_shares(), 0)

            self.assertTrue(path.exists())
            self.assertEqual(service.shares_replayed, 2)
            self.assertEqual(service.share_replay_conflicts, 2)
            self.assertEqual(len(ledger), 3)

    def test_unknown_recovery_disposition_is_conservative_and_retains_journal(
        self,
    ) -> None:
        class FutureLedger:
            def append_recovered_share(self, pending: PendingShare) -> ShareReplayResult:
                return ShareReplayResult(
                    "future-disposition",
                    SimpleNamespace(share_seq=1, share_id=pending.share_id),
                )

        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, _ledger, _controller, _runtime = self._service(
                ledger=FutureLedger(),
            )
            service.share_recovery_path = path
            pending = self._stamp(service)
            with patch("builtins.print"):
                service.recover_share_to_disk(self._entry(pending), "test")

            with patch("builtins.print"):
                self.assertEqual(service.replay_recovered_shares(), 0)

            self.assertTrue(path.exists())

    def test_untyped_append_only_ledger_keeps_legacy_duplicate_fallback(self) -> None:
        class LegacyLedger:
            def __init__(self) -> None:
                self.appended: list[PendingShare] = []

            def append(self, pending: PendingShare) -> SimpleNamespace:
                for existing in self.appended:
                    if existing.share_id == pending.share_id:
                        raise RuntimeError(
                            f"duplicate share_id: {pending.share_id}"
                        )
                self.appended.append(pending)
                return SimpleNamespace(share_seq=len(self.appended))

        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            ledger = LegacyLedger()
            service, _ledger, _controller, _runtime = self._service(ledger=ledger)
            service.share_recovery_path = path
            first = self._stamp(service, "aa" * 32)
            duplicate = self._stamp(service, "aa" * 32)
            second = self._stamp(service, "bb" * 32)
            with patch("builtins.print"):
                for pending in (first, duplicate, second):
                    service.recover_share_to_disk(self._entry(pending), "test")

            with patch("builtins.print"):
                self.assertEqual(service.replay_recovered_shares(), 2)

            self.assertFalse(path.exists())
            self.assertEqual(
                [pending.share_id for pending in ledger.appended],
                [first.share_id, second.share_id],
            )

    def test_torn_journal_line_is_preserved_while_intact_lines_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, ledger, _controller, _runtime = self._service()
            service.share_recovery_path = path
            pending = self._stamp(service)
            with patch("builtins.print"):
                service.recover_share_to_disk(self._entry(pending), "test")
            with open(path, "a", encoding="utf-8") as handle:
                handle.write('{"share_id": "miner-a:torn", "miner_')

            with patch("builtins.print"), patch("traceback.print_exc"):
                self.assertEqual(service.replay_recovered_shares(), 1)

            self.assertTrue(path.exists())
            self.assertEqual(len(ledger), 1)


if __name__ == "__main__":
    unittest.main()
