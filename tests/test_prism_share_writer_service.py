#!/usr/bin/env python3
"""Direct tests for the coordinator-free S3 share writer."""

from __future__ import annotations

from contextlib import contextmanager
import queue
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest

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
    PendingShareAppend,
    PendingShareInput,
    ShareWriter,
    ShareWriterConfig,
    ShareWriterPorts,
)


class ShareWriterServiceTests(unittest.TestCase):
    def test_coordinator_reexports_exact_pending_append_identity(self) -> None:
        from lab.prism.prism_coordinator import PendingShareAppend as compatibility

        self.assertIs(compatibility, PendingShareAppend)

    def test_append_failure_property_and_metrics_snapshot_are_readable(self) -> None:
        service, _ledger, _controller, _stop = self._service()

        service.append_failures = 7

        self.assertEqual(service.append_failures, 7)
        self.assertEqual(service.metrics_snapshot().append_failures, 7)

    def _service(
        self,
        *,
        ledger: object | None = None,
        wall_times: list[int] | None = None,
        recovery_path: Path | None = None,
        controller: CoordinatorShutdownController | None = None,
        reserve_error: BaseException | None = None,
        floor: dict[object, list[object]] | None = None,
    ) -> tuple[ShareWriter, object, CoordinatorShutdownController, threading.Event]:
        ledger = ledger or SingleWriterShareLedger()
        controller = controller or CoordinatorShutdownController(1.0)
        stop = threading.Event()
        times = list(wall_times or [100])
        last = times[-1]

        def wall_time_ms() -> int:
            return times.pop(0) if times else last

        @contextmanager
        def writer_operation(component: str):
            token = controller.enter_writer(component)
            try:
                yield
            finally:
                controller.exit_writer(token)

        def reserve_writer(component: str):
            if reserve_error is not None:
                raise reserve_error
            return controller.reserve_writer(component)

        service = ShareWriter(
            ShareWriterConfig(
                batch_size=8,
                linger_seconds=0,
                enqueue_timeout_seconds=0.01,
                recovery_path=recovery_path,
            ),
            ShareWriterPorts(
                ledger=lambda: ledger,
                writer_operation=writer_operation,
                reserve_writer=reserve_writer,
                writer_admission_closed=controller.writer_admission_closed,
                has_active_writer=controller.has_active_writer,
                heartbeat=lambda _name: None,
                monotonic=lambda: 10.0,
                wall_time_ms=wall_time_ms,
                stop_is_set=stop.is_set,
                stop_wait=stop.wait,
                log=lambda _message: None,
                log_exception=lambda: None,
                hot_path_log_enabled=lambda: False,
            ),
            append_queue=queue.Queue(maxsize=4),
            floor=floor,
        )
        return service, ledger, controller, stop

    @staticmethod
    def _input(share_id: str) -> PendingShareInput:
        return PendingShareInput(
            share_id=share_id,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            ntime=1_700_000_000,
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

    def test_floor_uses_stable_share_id_and_preserves_three_slot_compatibility(self) -> None:
        service, _ledger, _controller, _stop = self._service(wall_times=[100, 250])
        service.make_pending_share(self._input("miner-a:same"))
        second = service.make_pending_share(self._input("miner-a:same"))

        self.assertEqual(list(service.floor), ["miner-a:same"])
        self.assertEqual(len(service.floor["miner-a:same"]), 3)
        self.assertIs(service.floor["miner-a:same"][0], second)
        self.assertEqual(service.snapshot_anchor_ms(1_000), 99)

        # A reconstructed object with the same durable identity can finish the
        # logical lease even though neither original Python identity survives.
        reconstructed = PendingShare(**{**second.__dict__, "accepted_at_ms": 80})
        service.adopt_pending_share(reconstructed)
        self.assertEqual(service.snapshot_anchor_ms(1_000), 79)
        service.finish_pending_share(reconstructed)
        self.assertEqual(service.floor, {})

    def test_attempt_promotion_is_atomic_and_owner_specific(self) -> None:
        service, _ledger, _controller, _stop = self._service(wall_times=[100])
        pending = service.make_pending_share(self._input("miner-a:promoted"))

        service.adopt_pending_share(pending)

        self.assertNotIn(id(pending), service._attempt_holders)
        self.assertIn(pending.share_id, service._candidate_holders)
        service.finish_pending_attempt(pending)
        self.assertIn(pending.share_id, service.floor)
        service.finish_pending_candidate(PendingShare(**pending.__dict__))
        self.assertEqual(service.floor, {})

    def test_same_id_candidate_actors_survive_each_others_terminal_changes(
        self,
    ) -> None:
        service, _ledger, _controller, _stop = self._service(wall_times=[100, 200])
        first = service.make_pending_share(self._input("miner-a:actor"))
        entry = service.floor[first.share_id]
        service.begin_candidate_actor(first)
        second = service.make_pending_share(self._input("miner-a:actor"))
        service.begin_candidate_actor(second)

        # Actor B terminalizes the shared durable outbox source. Its own actor
        # can leave, but actor A independently preserves the older floor.
        service.finish_pending_candidate(second)
        service.finish_candidate_actor(second)
        self.assertIs(service.floor[first.share_id], entry)
        self.assertEqual(service.snapshot_anchor_ms(1_000), 99)

        # If A's credit append fails, retry adoption is established while A is
        # still held. Releasing the actor then leaves the durable retry source.
        service.adopt_pending_share(first)
        service.finish_candidate_actor(first)
        self.assertIs(service.floor[first.share_id], entry)
        self.assertEqual(service.snapshot_anchor_ms(1_000), 99)
        service.finish_pending_candidate(first)
        self.assertEqual(service.floor, {})

    def test_floor_entry_list_identity_survives_holder_transitions_and_warning(
        self,
    ) -> None:
        service, _ledger, _controller, _stop = self._service(wall_times=[100, 200])
        first = service.make_pending_share(self._input("miner-a:identity"))
        entry = service.floor[first.share_id]

        service.adopt_pending_share(first)
        self.assertIs(service.floor[first.share_id], entry)
        second = service.make_pending_share(self._input("miner-a:identity"))
        self.assertIs(service.floor[first.share_id], entry)
        service.config.pending_floor_warn_seconds = -1
        service.snapshot_anchor_ms(1_000)
        self.assertIs(service.floor[first.share_id], entry)
        self.assertTrue(entry[2])
        service.finish_pending_attempt(second)
        self.assertIs(service.floor[first.share_id], entry)
        self.assertEqual(len(entry), 3)

        service.finish_pending_candidate(first)
        self.assertEqual(service.floor, {})

    def test_nonempty_compatibility_floor_is_reachable_before_and_after_adoption(
        self,
    ) -> None:
        first = PendingShare(
            share_id="miner-a:first",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=100,
            ntime=1_700_000_000,
        )
        second = PendingShare(
            **{
                **first.__dict__,
                "share_id": "miner-a:second",
                "accepted_at_ms": 200,
            }
        )
        first_floor = {id(first): [first, 1.0, False]}
        first_entry = first_floor[id(first)]
        service, _ledger, _controller, _stop = self._service(floor=first_floor)

        service.adopt_pending_share(first)
        self.assertIs(service.floor[first.share_id], first_entry)
        service.finish_pending_candidate(first)
        self.assertEqual(service.floor, {})

        second_floor = {id(second): [second, 2.0, True]}
        second_entry = second_floor[id(second)]
        service.adopt_floor(second_floor)
        self.assertIs(service.floor, second_floor)
        service.adopt_pending_share(second)
        self.assertIs(service.floor[second.share_id], second_entry)
        service.finish_pending_candidate(second)
        self.assertEqual(service.floor, {})

    def test_parent_descendant_and_nonselected_candidates_keep_reachable_leases(self) -> None:
        service, _ledger, _controller, _stop = self._service(wall_times=[200, 150, 175])
        descendant = service.make_pending_share(self._input("miner-a:descendant"))
        parent = service.make_pending_share(self._input("miner-a:parent"))
        competitor = service.make_pending_share(self._input("miner-a:competitor"))

        # Retry selection may replace a descendant with its parent or decline
        # a competitor. All three durable identities remain terminally
        # reachable rather than being tied to the selected in-memory object.
        service.transfer_pending_floor(descendant, parent)
        self.assertEqual(
            set(service.floor),
            {"miner-a:descendant", "miner-a:parent", "miner-a:competitor"},
        )
        self.assertEqual(service.snapshot_anchor_ms(1_000), 149)

        for pending in (parent, competitor, descendant):
            reconstructed = PendingShare(**pending.__dict__)
            service.finish_pending_share(reconstructed)
        self.assertEqual(service.floor, {})

    def test_direct_enqueue_admission_refusal_releases_registered_floor(self) -> None:
        service, _ledger, _controller, _stop = self._service(
            reserve_error=RuntimeError("closed")
        )
        pending = service.make_pending_share(self._input("miner-a:closed"))

        with self.assertRaisesRegex(RuntimeError, "closed"):
            service.enqueue(self._entry(pending))

        self.assertEqual(service.floor, {})

    def test_append_admission_refusal_releases_unhanded_attempt(self) -> None:
        controller = CoordinatorShutdownController(1.0)
        service, _ledger, _controller, _stop = self._service(controller=controller)
        pending = service.make_pending_share(self._input("miner-a:closed-append"))
        controller.request_shutdown(None)

        with self.assertRaises(ShutdownInProgress):
            service.append_and_wait(self._entry(pending))

        self.assertEqual(service.floor, {})

    def test_invisible_queue_exception_releases_token_and_floor(self) -> None:
        class ExplodingQueue(queue.Queue):
            def put_nowait(self, _item: object) -> None:
                raise OSError("queue transport failed")

        service, _ledger, controller, _stop = self._service()
        service.adopt_queue(ExplodingQueue(maxsize=1))
        pending = service.make_pending_share(self._input("miner-a:invisible"))
        entry = self._entry(pending)

        with self.assertRaisesRegex(OSError, "transport failed"):
            service.enqueue(entry)

        self.assertIsNone(entry.writer_token)
        self.assertEqual(service.floor, {})
        self.assertEqual(controller.snapshot()["active_writers"], {})

    def test_interrupted_visible_wait_leaves_attempt_owned_by_writer(self) -> None:
        class InterruptingEvent:
            def __init__(self) -> None:
                self.was_set = False

            def wait(self) -> None:
                raise RuntimeError("wait interrupted")

            def set(self) -> None:
                self.was_set = True

        service, ledger, controller, _stop = self._service()
        service.active = True
        pending = service.make_pending_share(self._input("miner-a:visible"))
        committed = InterruptingEvent()
        entry = PendingShareAppend(
            **{
                **self._entry(pending).__dict__,
                "committed": committed,
            }
        )

        with self.assertRaisesRegex(RuntimeError, "wait interrupted"):
            service.append_and_wait(entry)

        self.assertIn(pending.share_id, service.floor)
        self.assertIsNotNone(entry.writer_token)
        service.append_batch([service.append_queue.get_nowait()])
        self.assertEqual(len(ledger), 1)
        self.assertTrue(committed.was_set)
        self.assertEqual(service.floor, {})
        self.assertEqual(controller.snapshot()["active_writers"], {})

    def test_nested_append_inherits_outer_submit_after_shutdown_closes(self) -> None:
        controller = CoordinatorShutdownController(1.0)
        service, ledger, _controller, _stop = self._service(controller=controller)
        outer = controller.enter_writer("share_submission")
        try:
            pending = service.make_pending_share(self._input("miner-a:inherited"))
            controller.request_shutdown(None)
            service.append_and_wait(self._entry(pending))
        finally:
            controller.exit_writer(outer)

        self.assertEqual(len(ledger), 1)
        self.assertEqual(service.floor, {})

    def test_startup_gate_orders_legacy_ack_replay_before_candidate_credit(self) -> None:
        service, ledger, _controller, _stop = self._service(wall_times=[200])
        candidate = service.make_pending_share(self._input("miner-a:candidate"))
        legacy = PendingShare(
            **{
                **candidate.__dict__,
                "share_id": "miner-a:legacy",
                "accepted_at_ms": 300,
            }
        )
        service.begin_startup_recovery()
        errors: list[BaseException] = []

        def append_candidate() -> None:
            try:
                service.append_and_wait(self._entry(candidate))
            except BaseException as exc:
                errors.append(exc)

        append = threading.Thread(
            target=append_candidate,
        )
        append.start()
        append.join(timeout=0.05)
        self.assertTrue(append.is_alive())
        self.assertEqual(len(ledger), 0)

        replay = ledger.append_recovered_share(legacy)
        service.finish_startup_recovery()
        append.join(timeout=2)

        self.assertFalse(append.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(replay.record.share_seq, 1)
        self.assertEqual(
            [record.share_id for record in ledger.all_shares()],
            ["miner-a:legacy", "miner-a:candidate"],
        )

    def test_startup_gate_cancellation_aborts_waiter_and_late_caller(self) -> None:
        service, ledger, _controller, _stop = self._service(wall_times=[200, 300])
        waiting = service.make_pending_share(self._input("miner-a:waiting"))
        service.begin_startup_recovery()
        errors: list[BaseException] = []

        def append_waiting() -> None:
            try:
                service.append_and_wait(self._entry(waiting))
            except BaseException as exc:
                errors.append(exc)

        append = threading.Thread(target=append_waiting)
        append.start()
        append.join(timeout=0.05)
        self.assertTrue(append.is_alive())

        service.cancel_startup_recovery()
        append.join(timeout=1)

        self.assertFalse(append.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ShutdownInProgress)
        self.assertEqual(len(ledger), 0)
        self.assertEqual(service.floor, {})

        late = service.make_pending_share(self._input("miner-a:late"))
        with self.assertRaisesRegex(ShutdownInProgress, "recovery was cancelled"):
            service.append_and_wait(self._entry(late))
        self.assertEqual(len(ledger), 0)
        self.assertEqual(service.floor, {})

    def test_startup_gate_fast_path_rechecks_interleaved_cancellation(self) -> None:
        service, ledger, _controller, _stop = self._service(wall_times=[200])
        pending = service.make_pending_share(self._input("miner-a:fast-cancel"))

        class CancellationInterleavingEvent:
            def __init__(self) -> None:
                self.opened = False
                self.interleaved = False

            def is_set(self) -> bool:
                if not self.interleaved:
                    self.interleaved = True
                    service.cancel_startup_recovery()
                return self.opened

            def set(self) -> None:
                self.opened = True

            def clear(self) -> None:
                self.opened = False

            def wait(self, _timeout: float | None = None) -> bool:
                return self.opened

        gate = CancellationInterleavingEvent()
        service._startup_recovery_complete = gate  # type: ignore[assignment]

        with self.assertRaisesRegex(ShutdownInProgress, "recovery was cancelled"):
            service.append_and_wait(self._entry(pending))

        self.assertTrue(gate.interleaved)
        self.assertEqual(len(ledger), 0)
        self.assertEqual(service.floor, {})

    def test_recovery_exact_existing_is_typed_and_clears_clean_journal(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, ledger, _controller, _stop = self._service(recovery_path=path)
            pending = service.make_pending_share(self._input("miner-a:exact"))
            entry = self._entry(pending)
            service.recover_to_disk(entry, "test")
            ledger.append_recovered_share(pending)

            self.assertEqual(service.replay_recovery_file(), 0)
            self.assertFalse(path.exists())
            self.assertEqual(service.metrics_snapshot().replay_exact_existing, 1)

    def test_recovery_payload_conflict_is_typed_and_retains_journal(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, ledger, _controller, _stop = self._service(recovery_path=path)
            pending = service.make_pending_share(self._input("miner-a:conflict"))
            service.recover_to_disk(self._entry(pending), "test")
            ledger.append_recovered_share(
                PendingShare(**{**pending.__dict__, "ntime": pending.ntime + 1})
            )

            self.assertEqual(service.replay_recovery_file(), 0)
            self.assertTrue(path.exists())
            self.assertEqual(service.metrics_snapshot().replay_conflicts, 1)

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
            service, _ledger, _controller, _stop = self._service(
                ledger=FutureLedger(),
                recovery_path=path,
            )
            pending = service.make_pending_share(self._input("miner-a:future"))
            service.recover_to_disk(self._entry(pending), "test")

            self.assertEqual(service.replay_recovery_file(), 0)
            self.assertTrue(path.exists())

    def test_recovery_identity_cannot_change_after_replay_begins(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, _ledger, _controller, _stop = self._service(recovery_path=path)
            self.assertEqual(service.replay_recovery_file(), 0)

            with self.assertRaisesRegex(RuntimeError, "after replay starts"):
                service.set_recovery_path(path.with_name("other.jsonl"))
            with self.assertRaisesRegex(RuntimeError, "after replay starts"):
                service.adopt_recovery_lock(threading.Lock())

    def test_recovery_append_serializes_with_replay_and_clean_unlink(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class BlockingReplayLedger:
            def append_recovered_share(self, pending: PendingShare) -> ShareReplayResult:
                entered.set()
                release.wait(timeout=2)
                return ShareReplayResult(
                    "inserted",
                    SimpleNamespace(share_seq=1, share_id=pending.share_id),
                )

        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "recovery.jsonl"
            service, _ledger, _controller, _stop = self._service(
                ledger=BlockingReplayLedger(),
                recovery_path=path,
            )
            first = service.make_pending_share(self._input("miner-a:first"))
            second = PendingShare(**{**first.__dict__, "share_id": "miner-a:second"})
            service.recover_to_disk(self._entry(first), "first")
            replay = threading.Thread(target=service.replay_recovery_file)
            replay.start()
            self.assertTrue(entered.wait(timeout=1))

            append = threading.Thread(
                target=service.recover_to_disk,
                args=(self._entry(second), "second"),
            )
            append.start()
            append.join(timeout=0.05)
            self.assertTrue(append.is_alive())

            release.set()
            replay.join(timeout=2)
            append.join(timeout=2)
            self.assertFalse(replay.is_alive())
            self.assertFalse(append.is_alive())
            self.assertIn("miner-a:second", path.read_text(encoding="utf-8"))
            self.assertNotIn("miner-a:first", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
