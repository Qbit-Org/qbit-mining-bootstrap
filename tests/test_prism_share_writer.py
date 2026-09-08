#!/usr/bin/env python3
"""Focused PRISM coordinator share writer tests."""
# ruff: noqa: F403, F405

from __future__ import annotations

import unittest
from tests.prism_vardiff_test_support import *


class PrismCoordinatorVardiffTests(unittest.TestCase):
    def test_share_ack_and_counters_wait_for_group_commit(self) -> None:
        server, state, ledger = submit_coordinator()
        server.share_writer_active = True
        server.share_commit_timeout_seconds = 2.0
        server.share_commit_linger_seconds = 0.0
        commit_started = threading.Event()
        release_commit = threading.Event()

        class BlockingBatchLedger(type(ledger)):
            def append_batch(self, entries: object) -> list[object]:
                commit_started.set()
                release_commit.wait(timeout=2)
                return [self.append(pending) for pending, _candidate in entries]

        ledger = BlockingBatchLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=False,
        )

        writer = threading.Thread(target=server.share_append_loop, daemon=True)
        writer.start()
        outcome: list[object] = []

        def submit() -> None:
            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                outcome.append(
                    server.handle_submit(
                        state,
                        ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                    )
                )

        submitter = threading.Thread(target=submit)
        submitter.start()
        self.assertTrue(commit_started.wait(timeout=1))
        self.assertTrue(submitter.is_alive())
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(server.worker_share_counts["miner-a"]["accepted"], 0)

        release_commit.set()
        submitter.join(timeout=2)
        self.assertFalse(submitter.is_alive())
        self.assertEqual(outcome, [False])
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].share_id, "miner-a:" + "cc" * 32)
        self.assertEqual(server.worker_share_counts["miner-a"]["accepted"], 1)
        server.request_shutdown()
        writer.join(timeout=2)

    def test_job_snapshot_anchor_precedes_stamped_uncommitted_share(self) -> None:
        # A share is stamped accepted_at_ms at validation time, before its
        # group commit. Anchors chosen while that commit is pending must
        # predate the stamp, or the issued window would omit a share that a
        # later re-derivation at the same anchor includes.
        server, state, ledger = submit_coordinator()
        server.share_writer_active = True
        server.share_commit_timeout_seconds = 2.0
        server.share_commit_linger_seconds = 0.0
        commit_started = threading.Event()
        release_commit = threading.Event()

        class BlockingBatchLedger(type(ledger)):
            def append_batch(self, entries: object) -> list[object]:
                commit_started.set()
                release_commit.wait(timeout=2)
                return [self.append(pending) for pending, _candidate in entries]

        ledger = BlockingBatchLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=False,
        )

        writer = threading.Thread(target=server.share_append_loop, daemon=True)
        writer.start()

        def submit() -> None:
            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        submitter = threading.Thread(target=submit)
        submitter.start()
        self.assertTrue(commit_started.wait(timeout=1))

        floor = dict(server._pending_share_commit_floor)
        self.assertEqual(len(floor), 1)
        (floor_entry,) = floor.values()
        stamped_ms = int(floor_entry[0].accepted_at_ms)
        self.assertEqual(
            server._job_snapshot_anchor_ms(stamped_ms + 60_000),
            stamped_ms - 1,
        )

        release_commit.set()
        submitter.join(timeout=2)
        self.assertFalse(submitter.is_alive())
        self.assertEqual(server._pending_share_commit_floor, {})
        # Drained floor: the anchor still sits strictly below the clamp
        # instant so a same-millisecond stamp can never tie it.
        self.assertEqual(
            server._job_snapshot_anchor_ms(stamped_ms + 60_000),
            stamped_ms + 60_000 - 1,
        )
        server.request_shutdown()
        writer.join(timeout=2)

    def test_share_batch_failure_still_restores_snapshot_anchor(self) -> None:
        server, _state, _ledger = submit_coordinator()
        entry = self._pending_append("aa", accepted_at_ms=2)
        server._ensure_pending_share_commit_state()
        with server._pending_share_commit_lock:
            server._pending_share_commit_floor[id(entry.pending_share)] = [
                entry.pending_share,
                time.monotonic(),
                False,
            ]
        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 1)

        class FailingLedger(FakeLedger):
            def append(self, pending: object) -> object:
                raise RuntimeError("ledger unavailable")

        server.ledger = FailingLedger()
        self.assertFalse(server._append_share_batch([entry]))
        self.assertTrue(entry.committed.is_set())
        self.assertEqual(server._pending_share_commit_floor, {})
        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 9_999)

    def test_failed_commit_releases_duplicate_key_for_exact_retry(self) -> None:
        server, state, healthy = submit_coordinator()
        server.share_writer_active = True
        server.share_commit_linger_seconds = 0.0
        server.share_commit_timeout_seconds = 1.0

        class FailedLedger:
            def append_batch(self, _entries: object) -> list[object]:
                raise RuntimeError("postgres unavailable")

        server.ledger = FailedLedger()
        writer = threading.Thread(target=server.share_append_loop, daemon=True)
        writer.start()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="dd" * 32,
            share_pass=True,
            block_pass=False,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaisesRegex(StratumError, "commit failed"):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        server.share_writer_active = False
        server.ledger = healthy
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )
        self.assertEqual(len(healthy.pending), 1)
        self.assertEqual(server.worker_share_counts["miner-a"]["accepted"], 1)
        server.request_shutdown()
        writer.join(timeout=2)

    def test_share_writer_retries_failed_append_until_it_lands(self) -> None:
        server, state, ledger = submit_coordinator()

        class FlakyLedger(type(ledger)):
            def __init__(self) -> None:
                super().__init__()
                self.failures_remaining = 2

            def append(self, pending: object) -> object:
                if self.failures_remaining > 0:
                    self.failures_remaining -= 1
                    raise RuntimeError("ledger briefly unavailable")
                return super().append(pending)

        flaky = FlakyLedger()
        server.ledger = flaky
        entry = PendingShareAppend(
            pending_share=SimpleNamespace(
                share_id="miner-a:" + "ee" * 32,
                job_issued_at_ms=0,
                accepted_at_ms=0,
            ),
            username="miner-a",
            job_id="job-1",
            block_hash_hex="ee" * 32,
            collection_only=False,
            credit_policy=None,
        )

        with patch.object(server.stop_event, "wait", return_value=False) as waited:
            server._append_share_entry(entry, retry_until_stopped=True)

        self.assertEqual(len(flaky.pending), 1)
        self.assertEqual(server.share_append_failure_count, 2)
        self.assertIn(
            "qbit_prism_share_append_failures_total 2",
            server.metrics_payload(),
        )
        self.assertEqual(waited.call_count, 2)

    def _pending_append(self, tag: str, accepted_at_ms: int = 2) -> PendingShareAppend:
        from lab.prism.share_ledger import PendingShare

        return PendingShareAppend(
            pending_share=PendingShare(
                share_id=f"miner-a:{tag}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=10,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1_700_000_000,
            ),
            username="miner-a",
            job_id="job-1",
            block_hash_hex=tag * 32,
            collection_only=False,
            credit_policy=None,
        )

    def test_share_append_backlog_overflow_never_reports_success(self) -> None:
        server, _state, _ledger = submit_coordinator()
        server.share_append_queue = queue.Queue(maxsize=2)
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"

            server.enqueue_share_append(self._pending_append("aa"))
            server.enqueue_share_append(self._pending_append("bb"))
            with self.assertRaisesRegex(StratumError, "queue is full"):
                server.enqueue_share_append(self._pending_append("cc"))

            self.assertEqual(server.shares_recovered_to_disk, 0)
            remaining = [
                server.share_append_queue.get_nowait().pending_share.share_id
                for _ in range(2)
            ]
            self.assertEqual(remaining, ["miner-a:aa", "miner-a:bb"])
            self.assertFalse(server.share_recovery_path.exists())

    def test_writer_recovers_acked_share_on_shutdown_during_outage(self) -> None:
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"

            class DownLedger(type(ledger)):
                def append(self, pending: object) -> object:
                    raise RuntimeError("postgres unavailable")

            server.ledger = DownLedger()
            entry = self._pending_append("ee")
            # First backoff wait returns True (stop requested): the share must
            # be recovered, not silently dropped.
            with patch.object(server.stop_event, "wait", return_value=True):
                server._append_share_entry(entry, retry_until_stopped=True)

            self.assertEqual(server.shares_recovered_to_disk, 1)
            recovered = json.loads(server.share_recovery_path.read_text().strip())
            self.assertEqual(recovered["share_id"], "miner-a:ee")

    def test_replay_recovered_shares_appends_and_clears_file(self) -> None:
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("f1"), "test")
            server._recover_share_to_disk(self._pending_append("f2"), "test")

            replayed = server.replay_recovered_shares()

            self.assertEqual(replayed, 2)
            self.assertEqual(server.shares_replayed, 2)
            self.assertEqual(
                [p.share_id for p in ledger.pending], ["miner-a:f1", "miner-a:f2"]
            )
            # File is cleared after a clean replay so shares are not re-added.
            self.assertFalse(server.share_recovery_path.exists())
            self.assertEqual(server.replay_recovered_shares(), 0)

    def test_replay_recovered_shares_orders_by_accepted_at(self) -> None:
        # A share can be recovered out of FIFO order (overflow of the newest, or
        # a ledger flap during the shutdown drain). Replay must reorder by
        # accepted_at_ms so share_seq reflects acceptance order, keeping the
        # reward window correctly ordered.
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("late", accepted_at_ms=300), "test")
            server._recover_share_to_disk(self._pending_append("early", accepted_at_ms=100), "test")
            server._recover_share_to_disk(self._pending_append("mid", accepted_at_ms=200), "test")

            replayed = server.replay_recovered_shares()

            self.assertEqual(replayed, 3)
            self.assertEqual(
                [p.share_id for p in ledger.pending],
                ["miner-a:early", "miner-a:mid", "miner-a:late"],
            )

    def test_replay_skips_torn_line_and_keeps_file(self) -> None:
        # A crash mid-append can leave the last line torn. That one line must
        # not block the intact shares before it, and the file is kept so the
        # torn line is preserved rather than silently discarded.
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("g1", accepted_at_ms=100), "test")
            server._recover_share_to_disk(self._pending_append("g2", accepted_at_ms=200), "test")
            with open(server.share_recovery_path, "a", encoding="utf-8") as handle:
                handle.write('{"share_id": "miner-a:torn", "miner_i')  # truncated, no newline

            replayed = server.replay_recovered_shares()

            self.assertEqual(replayed, 2)
            self.assertEqual(
                [p.share_id for p in ledger.pending], ["miner-a:g1", "miner-a:g2"]
            )
            # File kept because a line could not be parsed.
            self.assertTrue(server.share_recovery_path.exists())
            # Re-running classifies the intact rows as exact-existing. They are
            # not inserted or counted again, while the torn line keeps the
            # journal for inspection.
            self.assertEqual(server.replay_recovered_shares(), 0)
            self.assertEqual(
                [p.share_id for p in ledger.pending], ["miner-a:g1", "miner-a:g2"]
            )
            self.assertTrue(server.share_recovery_path.exists())

    def test_replay_is_idempotent_across_partial_replay(self) -> None:
        # Finding: a partial replay (A commits, B fails transiently) kept the
        # whole file; on retry, replay hit A's duplicate and stopped, stranding
        # B forever. Replay must skip the already-committed A and reach B.
        server, _state, _ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("A", accepted_at_ms=100), "test")
            server._recover_share_to_disk(self._pending_append("B", accepted_at_ms=200), "test")

            class DedupLedger:
                def __init__(self) -> None:
                    self.ids: list[str] = []
                    self.pending_by_id: dict[str, object] = {}
                    self.fail_b_once = True

                def append_recovered_share(self, pending: object) -> object:
                    if pending.share_id == "miner-a:B" and self.fail_b_once:
                        self.fail_b_once = False
                        raise RuntimeError("postgres unavailable")
                    if pending.share_id in self.ids:
                        if self.pending_by_id[pending.share_id] != pending:
                            raise ShareReplayConflict(pending.share_id)
                        return ShareReplayResult(
                            "exact_existing",
                            SimpleNamespace(
                                share_seq=self.ids.index(pending.share_id) + 1
                            ),
                        )
                    self.ids.append(pending.share_id)
                    self.pending_by_id[pending.share_id] = pending
                    return ShareReplayResult(
                        "inserted",
                        SimpleNamespace(share_seq=len(self.ids)),
                    )

            server.ledger = DedupLedger()

            # Pass 1: A commits, B raises a transient (non-duplicate) error, so
            # the pass stops and keeps the file.
            self.assertEqual(server.replay_recovered_shares(), 1)
            self.assertTrue(server.share_recovery_path.exists())
            self.assertEqual(server.ledger.ids, ["miner-a:A"])

            # Pass 2: A is now a duplicate (skipped, not fatal); B commits and
            # the file is cleared.
            self.assertEqual(server.replay_recovered_shares(), 1)
            self.assertEqual(server.ledger.ids, ["miner-a:A", "miner-a:B"])
            self.assertFalse(server.share_recovery_path.exists())

    def test_replay_quarantines_conflicting_row_and_replays_the_rest(self) -> None:
        # A ShareReplayConflict is keyed to one share_id; it must not strand
        # the journal rows behind it, and the conflict must be visible as a
        # counter, not only in stdout.
        server, _state, _ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("A", accepted_at_ms=100), "test")
            server._recover_share_to_disk(self._pending_append("B", accepted_at_ms=200), "test")
            server._recover_share_to_disk(self._pending_append("C", accepted_at_ms=300), "test")

            class ConflictingLedger(RecordingLedger):
                def __init__(self) -> None:
                    super().__init__()
                    self.ids: list[str] = []

                def append_recovered_share(self, pending: object) -> object:
                    if pending.share_id == "miner-a:A":
                        raise ShareReplayConflict(pending.share_id)
                    if pending.share_id in self.ids:
                        return ShareReplayResult(
                            "exact_existing",
                            SimpleNamespace(
                                share_seq=self.ids.index(pending.share_id) + 1
                            ),
                        )
                    self.ids.append(pending.share_id)
                    return ShareReplayResult(
                        "inserted",
                        SimpleNamespace(share_seq=len(self.ids)),
                    )

            server.ledger = ConflictingLedger()

            self.assertEqual(server.replay_recovered_shares(), 2)
            self.assertTrue(server.share_recovery_path.exists())
            self.assertEqual(server.ledger.ids, ["miner-a:B", "miner-a:C"])
            self.assertEqual(server.share_replay_conflicts, 1)
            self.assertIn(
                "qbit_prism_share_replay_conflicts_total 1",
                server.metrics_payload(),
            )

            # Re-running the retained journal credits nothing twice: B and C
            # are exact duplicates, A conflicts again, the file stays.
            self.assertEqual(server.replay_recovered_shares(), 0)
            self.assertTrue(server.share_recovery_path.exists())
            self.assertEqual(server.ledger.ids, ["miner-a:B", "miner-a:C"])
            self.assertEqual(server.share_replay_conflicts, 2)

    def test_append_share_entry_reports_persisted_vs_recovered(self) -> None:
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            # Healthy ledger: reports persisted.
            self.assertTrue(
                server._append_share_entry(self._pending_append("ok"), retry_until_stopped=True)
            )

            class DownLedger(type(ledger)):
                def append(self, pending: object) -> object:
                    raise RuntimeError("postgres unavailable")

            server.ledger = DownLedger()
            with patch.object(server.stop_event, "wait", return_value=True):
                # Shutdown mid-outage: reports recovered, not persisted.
                self.assertFalse(
                    server._append_share_entry(self._pending_append("down"), retry_until_stopped=True)
                )

    def test_group_commit_failure_releases_all_waiters_without_recovery_file(self) -> None:
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server.share_append_queue = queue.Queue(maxsize=MAX_PENDING_SHARE_APPENDS)

            append_calls: list[str] = []

            class DownLedger(type(ledger)):
                def append(self, pending: object) -> object:
                    append_calls.append(pending.share_id)
                    raise RuntimeError("postgres unavailable")

            server.ledger = DownLedger()
            entries = []
            for tag in ("s1", "s2", "s3"):
                entry = self._pending_append(tag)
                entries.append(entry)
                server.enqueue_share_append(entry)

            server.request_shutdown()
            server.share_append_loop()

            # The compatibility ledger fails on the first row; the whole batch
            # is reported failed and no uncommitted share is called durable.
            self.assertEqual(append_calls, ["miner-a:s1"])
            self.assertEqual(ledger.pending, [])
            self.assertTrue(all(entry.committed.is_set() for entry in entries))
            self.assertTrue(all(entry.error is not None for entry in entries))
            self.assertFalse(server.share_recovery_path.exists())

    def test_predating_append_commits_while_fast_lane_offer_is_in_flight(
        self,
    ) -> None:
        # The other side of the fence boundary flipped with the node fast
        # lane: submitblock no longer holds the landing fence, so a
        # predating append's durable commit does NOT wait for the RPC to
        # return. The row may become durable mid-offer with its epoch bump
        # landing under the fence; the landing's post-offer accounting
        # fences (not the offer itself) are what observe the invalidation.
        # Here the node rejects the block outright, so the rejection -- not
        # the epoch race -- terminally abandons the candidate.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        # Keep an anchor the late row predates exposed for the whole test,
        # independent of the landing's own declared-anchor lifetime.
        anchor_token = server._expose_inflight_scan_anchor(12000)
        self.addCleanup(server._retire_inflight_scan_anchor, anchor_token)

        late_entry = self._pending_append("mid-rpc-append")
        durable_mid_rpc: list[bool] = []
        append_threads: list[threading.Thread] = []

        class MidRpcAppendRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    thread = threading.Thread(
                        target=server._append_share_entry,
                        args=(late_entry,),
                        daemon=True,
                    )
                    thread.start()
                    append_threads.append(thread)
                    deadline = time.monotonic() + 0.25
                    became_durable = False
                    while time.monotonic() < deadline:
                        if late_entry.pending_share.share_id in ledger._share_ids:
                            became_durable = True
                            break
                        time.sleep(0.005)
                    durable_mid_rpc.append(became_durable)
                    return "rejected-by-test"
                return super().call(method, params)

        server.rpc = MidRpcAppendRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dd" * 32,
            block_hex="00",
        )
        pending = self._pending_append("landing-holds-fence").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        # The fast-lane RPC holds no fence, so the predating row became
        # durable while the offer was still in flight.
        self.assertEqual(durable_mid_rpc, [True])
        self.assertTrue(append_threads)
        append_threads[0].join(timeout=10.0)
        self.assertFalse(append_threads[0].is_alive())
        self.assertIn(late_entry.pending_share.share_id, ledger._share_ids)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
        self.assertEqual(
            server.block_candidate_abandoned_counts[
                PRISM_REJECTION_SUBMITBLOCK_REJECTED
            ],
            1,
        )

    def test_completed_append_predating_seedless_published_window_fences(
        self,
    ) -> None:
        # A build that seeds no artifact (the documented
        # PRISM_PAYOUT_ARTIFACT_REUSE=0 rollback mode) retires its walk
        # exposure at the publication fence, yet jobs stamped from it keep
        # serving that window until they retire. A replay-shaped append
        # that started AND finished in that gap used to see no live
        # anchor: no epoch bump, and its registry entry was popped on
        # completion -- so a later landing drained nothing, its
        # nonnegative context epoch skipped durable revalidation, and
        # submitblock accepted a coinbase omitting the durable share.
        # Publication now hands the declared anchor to the
        # published-window watermark, so the append commits under the
        # landing fence with its epoch bump -- and the landing, whose
        # offer already landed, keeps the as-issued snapshot for
        # accounting.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dd" * 32,
            block_hex="00",
        )
        pending = self._pending_append("seedless-window").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        # The publication fence of a seedless build hands the bundle's
        # declared anchor to the watermark when it retires the walk
        # exposure.
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._publish_seedless_job_window_anchor_locked(12000)

        # The ordinary share hot path stays off the fence: a row stamped
        # above the watermark commits unfenced and bumps nothing.
        ordinary_entry = self._pending_append(
            "ordinary-above-watermark", accepted_at_ms=13000
        )
        server._append_share_entry(ordinary_entry)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 0)
            self.assertEqual(
                server._payout_unfenced_append_inflight_stamps, {}
            )

        # The replay-shaped append starts and finishes entirely before the
        # landing begins: nothing is in flight for the landing to drain.
        late_entry = self._pending_append("completed-before-landing")
        server._append_share_entry(late_entry)
        self.assertIn(late_entry.pending_share.share_id, ledger._share_ids)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
            self.assertEqual(
                server._payout_unfenced_append_inflight_stamps, {}
            )

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "dd" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)

        self.assertTrue(server.submit_next_block_candidate())

        # The bump already happened at commit time; the landing's epoch
        # fence observes it and, with the node offer already accepted,
        # accounts the as-issued coinbase (the durable predating share
        # rides the next window).
        self.assertEqual(submitblock_calls, [["00"]])
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_replay_recovered_shares_commit_under_the_landing_fence(self) -> None:
        # Recovered rows reconstruct pre-crash timestamps, so they predate
        # live anchors. Each replay append must hold the landing fence across
        # the ledger commit and its epoch bump -- never bump after the row is
        # already durable, where a landing could verify the pre-bump epoch
        # and submit a coinbase omitting the replayed share.
        server, _state, ledger = submit_coordinator()
        anchor_token = server._expose_inflight_scan_anchor(12000)
        self.addCleanup(server._retire_inflight_scan_anchor, anchor_token)
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("h1"), "test")

            fence_held_during_commit: list[bool] = []
            original_append = ledger.append

            def recording_append(pending: object) -> object:
                fence_held_during_commit.append(
                    server._payout_append_landing_fence_lock.locked()
                )
                return original_append(pending)

            ledger.append = recording_append  # type: ignore[method-assign]
            replayed = server.replay_recovered_shares()

        self.assertEqual(replayed, 1)
        self.assertEqual(fence_held_during_commit, [True])
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)

    def test_sync_append_commits_predating_row_under_the_landing_fence(
        self,
    ) -> None:
        # The synchronous (no-writer) append path takes the same fence
        # boundary as the group-commit writer for a row that predates a
        # live anchor.
        server, _state, ledger = submit_coordinator()
        anchor_token = server._expose_inflight_scan_anchor(12000)
        self.addCleanup(server._retire_inflight_scan_anchor, anchor_token)

        fence_held_during_commit: list[bool] = []
        original_append = ledger.append

        def recording_append(pending: object) -> object:
            fence_held_during_commit.append(
                server._payout_append_landing_fence_lock.locked()
            )
            return original_append(pending)

        ledger.append = recording_append  # type: ignore[method-assign]
        self.assertTrue(server._append_share_entry(self._pending_append("h2")))
        self.assertEqual(fence_held_during_commit, [True])
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)


class PrismCoordinatorReliabilityTests(unittest.TestCase):
    def _bare_coordinator(self) -> PrismCoordinator:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.lock = threading.RLock()
        server.stop_event = threading.Event()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 120.0
        server.watchdog_interval_seconds = 15.0
        return server

    def test_release_ledger_lease_is_noop_without_lease_support(self) -> None:
        server = self._bare_coordinator()
        server.ledger = SimpleNamespace()

        # In-memory/regtest ledgers have no release_writer_lease; must not raise.
        server.release_ledger_lease()

    def test_release_ledger_lease_invokes_ledger_release(self) -> None:
        server = self._bare_coordinator()
        calls: list[bool] = []
        server.ledger = SimpleNamespace(
            release_writer_lease=lambda: (calls.append(True), True)[1]
        )

        server.release_ledger_lease()

        self.assertEqual(calls, [True])

    def test_release_ledger_lease_swallows_release_errors(self) -> None:
        server = self._bare_coordinator()

        def _boom() -> bool:
            raise RuntimeError("db unreachable during shutdown")

        server.ledger = SimpleNamespace(release_writer_lease=_boom)

        # Shutdown must not raise even if the lease release fails.
        server.release_ledger_lease()
