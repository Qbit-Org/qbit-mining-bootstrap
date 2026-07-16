#!/usr/bin/env python3
"""Focused coordinator coverage for CTV yielding to new-tip refreshes."""

from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism.ctv_broadcaster import CONFIRMED, BroadcastAttempt
from lab.prism.ctv_broadcaster_daemon import (
    CtvFanoutBroadcastDaemon,
    CtvFanoutChunkResult,
    CtvFanoutDaemonResult,
)
from lab.prism.prism_coordinator import PrismCoordinator


OLD_TIP = "11" * 32
NEW_TIP = "22" * 32
NEWER_TIP = "33" * 32


def pending_row(index: int) -> dict[str, object]:
    return {
        "fanout_txid": f"{index + 1:064x}",
        "fanout_tx_hex": "0300000001",
        "anchor_vout": None,
        "parent_coinbase_txid": "aa" * 32,
        "parent_coinbase_vout": 1,
        "block_hash": "bb" * 32,
        "block_height": 100 + index,
        "settlement_status": "broadcastable",
        "row_index": index,
    }


class FakeLedger:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.lock = threading.Lock()
        self.updates: list[tuple[str, str]] = []

    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]:
        with self.lock:
            eligible = [
                dict(row)
                for row in self.rows
                if row["settlement_status"] not in {"confirmed", "reorged", "failed"}
            ]
        return eligible[:limit]

    def update_ctv_fanout_status(
        self,
        *,
        fanout_txid: str,
        settlement_status: str,
    ) -> dict[str, int | str]:
        with self.lock:
            self.updates.append((fanout_txid, settlement_status))
            for row in self.rows:
                if row["fanout_txid"] == fanout_txid:
                    row["settlement_status"] = settlement_status
                    break
        return {"backend": "fake", "updated_count": 1}

    def record_ctv_fanout_broadcast_attempt(self, **_kwargs: object) -> dict[str, int | str]:
        raise AssertionError("confirmed fake rows must use the status-update path")


class BlockingBroadcaster:
    def __init__(self) -> None:
        self.first_row_entered = threading.Event()
        self.release_first_row = threading.Event()
        self.calls: list[str] = []

    def broadcast(self, artifact: object, _fee_sats: int) -> BroadcastAttempt:
        fanout_txid = str(getattr(artifact, "fanout_txid"))
        self.calls.append(fanout_txid)
        if len(self.calls) == 1:
            self.first_row_entered.set()
            if not self.release_first_row.wait(timeout=2.0):
                raise AssertionError("test did not release the first CTV chunk")
        return BroadcastAttempt(
            fanout_txid=fanout_txid,
            status=CONFIRMED,
            submitted=False,
        )


class ParentRpc:
    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getblock":
            assert params is not None
            return {"previousblockhash": OLD_TIP}
        raise AssertionError(f"unexpected RPC {method}")


class StopAfterOnePass:
    def is_set(self) -> bool:
        return False

    def wait(self, _timeout: float) -> bool:
        return True


def coordinator() -> PrismCoordinator:
    server = PrismCoordinator.__new__(PrismCoordinator)
    server.lock = threading.RLock()
    server.stop_event = threading.Event()
    server.rpc = ParentRpc()
    server.current_tip_first_seen = (OLD_TIP, None)
    server.current_tip_parent = None
    server.current_tip_observation_sequence = 1
    server.tip_observation_sequence = 1
    server.tip_template_snapshot = SimpleNamespace(bestblockhash=OLD_TIP)
    server.evicted_job_graveyard = {}
    server.prune_evicted_job_graveyard = lambda **_kwargs: None  # type: ignore[method-assign]
    server.ctv_broadcaster_limit = 3
    server.ctv_broadcaster_chunk_size = 1
    server.ctv_broadcaster_interval_seconds = 30.0
    server.ctv_fanout_broadcast_daemon = None
    return server


class PrismCtvRefreshPriorityTests(unittest.TestCase):
    def test_pending_tip_yields_after_current_chunk_and_later_pass_finishes_rows(self) -> None:
        server = coordinator()
        ledger = FakeLedger([pending_row(index) for index in range(3)])
        broadcaster = BlockingBroadcaster()
        server.ctv_fanout_broadcast_daemon = CtvFanoutBroadcastDaemon(
            ledger,
            broadcaster,  # type: ignore[arg-type]
            fee_sats=0,
        )
        results: list[CtvFanoutDaemonResult] = []
        errors: list[BaseException] = []

        def run_pass() -> None:
            try:
                results.append(server.run_ctv_fanout_broadcaster_once())
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        pass_thread = threading.Thread(target=run_pass)
        pass_thread.start()
        self.assertTrue(broadcaster.first_row_entered.wait(timeout=2.0))

        pending_token = server._mark_tip_refresh_pending(NEW_TIP)
        self.assertTrue(server._tip_refresh_pending())
        broadcaster.release_first_row.set()
        pass_thread.join(timeout=2.0)

        self.assertFalse(pass_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].scanned_count, 1)
        self.assertTrue(results[0].yielded_to_tip_refresh)
        self.assertEqual(len(broadcaster.calls), 1)
        # The priority check is made only after the row's durable mutation has
        # returned, so no ledger critical section survives the yielded pass.
        self.assertTrue(ledger.lock.acquire(blocking=False))
        ledger.lock.release()

        server._clear_tip_refresh_pending(pending_token)
        self.assertFalse(server._tip_refresh_pending())
        later_result = server.run_ctv_fanout_broadcaster_once()

        self.assertEqual(later_result.scanned_count, 2)
        self.assertFalse(later_result.yielded_to_tip_refresh)
        self.assertEqual(len(broadcaster.calls), 3)
        self.assertEqual(len(set(broadcaster.calls)), 3)
        self.assertEqual(
            {row["settlement_status"] for row in ledger.rows},
            {"confirmed"},
        )

    def test_older_refresh_cannot_clear_a_newer_pending_tip(self) -> None:
        server = coordinator()

        first_token = server._mark_tip_refresh_pending(NEW_TIP)
        newer_token = server._mark_tip_refresh_pending(NEWER_TIP)

        self.assertNotEqual(first_token, newer_token)
        server._clear_tip_refresh_pending(first_token)
        self.assertTrue(server._tip_refresh_pending())
        server._clear_tip_refresh_pending(newer_token)
        self.assertFalse(server._tip_refresh_pending())

    def test_routine_same_tip_observation_does_not_starve_ctv(self) -> None:
        server = coordinator()
        cancellations: list[bool] = []
        server._active_tip_refresh = (  # type: ignore[assignment]
            SimpleNamespace(tip_hash=OLD_TIP, observation_sequence=1),
            SimpleNamespace(cancel=lambda: cancellations.append(True)),
        )

        for _ in range(5):
            self.assertTrue(server.observe_tip_first_seen(OLD_TIP))

        self.assertEqual(cancellations, [])
        self.assertEqual(server.current_tip_observation_sequence, 1)
        self.assertFalse(server._tip_refresh_pending())

    def test_new_tip_observation_marks_priority_before_parent_lookup(self) -> None:
        server = coordinator()
        parent_lookup_entered = threading.Event()
        release_parent_lookup = threading.Event()
        observation_finished = threading.Event()
        errors: list[BaseException] = []

        def fetch_parent(_tip_hash: str) -> str:
            parent_lookup_entered.set()
            if not release_parent_lookup.wait(timeout=2.0):
                raise AssertionError("test did not release parent lookup")
            return OLD_TIP

        server._fetch_tip_parent_hash = fetch_parent  # type: ignore[method-assign]

        def observe_tip() -> None:
            try:
                server.observe_tip_first_seen(NEW_TIP)
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                observation_finished.set()

        observation_thread = threading.Thread(target=observe_tip)
        observation_thread.start()
        self.assertTrue(parent_lookup_entered.wait(timeout=2.0))

        self.assertTrue(server._tip_refresh_pending())
        self.assertFalse(observation_finished.is_set())
        release_parent_lookup.set()
        observation_thread.join(timeout=2.0)

        self.assertFalse(observation_thread.is_alive())
        self.assertEqual(errors, [])

    def test_yield_and_chunk_metrics_are_unlabeled_and_bounded(self) -> None:
        server = coordinator()
        server.stop_event = StopAfterOnePass()  # type: ignore[assignment]

        class MetricDaemon:
            def run_once(
                self,
                *,
                limit: int,
                chunk_size: int,
                progress_callback: object,
                tip_refresh_pending: object,
                chunk_callback: object,
            ) -> CtvFanoutDaemonResult:
                self_limit = limit
                self_chunk_size = chunk_size
                self.assert_wiring(self_limit, self_chunk_size)
                assert callable(progress_callback)
                assert callable(tip_refresh_pending)
                assert callable(chunk_callback)
                progress_callback()
                progress_callback()
                chunk_callback(CtvFanoutChunkResult(processed_count=2, elapsed_seconds=0.25))
                return CtvFanoutDaemonResult(
                    scanned_count=2,
                    submitted_count=0,
                    updated_count=2,
                    failed_count=0,
                    yielded_to_tip_refresh=True,
                )

            @staticmethod
            def assert_wiring(limit: int, chunk_size: int) -> None:
                if limit != 3 or chunk_size != 1:
                    raise AssertionError((limit, chunk_size))

        server.ctv_fanout_broadcast_daemon = MetricDaemon()  # type: ignore[assignment]
        with patch("builtins.print"):
            server.ctv_fanout_broadcaster_loop()

        lines = server.ctv_fanout_broadcaster_metrics_lines()
        metrics = "\n".join(lines)
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total 1",
            metrics,
        )
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_chunk_rows_sum 2",
            metrics,
        )
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_chunk_rows_count 1",
            metrics,
        )
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_chunk_seconds_sum 0.250000",
            metrics,
        )
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_chunk_seconds_count 1",
            metrics,
        )

        new_metric_lines = [
            line
            for line in lines
            if line.startswith("qbit_prism_ctv_fanout_broadcaster_chunk_")
            or line.startswith(
                "qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total"
            )
        ]
        for line in new_metric_lines:
            labels = line.split("{", 1)[1].split("}", 1)[0] if "{" in line else ""
            self.assertTrue(not labels or labels.startswith('le="'), line)
            self.assertNotIn("fanout", labels)
            self.assertNotIn("txid", labels)
            self.assertNotIn("address", labels)
            self.assertNotIn("row", labels)


if __name__ == "__main__":
    unittest.main()
