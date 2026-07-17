#!/usr/bin/env python3

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
import threading
import urllib.request
import unittest
from unittest.mock import patch

from lab.prism.ctv_broadcaster import (
    BROADCAST,
    BROADCASTABLE,
    CONFIRMED,
    REORGED,
    BroadcastAttempt,
)
from lab.prism.ctv_broadcaster_daemon import (
    CtvFanoutBroadcastDaemon,
    CtvFanoutChunkResult,
    CtvFanoutDaemonResult,
    MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE,
    artifact_from_status_row,
)
from lab.prism.run_ctv_broadcaster_daemon import env_positive_int, make_daemon_from_env
from lab.prism.prism_coordinator import JsonRpc


def pending_row(fanout_txid: str = "aa" * 32) -> dict[str, object]:
    return {
        "fanout_txid": fanout_txid,
        "fanout_tx_hex": "0300000001",
        "anchor_vout": 2,
        "parent_coinbase_txid": "bb" * 32,
        "parent_coinbase_vout": 1,
        "block_hash": "cc" * 32,
        "block_height": 100,
        "settlement_status": "broadcastable",
    }


class FakeLedger:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.updates: list[dict[str, object]] = []
        self.attempts: list[dict[str, object]] = []

    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]:
        return self.rows[:limit]

    def update_ctv_fanout_status(self, *, fanout_txid: str, settlement_status: str) -> dict[str, int | str]:
        self.updates.append({"fanout_txid": fanout_txid, "settlement_status": settlement_status})
        return {"backend": "fake", "updated_count": 1}

    def record_ctv_fanout_broadcast_attempt(
        self,
        *,
        fanout_txid: str,
        attempt_status: str,
        package_tx_hexes: list[str] | None = None,
        package_txids: list[str] | None = None,
        submit_result: dict[str, object] | None = None,
        error: str | None = None,
    ) -> dict[str, int | str]:
        self.attempts.append(
            {
                "fanout_txid": fanout_txid,
                "attempt_status": attempt_status,
                "package_tx_hexes": package_tx_hexes or [],
                "package_txids": package_txids or [],
                "submit_result": submit_result or {},
                "error": error,
            }
        )
        return {"backend": "fake", "attempt_count": len(self.attempts)}


class FakeBroadcaster:
    def __init__(self, attempts: dict[str, BroadcastAttempt]) -> None:
        self.attempts = attempts
        self.fees: list[int] = []

    def broadcast(self, artifact: object, fee_sats: int) -> BroadcastAttempt:
        self.fees.append(fee_sats)
        fanout_txid = getattr(artifact, "fanout_txid")
        return self.attempts[fanout_txid]


class CtvFanoutBroadcastDaemonTests(unittest.TestCase):
    def test_artifact_from_status_row_preserves_parent_coinbase_vout(self) -> None:
        artifact = artifact_from_status_row(pending_row())

        self.assertEqual(artifact.parent_coinbase_vout, 1)
        self.assertEqual(artifact.anchor_vout, 2)

    def test_artifact_from_status_row_allows_no_anchor_fanout(self) -> None:
        row = pending_row()
        row["anchor_vout"] = None
        artifact = artifact_from_status_row(row)

        self.assertIsNone(artifact.anchor_vout)

    def test_submitted_package_is_journaled(self) -> None:
        fanout_txid = "aa" * 32
        ledger = FakeLedger([pending_row(fanout_txid)])
        broadcaster = FakeBroadcaster(
            {
                fanout_txid: BroadcastAttempt(
                    fanout_txid=fanout_txid,
                    status=BROADCAST,
                    submitted=True,
                    child_txid="dd" * 32,
                    fee_sats=750,
                    package_msg="success",
                    detail="initial broadcast",
                )
            }
        )

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=750).run_once()

        self.assertEqual(result.submitted_count, 1)
        self.assertEqual(result.updated_count, 0)
        self.assertEqual(ledger.attempts[0]["attempt_status"], "submitted")
        self.assertEqual(ledger.attempts[0]["package_txids"], [fanout_txid, "dd" * 32])
        self.assertEqual(broadcaster.fees, [750])

    def test_terminal_and_maturity_statuses_update_without_journal(self) -> None:
        confirmed_txid = "11" * 32
        reorged_txid = "22" * 32
        broadcast_txid = "33" * 32
        ledger = FakeLedger([pending_row(confirmed_txid), pending_row(reorged_txid), pending_row(broadcast_txid)])
        broadcaster = FakeBroadcaster(
            {
                confirmed_txid: BroadcastAttempt(confirmed_txid, CONFIRMED, submitted=False),
                reorged_txid: BroadcastAttempt(reorged_txid, REORGED, submitted=False),
                broadcast_txid: BroadcastAttempt(broadcast_txid, BROADCAST, submitted=False),
            }
        )

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=500).run_once()

        self.assertEqual(result.submitted_count, 0)
        self.assertEqual(result.updated_count, 3)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(
            ledger.updates,
            [
                {"fanout_txid": confirmed_txid, "settlement_status": "confirmed"},
                {"fanout_txid": reorged_txid, "settlement_status": "reorged"},
                {"fanout_txid": broadcast_txid, "settlement_status": "broadcast_submitted"},
            ],
        )
        self.assertEqual(ledger.attempts, [])

    def test_rejected_package_is_journaled(self) -> None:
        fanout_txid = "44" * 32
        ledger = FakeLedger([pending_row(fanout_txid)])
        broadcaster = FakeBroadcaster(
            {
                fanout_txid: BroadcastAttempt(
                    fanout_txid=fanout_txid,
                    status=BROADCASTABLE,
                    submitted=False,
                    fee_sats=900,
                    package_msg="insufficient fee",
                    detail="initial broadcast: package rejected (insufficient fee)",
                )
            }
        )

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=900).run_once()

        self.assertEqual(result.failed_count, 1)
        self.assertEqual(ledger.updates, [])
        self.assertEqual(ledger.attempts[0]["attempt_status"], "rejected")
        self.assertIn("insufficient fee", str(ledger.attempts[0]["error"]))

    def test_failed_rows_are_not_rebroadcast(self) -> None:
        fanout_txid = "44" * 32
        row = pending_row(fanout_txid)
        row["settlement_status"] = "failed"
        ledger = FakeLedger([row])
        broadcaster = FakeBroadcaster({})

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=900).run_once()

        self.assertEqual(result.scanned_count, 1)
        self.assertEqual(result.submitted_count, 0)
        self.assertEqual(result.updated_count, 0)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(ledger.attempts, [])
        self.assertEqual(broadcaster.fees, [])

    def test_future_backoff_rows_are_not_rebroadcast(self) -> None:
        fanout_txid = "45" * 32
        row = pending_row(fanout_txid)
        row["next_broadcast_attempt_at"] = datetime.now(timezone.utc) + timedelta(minutes=5)
        ledger = FakeLedger([row])
        broadcaster = FakeBroadcaster({})

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=900).run_once()

        self.assertEqual(result.scanned_count, 1)
        self.assertEqual(result.submitted_count, 0)
        self.assertEqual(result.updated_count, 0)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(ledger.attempts, [])
        self.assertEqual(broadcaster.fees, [])

    def test_progress_callback_covers_skipped_due_updated_submitted_and_failed_rows(self) -> None:
        skipped_txid = "10" * 32
        not_due_txid = "20" * 32
        updated_txid = "30" * 32
        submitted_txid = "40" * 32
        failed_txid = "50" * 32
        skipped = pending_row(skipped_txid)
        skipped["settlement_status"] = "failed"
        not_due = pending_row(not_due_txid)
        not_due["next_broadcast_attempt_at"] = datetime.now(timezone.utc) + timedelta(minutes=5)
        ledger = FakeLedger(
            [
                skipped,
                not_due,
                pending_row(updated_txid),
                pending_row(submitted_txid),
                pending_row(failed_txid),
            ]
        )
        broadcaster = FakeBroadcaster(
            {
                updated_txid: BroadcastAttempt(updated_txid, CONFIRMED, submitted=False),
                submitted_txid: BroadcastAttempt(
                    submitted_txid,
                    BROADCAST,
                    submitted=True,
                    fee_sats=900,
                    package_msg="success",
                ),
                failed_txid: BroadcastAttempt(
                    failed_txid,
                    BROADCASTABLE,
                    submitted=False,
                    fee_sats=900,
                    package_msg="rejected",
                ),
            }
        )
        progress_snapshots: list[tuple[int, int, int]] = []

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=900).run_once(
            progress_callback=lambda: progress_snapshots.append(
                (len(broadcaster.fees), len(ledger.updates), len(ledger.attempts))
            )
        )

        self.assertEqual(
            result,
            CtvFanoutDaemonResult(
                scanned_count=5,
                submitted_count=1,
                updated_count=1,
                failed_count=1,
            ),
        )
        self.assertEqual(
            progress_snapshots,
            [
                (0, 0, 0),
                (0, 0, 0),
                (1, 1, 0),
                (2, 1, 1),
                (3, 1, 2),
            ],
        )

    def test_yields_between_committed_chunks_and_processes_remaining_rows_next_pass(self) -> None:
        txids = [f"{value:02x}" * 32 for value in range(1, 5)]

        class LockTrackingLedger(FakeLedger):
            def __init__(self, rows: list[dict[str, object]]) -> None:
                super().__init__(rows)
                self.write_active = False

            def update_ctv_fanout_status(
                self,
                *,
                fanout_txid: str,
                settlement_status: str,
            ) -> dict[str, int | str]:
                self.write_active = True
                try:
                    return super().update_ctv_fanout_status(
                        fanout_txid=fanout_txid,
                        settlement_status=settlement_status,
                    )
                finally:
                    self.write_active = False

            def record_ctv_fanout_broadcast_attempt(
                self,
                *,
                fanout_txid: str,
                attempt_status: str,
                package_tx_hexes: list[str] | None = None,
                package_txids: list[str] | None = None,
                submit_result: dict[str, object] | None = None,
                error: str | None = None,
            ) -> dict[str, int | str]:
                self.write_active = True
                try:
                    return super().record_ctv_fanout_broadcast_attempt(
                        fanout_txid=fanout_txid,
                        attempt_status=attempt_status,
                        package_tx_hexes=package_tx_hexes,
                        package_txids=package_txids,
                        submit_result=submit_result,
                        error=error,
                    )
                finally:
                    self.write_active = False

        class IdempotentBroadcaster:
            def __init__(self) -> None:
                self.submitted_txids: set[str] = set()
                self.calls: list[str] = []

            def broadcast(self, artifact: object, fee_sats: int) -> BroadcastAttempt:
                fanout_txid = str(getattr(artifact, "fanout_txid"))
                self.calls.append(fanout_txid)
                if fanout_txid in self.submitted_txids:
                    return BroadcastAttempt(fanout_txid, BROADCAST, submitted=False)
                self.submitted_txids.add(fanout_txid)
                return BroadcastAttempt(
                    fanout_txid,
                    BROADCAST,
                    submitted=True,
                    fee_sats=fee_sats,
                    package_msg="success",
                )

        ledger = LockTrackingLedger([pending_row(txid) for txid in txids])
        broadcaster = IdempotentBroadcaster()
        daemon = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=0)
        refresh_pending = threading.Event()
        progress_count = 0
        lock_states_at_refresh_checks: list[bool] = []
        chunks: list[CtvFanoutChunkResult] = []

        def record_progress_and_request_refresh() -> None:
            nonlocal progress_count
            progress_count += 1
            if progress_count == 2:
                refresh_pending.set()

        def tip_refresh_pending() -> bool:
            lock_states_at_refresh_checks.append(ledger.write_active)
            return refresh_pending.is_set()

        first_result = daemon.run_once(
            limit=4,
            chunk_size=2,
            progress_callback=record_progress_and_request_refresh,
            tip_refresh_pending=tip_refresh_pending,
            chunk_callback=chunks.append,
        )

        self.assertEqual(
            first_result,
            CtvFanoutDaemonResult(
                scanned_count=2,
                submitted_count=2,
                updated_count=0,
                failed_count=0,
                yielded_to_tip_refresh=True,
            ),
        )
        self.assertEqual([chunk.processed_count for chunk in chunks], [2])
        self.assertEqual([attempt["fanout_txid"] for attempt in ledger.attempts], txids[:2])
        self.assertEqual(lock_states_at_refresh_checks, [False, False])

        refresh_pending.clear()
        second_result = daemon.run_once(limit=4, chunk_size=2)

        self.assertEqual(second_result.scanned_count, 4)
        self.assertEqual(second_result.submitted_count, 2)
        self.assertEqual(second_result.updated_count, 2)
        self.assertFalse(second_result.yielded_to_tip_refresh)
        self.assertEqual(broadcaster.calls, txids[:2] + txids)
        self.assertEqual(broadcaster.submitted_txids, set(txids))
        self.assertEqual([attempt["fanout_txid"] for attempt in ledger.attempts], txids)

    def test_pending_refresh_can_prepare_while_current_chunk_finishes(self) -> None:
        first_txid = "61" * 32
        second_txid = "62" * 32
        ledger = FakeLedger([pending_row(first_txid), pending_row(second_txid)])
        current_row_started = threading.Event()
        release_current_row = threading.Event()
        refresh_pending = threading.Event()
        refresh_prepared = threading.Event()

        class BlockingBroadcaster:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def broadcast(self, artifact: object, fee_sats: int) -> BroadcastAttempt:
                fanout_txid = str(getattr(artifact, "fanout_txid"))
                self.calls.append(fanout_txid)
                current_row_started.set()
                if not release_current_row.wait(timeout=5):
                    raise AssertionError("test did not release broadcaster row")
                return BroadcastAttempt(
                    fanout_txid,
                    BROADCAST,
                    submitted=True,
                    fee_sats=fee_sats,
                    package_msg="success",
                )

        broadcaster = BlockingBroadcaster()
        daemon = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=0)
        results: list[CtvFanoutDaemonResult] = []
        failures: list[BaseException] = []

        def run_broadcaster_pass() -> None:
            try:
                results.append(
                    daemon.run_once(
                        limit=2,
                        chunk_size=1,
                        tip_refresh_pending=refresh_pending.is_set,
                    )
                )
            except BaseException as exc:  # pragma: no cover - surfaced below
                failures.append(exc)

        broadcaster_thread = threading.Thread(target=run_broadcaster_pass)
        broadcaster_thread.start()
        self.assertTrue(current_row_started.wait(timeout=5))

        def prepare_refresh() -> None:
            refresh_pending.set()
            refresh_prepared.set()

        refresh_thread = threading.Thread(target=prepare_refresh)
        refresh_thread.start()
        refresh_thread.join(timeout=5)
        self.assertFalse(refresh_thread.is_alive())
        self.assertTrue(refresh_prepared.is_set())

        release_current_row.set()
        broadcaster_thread.join(timeout=5)
        self.assertFalse(broadcaster_thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(broadcaster.calls, [first_txid])
        self.assertEqual(len(results), 1)
        self.assertTrue(results[0].yielded_to_tip_refresh)
        self.assertEqual(results[0].scanned_count, 1)

    def test_chunk_observations_report_bounded_rows_and_deterministic_durations(self) -> None:
        rows = [pending_row(f"{value:02x}" * 32) for value in range(3)]
        for row in rows:
            row["settlement_status"] = "failed"
        chunks: list[CtvFanoutChunkResult] = []

        with patch(
            "lab.prism.ctv_broadcaster_daemon.monotonic",
            side_effect=[10.0, 10.25, 20.0, 20.5],
        ):
            result = CtvFanoutBroadcastDaemon(
                FakeLedger(rows),
                FakeBroadcaster({}),
                fee_sats=0,
            ).run_once(chunk_size=2, chunk_callback=chunks.append)

        self.assertEqual(result.scanned_count, 3)
        self.assertEqual(
            chunks,
            [
                CtvFanoutChunkResult(processed_count=2, elapsed_seconds=0.25),
                CtvFanoutChunkResult(processed_count=1, elapsed_seconds=0.5),
            ],
        )

    def test_rejects_nonpositive_chunk_size_before_querying_ledger(self) -> None:
        ledger = FakeLedger([])
        daemon = CtvFanoutBroadcastDaemon(ledger, FakeBroadcaster({}), fee_sats=0)

        for chunk_size in (0, -1):
            with self.subTest(chunk_size=chunk_size), self.assertRaisesRegex(
                ValueError,
                "chunk_size",
            ):
                daemon.run_once(chunk_size=chunk_size)

        with self.assertRaisesRegex(ValueError, "at most"):
            daemon.run_once(
                chunk_size=MAX_CTV_FANOUT_BROADCASTER_CHUNK_SIZE + 1
            )

    def test_direct_parent_failure_is_journaled_without_terminal_rejection(self) -> None:
        fanout_txid = "45" * 32
        ledger = FakeLedger([pending_row(fanout_txid)])
        broadcaster = FakeBroadcaster(
            {
                fanout_txid: BroadcastAttempt(
                    fanout_txid=fanout_txid,
                    status=BROADCASTABLE,
                    submitted=False,
                    fee_sats=0,
                    package_msg="error",
                    detail="initial broadcast: transient RPC error",
                )
            }
        )

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=0).run_once()

        self.assertEqual(result.failed_count, 1)
        self.assertEqual(ledger.updates, [])
        self.assertEqual(ledger.attempts[0]["attempt_status"], "planned")
        self.assertEqual(ledger.attempts[0]["submit_result"]["fee_sats"], 0)

    def test_cpfp_rpc_error_is_journaled_without_terminal_rejection(self) -> None:
        fanout_txid = "46" * 32
        ledger = FakeLedger([pending_row(fanout_txid)])
        broadcaster = FakeBroadcaster(
            {
                fanout_txid: BroadcastAttempt(
                    fanout_txid=fanout_txid,
                    status=BROADCASTABLE,
                    submitted=False,
                    fee_sats=900,
                    package_msg="error",
                    detail="initial broadcast: transient RPC error",
                )
            }
        )

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=900).run_once()

        self.assertEqual(result.failed_count, 1)
        self.assertEqual(ledger.updates, [])
        self.assertEqual(ledger.attempts[0]["attempt_status"], "planned")
        self.assertEqual(ledger.attempts[0]["submit_result"]["fee_sats"], 900)

    def test_direct_txid_mismatch_is_terminal_rejection(self) -> None:
        fanout_txid = "47" * 32
        ledger = FakeLedger([pending_row(fanout_txid)])
        broadcaster = FakeBroadcaster(
            {
                fanout_txid: BroadcastAttempt(
                    fanout_txid=fanout_txid,
                    status=BROADCASTABLE,
                    submitted=False,
                    fee_sats=0,
                    package_msg="txid_mismatch",
                    detail="initial broadcast: submitted txid did not match artifact",
                )
            }
        )

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=0).run_once()

        self.assertEqual(result.failed_count, 1)
        self.assertEqual(ledger.attempts[0]["attempt_status"], "rejected")
        self.assertEqual(ledger.attempts[0]["submit_result"]["fee_sats"], 0)

    def test_allows_zero_fee_for_direct_parent_broadcast(self) -> None:
        fanout_txid = "55" * 32
        ledger = FakeLedger([pending_row(fanout_txid)])
        broadcaster = FakeBroadcaster(
            {
                fanout_txid: BroadcastAttempt(
                    fanout_txid=fanout_txid,
                    status=BROADCAST,
                    submitted=True,
                    fee_sats=0,
                    package_msg="success",
                    detail="initial broadcast",
                )
            }
        )

        result = CtvFanoutBroadcastDaemon(ledger, broadcaster, fee_sats=0).run_once()

        self.assertEqual(result.submitted_count, 1)
        self.assertEqual(ledger.attempts[0]["package_txids"], [fanout_txid])
        self.assertEqual(ledger.attempts[0]["submit_result"]["fee_sats"], 0)

    def test_rejects_negative_fee(self) -> None:
        with self.assertRaisesRegex(ValueError, "fee_sats"):
            CtvFanoutBroadcastDaemon(FakeLedger([]), FakeBroadcaster({}), fee_sats=-1)

    def test_env_positive_int_rejects_missing_or_invalid_values(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(env_positive_int("MISSING_WITH_DEFAULT", 5), 5)
            with self.assertRaisesRegex(SystemExit, "REQUIRED"):
                env_positive_int("REQUIRED")
        for value in ("0", "-1", "nope"):
            with self.subTest(value=value), patch.dict("os.environ", {"FEE": value}, clear=True):
                with self.assertRaises(SystemExit):
                    env_positive_int("FEE")

    def test_make_daemon_from_env_requires_wallet_for_cpfp_fee(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "QBIT_RPC_HOST": "127.0.0.1",
                "QBIT_RPC_USER": "user",
                "QBIT_RPC_PASSWORD": "password",
                "PRISM_CTV_BROADCASTER_FEE_BITS": "1",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "PRISM_CTV_BROADCASTER_WALLET"):
                make_daemon_from_env()

    def test_json_rpc_wallet_call_uses_wallet_url(self) -> None:
        seen: list[tuple[str, str]] = []

        class FakeResponse:
            status = 200

            def read(self) -> bytes:
                return json.dumps({"error": None, "result": {"ok": True}}).encode()

        class FakeConnection:
            def __init__(self, host: str, port: int, timeout: float) -> None:
                self.host = host
                self.port = port
                self.timeout = timeout
                self.sock = None

            def request(self, method: str, path: str, body: bytes, headers: dict) -> None:
                seen.append((method, path))

            def getresponse(self) -> FakeResponse:
                return FakeResponse()

            def close(self) -> None:
                return None

        rpc = JsonRpc(host="127.0.0.1", port=18452, user="u", password="p")
        with patch("http.client.HTTPConnection", FakeConnection):
            self.assertEqual(rpc.call("getwalletinfo", wallet="fee wallet"), {"ok": True})

        self.assertEqual(seen, [("POST", "/wallet/fee%20wallet")])

    def test_json_rpc_reuses_connection_and_retries_after_stale_drop(self) -> None:
        # First call establishes a keep-alive connection; the second reuses it;
        # a mid-call disconnect (idle keep-alive closed by the server) is
        # retried once on a fresh connection rather than surfacing an error.
        constructed: list[object] = []

        class FakeResponse:
            status = 200

            def read(self) -> bytes:
                return json.dumps({"error": None, "result": "tip"}).encode()

        class FakeConnection:
            def __init__(self, host: str, port: int, timeout: float) -> None:
                self.sock = None
                self.requests = 0
                self.closed = False
                self.fail_next = False
                constructed.append(self)

            def request(self, method: str, path: str, body: bytes, headers: dict) -> None:
                self.requests += 1
                if self.fail_next:
                    raise ConnectionResetError("stale keep-alive")

            def getresponse(self) -> FakeResponse:
                return FakeResponse()

            def close(self) -> None:
                self.closed = True

        rpc = JsonRpc(host="127.0.0.1", port=18452, user="u", password="p")
        with patch("http.client.HTTPConnection", FakeConnection):
            self.assertEqual(rpc.call("getbestblockhash"), "tip")
            self.assertEqual(rpc.call("getbestblockhash"), "tip")
            self.assertEqual(len(constructed), 1)  # one connection reused
            self.assertEqual(constructed[0].requests, 2)

            constructed[0].fail_next = True  # live connection goes stale mid-request
            self.assertEqual(rpc.call("getbestblockhash"), "tip")
            self.assertTrue(constructed[0].closed)
            self.assertEqual(len(constructed), 2)  # reconnected once
            self.assertEqual(constructed[1].requests, 1)


if __name__ == "__main__":
    unittest.main()
