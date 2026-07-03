#!/usr/bin/env python3

from __future__ import annotations

import json
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
from lab.prism.ctv_broadcaster_daemon import CtvFanoutBroadcastDaemon, artifact_from_status_row
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
        seen_urls: list[str] = []

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps({"error": None, "result": {"ok": True}}).encode()

        def fake_urlopen(request: urllib.request.Request, timeout: int) -> FakeResponse:
            self.assertEqual(timeout, 10)
            seen_urls.append(request.full_url)
            return FakeResponse()

        rpc = JsonRpc(host="127.0.0.1", port=18452, user="u", password="p")
        with patch("urllib.request.urlopen", fake_urlopen):
            self.assertEqual(rpc.call("getwalletinfo", wallet="fee wallet"), {"ok": True})

        self.assertEqual(seen_urls, ["http://127.0.0.1:18452/wallet/fee%20wallet"])


if __name__ == "__main__":
    unittest.main()
