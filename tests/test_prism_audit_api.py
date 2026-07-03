#!/usr/bin/env python3

from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from lab.prism.prism_coordinator import make_audit_handler


class FakeLedger:
    backend_name = "fake-ledger"
    block_hash = "a" * 64
    fanout_txid = "b" * 64
    audit_commitment_leaf_hex = "d" * 64

    def audit_share_window(self, *, anchor_job_issued_at_ms: int, network_difficulty: int) -> list[dict[str, object]]:
        return [
            {
                "share_id": "share-2",
                "counted_difficulty": network_difficulty,
                "job_issued_at_ms": anchor_job_issued_at_ms,
            }
        ]

    def audit_block_payouts(self, *, block_hash: str) -> list[dict[str, object]]:
        if block_hash == "a" * 64:
            return [{"block_hash": block_hash, "chain_state": "inactive", "miner_id": "miner-a"}]
        return []

    def audit_bundle(self, *, block_hash: str) -> dict[str, object] | None:
        if block_hash == self.block_hash:
            return {"block_hash": block_hash, "audit_bundle": {"schema": "qbit.prism.audit-bundle.v1"}}
        return None

    def audit_bundle_by_commitment(self, *, commitment_leaf_hex: str) -> dict[str, object] | None:
        if commitment_leaf_hex == self.audit_commitment_leaf_hex:
            return {
                "block_hash": self.block_hash,
                "audit_commitment_leaf_hex": commitment_leaf_hex,
                "audit_bundle": {
                    "schema": "qbit.prism.audit-bundle.v1",
                    "audit_commitment_leaves_hex": [commitment_leaf_hex],
                },
            }
        return None

    def audit_ctv_fanouts(self, *, block_hash: str) -> list[dict[str, object]]:
        if block_hash == self.block_hash:
            return [
                {
                    "block_hash": block_hash,
                    "fanout_txid": self.fanout_txid,
                    "fanout_tx_hex": "03",
                    "anchor_vout": 2,
                    "settlement_status": "awaiting_maturity",
                }
            ]
        return []

    def audit_ctv_fanout_manifest_set(self, *, block_hash: str) -> dict[str, object] | None:
        if block_hash == self.block_hash:
            return {
                "schema": "qbit.prism.ctv-fanout-recovery.v1",
                "block_hash": block_hash,
                "manifest_set_sha256": "c" * 64,
                "manifest_set_json": "{}",
                "manifest_set": {"schema": "qbit.prism.ctv-fanout-manifest-set.v1"},
            }
        return None

    def ctv_fanout_status(self, *, fanout_txid: str) -> dict[str, object] | None:
        if fanout_txid == self.fanout_txid:
            return {
                "schema": "qbit.prism.ctv-fanout-status.v1",
                "fanout_txid": fanout_txid,
                "settlement_status": "awaiting_maturity",
                "broadcast_attempts": [],
            }
        return None

    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]:
        return [
            {
                "schema": "qbit.prism.ctv-fanout-status.v1",
                "fanout_txid": self.fanout_txid,
                "block_hash": self.block_hash,
                "block_height": 100,
                "chunk_index": 0,
                "chunk_count": 1,
                "settlement_status": "broadcastable",
            }
        ][:limit]

    def current_owed_balances(self) -> list[dict[str, object]]:
        return [
            {"recipient_id": "miner-a", "order_key": "a", "p2mr_program_hex": "11" * 32, "balance_sats": 42},
            {"recipient_id": "miner-a", "order_key": "b", "p2mr_program_hex": "22" * 32, "balance_sats": 8},
            {"recipient_id": "miner-b", "order_key": "c", "p2mr_program_hex": "33" * 32, "balance_sats": 100},
        ]

    def recipient_payout_history(self, *, recipient_id: str, limit: int = 50) -> list[dict[str, object]]:
        rows = [
            {
                "block_hash": self.block_hash,
                "block_height": 100,
                "coinbase_txid": "e" * 64,
                "payout_manifest_sha256": "f" * 64,
                "recipient_id": "miner-a",
                "order_key": "a",
                "p2mr_program_hex": "11" * 32,
                "onchain_amount_sats": 25,
                "carry_forward_balance_sats": 17,
                "action": "onchain",
                "maturity_state": "immature",
            },
            {
                "block_hash": "9" * 64,
                "block_height": 99,
                "coinbase_txid": "8" * 64,
                "payout_manifest_sha256": "7" * 64,
                "recipient_id": "miner-b",
                "order_key": "c",
                "p2mr_program_hex": "33" * 32,
                "onchain_amount_sats": 100,
                "carry_forward_balance_sats": 0,
                "action": "onchain",
                "maturity_state": "mature",
            },
        ]
        return [row for row in rows if row["recipient_id"] == recipient_id][:limit]

    def carry_forward_integrity_report(self) -> dict[str, object]:
        return {
            "schema": "qbit.prism.carry-forward-integrity.v1",
            "backend": "fake-ledger",
            "checked_active_rows": 2,
            "audit_chain_version": "qbit.prism.carry-forward-active-delta-chain.v1",
            "audit_row_count": 2,
            "audit_head_sha256": "ab" * 32,
            "mismatch_count": 1,
            "mismatches": [
                {
                    "carry_forward_seq": 2,
                    "block_hash": self.block_hash,
                    "block_height": 100,
                    "recipient_id": "miner-a",
                    "order_key": "a",
                    "p2mr_program_hex": "11" * 32,
                    "prior_balance_sats": 0,
                    "expected_prior_balance_sats": 17,
                    "mismatch_reason": "prior_balance",
                }
            ],
        }

    def metrics(self) -> dict[str, int]:
        return {"blocks": 1, "owed_accounts": 1}

    def all_shares(self) -> list[object]:
        return [object(), object()]


class FakeCoordinator:
    def __init__(self) -> None:
        self.ledger = FakeLedger()
        self.accepted_block_count = 1

    def health_payload(self) -> dict[str, object]:
        return {"ok": True, "ledger_backend": self.ledger.backend_name}

    def latest_evidence_payload(self) -> dict[str, object]:
        return {"schema": "qbit.prism.live-stratum-evidence.v1", "block_hash": "a" * 64}

    def owed_balances_payload(self) -> dict[str, object]:
        return {
            "schema": "qbit.prism.owed-balances.v1",
            "balances": self.ledger.current_owed_balances(),
        }

    def carry_forward_integrity_payload(self) -> dict[str, object]:
        report = self.ledger.carry_forward_integrity_report()
        report["ledger_backend"] = self.ledger.backend_name
        return report

    def miner_status_payload(self, recipient_id: str) -> dict[str, object]:
        balances = [
            balance
            for balance in self.ledger.current_owed_balances()
            if str(balance.get("recipient_id")) == recipient_id
        ]
        return {
            "schema": "qbit.prism.miner-status.v1",
            "recipient_id": recipient_id,
            "owed_balance_sats": sum(int(balance["balance_sats"]) for balance in balances),
            "owed_balances": balances,
            "recent_payouts": self.ledger.recipient_payout_history(recipient_id=recipient_id),
        }

    def metrics_payload(self) -> str:
        return (
            "qbit_prism_accepted_shares_total 2\n"
            "qbit_prism_blocks_accepted_total 1\n"
            "qbit_prism_ctv_fanouts_pending 1\n"
            "qbit_prism_ctv_fanouts_broadcastable 1\n"
            "qbit_prism_ctv_fanouts_failed 0\n"
        )


class PrismAuditApiTests(unittest.TestCase):
    def setUp(self) -> None:
        handler = make_audit_handler(FakeCoordinator())  # type: ignore[arg-type]
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def get_json(self, path: str) -> dict[str, object]:
        with urllib.request.urlopen(self.base_url + path, timeout=5) as response:
            return json.loads(response.read())

    def get_text(self, path: str) -> str:
        with urllib.request.urlopen(self.base_url + path, timeout=5) as response:
            return response.read().decode()

    def test_health_latest_owed_and_metrics_endpoints(self) -> None:
        self.assertTrue(self.get_json("/healthz")["ok"])
        self.assertEqual(self.get_json("/audit/latest")["schema"], "qbit.prism.live-stratum-evidence.v1")
        self.assertEqual(len(self.get_json("/owed-balances")["balances"]), 3)
        miner_status = self.get_json("/miners/miner-a/status")
        payout_status = self.get_json("/payouts/miner-b/status")
        missing_status = self.get_json("/miners/unknown/status")
        self.assertEqual(miner_status["owed_balance_sats"], 50)
        self.assertEqual(len(miner_status["owed_balances"]), 2)
        self.assertEqual(len(miner_status["recent_payouts"]), 1)
        self.assertEqual(miner_status["recent_payouts"][0]["action"], "onchain")
        self.assertEqual(miner_status["recent_payouts"][0]["maturity_state"], "immature")
        self.assertEqual(payout_status["owed_balance_sats"], 100)
        self.assertEqual(payout_status["recent_payouts"][0]["block_height"], 99)
        self.assertEqual(missing_status["owed_balance_sats"], 0)
        self.assertEqual(missing_status["owed_balances"], [])
        self.assertEqual(missing_status["recent_payouts"], [])

        metrics = self.get_text("/metrics")

        self.assertIn("qbit_prism_accepted_shares_total 2", metrics)
        self.assertIn("qbit_prism_blocks_accepted_total 1", metrics)
        self.assertIn("qbit_prism_ctv_fanouts_pending 1", metrics)
        self.assertIn("qbit_prism_ctv_fanouts_broadcastable 1", metrics)
        self.assertIn("qbit_prism_ctv_fanouts_failed 0", metrics)

    def test_audit_endpoints_serialize_rows_and_bundle(self) -> None:
        block_hash = "a" * 64
        commitment_leaf = "d" * 64

        window = self.get_json("/audit/share-window?anchor_job_issued_at_ms=1234&network_difficulty=99")
        payouts = self.get_json(f"/audit/blocks/{block_hash}/payouts")
        bundle = self.get_json(f"/audit/blocks/{block_hash}/bundle")
        commitment_bundle = self.get_json(f"/audit/commitments/{commitment_leaf}/bundle")

        self.assertEqual(window["rows"], [{"share_id": "share-2", "counted_difficulty": 99, "job_issued_at_ms": 1234}])
        self.assertEqual(
            payouts["rows"],
            [{"block_hash": block_hash, "chain_state": "inactive", "miner_id": "miner-a"}],
        )
        self.assertEqual(bundle["audit_bundle"], {"schema": "qbit.prism.audit-bundle.v1"})
        self.assertEqual(commitment_bundle["block_hash"], block_hash)
        self.assertEqual(commitment_bundle["audit_commitment_leaf_hex"], commitment_leaf)
        self.assertEqual(commitment_bundle["audit_bundle"]["audit_commitment_leaves_hex"], [commitment_leaf])

    def test_carry_forward_integrity_endpoint(self) -> None:
        report = self.get_json("/audit/carry-forward-integrity")
        alias = self.get_json("/audit/ledger-integrity")

        self.assertEqual(report["schema"], "qbit.prism.carry-forward-integrity.v1")
        self.assertEqual(report["ledger_backend"], "fake-ledger")
        self.assertEqual(report["checked_active_rows"], 2)
        self.assertEqual(report["audit_chain_version"], "qbit.prism.carry-forward-active-delta-chain.v1")
        self.assertEqual(report["audit_row_count"], 2)
        self.assertEqual(report["audit_head_sha256"], "ab" * 32)
        self.assertEqual(report["mismatch_count"], 1)
        self.assertEqual(report["mismatches"][0]["mismatch_reason"], "prior_balance")
        self.assertEqual(alias, report)

    def test_ctv_fanout_recovery_and_status_endpoints(self) -> None:
        block_hash = "a" * 64
        fanout_txid = "b" * 64

        fanouts = self.get_json(f"/audit/blocks/{block_hash}/ctv-fanouts")
        manifest_set = self.get_json(f"/audit/blocks/{block_hash}/ctv-fanout-manifest-set")
        status = self.get_json(f"/audit/fanouts/{fanout_txid}/status")
        pending = self.get_json("/audit/fanouts/pending?limit=1")

        self.assertEqual(fanouts["schema"], "qbit.prism.audit-ctv-fanouts.v1")
        self.assertEqual(fanouts["rows"][0]["fanout_txid"], fanout_txid)
        self.assertEqual(manifest_set["manifest_set_sha256"], "c" * 64)
        self.assertEqual(status["settlement_status"], "awaiting_maturity")
        self.assertEqual(pending["schema"], "qbit.prism.pending-ctv-fanouts.v1")
        self.assertEqual(pending["count"], 1)
        self.assertEqual(pending["rows"][0]["settlement_status"], "broadcastable")

    def test_bad_block_hash_returns_400(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json("/audit/blocks/not-a-hash/payouts")

        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()

    def test_bad_fanout_txid_returns_400(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json("/audit/fanouts/not-a-txid/status")

        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()

    def test_bad_commitment_hash_returns_400(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json("/audit/commitments/not-a-hash/bundle")

        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()

    def test_unknown_commitment_hash_returns_404(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json(f"/audit/commitments/{'e' * 64}/bundle")

        self.assertEqual(raised.exception.code, 404)
        raised.exception.close()


if __name__ == "__main__":
    unittest.main()
