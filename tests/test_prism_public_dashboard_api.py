#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from decimal import Decimal
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism import direct_stratum, public_api
from lab.prism.prism_coordinator import make_audit_handler
from lab.prism.share_ledger import PendingShare, PsqlShareLedger, SingleWriterShareLedger


class FanoutPublicRowTests(unittest.TestCase):
    def test_cpfp_anchor_spendable_requires_zero_fee_anchor(self) -> None:
        base = {
            "fanout_txid": "aa" * 32,
            "block_hash": "bb" * 32,
            "block_height": 123,
            "settlement_status": "broadcastable",
            "manifest_set_sha256": "cc" * 32,
            "manifest_sha256": "dd" * 32,
            "parent_coinbase_txid": "ee" * 32,
            "parent_coinbase_vout": 0,
            "covenant_output_value_sats": 10_000,
            "fanout_output_sum_sats": 10_000,
            "fanout_tx_hex": "00",
        }

        anchored = public_api.fanout_public_row({**base, "anchor_vout": 1})
        fee_bearing = public_api.fanout_public_row(
            {
                **base,
                "anchor_vout": None,
                "covenant_output_value_sats": 10_100,
            }
        )

        self.assertTrue(anchored["cpfp_anchor_spendable"])
        self.assertFalse(fee_bearing["cpfp_anchor_spendable"])
        self.assertIsNone(fee_bearing["anchor_vout"])


class FakeRpc:
    def __init__(
        self,
        *,
        template_difficulty: object | None = None,
        template_bits: str | None = "207fffff",
        blockchain_info: dict[str, object] | None = None,
        network_info: dict[str, object] | None = None,
    ) -> None:
        self.template_difficulty = template_difficulty
        self.template_bits = template_bits
        self.blockchain_info = blockchain_info or {}
        self.network_info = network_info or {}

    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getblockchaininfo":
            return {
                "chain": "qbit-signet",
                "blocks": 123456,
                "bestblockhash": "0" * 63 + "1",
                "initialblockdownload": False,
                **self.blockchain_info,
            }
        if method == "getblocktemplate":
            template: dict[str, object] = {}
            if self.template_bits is not None:
                template["bits"] = self.template_bits
            if self.template_difficulty is not None:
                template["network_difficulty"] = self.template_difficulty
            return template
        if method == "getnetworkinfo":
            return {"connections": 8, **self.network_info}
        raise AssertionError(f"unexpected RPC method {method}")


class FakePublicLedger:
    backend_name = "fake-ledger"
    block_hash = "a" * 64
    fanout_txid = "b" * 64
    audit_bundle_sha256 = "c" * 64
    manifest_set_sha256 = "d" * 64
    manifest_sha256 = "e" * 64

    def __init__(self) -> None:
        self.now_ms = int(time.time() * 1000)
        self.current_network_difficulty: object | None = None
        self.reward_window_network_difficulty: object | None = None
        self.leaderboard_calls = 0
        self.pool_snapshot_calls = 0

    def dashboard_pool_snapshot(self, *, current_network_difficulty: object, generated_at: str) -> dict[str, object]:
        self.pool_snapshot_calls += 1
        self.current_network_difficulty = current_network_difficulty
        requested_window_weight = public_api.decimal_string(Decimal(str(current_network_difficulty)) * Decimal(8))
        return {
            "hashrate_ths": {"h1": "1.5", "h3": "2.5", "h24": "3.5"},
            "participants_3h": 2,
            "blocks_found_total": 3,
            "prism_blocks_total": 3,
            "total_mined_bits": 600,
            "latest_block": {
                "height": 123450,
                "hash": "a" * 64,
                "found_at": "2026-06-26T19:55:00Z",
                "age_seconds": 300,
                "solver_recipient_id": "miner-a",
                "solver_worker_name": None,
            },
            "reward_window": {
                "window_multiplier": 8,
                "requested_window_weight": requested_window_weight,
                "oldest_share_accepted_at": "2026-06-26T11:20:00Z",
                "newest_share_accepted_at": "2026-06-26T20:44:53Z",
                "included_share_count": 4,
            },
        }

    def dashboard_blocks(self, *, page: int, limit: int) -> dict[str, object]:
        return {
            "pagination": {"page": page, "limit": limit, "total_count": 1, "total_pages": 1},
            "rows": [
                {
                    "height": 123450,
                    "hash": "a" * 64,
                    "found_at": "2026-06-26T19:55:00Z",
                    "network_difficulty": "1000",
                    "bits": "207fffff",
                    "solver_recipient_id": "miner-a",
                    "solver_worker_name": None,
                    "solver_share_difficulty": "99",
                    "reward_window_weight": "792",
                    "coinbase_value_bits": 600,
                    "audit_bundle_sha256": "b" * 64,
                    "payout_manifest_sha256": "c" * 64,
                    "explorer_url": None,
                }
            ],
        }

    def dashboard_leaderboard(self, *, page: int, limit: int, search: str | None = None) -> dict[str, object]:
        self.leaderboard_calls += 1
        rows = [
            {
                "rank": 1,
                "recipient_id": "miner-a",
                "display_name": None,
                "hashrate_ths_3h": "2.5",
                "share_percent": "60",
                "hash_percent": "60",
                "blocks_found": 1,
                "last_share_at": "2026-06-26T20:44:53Z",
            }
        ]
        return {
            "started_at": "2026-06-26T17:45:00Z",
            "ended_at": "2026-06-26T20:45:00Z",
            "totals": {
                "pool_hashrate_ths": "2.5",
                "pool_accepted_share_difficulty": "100",
                "participant_count": 1,
            },
            "pagination": {"page": page, "limit": limit, "total_count": 1, "total_pages": 1},
            "rows": rows,
        }

    def dashboard_miner_reward_window(self, *, recipient_id: str, current_network_difficulty: object) -> dict[str, object]:
        self.reward_window_network_difficulty = current_network_difficulty
        if recipient_id != "miner-a":
            return {"accepted_difficulty": "0", "pool_accepted_difficulty": "0", "share_percent": None}
        return {"accepted_difficulty": "50", "pool_accepted_difficulty": "100", "share_percent": "50"}

    def audit_ctv_fanout_manifest_set(self, *, block_hash: str) -> dict[str, object] | None:
        if block_hash != self.block_hash:
            return None
        return {
            "schema": "qbit.prism.ctv-fanout-recovery.v1",
            "block_hash": self.block_hash,
            "block_height": None,
            "settlement_mode": "hybrid_coinbase_ctv_fanout",
            "audit_bundle_sha256": self.audit_bundle_sha256,
            "payout_manifest_sha256": "f" * 64,
            "manifest_set_sha256": self.manifest_set_sha256,
            "manifest_set_json": "{\"schema\":\"qbit.prism.ctv-fanout-manifest-set.v1\"}",
            "manifest_set": {"schema": "qbit.prism.ctv-fanout-manifest-set.v1"},
            "artifacts": [self._fanout_artifact()],
        }

    def pending_ctv_fanout_statuses(self, *, limit: int = 100) -> list[dict[str, object]]:
        return [self._fanout_status()][:limit]

    def dashboard_pending_fanout_rows(self, *, page: int, limit: int) -> dict[str, object]:
        rows = self._pending_fanout_statuses()
        offset = (page - 1) * limit
        total_count = len(rows)
        return {
            "pagination": {
                "page": page,
                "limit": limit,
                "total_count": total_count,
                "total_pages": (total_count + limit - 1) // limit,
            },
            "rows": rows[offset : offset + limit],
        }

    def ctv_fanout_status(self, *, fanout_txid: str) -> dict[str, object] | None:
        if fanout_txid != self.fanout_txid:
            return None
        return {**self._fanout_status(), "broadcast_attempts": [{"attempted_at": "2026-06-26 20:45:00+00", "error": None}]}

    def dashboard_public_artifact(self, *, sha256: str) -> dict[str, object] | None:
        if sha256 == self.audit_bundle_sha256:
            return {"schema": "qbit.prism.audit-bundle.v1"}
        if sha256 == self.manifest_set_sha256:
            return {"schema": "qbit.prism.ctv-fanout-manifest-set.v1"}
        if sha256 == self.manifest_sha256:
            return {"schema": "qbit.prism.ctv-fanout-manifest.v1"}
        return None

    def _fanout_artifact(self) -> dict[str, object]:
        row = self._fanout_status()
        row["settlement_status"] = "awaiting_maturity"
        row["broadcast_attempts"] = []
        return row

    def _fanout_status(self) -> dict[str, object]:
        return {
            "schema": "qbit.prism.ctv-fanout-status.v1",
            "fanout_txid": self.fanout_txid,
            "block_hash": self.block_hash,
            "block_height": 123450,
            "settlement_status": "broadcastable",
            "manifest_set_sha256": self.manifest_set_sha256,
            "manifest_sha256": self.manifest_sha256,
            "audit_bundle_sha256": self.audit_bundle_sha256,
            "parent_coinbase_txid": "1" * 64,
            "parent_coinbase_vout": 2,
            "anchor_vout": None,
            "covenant_output_value_sats": 4200,
            "fanout_output_sum_sats": 4100,
            "fanout_tx_hex": "02000000000100",
            "broadcast_attempts": [],
        }

    def _pending_fanout_statuses(self) -> list[dict[str, object]]:
        rows = []
        for index in range(1_001):
            row = self._fanout_status()
            row["fanout_txid"] = self.fanout_txid if index == 0 else f"{index:064x}"
            row["chunk_index"] = index
            row["chunk_count"] = 1_001
            rows.append(row)
        return rows

    def dashboard_hashrate_series(
        self,
        *,
        subject_type: str,
        subject_id: str | None,
        range_id: str,
        bucket: str,
    ) -> list[dict[str, object]]:
        return [
            {
                "timestamp": "2026-06-26T20:00:00Z",
                "hashrate_ths": "2.5",
                "accepted_share_count": 3,
                "accepted_share_difficulty": "100",
            }
        ]

    def all_shares(self) -> list[object]:
        return [
            SimpleNamespace(
                miner_id="miner-a",
                share_id="miner-a.rig-a:" + "1" * 64,
                share_difficulty=100,
                accepted_at_ms=self.now_ms,
            ),
            SimpleNamespace(
                miner_id="miner-a",
                share_id="miner-a.rig-b:" + "2" * 64,
                share_difficulty=80,
                accepted_at_ms=self.now_ms,
            ),
            SimpleNamespace(
                miner_id="miner-b",
                share_id="miner-b.rig-z:" + "3" * 64,
                share_difficulty=50,
                accepted_at_ms=self.now_ms,
            ),
        ]

    def current_owed_balances(self) -> list[dict[str, object]]:
        return [{"recipient_id": "miner-a", "balance_sats": 42}]

    def recipient_payout_history(self, *, recipient_id: str, limit: int = 50) -> list[dict[str, object]]:
        if recipient_id != "miner-a":
            return []
        return self._payout_history()[:limit]

    def dashboard_miner_lifetime_earnings_bits(self, *, recipient_id: str) -> int:
        if recipient_id != "miner-a":
            return 0
        total = 0
        for row in self._payout_history():
            fallback_gross = int(row.get("onchain_amount_sats", 0)) + int(row.get("carry_forward_balance_sats", 0))
            total += int(row.get("gross_amount_sats", fallback_gross))
        return total

    def dashboard_miner_pending_maturity_bits(self, *, recipient_id: str) -> int:
        if recipient_id != "miner-a":
            return 0
        return sum(
            max(0, int(row.get("onchain_amount_sats", 0)) - int(row.get("settlement_fee_sats", 0)))
            for row in self._payout_history()
            if row["action"] == "onchain" and row["maturity_state"] == "immature"
        )

    def dashboard_miner_payout_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        history = self.recipient_payout_history(recipient_id=recipient_id, limit=1_000)
        rows = [public_api.miner_payout_row(row) for row in history]
        offset = (page - 1) * limit
        return {"pagination": public_api.pagination(page, limit, len(rows)), "rows": rows[offset : offset + limit]}

    def dashboard_miner_earning_rows(self, *, recipient_id: str, page: int, limit: int) -> dict[str, object]:
        history = self.recipient_payout_history(recipient_id=recipient_id, limit=1_000)
        rows = [public_api.miner_earning_row(row) for row in history]
        offset = (page - 1) * limit
        return {"pagination": public_api.pagination(page, limit, len(rows)), "rows": rows[offset : offset + limit]}

    def _payout_history(self) -> list[dict[str, object]]:
        return [
            {
                "block_hash": self.block_hash,
                "block_height": 123450,
                "coinbase_txid": "1" * 64,
                "recipient_id": "miner-a",
                "onchain_amount_sats": 40,
                "settlement_fee_sats": 3,
                "carry_forward_balance_sats": 2,
                "block_gross_amount_sats": 84,
                "action": "onchain",
                "maturity_state": "immature",
                "created_at": "2026-06-26 20:45:00+00",
                "found_at": "2026-06-26 20:40:00+00",
            },
            {
                "block_hash": "2" * 64,
                "block_height": 123449,
                "coinbase_txid": "3" * 64,
                "recipient_id": "miner-a",
                "gross_amount_sats": 7,
                "onchain_amount_sats": 0,
                "settlement_fee_sats": 1,
                "carry_forward_balance_sats": 7,
                "block_gross_amount_sats": 28,
                "action": "accrued",
                "maturity_state": "immature",
                "created_at": "2026-06-26 19:45:00+00",
                "found_at": "2026-06-26 19:40:00+00",
            },
        ]


class BrokenPublicLedger(FakePublicLedger):
    def dashboard_leaderboard(self, *, page: int, limit: int, search: str | None = None) -> dict[str, object]:
        self.leaderboard_calls += 1
        raise RuntimeError("database password=secret failed")


class MissingPendingReadModelLedger(FakePublicLedger):
    dashboard_pending_fanout_rows = None


class MissingAuditBundleBodyLedger(FakePublicLedger):
    def dashboard_public_artifact(self, *, sha256: str) -> dict[str, object] | None:
        if sha256 == self.audit_bundle_sha256:
            return None
        return super().dashboard_public_artifact(sha256=sha256)


class DirectCoinbasePublicLedger(FakePublicLedger):
    block_hash = "00000000000026e9383a5aeae5fe5c3297f24884f29c4cf1585f71829491f0d9"
    block_height = 23342
    audit_bundle_sha256 = "92adad1828dfe2b68deceddf1bfcd153e9a4fd3e9ec8516fd2c13c295129b49f"
    payout_manifest_sha256 = "8c37b6b595e76f69c3f7c68a04c5acf34ca4ba10e508be6d56f8398d4a70ba9a"

    def audit_ctv_fanout_manifest_set(self, *, block_hash: str) -> dict[str, object] | None:
        return None

    def audit_bundle(self, *, block_hash: str) -> dict[str, object] | None:
        if block_hash != self.block_hash:
            return None
        return {
            "block_hash": self.block_hash,
            "block_height": self.block_height,
            "payout_manifest_sha256": self.payout_manifest_sha256,
            "audit_bundle_sha256": self.audit_bundle_sha256,
            "audit_bundle": {
                "schema": "qbit.prism.audit-bundle.v1",
                "found_block": {"block_height": self.block_height},
                "settlement_mode_decision": {"mode": "direct_coinbase"},
            },
        }


class ArtifactExistsDirectCoinbasePublicLedger(DirectCoinbasePublicLedger):
    def __init__(self) -> None:
        self.exists_calls = 0
        self.artifact_calls = 0

    def dashboard_public_artifact_exists(self, *, sha256: str) -> bool:
        self.exists_calls += 1
        return sha256 == self.audit_bundle_sha256

    def dashboard_public_artifact(self, *, sha256: str) -> dict[str, object] | None:
        self.artifact_calls += 1
        raise AssertionError("settlement link availability should use metadata-only exists hook")


class ExternalizedAuditBodyResolver(PsqlShareLedger):
    def __init__(self, audit_body_dir: str | Path) -> None:
        self._audit_body_dir = Path(audit_body_dir)
        self.body_read_count = 0

    def _read_external_body(self, body_uri: object, *, expected_sha256: object) -> dict[str, object] | None:
        self.body_read_count += 1
        return super()._read_external_body(body_uri, expected_sha256=expected_sha256)


class ExternalizedDirectCoinbasePublicLedger(DirectCoinbasePublicLedger):
    def __init__(self, audit_body_dir: str | Path) -> None:
        bundle = {
            "schema": "qbit.prism.audit-bundle.v1",
            "found_block": {"block_height": self.block_height},
            "settlement_mode_decision": {"mode": "direct_coinbase"},
        }
        body_bytes = json.dumps(bundle, separators=(",", ":")).encode()
        self.audit_bundle_sha256 = hashlib.sha256(body_bytes).hexdigest()
        self.body_uri = (
            Path(audit_body_dir)
            / f"prism-audit-bundle-body-{self.block_hash}-{self.audit_bundle_sha256}.json"
        )
        self.body_uri.write_bytes(body_bytes)
        self.resolver = ExternalizedAuditBodyResolver(audit_body_dir)

    def audit_bundle(self, *, block_hash: str) -> dict[str, object] | None:
        if block_hash != self.block_hash:
            return None
        return self.resolver._resolve_audit_bundle_row(
            {
                "block_hash": self.block_hash,
                "block_height": self.block_height,
                "payout_manifest_sha256": self.payout_manifest_sha256,
                "audit_bundle_sha256": self.audit_bundle_sha256,
                "audit_bundle": None,
                "body_uri": str(self.body_uri),
            }
        )


class ValueErrorPublicLedger(FakePublicLedger):
    def dashboard_leaderboard(self, *, page: int, limit: int, search: str | None = None) -> dict[str, object]:
        self.leaderboard_calls += 1
        raise ValueError("json parse password=secret failed")


class SlowPublicLedger(FakePublicLedger):
    def __init__(self) -> None:
        super().__init__()
        self._leaderboard_lock = threading.Lock()

    def dashboard_leaderboard(self, *, page: int, limit: int, search: str | None = None) -> dict[str, object]:
        with self._leaderboard_lock:
            self.leaderboard_calls += 1
        time.sleep(0.2)
        rows = [
            {
                "rank": 1,
                "recipient_id": "miner-a",
                "display_name": None,
                "hashrate_ths_3h": "2.5",
                "share_percent": "60",
                "hash_percent": "60",
                "blocks_found": 1,
                "last_share_at": "2026-06-26T20:44:53Z",
            }
        ]
        return {
            "started_at": "2026-06-26T17:45:00Z",
            "ended_at": "2026-06-26T20:45:00Z",
            "totals": {
                "pool_hashrate_ths": "2.5",
                "pool_accepted_share_difficulty": "100",
                "participant_count": 1,
            },
            "pagination": {"page": page, "limit": limit, "total_count": 1, "total_pages": 1},
            "rows": rows,
        }


class FakeCoordinator:
    def __init__(self, ledger: FakePublicLedger | None = None, rpc: FakeRpc | None = None) -> None:
        self.ledger = ledger or FakePublicLedger()
        self.rpc = rpc or FakeRpc()
        self.bind = "127.0.0.1"
        self.port = 3340


class MemoryCoordinator:
    def __init__(self) -> None:
        self.ledger = SingleWriterShareLedger()
        self.rpc = FakeRpc()


class PrismPublicDashboardApiTests(unittest.TestCase):
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

    def test_pool_summary_public_shape(self) -> None:
        payload = self.get_json("/public/v1/pool-summary")

        self.assertEqual(payload["schema"], "prism.dashboard.pool-summary.v1")
        self.assertEqual(payload["network"]["name"], "qbit-signet")
        self.assertEqual(payload["network"]["bits"], "207fffff")
        self.assertEqual(payload["pool"]["total_mined_bits"], 600)
        self.assertEqual(payload["pool"]["latest_block"]["solver_recipient_id"], "miner-a")

    def test_pool_summary_derives_scaled_network_difficulty_from_bits(self) -> None:
        # network_difficulty must be PRISM's scaled difficulty (derived from the
        # compact bits), NOT the raw getblocktemplate/getblockchaininfo difficulty
        # float, so the reward-window weight and ETA line up with scaled per-share
        # difficulties. The raw "1.2" below must be ignored. This covers dashboard difficulty and reward-estimate regressions.
        ledger = FakePublicLedger()
        payload = public_api.pool_summary(
            FakeCoordinator(ledger=ledger, rpc=FakeRpc(template_difficulty="1.2", template_bits="207fffff"))
        )

        scaled = str(public_api.scaled_network_difficulty("207fffff"))
        self.assertEqual(scaled, "1000000")
        self.assertEqual(payload["network"]["network_difficulty"], scaled)
        self.assertEqual(ledger.current_network_difficulty, scaled)
        self.assertEqual(payload["pool"]["reward_window"]["requested_window_weight"], "8000000")

    def test_pool_summary_expected_time_to_block_is_nonzero_for_real_difficulty(self) -> None:
        # Regression: expected_time_to_block_seconds returned 0 because
        # network_summary fed the raw (unscaled, ~1e6x too small) RPC difficulty into
        # the scaled-units formula. Deriving the scaled difficulty from bits produces
        # the real ETA (~tens of seconds for the fake 2.5 TH/s pool at this difficulty).
        ledger = FakePublicLedger()
        payload = public_api.pool_summary(FakeCoordinator(ledger=ledger, rpc=FakeRpc(template_bits="1b0404cb")))

        scaled = public_api.scaled_network_difficulty("1b0404cb")
        expected_eta = public_api.expected_time_to_block_seconds(
            hashrate_ths="2.5", network_difficulty=str(scaled)
        )
        self.assertEqual(payload["network"]["network_difficulty"], str(scaled))
        self.assertGreater(payload["pool"]["expected_time_to_block_seconds"], 0)
        self.assertEqual(payload["pool"]["expected_time_to_block_seconds"], expected_eta)

    def test_network_summary_falls_back_to_difficulty_one_without_bits(self) -> None:
        payload = public_api.network_summary(FakeCoordinator(rpc=FakeRpc(template_bits=None)))

        self.assertEqual(payload["network_difficulty"], "1")

    def test_network_summary_tolerates_null_or_invalid_rpc_counters(self) -> None:
        payload = public_api.network_summary(
            FakeCoordinator(
                rpc=FakeRpc(
                    blockchain_info={"blocks": None, "headers": "not-a-height"},
                    network_info={"connections": None},
                )
            )
        )

        self.assertEqual(payload["height"], 0)
        self.assertEqual(payload["peers"], 0)

    def test_blocks_leaderboard_and_hashrate_series(self) -> None:
        blocks = self.get_json("/public/v1/blocks?page=1&limit=15")
        leaderboard = self.get_json("/public/v1/leaderboard?window=3h&page=1&limit=15")
        series = self.get_json("/public/v1/hashrate-series?subject=miner:miner-a&range=1w&bucket=auto")

        self.assertEqual(blocks["schema"], "prism.dashboard.blocks.v1")
        self.assertEqual(blocks["rows"][0]["bits"], "207fffff")
        self.assertIsNotNone(blocks["rows"][0]["network_difficulty"])
        self.assertEqual(leaderboard["schema"], "prism.dashboard.leaderboard.v1")
        self.assertEqual(leaderboard["rows"][0]["recipient_id"], "miner-a")
        self.assertEqual(series["schema"], "prism.dashboard.hashrate-series.v1")
        self.assertEqual(series["subject"], {"type": "miner", "id": "miner-a"})
        self.assertEqual(series["bucket"], "1h")

    def test_public_api_successes_emit_cache_headers_and_hit_origin_cache(self) -> None:
        ledger = FakePublicLedger()
        handler = make_audit_handler(FakeCoordinator(ledger=ledger))  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.dict(
                os.environ,
                {
                    "PRISM_PUBLIC_CACHE_TTL_SECONDS": "30",
                    "PRISM_PUBLIC_CACHE_DEBUG_HEADERS": "1",
                },
                clear=True,
            ):
                first_url = f"http://127.0.0.1:{server.server_port}/public/v1/leaderboard?window=3h&page=1&limit=15"
                second_url = f"http://127.0.0.1:{server.server_port}/public/v1/leaderboard?limit=15&page=1&window=3h"
                with urllib.request.urlopen(first_url, timeout=5) as first:
                    first_payload = json.loads(first.read())
                    first_cache = first.headers.get("X-Prism-Public-Cache")
                    first_age = first.headers.get("Age")
                    first_cdn_cache = first.headers.get("CDN-Cache-Control")
                    first_browser_cache = first.headers.get("Cache-Control")
                with urllib.request.urlopen(second_url, timeout=5) as second:
                    second_payload = json.loads(second.read())
                    second_cache = second.headers.get("X-Prism-Public-Cache")
                    second_age = second.headers.get("Age")
                    second_cdn_cache = second.headers.get("CDN-Cache-Control")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(first_payload, second_payload)
        self.assertEqual(first_cache, "miss")
        self.assertEqual(second_cache, "hit")
        self.assertEqual(first_age, "0")
        self.assertEqual(second_age, "0")
        self.assertEqual(first_browser_cache, "public, max-age=0, must-revalidate")
        self.assertEqual(first_cdn_cache, "public, max-age=30, stale-while-revalidate=30")
        self.assertEqual(second_cdn_cache, first_cdn_cache)
        self.assertEqual(ledger.leaderboard_calls, 1)

    def test_public_api_errors_are_no_store_and_not_cached(self) -> None:
        ledger = BrokenPublicLedger()
        handler = make_audit_handler(FakeCoordinator(ledger))  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}/public/v1/leaderboard"
            for _ in range(2):
                with self.assertRaises(urllib.error.HTTPError) as raised:
                    urllib.request.urlopen(url, timeout=5)
                self.assertEqual(raised.exception.code, 500)
                self.assertEqual(raised.exception.headers.get("Cache-Control"), "no-store")
                self.assertIsNone(raised.exception.headers.get("CDN-Cache-Control"))
                raised.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(ledger.leaderboard_calls, 2)

    def test_public_api_coalesces_concurrent_origin_cache_misses(self) -> None:
        ledger = SlowPublicLedger()
        handler = make_audit_handler(FakeCoordinator(ledger))  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        errors: list[BaseException] = []
        payloads: list[dict[str, object]] = []
        start_barrier = threading.Barrier(6)

        def fetch() -> None:
            try:
                start_barrier.wait(timeout=5)
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/public/v1/leaderboard",
                    timeout=5,
                ) as response:
                    payloads.append(json.loads(response.read()))
            except BaseException as exc:  # pragma: no cover - re-raised below for clear failure
                errors.append(exc)

        workers = [threading.Thread(target=fetch) for _ in range(5)]
        try:
            with patch.dict(os.environ, {"PRISM_PUBLIC_CACHE_TTL_SECONDS": "30"}, clear=True):
                for worker in workers:
                    worker.start()
                start_barrier.wait(timeout=5)
                for worker in workers:
                    worker.join(timeout=10)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(errors, [])
        self.assertEqual(len(payloads), 5)
        self.assertEqual({payload["schema"] for payload in payloads}, {"prism.dashboard.leaderboard.v1"})
        self.assertEqual(ledger.leaderboard_calls, 1)

    def test_public_api_coalesces_concurrent_misses_when_response_not_cacheable(self) -> None:
        # An oversize (non-cacheable) response must still coalesce concurrent
        # waiters onto the owner's single origin call instead of re-running it.
        ledger = SlowPublicLedger()
        handler = make_audit_handler(FakeCoordinator(ledger))  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        errors: list[BaseException] = []
        payloads: list[dict[str, object]] = []
        start_barrier = threading.Barrier(6)

        def fetch() -> None:
            try:
                start_barrier.wait(timeout=5)
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/public/v1/leaderboard",
                    timeout=5,
                ) as response:
                    payloads.append(json.loads(response.read()))
            except BaseException as exc:  # pragma: no cover - re-raised below for clear failure
                errors.append(exc)

        workers = [threading.Thread(target=fetch) for _ in range(5)]
        try:
            with patch.dict(
                os.environ,
                {
                    "PRISM_PUBLIC_CACHE_TTL_SECONDS": "30",
                    "PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES": "1",
                },
                clear=True,
            ):
                for worker in workers:
                    worker.start()
                start_barrier.wait(timeout=5)
                for worker in workers:
                    worker.join(timeout=10)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(errors, [])
        self.assertEqual(len(payloads), 5)
        self.assertEqual({payload["schema"] for payload in payloads}, {"prism.dashboard.leaderboard.v1"})
        self.assertEqual(ledger.leaderboard_calls, 1)

    def test_public_cache_key_canonicalizes_hash_casing(self) -> None:
        hex_hash = "ab" * 32
        upper_hash = hex_hash.upper()

        artifact_lower = public_api.public_cache_key(f"/public/v1/artifacts/{hex_hash}", {})
        artifact_upper = public_api.public_cache_key(f"/public/v1/artifacts/{upper_hash}", {})
        self.assertEqual(artifact_lower, artifact_upper)

        block_lower = public_api.public_cache_key(
            f"/public/v1/blocks/{hex_hash}/settlement-artifacts", {}
        )
        block_upper = public_api.public_cache_key(
            f"/public/v1/blocks/{upper_hash}/settlement-artifacts", {}
        )
        self.assertEqual(block_lower, block_upper)

        fanout_lower = public_api.public_cache_key(f"/public/v1/fanouts/{hex_hash}", {})
        fanout_upper = public_api.public_cache_key(f"/public/v1/fanouts/{upper_hash}", {})
        self.assertEqual(fanout_lower, fanout_upper)

        # Case-sensitive identity routes and the literal pending route are untouched.
        self.assertNotEqual(
            public_api.public_cache_key("/public/v1/miners/AbC", {}),
            public_api.public_cache_key("/public/v1/miners/abc", {}),
        )
        self.assertEqual(
            public_api.public_cache_key("/public/v1/fanouts/pending", {})[0],
            "/public/v1/fanouts/pending",
        )
        # A non-hash fanout segment (a case-variant of the literal pending route,
        # or any id dispatch would reject) must NOT fold onto the pending list's
        # cache key, or a cached pending response could be served for it.
        self.assertNotEqual(
            public_api.public_cache_key("/public/v1/fanouts/PENDING", {}),
            public_api.public_cache_key("/public/v1/fanouts/pending", {}),
        )
        self.assertEqual(
            public_api.public_cache_key("/public/v1/fanouts/not-a-hash", {})[0],
            "/public/v1/fanouts/not-a-hash",
        )

    def test_public_cache_reaps_expired_before_evicting_fresh(self) -> None:
        with patch.dict(os.environ, {"PRISM_PUBLIC_CACHE_MAX_ENTRIES": "2"}, clear=True):
            cache = public_api.PublicResponseCache()
            cache.get_or_compute(key=("/fresh", ()), ttl_seconds=60, compute=lambda: (200, {"v": "fresh"}))
            cache.get_or_compute(key=("/expiring", ()), ttl_seconds=1, compute=lambda: (200, {"v": "expiring"}))
            time.sleep(1.2)  # /expiring is now expired; /fresh is still valid
            # Storing a third fresh entry over the 2-entry bound must reap the
            # expired /expiring, not evict the still-fresh (but oldest) /fresh.
            cache.get_or_compute(key=("/new", ()), ttl_seconds=60, compute=lambda: (200, {"v": "new"}))

            recompute_calls: list[int] = []

            def recompute() -> tuple[int, object]:
                recompute_calls.append(1)
                return 200, {"v": "recomputed"}

            status, payload, cache_state, _age = cache.get_or_compute(
                key=("/fresh", ()), ttl_seconds=60, compute=recompute
            )
        self.assertEqual(cache_state, "HIT")
        self.assertEqual(payload, {"v": "fresh"})
        self.assertEqual(recompute_calls, [])

    def test_public_api_cache_headers_use_route_specific_ttls(self) -> None:
        ledger = FakePublicLedger()
        handler = make_audit_handler(FakeCoordinator(ledger=ledger))  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.dict(
                os.environ,
                {
                    "PRISM_PUBLIC_CONFIG_CACHE_TTL_SECONDS": "600",
                    "PRISM_PUBLIC_ARTIFACT_CACHE_TTL_SECONDS": "7200",
                },
                clear=True,
            ):
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/public/v1/mining-configuration",
                    timeout=5,
                ) as config_response:
                    json.loads(config_response.read())
                    config_cache = config_response.headers.get("CDN-Cache-Control")
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/public/v1/artifacts/{ledger.audit_bundle_sha256}",
                    timeout=5,
                ) as artifact_response:
                    json.loads(artifact_response.read())
                    artifact_cache = artifact_response.headers.get("CDN-Cache-Control")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(config_cache, "public, max-age=600, stale-while-revalidate=3600")
        self.assertEqual(artifact_cache, "public, max-age=7200, stale-while-revalidate=86400, immutable")

    def test_public_api_errors_use_public_error_schema(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json("/public/v1/leaderboard?window=24h")

        self.assertEqual(raised.exception.code, 400)
        payload = json.loads(raised.exception.read())
        raised.exception.close()
        self.assertEqual(set(payload.keys()), {"schema", "error"})
        self.assertEqual(payload["schema"], "prism.dashboard.error.v1")
        self.assertEqual(payload["error"], {"code": "bad_request", "message": "window must be 3h", "request_id": None})

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json(f"/public/v1/leaderboard?search={'x' * 129}")

        self.assertEqual(raised.exception.code, 400)
        payload = json.loads(raised.exception.read())
        raised.exception.close()
        self.assertEqual(payload["error"], {"code": "bad_request", "message": "search must be 128 characters or fewer", "request_id": None})

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json("/public/v1/hashrate-series?range=window")

        self.assertEqual(raised.exception.code, 400)
        payload = json.loads(raised.exception.read())
        raised.exception.close()
        self.assertEqual(payload["error"], {"code": "bad_request", "message": "range must be one of 1w, 1m, 6m, all", "request_id": None})

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json("/public/v1")

        self.assertEqual(raised.exception.code, 404)
        payload = json.loads(raised.exception.read())
        raised.exception.close()
        self.assertEqual(payload["schema"], "prism.dashboard.error.v1")
        self.assertEqual(payload["error"]["code"], "not_found")

    def test_hashrate_from_difficulty_uses_qbit_scaled_pow_limit_units(self) -> None:
        live_bucket_difficulty = Decimal("8231979598912131605603")

        hashrate_ths = Decimal(public_api.hashrate_ths_from_difficulty(live_bucket_difficulty, 60 * 60))

        self.assertGreater(hashrate_ths, Decimal("4.57"))
        self.assertLess(hashrate_ths, Decimal("4.58"))

    def test_expected_time_to_block_uses_qbit_scaled_network_difficulty(self) -> None:
        self.assertEqual(
            public_api.expected_time_to_block_seconds(
                hashrate_ths="0.000000000001",
                network_difficulty="1000000",
            ),
            2,
        )

    def test_expected_time_to_block_matches_hashrate_window_when_difficulty_matches(self) -> None:
        network_difficulty = Decimal("4000000")
        window_seconds = 3 * 60 * 60
        hashrate_ths = public_api.hashrate_ths_from_difficulty(network_difficulty, window_seconds)

        self.assertEqual(
            public_api.expected_time_to_block_seconds(
                hashrate_ths=hashrate_ths,
                network_difficulty=str(network_difficulty),
            ),
            window_seconds,
        )

    def test_scaled_network_difficulty_uses_qbit_pow_limit_units(self) -> None:
        self.assertEqual(public_api.scaled_network_difficulty("207fffff"), 1_000_000)

    def test_scaled_network_difficulty_rejects_non_positive_targets(self) -> None:
        with self.assertRaises(ValueError):
            public_api.scaled_network_difficulty("00000000")

    def test_unexpected_public_api_errors_do_not_leak_internal_details(self) -> None:
        handler = make_audit_handler(FakeCoordinator(BrokenPublicLedger()))  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with self.assertRaises(urllib.error.HTTPError) as raised:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/public/v1/leaderboard",
                    timeout=5,
                )

            self.assertEqual(raised.exception.code, 500)
            raw_payload = raised.exception.read().decode()
            raised.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertNotIn("password=secret", raw_payload)
        payload = json.loads(raw_payload)
        self.assertEqual(payload["schema"], "prism.dashboard.error.v1")
        self.assertEqual(payload["error"], {"code": "internal_error", "message": "internal server error", "request_id": None})

    def test_public_value_errors_do_not_leak_internal_details(self) -> None:
        handler = make_audit_handler(FakeCoordinator(ValueErrorPublicLedger()))  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with self.assertRaises(urllib.error.HTTPError) as raised:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/public/v1/leaderboard",
                    timeout=5,
                )

            self.assertEqual(raised.exception.code, 500)
            raw_payload = raised.exception.read().decode()
            raised.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertNotIn("password=secret", raw_payload)
        payload = json.loads(raw_payload)
        self.assertEqual(payload["schema"], "prism.dashboard.error.v1")
        self.assertEqual(payload["error"], {"code": "internal_error", "message": "internal server error", "request_id": None})

    def test_settlement_fanout_and_artifact_endpoints(self) -> None:
        ledger = FakePublicLedger()
        settlement = self.get_json(f"/public/v1/blocks/{ledger.block_hash}/settlement-artifacts")
        pending = self.get_json("/public/v1/fanouts/pending?page=1&limit=15")
        fanout = self.get_json(f"/public/v1/fanouts/{ledger.fanout_txid}")
        artifact = self.get_json(f"/public/v1/artifacts/{ledger.manifest_set_sha256}")
        audit_bundle = self.get_json(f"/public/v1/artifacts/{ledger.audit_bundle_sha256}")

        self.assertEqual(settlement["schema"], "prism.dashboard.settlement-artifacts.v1")
        self.assertEqual(settlement["block_height"], 123450)
        self.assertEqual(settlement["artifact_links"][0]["url"], f"/public/v1/artifacts/{ledger.audit_bundle_sha256}")
        self.assertEqual(settlement["fanouts"][0]["status"], "broadcastable")
        self.assertEqual(settlement["fanouts"][0]["broadcastable_at_height"], 124450)
        self.assertEqual(settlement["fanouts"][0]["covenant_output_value_bits"], 4200)
        self.assertEqual(settlement["fanouts"][0]["fanout_fee_bits"], 100)
        self.assertEqual(settlement["fanouts"][0]["fanout_tx_sha256"], ledger.fanout_txid)
        self.assertEqual(pending["schema"], "prism.dashboard.pending-fanouts.v1")
        self.assertEqual(pending["rows"][0]["fanout_txid"], ledger.fanout_txid)
        self.assertEqual(pending["rows"][0]["broadcastable_at_height"], 124450)
        self.assertEqual(pending["rows"][0]["fanout_fee_bits"], 100)
        self.assertEqual(fanout["schema"], "prism.dashboard.fanout.v1")
        self.assertEqual(fanout["fanout"]["broadcastable_at_height"], 124450)
        self.assertEqual(fanout["fanout"]["fanout_fee_bits"], 100)
        self.assertEqual(fanout["fanout"]["last_broadcast_attempt_at"], "2026-06-26T20:45:00Z")
        self.assertEqual(artifact["schema"], "qbit.prism.ctv-fanout-manifest-set.v1")
        self.assertEqual(audit_bundle["schema"], "qbit.prism.audit-bundle.v1")

    def test_settlement_artifacts_returns_direct_coinbase_bundle_without_fanouts(self) -> None:
        ledger = DirectCoinbasePublicLedger()
        handler = make_audit_handler(FakeCoordinator(ledger=ledger))  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/public/v1/blocks/{ledger.block_hash}/settlement-artifacts",
                timeout=5,
            ) as response:
                self.assertEqual(response.status, 200)
                settlement = json.loads(response.read())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(settlement["schema"], "prism.dashboard.settlement-artifacts.v1")
        self.assertEqual(settlement["block_hash"], ledger.block_hash)
        self.assertEqual(settlement["block_height"], ledger.block_height)
        self.assertEqual(settlement["settlement_mode"], "direct_coinbase")
        self.assertEqual(settlement["audit_bundle_sha256"], ledger.audit_bundle_sha256)
        self.assertEqual(settlement["payout_manifest_sha256"], ledger.payout_manifest_sha256)
        self.assertEqual(settlement["artifact_links"][0]["kind"], "audit_bundle")
        self.assertEqual(settlement["artifact_links"][0]["url"], f"/public/v1/artifacts/{ledger.audit_bundle_sha256}")
        self.assertEqual(settlement["fanouts"], [])

    def test_settlement_artifacts_uses_metadata_only_artifact_availability_when_present(self) -> None:
        ledger = ArtifactExistsDirectCoinbasePublicLedger()

        settlement = public_api.settlement_artifacts(FakeCoordinator(ledger=ledger), block_hash=ledger.block_hash)

        self.assertEqual(ledger.exists_calls, 1)
        self.assertEqual(ledger.artifact_calls, 0)
        self.assertEqual(settlement["artifact_links"][0]["kind"], "audit_bundle")

    def test_settlement_artifacts_supports_externalized_direct_coinbase_audit_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExternalizedDirectCoinbasePublicLedger(tmp)
            handler = make_audit_handler(FakeCoordinator(ledger=ledger))  # type: ignore[arg-type]
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{server.server_port}/public/v1/blocks/{ledger.block_hash}/settlement-artifacts",
                    timeout=5,
                ) as response:
                    self.assertEqual(response.status, 200)
                    settlement = json.loads(response.read())
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(ledger.resolver.body_read_count, 1)
            self.assertEqual(settlement["settlement_mode"], "direct_coinbase")
            self.assertEqual(settlement["block_height"], ledger.block_height)
            self.assertEqual(settlement["audit_bundle_sha256"], ledger.audit_bundle_sha256)
            self.assertEqual(settlement["payout_manifest_sha256"], ledger.payout_manifest_sha256)
            self.assertEqual(settlement["fanouts"], [])

    def test_settlement_artifacts_returns_404_when_externalized_audit_body_is_unreadable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExternalizedDirectCoinbasePublicLedger(tmp)
            ledger.body_uri.unlink()
            handler = make_audit_handler(FakeCoordinator(ledger=ledger))  # type: ignore[arg-type]
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                with self.assertRaises(urllib.error.HTTPError) as raised:
                    urllib.request.urlopen(
                        f"http://127.0.0.1:{server.server_port}/public/v1/blocks/{ledger.block_hash}/settlement-artifacts",
                        timeout=5,
                    )
                self.assertEqual(raised.exception.code, 404)
                raised.exception.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(ledger.resolver.body_read_count, 1)

    def test_settlement_artifacts_omit_unavailable_audit_bundle_link(self) -> None:
        ledger = MissingAuditBundleBodyLedger()

        settlement = public_api.settlement_artifacts(FakeCoordinator(ledger=ledger), block_hash=ledger.block_hash)

        artifact_kinds = {link["kind"] for link in settlement["artifact_links"]}  # type: ignore[index]
        self.assertEqual(settlement["audit_bundle_sha256"], ledger.audit_bundle_sha256)
        self.assertNotIn("audit_bundle", artifact_kinds)
        self.assertIn("ctv_manifest_set", artifact_kinds)

    def test_pending_fanout_pagination_counts_beyond_first_thousand_rows(self) -> None:
        pending = self.get_json("/public/v1/fanouts/pending?page=11&limit=100")

        self.assertEqual(pending["pagination"], {"page": 11, "limit": 100, "total_count": 1001, "total_pages": 11})
        self.assertEqual(len(pending["rows"]), 1)
        self.assertEqual(pending["rows"][0]["fanout_txid"], f"{1000:064x}")

    def test_pending_fanouts_requires_uncapped_read_model(self) -> None:
        with self.assertRaises(public_api.PublicApiError):
            public_api.pending_fanouts(
                FakeCoordinator(MissingPendingReadModelLedger()),  # type: ignore[arg-type]
                page=1,
                limit=15,
            )

    def test_fanout_transaction_hash_uses_non_witness_txid_fallback(self) -> None:
        witness_tx_hex = (
            "02000000"
            "0001"
            "01"
            + "00" * 32
            + "ffffffff"
            "00"
            "ffffffff"
            "00"
            "00"
            "00000000"
        )

        self.assertEqual(public_api.txid_from_tx_hex(witness_tx_hex), direct_stratum.transaction_txid_display(witness_tx_hex))

    def test_settlement_bad_hash_and_missing_rows(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json("/public/v1/fanouts/not-a-hash")

        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json(f"/public/v1/fanouts/{'9' * 64}")

        self.assertEqual(raised.exception.code, 404)
        raised.exception.close()

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json(f"/public/v1/blocks/{'9' * 64}/settlement-artifacts")

        self.assertEqual(raised.exception.code, 404)
        raised.exception.close()

    def test_miner_route_validation_uses_public_errors(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json("/public/v1/miners/miner-a/unknown")

        self.assertEqual(raised.exception.code, 404)
        raised.exception.close()

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json(f"/public/v1/miners/{'x' * 257}")

        self.assertEqual(raised.exception.code, 400)
        payload = json.loads(raised.exception.read())
        raised.exception.close()
        self.assertEqual(payload["error"]["code"], "bad_request")
        self.assertEqual(payload["error"]["message"], "recipient_id must be 256 characters or fewer")

    def test_mining_configuration_and_miner_detail_endpoints(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            config = self.get_json("/public/v1/mining-configuration")
        miner = self.get_json("/public/v1/miners/miner-a")
        earnings = self.get_json("/public/v1/miners/miner-a/earnings?page=1&limit=15")
        payouts = self.get_json("/public/v1/miners/miner-a/payouts?page=1&limit=15")
        workers = self.get_json("/public/v1/miners/miner-a/workers?page=1&limit=15")

        endpoint = config["configurations"][0]["stratum_endpoints"][0]
        self.assertEqual(config["schema"], "prism.dashboard.mining-configuration.v1")
        self.assertEqual(config["configurations"][0]["pool_fee_bps"], 0)
        self.assertEqual(len(config["configurations"][0]["stratum_endpoints"]), 1)
        self.assertEqual(endpoint["url"], "stratum+tcp://127.0.0.1:3340")
        self.assertEqual(endpoint["default_port"], 3340)
        self.assertEqual(miner["schema"], "prism.dashboard.miner.v1")
        self.assertEqual(miner["owed_balance_bits"], 42)
        self.assertEqual(miner["lifetime_earnings_bits"], 49)
        self.assertEqual(miner["pending_maturity_bits"], 37)
        self.assertEqual(miner["minimum_payout_bits"], 0)
        self.assertEqual(miner["estimated_time_to_minimum_payout_seconds"], 0)
        self.assertEqual(miner["reward_window_percent"], "50")
        self.assertEqual(miner["estimated_next_block"]["share_percent"], "50")
        # estimated_reward_bits is now computed (share_percent x distributable coinbase),
        # no longer hardcoded null.
        self.assertIsInstance(miner["estimated_next_block"]["estimated_reward_bits"], int)
        self.assertEqual(miner["workers_currently_hashing"], 2)
        self.assertEqual(miner["workers"][0]["worker_name"], "rig-a")
        self.assertEqual(earnings["schema"], "prism.dashboard.miner-earnings.v1")
        self.assertEqual(earnings["rows"][0]["found_at"], "2026-06-26T20:40:00Z")
        self.assertEqual(earnings["rows"][0]["reward_share_percent"], "50")
        self.assertEqual(earnings["rows"][0]["gross_earning_bits"], 42)
        self.assertEqual(payouts["schema"], "prism.dashboard.miner-payouts.v1")
        self.assertEqual(payouts["rows"][0]["created_at"], "2026-06-26T20:45:00Z")
        self.assertEqual(payouts["rows"][0]["onchain_amount_bits"], 40)
        self.assertEqual(workers["schema"], "prism.dashboard.miner-workers.v1")
        self.assertEqual(workers["pagination"]["total_count"], 2)
        self.assertEqual(workers["rows"][0]["worker_name"], "rig-a")
        self.assertEqual(workers["rows"][0]["status"], "active")

    def test_miner_payout_row_labels_ctv_fanout_outputs(self) -> None:
        fanout_txid = "c5" * 32
        base_row = {
            "block_hash": "a1" * 32,
            "block_height": 19,
            "coinbase_txid": "b2" * 32,
            "recipient_id": "miner-a",
            "onchain_amount_sats": 7_767_471,
            "carry_forward_balance_sats": 0,
            "action": "onchain",
            "maturity_state": "immature",
            "created_at": "2026-07-15 21:50:03+00",
        }

        with patch.dict(os.environ, {"PRISM_PUBLIC_EXPLORER_TX_URL_PREFIX": "https://explorer.example/tx"}, clear=False):
            ctv_row = public_api.miner_payout_row(
                {
                    **base_row,
                    "fanout_txid": fanout_txid,
                    "fanout_vout": 1,
                    "fanout_amount_sats": 7_767_319,
                    "fanout_fee_sats": 152,
                    "fanout_gross_amount_sats": 7_767_471,
                    "fanout_status": "awaiting_maturity",
                }
            )
        self.assertEqual(ctv_row["transaction_kind"], "ctv_fanout")
        self.assertEqual(ctv_row["transaction_id"], fanout_txid)
        self.assertEqual(ctv_row["onchain_amount_bits"], 7_767_319)
        self.assertEqual(ctv_row["explorer_url"], f"https://explorer.example/tx/{fanout_txid}")

        coinbase_row = public_api.miner_payout_row(dict(base_row))
        self.assertEqual(coinbase_row["transaction_kind"], "coinbase")
        self.assertEqual(coinbase_row["transaction_id"], "b2" * 32)
        self.assertEqual(coinbase_row["onchain_amount_bits"], 7_767_471)

        # Accrued rows stay carry_forward even if a fanout/coinbase column
        # leaks in from a wider read-model row.
        accrued_row = public_api.miner_payout_row(
            {
                **base_row,
                "action": "accrued",
                "onchain_amount_sats": 0,
                "carry_forward_balance_sats": 7,
                "fanout_txid": fanout_txid,
            }
        )
        self.assertEqual(accrued_row["transaction_kind"], "carry_forward")
        self.assertIsNone(accrued_row["transaction_id"])
        self.assertEqual(accrued_row["onchain_amount_bits"], 0)

    def test_estimated_next_block_reward_bits_computes_and_handles_edges(self) -> None:
        # 50% of a 600-bit coinbase, no pool fee.
        self.assertEqual(
            public_api.estimated_next_block_reward_bits(
                share_percent="50", expected_coinbase_bits=600, pool_fee_bps=0
            ),
            300,
        )
        # A 10% pool fee reduces the distributable coinbase before applying the share.
        self.assertEqual(
            public_api.estimated_next_block_reward_bits(
                share_percent="50", expected_coinbase_bits=600, pool_fee_bps=1000
            ),
            270,
        )
        # A 0% share is a real answer (0 bits), not "unknown".
        self.assertEqual(
            public_api.estimated_next_block_reward_bits(
                share_percent="0", expected_coinbase_bits=600, pool_fee_bps=0
            ),
            0,
        )
        # No share, no coinbase, or a non-positive coinbase -> None so the dashboard
        # can fall back to showing only the share percent.
        for kwargs in (
            {"share_percent": None, "expected_coinbase_bits": 600, "pool_fee_bps": 0},
            {"share_percent": "50", "expected_coinbase_bits": None, "pool_fee_bps": 0},
            {"share_percent": "50", "expected_coinbase_bits": 0, "pool_fee_bps": 0},
            {"share_percent": "not-a-number", "expected_coinbase_bits": 600, "pool_fee_bps": 0},
        ):
            self.assertIsNone(public_api.estimated_next_block_reward_bits(**kwargs))

    def test_miner_estimated_reward_bits_end_to_end(self) -> None:
        coordinator = FakeCoordinator()  # latest block coinbase_value_bits=600, share_percent=50
        with patch.dict(os.environ, {"PRISM_PUBLIC_POOL_FEE_BPS": "0"}, clear=False):
            miner = public_api.miner(coordinator, recipient_id="miner-a")
        self.assertEqual(miner["estimated_next_block"]["estimated_reward_bits"], 300)

        with patch.dict(os.environ, {"PRISM_PUBLIC_POOL_FEE_BPS": "1000"}, clear=False):
            miner_with_fee = public_api.miner(coordinator, recipient_id="miner-a")
        self.assertEqual(miner_with_fee["estimated_next_block"]["estimated_reward_bits"], 270)

    def test_miner_estimated_reward_bits_none_when_no_reward_window_share(self) -> None:
        coordinator = FakeCoordinator()
        miner = public_api.miner(coordinator, recipient_id="miner-b")  # no window share
        self.assertIsNone(miner["estimated_next_block"]["share_percent"])
        self.assertIsNone(miner["estimated_next_block"]["estimated_reward_bits"])

    def test_public_pool_fee_bps_clamps_and_tolerates_invalid_env(self) -> None:
        for value, expected in (("", 0), ("250", 250), ("-5", 0), ("20000", 10_000), ("nan", 0)):
            with patch.dict(os.environ, {"PRISM_PUBLIC_POOL_FEE_BPS": value}, clear=False):
                self.assertEqual(public_api.public_pool_fee_bps(), expected)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(public_api.public_pool_fee_bps(), 0)

    def test_mining_configuration_pool_fee_uses_clamped_helper(self) -> None:
        # The displayed fee and the reward estimate must read the fee the same way.
        coordinator = FakeCoordinator()
        with patch.dict(os.environ, {"PRISM_PUBLIC_POOL_FEE_BPS": "20000"}, clear=False):
            config = public_api.mining_configuration(coordinator)
        self.assertEqual(config["configurations"][0]["pool_fee_bps"], 10_000)

    def test_miner_detail_tolerates_blank_minimum_payout_env(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PRISM_PUBLIC_MINIMUM_PAYOUT_BITS": "",
                "PRISM_PAYOUT_MIN_OUTPUT_BITS": "",
                "PRISM_PAYOUT_MIN_OUTPUT_SATS": "",
            },
            clear=True,
        ):
            miner = self.get_json("/public/v1/miners/miner-a")

        self.assertEqual(miner["minimum_payout_bits"], 0)
        self.assertEqual(miner["estimated_time_to_minimum_payout_seconds"], 0)

    def test_miner_detail_uses_payout_minimum_bits_env_as_fallback(self) -> None:
        with patch.dict(os.environ, {"PRISM_PAYOUT_MIN_OUTPUT_BITS": "43"}, clear=True):
            miner = self.get_json("/public/v1/miners/miner-a")

        self.assertEqual(miner["minimum_payout_bits"], 43)
        self.assertIsNone(miner["estimated_time_to_minimum_payout_seconds"])

    def test_miner_detail_falls_back_to_legacy_sats_env(self) -> None:
        with patch.dict(os.environ, {"PRISM_PAYOUT_MIN_OUTPUT_SATS": "43"}, clear=True):
            miner = self.get_json("/public/v1/miners/miner-a")

        self.assertEqual(miner["minimum_payout_bits"], 43)

    def test_miner_detail_public_minimum_payout_env_overrides_internal_env(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PRISM_PUBLIC_MINIMUM_PAYOUT_BITS": "41",
                "PRISM_PAYOUT_MIN_OUTPUT_BITS": "43",
            },
            clear=True,
        ):
            miner = self.get_json("/public/v1/miners/miner-a")

        self.assertEqual(miner["minimum_payout_bits"], 41)
        self.assertEqual(miner["estimated_time_to_minimum_payout_seconds"], 0)

    def test_miner_detail_skips_invalid_minimum_payout_env_values(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PRISM_PUBLIC_MINIMUM_PAYOUT_BITS": "not-an-int",
                "PRISM_PAYOUT_MIN_OUTPUT_BITS": "43",
            },
            clear=True,
        ):
            miner = self.get_json("/public/v1/miners/miner-a")

        self.assertEqual(miner["minimum_payout_bits"], 43)

    def test_mining_configuration_uses_public_stratum_url_and_fee_env(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://public-pool.example:3335",
                "PRISM_PUBLIC_POOL_FEE_BPS": "200",
            },
            clear=True,
        ):
            config = self.get_json("/public/v1/mining-configuration")

        endpoint = config["configurations"][0]["stratum_endpoints"][0]
        self.assertEqual(config["configurations"][0]["pool_fee_bps"], 200)
        self.assertEqual(endpoint["url"], "stratum+tcp://public-pool.example:3335")
        self.assertEqual(endpoint["default_port"], 3335)

    def test_mining_configuration_includes_highdiff_endpoint_when_enabled(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PRISM_PUBLIC_STRATUM_HOST": "mine.prism.example",
                "PRISM_STRATUM_HIGHDIFF_PORT": "4334",
            },
            clear=True,
        ):
            config = public_api.mining_configuration(FakeCoordinator())

        endpoints = config["configurations"][0]["stratum_endpoints"]
        self.assertEqual(
            endpoints,
            [
                {
                    "label": "Primary",
                    "url": "stratum+tcp://mine.prism.example:3340",
                    "protocol": "stratum_v1",
                    "default_port": 3340,
                },
                {
                    "label": "High-diff",
                    "url": "stratum+tcp://mine.prism.example:4334",
                    "protocol": "stratum_v1",
                    "default_port": 4334,
                },
            ],
        )

    def test_mining_configuration_derives_highdiff_endpoint_from_public_stratum_url(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://public-pool.example:3335",
                "PRISM_STRATUM_HIGHDIFF_PORT": "4334",
            },
            clear=True,
        ):
            config = public_api.mining_configuration(FakeCoordinator())

        endpoints = config["configurations"][0]["stratum_endpoints"]
        self.assertEqual(endpoints[0]["url"], "stratum+tcp://public-pool.example:3335")
        self.assertEqual(endpoints[1]["label"], "High-diff")
        self.assertEqual(endpoints[1]["url"], "stratum+tcp://public-pool.example:4334")
        self.assertEqual(endpoints[1]["default_port"], 4334)

    def test_mining_configuration_uses_public_highdiff_stratum_url_override(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://mine.prism.example:3333",
                "PRISM_STRATUM_HIGHDIFF_PORT": "4334",
                "PRISM_PUBLIC_STRATUM_HIGHDIFF_URL": "stratum+tcp://rentals.prism.example:14334",
            },
            clear=True,
        ):
            config = public_api.mining_configuration(FakeCoordinator())

        endpoints = config["configurations"][0]["stratum_endpoints"]
        self.assertEqual(endpoints[0]["url"], "stratum+tcp://mine.prism.example:3333")
        self.assertEqual(endpoints[0]["default_port"], 3333)
        self.assertEqual(endpoints[1]["label"], "High-diff")
        self.assertEqual(endpoints[1]["url"], "stratum+tcp://rentals.prism.example:14334")
        self.assertEqual(endpoints[1]["default_port"], 14334)

    def test_miner_worker_search_and_pagination_use_real_worker_names(self) -> None:
        page_two = self.get_json("/public/v1/miners/miner-a/workers?hide_inactive=false&page=2&limit=1")
        search = self.get_json("/public/v1/miners/miner-a/workers?hide_inactive=false&search=rig-b&page=1&limit=15")

        self.assertEqual(page_two["pagination"], {"page": 2, "limit": 1, "total_count": 2, "total_pages": 2})
        self.assertEqual(page_two["rows"][0]["worker_name"], "rig-b")
        self.assertEqual(search["pagination"]["total_count"], 1)
        self.assertEqual(search["rows"][0]["worker_name"], "rig-b")

        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.get_json(f"/public/v1/miners/miner-a/workers?search={'x' * 129}")

        self.assertEqual(raised.exception.code, 400)
        raised.exception.close()

    def test_miner_payout_and_earning_pagination_is_not_capped_to_first_row(self) -> None:
        earnings = self.get_json("/public/v1/miners/miner-a/earnings?page=2&limit=1")
        payouts = self.get_json("/public/v1/miners/miner-a/payouts?page=2&limit=1")

        self.assertEqual(earnings["pagination"]["total_count"], 2)
        self.assertEqual(earnings["rows"][0]["gross_earning_bits"], 7)
        self.assertEqual(earnings["rows"][0]["settlement_fee_bits"], 1)
        self.assertEqual(earnings["rows"][0]["net_earning_bits"], 6)
        self.assertEqual(earnings["rows"][0]["reward_share_percent"], "25")
        self.assertEqual(payouts["pagination"]["total_count"], 2)
        self.assertEqual(payouts["rows"][0]["action"], "accrued")

    def test_miner_row_builders_do_not_invent_missing_internal_values(self) -> None:
        with self.assertRaises(public_api.PublicApiError):
            public_api.miner_payout_row({"block_hash": "a" * 64, "block_height": 1})

        with self.assertRaises(public_api.PublicApiError):
            public_api.miner_earning_row(
                {
                    "block_hash": "a" * 64,
                    "block_height": 1,
                    "gross_amount_sats": 1,
                    "block_gross_amount_sats": 10,
                }
            )

        with self.assertRaises(public_api.PublicApiError):
            public_api.miner_earning_row(
                {
                    "block_hash": "a" * 64,
                    "block_height": 1,
                    "gross_amount_sats": 1,
                    "created_at": "2026-06-26 20:45:00+00",
                }
            )

    def test_pending_maturity_fallback_filters_and_nets_immature_onchain_rows(self) -> None:
        class HistoryOnlyLedger:
            def recipient_payout_history(self, *, recipient_id: str, limit: int = 50) -> list[dict[str, object]]:
                self.request = (recipient_id, limit)
                return [
                    {
                        "action": "onchain",
                        "maturity_state": "immature",
                        "onchain_amount_sats": 20,
                        "settlement_fee_sats": 3,
                    },
                    {
                        "action": "onchain",
                        "maturity_state": "mature",
                        "onchain_amount_sats": 50,
                        "settlement_fee_sats": 2,
                    },
                    {
                        "action": "accrued",
                        "maturity_state": "immature",
                        "onchain_amount_sats": 70,
                    },
                    {
                        "action": "onchain",
                        "maturity_state": "immature",
                        "onchain_amount_sats": 1,
                        "settlement_fee_sats": 2,
                    },
                ]

        ledger = HistoryOnlyLedger()

        self.assertEqual(public_api.pending_maturity_bits_for_recipient(ledger, "miner-a"), 17)
        self.assertEqual(ledger.request, ("miner-a", 1_000))


class PrismPublicDashboardMemoryLedgerTests(unittest.TestCase):
    def test_memory_ledger_public_read_models_are_empty_safe(self) -> None:
        coordinator = MemoryCoordinator()
        now_ms = int(time.time() * 1000)
        coordinator.ledger.append(
            PendingShare(
                share_id="miner-a:share-1",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=100,
                network_difficulty=1000,
                template_height=123,
                job_id="job-1",
                job_issued_at_ms=now_ms,
                accepted_at_ms=now_ms,
                ntime=1,
            )
        )
        handler = make_audit_handler(coordinator)  # type: ignore[arg-type]
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/public/v1/leaderboard",
                timeout=5,
            ) as response:
                payload = json.loads(response.read())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(payload["schema"], "prism.dashboard.leaderboard.v1")
        self.assertEqual(payload["pagination"]["total_count"], 1)
        self.assertEqual(payload["rows"][0]["recipient_id"], "miner-a")


if __name__ == "__main__":
    unittest.main()
