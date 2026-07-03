#!/usr/bin/env python3

from __future__ import annotations

import json
import math
import re
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OPENAPI_PATH = ROOT / "docs" / "public-dashboard-api-v1.openapi.yaml"
CONTRACT_DIR = ROOT / "docs" / "public-dashboard-api"
FIXTURE_DIR = CONTRACT_DIR / "fixtures"

EXPECTED_FIXTURES = {
    "pool-summary.json": "prism.dashboard.pool-summary.v1",
    "hashrate-series.json": "prism.dashboard.hashrate-series.v1",
    "leaderboard.json": "prism.dashboard.leaderboard.v1",
    "blocks.json": "prism.dashboard.blocks.v1",
    "settlement-artifacts.json": "prism.dashboard.settlement-artifacts.v1",
    "settlement-artifacts-direct-coinbase.json": "prism.dashboard.settlement-artifacts.v1",
    "pending-fanouts.json": "prism.dashboard.pending-fanouts.v1",
    "fanout.json": "prism.dashboard.fanout.v1",
    "mining-configuration.json": "prism.dashboard.mining-configuration.v1",
    "miner.json": "prism.dashboard.miner.v1",
    "miner-earnings.json": "prism.dashboard.miner-earnings.v1",
    "miner-payouts.json": "prism.dashboard.miner-payouts.v1",
    "miner-workers.json": "prism.dashboard.miner-workers.v1",
    "error.json": "prism.dashboard.error.v1",
}

EXPECTED_ROUTES = (
    "/pool-summary:",
    "/hashrate-series:",
    "/leaderboard:",
    "/blocks:",
    "/blocks/{block_hash}/settlement-artifacts:",
    "/fanouts/pending:",
    "/fanouts/{fanout_txid}:",
    "/artifacts/{sha256}:",
    "/mining-configuration:",
    "/miners/{recipient_id}:",
    "/miners/{recipient_id}/earnings:",
    "/miners/{recipient_id}/payouts:",
    "/miners/{recipient_id}/workers:",
)

DECIMAL_KEYS = {
    "network_difficulty",
    "requested_window_weight",
    "accepted_share_difficulty",
    "pool_accepted_share_difficulty",
    "solver_share_difficulty",
    "reward_window_weight",
    "accepted_difficulty_3h",
}

HASHRATE_ROLLUP_KEYS = {"h1", "h3", "h24", "m1", "m5", "m10"}
HEX_HASH_KEYS = {"hash", "tip_hash", "block_hash", "fanout_txid", "transaction_id", "parent_coinbase_txid", "ctv_hash", "sha256"}
HEX_STRING_KEYS = {"fanout_tx_hex"}

DECIMAL_PATTERN = re.compile(r"^-?[0-9]+(?:\.[0-9]+)?$")
HEX_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
HEX_STRING_PATTERN = re.compile(r"^(?:[0-9a-f]{2})+$")


class PublicDashboardApiContractTests(unittest.TestCase):
    def test_contract_files_exist(self) -> None:
        self.assertTrue(OPENAPI_PATH.is_file())
        self.assertTrue((CONTRACT_DIR / "README.md").is_file())
        for fixture_name in EXPECTED_FIXTURES:
            self.assertTrue((FIXTURE_DIR / fixture_name).is_file(), fixture_name)

    def test_openapi_declares_expected_routes_and_schema_tags(self) -> None:
        text = OPENAPI_PATH.read_text(encoding="utf-8")
        for route in EXPECTED_ROUTES:
            self.assertIn(route, text)
        for schema_tag in EXPECTED_FIXTURES.values():
            self.assertIn(schema_tag, text)

    def test_openapi_narrows_ambiguous_or_contextual_types(self) -> None:
        text = OPENAPI_PATH.read_text(encoding="utf-8")
        self.assertIn('pattern: "^(?:[0-9a-f]{2})+$"', text)
        self.assertIn("1m = 30 days", text)
        self.assertIn("5m = 5 minutes", text)
        self.assertIn("PendingSettlementStatus:", text)
        self.assertIn("$ref: \"#/components/schemas/PendingFanoutArtifact\"", text)
        self.assertIn("HashrateSubject:", text)
        self.assertIn("const: pool", text)
        self.assertIn("type: \"null\"", text)
        self.assertIn("const: miner", text)
        self.assertIn("minLength: 1", text)
        self.assertIn(
            "pool_fee_bps:\n          type: integer\n          minimum: 0\n          maximum: 10000",
            text,
        )

    def test_public_contract_avoids_retired_nomenclature(self) -> None:
        contract_files = [OPENAPI_PATH, CONTRACT_DIR / "README.md", *FIXTURE_DIR.glob("*.json")]
        for path in contract_files:
            text = path.read_text(encoding="utf-8").lower()
            self.assertNotIn("tides", text, str(path))
            self.assertNotIn("qbit_tides", text, str(path))
            self.assertNotIn("qbit.tides", text, str(path))
            self.assertNotIn("_sats", text, str(path))
            self.assertNotIn("satoshis", text, str(path))

    def test_fixtures_have_expected_schema_tags(self) -> None:
        for fixture_name, schema_tag in EXPECTED_FIXTURES.items():
            payload = self.load_fixture(fixture_name)
            self.assertEqual(payload["schema"], schema_tag)

    def test_fixtures_follow_common_field_conventions(self) -> None:
        for fixture_name in EXPECTED_FIXTURES:
            payload = self.load_fixture(fixture_name)
            self.assert_conventions(payload, fixture_name)

    def test_pagination_fixtures_are_consistent(self) -> None:
        for fixture_name in (
            "leaderboard.json",
            "blocks.json",
            "pending-fanouts.json",
            "miner-earnings.json",
            "miner-payouts.json",
            "miner-workers.json",
        ):
            fixture = self.load_fixture(fixture_name)
            pagination = fixture["pagination"]
            rows = fixture["rows"]
            self.assertGreaterEqual(pagination["page"], 1)
            self.assertGreaterEqual(pagination["limit"], 1)
            self.assertGreaterEqual(pagination["total_count"], 0)
            self.assertGreaterEqual(pagination["total_pages"], 0)
            expected_total_pages = math.ceil(pagination["total_count"] / pagination["limit"]) if pagination["total_count"] else 0
            self.assertEqual(pagination["total_pages"], expected_total_pages)
            self.assertLessEqual(len(rows), pagination["limit"])
            if pagination["total_count"]:
                self.assertLessEqual(len(rows), pagination["total_count"])
            else:
                self.assertEqual(rows, [])

    def test_miner_earnings_fixture_math_is_consistent(self) -> None:
        blocks_by_hash = {
            row["hash"]: row
            for row in self.load_fixture("blocks.json")["rows"]
        }
        payouts_by_hash = {
            row["block_hash"]: row
            for row in self.load_fixture("miner-payouts.json")["rows"]
        }
        recent_payouts_by_hash = {
            row["block_hash"]: row
            for row in self.load_fixture("miner.json")["recent_payouts"]
        }
        for row in self.load_fixture("miner-earnings.json")["rows"]:
            gross = row["gross_earning_bits"]
            settlement_fee = row["settlement_fee_bits"]
            net = row["net_earning_bits"]
            self.assertGreaterEqual(settlement_fee, 0)
            self.assertEqual(net, gross - settlement_fee)

            block = blocks_by_hash[row["block_hash"]]
            expected_percent = (Decimal(gross) * Decimal("100") / Decimal(block["coinbase_value_bits"])).quantize(
                Decimal("0.01")
            )
            self.assertEqual(Decimal(row["reward_share_percent"]), expected_percent)

            miner = self.load_fixture("miner.json")
            self.assertEqual(Decimal(miner["estimated_next_block"]["share_percent"]), expected_percent)
            self.assertEqual(Decimal(miner["reward_window_percent"]), expected_percent)

            leaderboard = self.load_fixture("leaderboard.json")
            matching_rows = [
                item
                for item in leaderboard["rows"]
                if item["recipient_id"] == self.load_fixture("miner-earnings.json")["recipient_id"]
            ]
            self.assertEqual(len(matching_rows), 1)
            self.assertEqual(Decimal(matching_rows[0]["share_percent"]), expected_percent)

            self.assertIn(row["block_hash"], payouts_by_hash)
            self.assertIn(row["block_hash"], recent_payouts_by_hash)
            payout = payouts_by_hash[row["block_hash"]]
            recent_payout = recent_payouts_by_hash[row["block_hash"]]
            self.assertLessEqual(payout["onchain_amount_bits"], net)
            self.assertEqual(recent_payout["onchain_amount_bits"], payout["onchain_amount_bits"])

    def test_leaderboard_hash_percent_matches_hashrate_share(self) -> None:
        leaderboard = self.load_fixture("leaderboard.json")
        pool_hashrate = Decimal(leaderboard["totals"]["pool_hashrate_ths"])
        for row in leaderboard["rows"]:
            expected_percent = (Decimal(row["hashrate_ths_3h"]) * Decimal("100") / pool_hashrate).quantize(
                Decimal("0.01")
            )
            self.assertEqual(Decimal(row["hash_percent"]), expected_percent)

    def test_hashrate_subject_fixture_matches_subject_type(self) -> None:
        subject = self.load_fixture("hashrate-series.json")["subject"]
        if subject["type"] == "pool":
            self.assertIsNone(subject["id"])
        else:
            self.assertEqual(subject["type"], "miner")
            self.assertIsInstance(subject["id"], str)
            self.assertGreater(len(subject["id"]), 0)

    def test_fanout_fixture_status_matches_tip_height(self) -> None:
        tip_height = self.load_fixture("pool-summary.json")["network"]["height"]
        fanout_rows = [
            self.load_fixture("fanout.json")["fanout"],
            *self.load_fixture("pending-fanouts.json")["rows"],
            *self.load_fixture("settlement-artifacts.json")["fanouts"],
        ]
        for row in fanout_rows:
            broadcastable_at_height = row["broadcastable_at_height"]
            if broadcastable_at_height is not None and tip_height < broadcastable_at_height:
                self.assertEqual(row["status"], "awaiting_maturity")
                self.assertFalse(row["cpfp_anchor_spendable"])

    def test_reward_window_weight_fixtures_match_network_difficulty(self) -> None:
        pool_summary = self.load_fixture("pool-summary.json")
        window_multiplier = Decimal(pool_summary["pool"]["reward_window"]["window_multiplier"])
        expected_window_weight = Decimal(pool_summary["network"]["network_difficulty"]) * Decimal(
            window_multiplier
        )
        self.assertEqual(Decimal(pool_summary["pool"]["reward_window"]["requested_window_weight"]), expected_window_weight)

        for row in self.load_fixture("blocks.json")["rows"]:
            if row["reward_window_weight"] is None:
                continue
            self.assertEqual(Decimal(row["reward_window_weight"]), Decimal(row["network_difficulty"]) * window_multiplier)

    def test_miner_summary_embeds_only_bounded_previews(self) -> None:
        miner = self.load_fixture("miner.json")
        self.assertLessEqual(len(miner["workers"]), 5)
        self.assertLessEqual(len(miner["recent_payouts"]), 5)

        text = OPENAPI_PATH.read_text(encoding="utf-8")
        self.assertIn("workers:\n          type: array\n          maxItems: 5", text)
        self.assertIn("recent_payouts:\n          type: array\n          maxItems: 5", text)

    def test_miner_fixture_uses_default_minimum_payout_floor(self) -> None:
        miner = self.load_fixture("miner.json")
        self.assertEqual(miner["minimum_payout_bits"], 10_485_760)
        self.assertLess(miner["owed_balance_bits"], miner["minimum_payout_bits"])
        self.assertIsNone(miner["estimated_time_to_minimum_payout_seconds"])

    def test_dashboard_api_is_public_read_model_not_internal_audit_api(self) -> None:
        text = OPENAPI_PATH.read_text(encoding="utf-8")
        self.assertNotIn("/audit/", text)
        self.assertNotIn("postgres", text.lower())
        self.assertNotIn("rpc password", text.lower())

    def test_public_api_contract_documents_cache_headers_and_knobs(self) -> None:
        openapi_text = OPENAPI_PATH.read_text(encoding="utf-8")
        readme_text = (CONTRACT_DIR / "README.md").read_text(encoding="utf-8")

        for header in ("Cache-Control", "CDN-Cache-Control", "Vercel-CDN-Cache-Control", "Age"):
            self.assertIn(header, openapi_text)
        self.assertIn("BrowserCacheControl:", openapi_text)
        self.assertIn("SharedCacheControl:", openapi_text)
        self.assertIn("Cache-Control: no-store", readme_text)
        self.assertIn("PRISM_PUBLIC_CACHE_TTL_SECONDS", readme_text)
        self.assertIn("PRISM_PUBLIC_CACHE_MAX_ENTRIES", readme_text)

    def load_fixture(self, fixture_name: str) -> dict[str, Any]:
        with (FIXTURE_DIR / fixture_name).open(encoding="utf-8") as fixture_file:
            payload = json.load(fixture_file)
        self.assertIsInstance(payload, dict)
        return payload

    def assert_conventions(self, value: Any, source: str, path: tuple[str, ...] = ()) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = (*path, key)
                if key == "schema":
                    self.assertIsInstance(child, str, ".".join(child_path))
                    self.assertTrue(child.startswith("prism.dashboard."), ".".join(child_path))
                if key == "generated_at" or key.endswith("_at"):
                    if child is not None:
                        self.assert_iso_timestamp(child, source, child_path)
                if key.endswith("_bits"):
                    if child is not None:
                        self.assertIsInstance(child, int, ".".join(child_path))
                        self.assertGreaterEqual(child, 0, ".".join(child_path))
                if self.is_decimal_key(key, child_path) and not isinstance(child, (dict, list)):
                    if child is not None:
                        self.assertIsInstance(child, str, ".".join(child_path))
                        self.assertRegex(child, DECIMAL_PATTERN, ".".join(child_path))
                if self.is_hex_hash_key(key) and not isinstance(child, (dict, list)):
                    if child is not None:
                        self.assertIsInstance(child, str, ".".join(child_path))
                        self.assertRegex(child, HEX_HASH_PATTERN, ".".join(child_path))
                if key in HEX_STRING_KEYS and not isinstance(child, (dict, list)):
                    if child is not None:
                        self.assertIsInstance(child, str, ".".join(child_path))
                        self.assertRegex(child, HEX_STRING_PATTERN, ".".join(child_path))
                self.assert_conventions(child, source, child_path)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                self.assert_conventions(child, source, (*path, str(index)))

    def is_decimal_key(self, key: str, path: tuple[str, ...] = ()) -> bool:
        return (
            key in DECIMAL_KEYS
            or key.startswith("hashrate_ths_")
            or (len(path) >= 2 and path[-2] == "hashrate_ths" and key in HASHRATE_ROLLUP_KEYS)
            or key.endswith("_ths")
            or key.endswith("_percent")
            or key.endswith("_difficulty")
            or key.endswith("_weight")
        )

    def is_hex_hash_key(self, key: str) -> bool:
        return key in HEX_HASH_KEYS or key.endswith("_sha256") or key.endswith("_txid")

    def assert_iso_timestamp(self, value: object, source: str, path: tuple[str, ...]) -> None:
        self.assertIsInstance(value, str, ".".join(path))
        text = str(value)
        self.assertTrue(text.endswith("Z"), f"{source}:{'.'.join(path)} must be UTC/Z")
        datetime.fromisoformat(text.removesuffix("Z") + "+00:00")


if __name__ == "__main__":
    unittest.main()
