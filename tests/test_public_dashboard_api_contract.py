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

from lab.prism import public_api


ROOT = Path(__file__).resolve().parents[1]
OPENAPI_PATH = ROOT / "docs" / "public-dashboard-api-v1.openapi.yaml"
CONTRACT_DIR = ROOT / "docs" / "public-dashboard-api"
FIXTURE_DIR = CONTRACT_DIR / "fixtures"

EXPECTED_FIXTURES = {
    "pool-summary.json": "prism.dashboard.pool-summary.v1",
    "hashrate-series.json": "prism.dashboard.hashrate-series.v1",
    "hashrate-series-dual-rate.json": "prism.dashboard.hashrate-series.v2",
    "leaderboard.json": "prism.dashboard.leaderboard.v2",
    "leaderboard-legacy.json": "prism.dashboard.leaderboard.v1",
    "blocks.json": "prism.dashboard.blocks.v1",
    "blocks-chain-states.json": "prism.dashboard.blocks.v2",
    "block-markers.json": "prism.dashboard.block-markers.v1",
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
    "/block-markers:",
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
        self.assertIn("Rejected for `window=3h`", text)
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
            "leaderboard-legacy.json",
            "blocks.json",
            "blocks-chain-states.json",
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

            self.assertIn(row["block_hash"], payouts_by_hash)
            self.assertIn(row["block_hash"], recent_payouts_by_hash)
            payout = payouts_by_hash[row["block_hash"]]
            recent_payout = recent_payouts_by_hash[row["block_hash"]]
            self.assertLessEqual(payout["onchain_amount_bits"], net)
            self.assertEqual(recent_payout["onchain_amount_bits"], payout["onchain_amount_bits"])

    def test_live_reward_share_is_consistent_and_distinct_from_historical_payout(self) -> None:
        miner = self.load_fixture("miner.json")
        earnings = self.load_fixture("miner-earnings.json")
        leaderboard = self.load_fixture("leaderboard.json")
        matching_rows = [
            row
            for row in leaderboard["rows"]
            if row["recipient_id"] == earnings["recipient_id"]
        ]

        self.assertEqual(len(matching_rows), 1)
        live_percent = Decimal(miner["reward_window_percent"])
        self.assertEqual(Decimal(miner["estimated_next_block"]["share_percent"]), live_percent)
        self.assertEqual(Decimal(matching_rows[0]["share_percent"]), live_percent)
        self.assertNotEqual(Decimal(earnings["rows"][0]["reward_share_percent"]), live_percent)

        latest_coinbase = self.load_fixture("blocks.json")["rows"][0]["coinbase_value_bits"]
        pool_fee_bps = self.load_fixture("mining-configuration.json")["configurations"][0]["pool_fee_bps"]
        self.assertEqual(
            miner["estimated_next_block"]["estimated_reward_bits"],
            public_api.estimated_next_block_reward_bits(
                share_percent=str(live_percent),
                expected_coinbase_bits=latest_coinbase,
                pool_fee_bps=pool_fee_bps,
            ),
        )

    def test_reward_leaderboard_share_percent_matches_counted_work(self) -> None:
        leaderboard = self.load_fixture("leaderboard.json")
        counted_window_weight = Decimal(leaderboard["window"]["counted_window_weight"])
        pool_hashrate = Decimal(leaderboard["totals"]["pool_hashrate_ths"])
        observed_span = leaderboard["window"]["observed_span_seconds"]
        self.assertEqual(
            counted_window_weight,
            Decimal(leaderboard["totals"]["pool_counted_share_difficulty"]),
        )
        self.assertEqual(
            pool_hashrate,
            Decimal(public_api.hashrate_ths_from_difficulty(counted_window_weight, observed_span)),
        )
        for row in leaderboard["rows"]:
            expected_percent = (Decimal(row["counted_share_difficulty"]) * Decimal("100") / counted_window_weight).quantize(
                Decimal("0.01")
            )
            self.assertEqual(Decimal(row["share_percent"]), expected_percent)
            self.assertEqual(
                Decimal(row["hashrate_ths"]),
                Decimal(public_api.hashrate_ths_from_difficulty(row["counted_share_difficulty"], observed_span)),
            )
            hashrate_percent = (Decimal(row["hashrate_ths"]) * Decimal("100") / pool_hashrate).quantize(
                Decimal("0.01")
            )
            self.assertEqual(Decimal(row["share_percent"]), hashrate_percent)

    def test_complete_reward_window_fixture_eta_matches_observed_span(self) -> None:
        leaderboard = self.load_fixture("leaderboard.json")

        self.assertTrue(leaderboard["window"]["is_complete"])
        observed_span = leaderboard["window"]["observed_span_seconds"]
        expected_eta = leaderboard["totals"]["expected_time_to_block_seconds"]
        self.assertLessEqual(abs(expected_eta - observed_span / leaderboard["window"]["window_multiplier"]), 1)

    def test_legacy_leaderboard_fixture_retains_exact_v1_shape(self) -> None:
        leaderboard = self.load_fixture("leaderboard-legacy.json")

        self.assertEqual(leaderboard["window"]["id"], "3h")
        self.assertEqual(
            set(leaderboard["totals"]),
            {"pool_hashrate_ths", "pool_accepted_share_difficulty", "participant_count"},
        )
        self.assertEqual(
            set(leaderboard["rows"][0]),
            {
                "rank",
                "recipient_id",
                "display_name",
                "hashrate_ths_3h",
                "share_percent",
                "hash_percent",
                "blocks_found",
                "last_share_at",
            },
        )

    def test_hashrate_subject_fixture_matches_subject_type(self) -> None:
        for fixture_name in ("hashrate-series.json", "hashrate-series-dual-rate.json"):
            subject = self.load_fixture(fixture_name)["subject"]
            if subject["type"] == "pool":
                self.assertIsNone(subject["id"], fixture_name)
            else:
                self.assertEqual(subject["type"], "miner", fixture_name)
                self.assertIsInstance(subject["id"], str, fixture_name)
                self.assertGreater(len(subject["id"]), 0, fixture_name)

    def test_legacy_hashrate_series_fixture_retains_exact_v1_shape(self) -> None:
        series = self.load_fixture("hashrate-series.json")

        self.assertEqual(set(series), {"schema", "generated_at", "subject", "range", "bucket", "unit", "points"})
        self.assertGreater(len(series["points"]), 0)
        for point in series["points"]:
            self.assertEqual(
                set(point),
                {"timestamp", "hashrate_ths", "accepted_share_count", "accepted_share_difficulty"},
            )

    def test_dual_rate_hashrate_series_fixture_is_internally_consistent(self) -> None:
        series = self.load_fixture("hashrate-series-dual-rate.json")

        self.assertEqual(
            set(series),
            {
                "schema",
                "generated_at",
                "subject",
                "range",
                "bucket",
                "bucket_seconds",
                "unit",
                "rate_basis",
                "smoothing",
                "points",
            },
        )
        self.assertEqual(series["unit"], "ths")
        self.assertEqual(series["rate_basis"], "accepted_share_difficulty")
        bucket_seconds = series["bucket_seconds"]
        self.assertEqual(bucket_seconds, public_api.HASHRATE_SERIES_BUCKET_SECONDS[series["bucket"]])
        smoothing = series["smoothing"]
        self.assertEqual(set(smoothing), {"method", "window_seconds"})
        window_seconds = smoothing["window_seconds"]
        self.assertIsInstance(window_seconds, int)
        self.assertEqual(window_seconds % bucket_seconds, 0)
        if smoothing["method"] == "none":
            self.assertEqual(window_seconds, bucket_seconds)
        else:
            self.assertEqual(smoothing["method"], "trailing")
            self.assertGreaterEqual(window_seconds // bucket_seconds, 2)
        self.assertEqual(
            smoothing,
            public_api.hashrate_series_smoothing(bucket_seconds=bucket_seconds, window_seconds=window_seconds),
        )

        generated_at_epoch = self.epoch(series["generated_at"])
        points = series["points"]
        self.assertGreater(len(points), 0)
        epochs = [self.epoch(point["timestamp"]) for point in points]
        self.assertEqual(epochs, sorted(set(epochs)))
        for point, epoch in zip(points, epochs):
            self.assertEqual(
                set(point),
                {
                    "timestamp",
                    "raw_hashrate_ths",
                    "smoothed_hashrate_ths",
                    "accepted_share_count",
                    "accepted_share_difficulty",
                    "complete",
                },
            )
            self.assertEqual(epoch % bucket_seconds, 0, point["timestamp"])
            self.assertIsInstance(point["complete"], bool)
            self.assertEqual(point["complete"], generated_at_epoch >= epoch + bucket_seconds, point["timestamp"])
            # Every raw rate is credit over the full bucket, so the trailing
            # estimate is the window's raw rates summed and re-scaled from
            # bucket duration to window duration, with missing buckets as zero.
            window_raw_total = sum(
                Decimal(other["raw_hashrate_ths"])
                for other, other_epoch in zip(points, epochs)
                if epoch - window_seconds < other_epoch <= epoch
            )
            # Each public rate is independently rounded by decimal_string, so
            # their re-scaled totals may differ by the final decimal place.
            smoothed_total = Decimal(point["smoothed_hashrate_ths"]) * Decimal(
                window_seconds
            )
            raw_total = window_raw_total * Decimal(bucket_seconds)
            self.assertLessEqual(
                abs(smoothed_total - raw_total),
                max(abs(raw_total) * Decimal("1e-30"), Decimal("1e-30")),
                point["timestamp"],
            )
            if smoothing["method"] == "none":
                self.assertEqual(Decimal(point["smoothed_hashrate_ths"]), Decimal(point["raw_hashrate_ths"]))

    def test_block_markers_fixture_is_internally_consistent(self) -> None:
        markers = self.load_fixture("block-markers.json")

        self.assertEqual(
            set(markers),
            {"schema", "generated_at", "range", "bucket", "bucket_seconds", "total_blocks", "points"},
        )
        bucket_seconds = markers["bucket_seconds"]
        self.assertEqual(bucket_seconds, public_api.HASHRATE_SERIES_BUCKET_SECONDS[markers["bucket"]])
        self.assertIn(markers["bucket"], public_api.allowed_block_marker_buckets(markers["range"]))
        points = markers["points"]
        self.assertGreater(len(points), 0)
        epochs = [self.epoch(point["timestamp"]) for point in points]
        self.assertEqual(epochs, sorted(set(epochs)))
        # total_blocks counts every non-reversed block in range and every such
        # block lands in some listed bucket, so the two totals must agree.
        self.assertEqual(
            markers["total_blocks"],
            sum(point["block_count"] for point in points),
        )
        for point, epoch in zip(points, epochs):
            self.assertEqual(set(point), {"timestamp", "block_count", "blocks", "truncated"})
            # Marker buckets sit on the hashrate chart's floor-aligned grid.
            self.assertEqual(epoch % bucket_seconds, 0, point["timestamp"])
            self.assertGreater(point["block_count"], 0)
            blocks = point["blocks"]
            self.assertEqual(
                len(blocks),
                min(point["block_count"], public_api.BLOCK_MARKERS_MAX_BLOCKS_PER_BUCKET),
            )
            self.assertEqual(
                point["truncated"],
                point["block_count"] > public_api.BLOCK_MARKERS_MAX_BLOCKS_PER_BUCKET,
            )
            found_epochs = [self.epoch(block["found_at"]) for block in blocks]
            self.assertEqual(found_epochs, sorted(found_epochs, reverse=True))
            for block, found_epoch in zip(blocks, found_epochs):
                self.assertEqual(set(block), {"height", "hash", "found_at"})
                self.assertEqual(found_epoch // bucket_seconds * bucket_seconds, epoch, block["hash"])

    def test_block_markers_fixture_shares_the_blocks_fixture_pool(self) -> None:
        # The newest marker is blocks.json's latest block, so the two fixtures
        # describe one pool and a dashboard can render them together.
        markers = self.load_fixture("block-markers.json")
        latest = self.load_fixture("blocks.json")["rows"][0]
        newest_marker = markers["points"][-1]["blocks"][0]
        self.assertEqual(newest_marker["hash"], latest["hash"])
        self.assertEqual(newest_marker["height"], latest["height"])
        self.assertEqual(newest_marker["found_at"], latest["found_at"])

    def test_openapi_block_markers_declares_marker_contract(self) -> None:
        text = OPENAPI_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "    BlockMarkerPoint:\n"
            "      type: object\n"
            "      additionalProperties: false\n"
            "      required: [timestamp, block_count, blocks, truncated]\n",
            text,
        )
        self.assertIn(
            "    BlockMarkerBlock:\n"
            "      type: object\n"
            "      additionalProperties: false\n"
            "      required: [height, hash, found_at]\n",
            text,
        )
        self.assertIn("const: prism.dashboard.block-markers.v1", text)
        self.assertIn("maxItems: 3", text)

        readme_text = (CONTRACT_DIR / "README.md").read_text(encoding="utf-8")
        self.assertIn("/public/v1/block-markers", readme_text)
        self.assertIn("block-markers.json", readme_text)

    def test_openapi_hashrate_series_declares_dual_rate_view(self) -> None:
        text = OPENAPI_PATH.read_text(encoding="utf-8")
        self.assertIn("- name: view\n          in: query", text)
        self.assertIn("enum: [both]", text)
        self.assertIn('- $ref: "#/components/schemas/HashrateSeriesResponse"', text)
        self.assertIn('- $ref: "#/components/schemas/HashrateSeriesDualRateResponse"', text)
        self.assertIn("HashrateSmoothing:", text)
        self.assertIn("enum: [trailing, none]", text)
        self.assertIn("const: accepted_share_difficulty", text)
        self.assertIn("credited hashrate", text)
        # The documented required fields are exactly the fixture's keys.
        self.assertIn(
            "    DualRateHashratePoint:\n"
            "      type: object\n"
            "      additionalProperties: false\n"
            "      required:\n"
            "        - timestamp\n"
            "        - raw_hashrate_ths\n"
            "        - smoothed_hashrate_ths\n"
            "        - accepted_share_count\n"
            "        - accepted_share_difficulty\n"
            "        - complete\n",
            text,
        )
        self.assertIn(
            "    HashrateSeriesDualRateResponse:\n"
            "      type: object\n"
            "      additionalProperties: false\n"
            "      required:\n"
            "        - schema\n"
            "        - generated_at\n"
            "        - subject\n"
            "        - range\n"
            "        - bucket\n"
            "        - bucket_seconds\n"
            "        - unit\n"
            "        - rate_basis\n"
            "        - smoothing\n"
            "        - points\n",
            text,
        )
        # The legacy point schema is untouched.
        self.assertIn(
            "    HashratePoint:\n"
            "      type: object\n"
            "      additionalProperties: false\n"
            "      required:\n"
            "        - timestamp\n"
            "        - hashrate_ths\n"
            "        - accepted_share_count\n"
            "        - accepted_share_difficulty\n",
            text,
        )

        readme_text = (CONTRACT_DIR / "README.md").read_text(encoding="utf-8")
        self.assertIn("credited hashrate", readme_text)
        self.assertIn("`view=both`", readme_text)
        self.assertIn("prism.dashboard.hashrate-series.v2", readme_text)
        self.assertIn("PRISM_PUBLIC_HASHRATE_SMOOTHING_SECONDS", readme_text)
        self.assertIn("router telemetry", readme_text)

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

    def test_current_fixture_difficulty_is_derived_from_compact_bits(self) -> None:
        pool_summary = self.load_fixture("pool-summary.json")
        expected_difficulty = public_api.scaled_network_difficulty(pool_summary["network"]["bits"])

        self.assertEqual(
            Decimal(pool_summary["network"]["network_difficulty"]),
            expected_difficulty,
        )
        self.assertEqual(
            Decimal(self.load_fixture("leaderboard.json")["window"]["network_difficulty"]),
            expected_difficulty,
        )
        self.assertEqual(
            pool_summary["pool"]["expected_time_to_block_seconds"],
            public_api.expected_time_to_block_seconds(
                hashrate_ths=pool_summary["pool"]["hashrate_ths"]["h3"],
                network_difficulty=str(expected_difficulty),
            ),
        )

    def test_historical_block_fixture_difficulty_is_derived_from_compact_bits(self) -> None:
        for fixture_name in ("blocks.json", "blocks-chain-states.json"):
            for row in self.load_fixture(fixture_name)["rows"]:
                self.assertEqual(
                    Decimal(row["network_difficulty"]),
                    public_api.scaled_network_difficulty(row["bits"]),
                    fixture_name,
                )

    BLOCK_ROW_V1_KEYS = {
        "height",
        "hash",
        "found_at",
        "network_difficulty",
        "bits",
        "solver_recipient_id",
        "solver_worker_name",
        "solver_share_difficulty",
        "reward_window_weight",
        "coinbase_value_bits",
        "audit_bundle_sha256",
        "payout_manifest_sha256",
        "explorer_url",
    }

    def test_default_blocks_fixture_keeps_exact_v1_row_shape(self) -> None:
        # The chain_state filter must not leak into the default response: the
        # v1 fixture stays byte-shape identical to the pre-filter contract.
        for row in self.load_fixture("blocks.json")["rows"]:
            self.assertEqual(set(row), self.BLOCK_ROW_V1_KEYS)

    def test_chain_state_blocks_fixture_adds_only_reorg_visibility_fields(self) -> None:
        fixture = self.load_fixture("blocks-chain-states.json")
        rows = fixture["rows"]

        self.assertGreater(len(rows), 0)
        reversed_rows = [row for row in rows if row["chain_state"] == "reversed"]
        self.assertGreater(len(reversed_rows), 0)
        for row in rows:
            self.assertEqual(
                set(row),
                self.BLOCK_ROW_V1_KEYS | {"chain_state", "disconnected_at"},
            )
            self.assertIn(
                row["chain_state"],
                {"prepared", "confirmed", "inactive", "rejected", "reversed"},
            )
            if row["chain_state"] == "reversed":
                self.assertIsNotNone(row["disconnected_at"])
            else:
                self.assertIsNone(row["disconnected_at"])

    def test_pool_summary_fixture_reports_reorg_counts_and_network_hashrate(self) -> None:
        pool_summary = self.load_fixture("pool-summary.json")
        pool = pool_summary["pool"]

        for key in ("blocks_reversed_total", "blocks_inactive_total"):
            self.assertIsInstance(pool[key], int)
            self.assertGreaterEqual(pool[key], 0)
        network_hashrate = pool_summary["network"]["hashrate_ths"]
        self.assertIsInstance(network_hashrate, str)
        self.assertRegex(network_hashrate, DECIMAL_PATTERN)
        # The pool's 3h credited rate divided by the network estimate is the
        # documented share-of-network ratio; the fixture keeps it plausible.
        share = Decimal(pool["hashrate_ths"]["h3"]) / Decimal(network_hashrate)
        self.assertGreater(share, 0)
        self.assertLess(share, 1)

    def test_openapi_declares_blocks_chain_state_filter_and_summary_reorg_fields(self) -> None:
        text = OPENAPI_PATH.read_text(encoding="utf-8")
        self.assertIn("- name: chain_state\n          in: query", text)
        self.assertIn("enum: [active, all, reversed]", text)
        self.assertIn('- $ref: "#/components/schemas/BlocksResponse"', text)
        self.assertIn('- $ref: "#/components/schemas/BlocksChainStateResponse"', text)
        self.assertIn("ChainStateBlockRow:", text)
        self.assertIn("const: prism.dashboard.blocks.v2", text)
        self.assertIn("ChainState:", text)
        self.assertIn("- disconnected_at", text)
        self.assertIn("- blocks_reversed_total", text)
        self.assertIn("- blocks_inactive_total", text)
        # network.hashrate_ths is required-but-nullable and documents the
        # credited-work vs chain-estimate caveat.
        self.assertIn("- hashrate_ths\n        - initial_block_download", text)
        self.assertIn("ratio is approximate", text)

        readme_text = (CONTRACT_DIR / "README.md").read_text(encoding="utf-8")
        self.assertIn("`chain_state=all`", readme_text)
        self.assertIn("`chain_state=reversed`", readme_text)
        self.assertIn("prism.dashboard.blocks.v2", readme_text)
        self.assertIn("blocks_reversed_total", readme_text)
        self.assertIn("blocks_inactive_total", readme_text)
        self.assertIn("network.hashrate_ths", readme_text)

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

    def test_miner_contract_exposes_pending_maturity_total(self) -> None:
        miner = self.load_fixture("miner.json")
        earnings = self.load_fixture("miner-earnings.json")["rows"]
        expected_pending = sum(
            row["net_earning_bits"]
            for row in earnings
            if row["maturity_state"] == "immature"
        )

        self.assertEqual(miner["pending_maturity_bits"], expected_pending)
        text = OPENAPI_PATH.read_text(encoding="utf-8")
        self.assertIn("- pending_maturity_bits", text)
        self.assertIn('pending_maturity_bits:\n          $ref: "#/components/schemas/Bits"', text)

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
        self.assertIn("PRISM_PUBLIC_AGGREGATE_CACHE_TTL_SECONDS", readme_text)
        self.assertIn("PRISM_PUBLIC_AGGREGATE_CACHE_STALE_WHILE_REVALIDATE_SECONDS", readme_text)
        self.assertIn("PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS", readme_text)
        self.assertIn("PRISM_PUBLIC_CACHE_MAX_ENTRIES", readme_text)
        self.assertIn("PRISM_PUBLIC_STRATUM_HIGHDIFF_URL", readme_text)

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

    def epoch(self, timestamp: str) -> int:
        self.assertTrue(timestamp.endswith("Z"), timestamp)
        return int(datetime.fromisoformat(timestamp.removesuffix("Z") + "+00:00").timestamp())

    def assert_iso_timestamp(self, value: object, source: str, path: tuple[str, ...]) -> None:
        self.assertIsInstance(value, str, ".".join(path))
        text = str(value)
        self.assertTrue(text.endswith("Z"), f"{source}:{'.'.join(path)} must be UTC/Z")
        datetime.fromisoformat(text.removesuffix("Z") + "+00:00")


if __name__ == "__main__":
    unittest.main()
