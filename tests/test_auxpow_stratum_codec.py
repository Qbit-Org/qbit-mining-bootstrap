#!/usr/bin/env python3

from __future__ import annotations

import json
import unittest
from pathlib import Path

from lab.auxpow import stratum_codec


DISPLAY_PREVHASH = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
STRATUM_PREVHASH = "1c1d1e1f18191a1b14151617101112130c0d0e0f08090a0b0405060700010203"
LEGACY_PREVHASH = "03020100070605040b0a09080f0e0d0c13121110171615141b1a19181f1e1d1c"
MERKLE_ROOT = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
CANONICAL_HEADER = (
    "00000020"
    "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"
    "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
    "00105e5f"
    "ffff001d"
    "2a000000"
)


class AuxPowStratumCodecTests(unittest.TestCase):
    def test_prevhash_for_stratum_roundtrips_to_serialized_uint256(self) -> None:
        self.assertEqual(stratum_codec.stratum_prevhash_from_display_hash(DISPLAY_PREVHASH), STRATUM_PREVHASH)

        header = stratum_codec.serialize_header_from_stratum_fields(
            version_hex="20000000",
            prevhash_hex=STRATUM_PREVHASH,
            merkle_root_serialized=bytes.fromhex(MERKLE_ROOT),
            ntime_hex="5f5e1000",
            nbits_hex="1d00ffff",
            nonce_hex="0000002a",
        )

        self.assertEqual(header.hex(), CANONICAL_HEADER)
        self.assertEqual(
            stratum_codec.header_hash_hex(header),
            "82c4f1195720ff49a60cdd9c46368ea18d5f9d6bb774927f8b34e86ec13ecd4b",
        )

    def test_apply_version_bits_uses_negotiated_mask_only(self) -> None:
        self.assertEqual(
            stratum_codec.apply_version_bits("20000000", "000000ff", 0x000000FF),
            "200000ff",
        )
        self.assertEqual(
            stratum_codec.apply_version_bits("20000000", "00000020", 0x000000FF),
            "20000020",
        )
        with self.assertRaisesRegex(ValueError, "outside the negotiated mask"):
            stratum_codec.apply_version_bits("20000000", "00000100", 0x000000FF)

    def test_legacy_prevhash_explains_display_revfields_variant(self) -> None:
        legacy_header = stratum_codec.serialize_header_from_stratum_fields(
            version_hex="20000000",
            prevhash_hex=LEGACY_PREVHASH,
            merkle_root_serialized=bytes.fromhex(MERKLE_ROOT),
            ntime_hex="5f5e1000",
            nbits_hex="1d00ffff",
            nonce_hex="0000002a",
        )
        variants = stratum_codec.diagnostic_header_variants(
            version_hex="20000000",
            prevhash_stratum_hex=LEGACY_PREVHASH,
            previousblockhash_display_hex=DISPLAY_PREVHASH,
            merkle_root_serialized=bytes.fromhex(MERKLE_ROOT),
            ntime_hex="5f5e1000",
            nbits_hex="1d00ffff",
            nonce_hex="0000002a",
        )

        matching_variants = [variant.name for variant in variants if variant.header == legacy_header]

        self.assertEqual(matching_variants, ["display-revfields"])

    def test_replay_fixture_records_legacy_variant(self) -> None:
        fixture_path = (
            Path(__file__).parent
            / "fixtures"
            / "auxpow_stratum_replay"
            / "legacy-prevhash-display-revfields.json"
        )
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        notify = fixture["notify"]
        submit = fixture["submit"]
        expected = fixture["expected"]
        coinbase, legacy_header = stratum_codec.assemble_header_from_notify_submit(
            coinb1_hex=notify["coinb1"],
            extranonce1_hex=fixture["subscribe"]["extranonce1"],
            extranonce2_hex=submit["extranonce2"],
            coinb2_hex=notify["coinb2"],
            merkle_branch_hex=notify["merkle_branch"],
            version_hex=notify["version"],
            prevhash_hex=notify["prevhash"],
            ntime_hex=submit["ntime"],
            nbits_hex=notify["nbits"],
            nonce_hex=submit["nonce"],
        )
        merkle_root = stratum_codec.compute_merkle_root_from_branch_hex(coinbase, notify["merkle_branch"])
        variants = stratum_codec.diagnostic_header_variants(
            version_hex=notify["version"],
            prevhash_stratum_hex=notify["prevhash"],
            previousblockhash_display_hex=notify["previousblockhash_display"],
            merkle_root_serialized=merkle_root,
            ntime_hex=submit["ntime"],
            nbits_hex=notify["nbits"],
            nonce_hex=submit["nonce"],
        )

        matching_variants = [variant.name for variant in variants if variant.header == legacy_header]

        self.assertEqual(matching_variants, [expected["diagnostic_variant"]])


if __name__ == "__main__":
    unittest.main()
