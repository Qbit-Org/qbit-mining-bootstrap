#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).parents[1] / "docker" / "ckpool" / "ckpool-version-mask.py"
SPEC = importlib.util.spec_from_file_location("ckpool_version_mask", SCRIPT_PATH)
assert SPEC is not None
ckpool_version_mask = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = ckpool_version_mask
SPEC.loader.exec_module(ckpool_version_mask)


class CkpoolVersionMaskTests(unittest.TestCase):
    def test_selects_advertised_qbit_versionrollingmask(self) -> None:
        result = ckpool_version_mask.select_version_mask(
            {"versionrollingmask": "1fffe000"},
            "1fffe000",
        )

        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.source, "qbit_getblocktemplate")
        self.assertEqual(result.detail, "advertised")

    def test_missing_advertised_mask_falls_back_to_configured_mask(self) -> None:
        result = ckpool_version_mask.select_version_mask({}, "1fffe000")

        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.source, "fallback")
        self.assertEqual(result.detail, "missing_versionrollingmask")

    def test_invalid_advertised_mask_falls_back_to_configured_mask(self) -> None:
        result = ckpool_version_mask.select_version_mask(
            {"versionrollingmask": "not-hex"},
            "1fffe000",
        )

        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.source, "fallback")
        self.assertTrue(result.detail.startswith("invalid_versionrollingmask:"))

    def test_normalizes_integer_and_short_hex_masks(self) -> None:
        self.assertEqual(ckpool_version_mask.normalize_mask(0x1FFFE000, field="mask"), "1fffe000")
        self.assertEqual(ckpool_version_mask.normalize_mask("ff", field="mask"), "000000ff")
        self.assertEqual(ckpool_version_mask.normalize_mask("0x1fffe000", field="mask"), "1fffe000")

    def test_rejects_invalid_fallback_mask(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid fallback CKPOOL_VERSION_MASK"):
            ckpool_version_mask.select_version_mask({}, "not-hex")

    def test_mode_parsing(self) -> None:
        self.assertTrue(ckpool_version_mask.mode_is_dynamic("dynamic"))
        self.assertTrue(ckpool_version_mask.mode_is_dynamic("auto"))
        self.assertFalse(ckpool_version_mask.mode_is_dynamic("static"))
        self.assertFalse(ckpool_version_mask.mode_is_dynamic("off"))
        with self.assertRaisesRegex(ValueError, "CKPOOL_VERSION_MASK_MODE"):
            ckpool_version_mask.mode_is_dynamic("sometimes")

    def test_signet_gbt_rules_include_signet(self) -> None:
        self.assertEqual(ckpool_version_mask.gbt_rules("signet"), ["segwit", "signet"])
        self.assertEqual(ckpool_version_mask.gbt_rules("testnet4"), ["segwit"])


if __name__ == "__main__":
    unittest.main()
