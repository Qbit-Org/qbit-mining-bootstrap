#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("fake_qbit_rpc.py")
SPEC = importlib.util.spec_from_file_location("fake_qbit_rpc", SCRIPT_PATH)
assert SPEC is not None
fake_qbit_rpc = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fake_qbit_rpc)


class FakeQbitStateTests(unittest.TestCase):
    def test_validateaddress_includes_ckpool_required_fields(self) -> None:
        state = fake_qbit_rpc.FakeQbitState(
            bits=fake_qbit_rpc.DEFAULT_BITS,
            target=fake_qbit_rpc.DEFAULT_TARGET,
            log_requests=0,
        )

        result, error = state.result_for("validateaddress", ["fake-address"])

        self.assertIsNone(error)
        self.assertEqual(result["address"], "fake-address")
        self.assertIs(result["isvalid"], True)
        self.assertIs(result["isscript"], False)
        self.assertIs(result["iswitness"], False)

    def test_getblocktemplate_advertises_configured_target(self) -> None:
        state = fake_qbit_rpc.FakeQbitState(
            bits=fake_qbit_rpc.DEFAULT_BITS,
            target=fake_qbit_rpc.DEFAULT_TARGET,
            log_requests=0,
        )

        result, error = state.result_for("getblocktemplate", [])

        self.assertIsNone(error)
        self.assertEqual(result["bits"], fake_qbit_rpc.DEFAULT_BITS)
        self.assertEqual(result["target"], fake_qbit_rpc.DEFAULT_TARGET)
        self.assertEqual(result["height"], 1)
        self.assertEqual(result["transactions"], [])
        self.assertEqual(result["versionrollingmask"], fake_qbit_rpc.DEFAULT_VERSION_ROLLING_MASK)
        self.assertIn("segwit", result["rules"])

    def test_submitblock_advances_height(self) -> None:
        state = fake_qbit_rpc.FakeQbitState(
            bits=fake_qbit_rpc.DEFAULT_BITS,
            target=fake_qbit_rpc.DEFAULT_TARGET,
            log_requests=0,
        )

        result, error = state.result_for("submitblock", ["00"])

        self.assertIsNone(result)
        self.assertIsNone(error)
        self.assertEqual(state.height, 2)
        self.assertEqual(state.result_for("getblockcount", [])[0], 1)


if __name__ == "__main__":
    unittest.main()
