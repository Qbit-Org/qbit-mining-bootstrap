#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import patch


def load_self_check_module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "prism-self-check.py"
    spec = importlib.util.spec_from_file_location("prism_self_check", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class PrismSelfCheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.self_check = load_self_check_module()

    def valid_env(self) -> dict[str, str]:
        return {
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "11" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "22" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "33" * 32,
            "PRISM_LEDGER_WRITER_ID": "prism-coordinator",
            "PRISM_LEDGER_WRITER_EPOCH": "1",
            "PRISM_DATABASE_URL": "postgresql://qbit:secret@prism-postgres:5432/qbit",
            "PRISM_AUDIT_DIR": "/var/lib/qbit-prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit-prism/audit/prism-live-evidence.json",
            "PRISM_ALLOW_MEMORY_LEDGER": "0",
            "PRISM_ALLOW_TEST_SIGNING_SEEDS": "0",
            "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "0",
            "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN": "0",
            "PRISM_MIN_READY_MINERS": "3",
            "PRISM_STRATUM_SHARE_DIFF": "0.000000001",
            "PRISM_STRATUM_VARDIFF": "1",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "0.000000001",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "0.000000001",
            "PRISM_POOL_FEE_ENABLED": "0",
        }

    def test_parse_host_port_accepts_port_only_and_host_port(self) -> None:
        self.assertEqual(
            self.self_check.parse_host_port("3340", default_host="127.0.0.1", default_port=3340),
            ("127.0.0.1", 3340),
        )
        self.assertEqual(
            self.self_check.parse_host_port("0.0.0.0:43340", default_host="127.0.0.1", default_port=3340),
            ("0.0.0.0", 43340),
        )

    def test_compose_commands_include_prism_profile(self) -> None:
        command = self.self_check.compose_base_command()

        self.assertIn("--profile", command)
        self.assertEqual(command[command.index("--profile") + 1], "prism")

    def test_env_value_prefers_compose_resolved_value_over_host_env(self) -> None:
        with patch.dict(os.environ, {"QBIT_CHAIN": "mainnet"}):
            self.assertEqual(
                self.self_check.env_value({"QBIT_CHAIN": "signet"}, "QBIT_CHAIN", "regtest"),
                "signet",
            )
            self.assertEqual(
                self.self_check.env_value({}, "QBIT_CHAIN", "regtest"),
                "regtest",
            )

    def test_static_checks_accept_valid_prism_operator_env(self) -> None:
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(self.valid_env(), reporter)

        self.assertFalse(reporter.failed)

    def test_static_checks_fail_testnet_chain_flag_mismatch(self) -> None:
        env = self.valid_env()
        env["QBIT_CHAIN"] = "testnet"
        env["QBIT_CHAIN_FLAG"] = "-regtest"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "qbit.chain_flag"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_fail_production_test_bypass(self) -> None:
        env = self.valid_env()
        env["QBIT_PRODUCTION"] = "1"
        env["PRISM_ALLOW_TEST_SIGNING_SEEDS"] = "1"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "env.PRISM_ALLOW_TEST_SIGNING_SEEDS"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_ready_miner_threshold_fails_when_below_minimum(self) -> None:
        env = self.valid_env()
        env["PRISM_MIN_READY_MINERS"] = "3"
        reporter = self.self_check.Reporter()

        self.self_check.check_ready_miner_threshold({"ready_miner_count": 2}, env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "coordinator.ready_miners"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_ready_miner_threshold_passes_when_minimum_met(self) -> None:
        env = self.valid_env()
        env["PRISM_MIN_READY_MINERS"] = "3"
        reporter = self.self_check.Reporter()

        self.self_check.check_ready_miner_threshold({"ready_miner_count": 3}, env, reporter)

        self.assertFalse(reporter.failed)
        self.assertIn(
            ("PASS", "coordinator.ready_miners"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_public_chain_peer_rpc_error_is_hard_failure(self) -> None:
        reporter = self.self_check.Reporter()

        def fake_qbit_rpc_call(env: dict[str, str], method: str) -> object:
            if method == "getblockchaininfo":
                return {"chain": "signet", "initialblockdownload": False}
            if method == "getnetworkinfo":
                raise RuntimeError("rpc unavailable")
            raise AssertionError(method)

        with patch.object(self.self_check, "qbit_rpc_call", fake_qbit_rpc_call):
            self.self_check.qbit_live_checks({"QBIT_CHAIN": "signet"}, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "qbit.peers"),
            {(row.status, row.name) for row in reporter.rows},
        )


if __name__ == "__main__":
    unittest.main()
