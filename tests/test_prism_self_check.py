#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import os
import socket
import sys
import tempfile
import threading
import time
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


def authorize_mainnet_prelaunch(env: dict[str, str]) -> dict[str, str]:
    env.update(
        {
            "QBIT_CHAIN": "mainnet",
            "QBIT_CHAIN_FLAG": "-chain=main",
            "QBIT_EXPECTED_GENESIS_HASH": "11" * 32,
            "QBIT_PRODUCTION": "1",
            "QBIT_TOOLS_PRODUCTION": "1",
            "CKPOOL_NON_TEST_READINESS_GATE": "0",
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "0",
            "PRISM_POSTGRES_PASSWORD": "not-default",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
        }
    )
    return env


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
            "PRISM_BLOCKWAIT_ENABLED": "1",
            "PRISM_BLOCKWAIT_TIMEOUT_SECONDS": "5",
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
            "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS": "15",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "3",
            "PRISM_WORKER_METRICS_LIMIT": "100",
            "PRISM_POOL_FEE_ENABLED": "0",
        }

    @staticmethod
    def public_blockchain_info(chain: str) -> dict[str, object]:
        return {
            "chain": chain,
            "initialblockdownload": False,
            "blocks": 100,
            "headers": 100,
        }

    @staticmethod
    def fresh_template() -> dict[str, object]:
        return {"previousblockhash": "11" * 32, "curtime": int(time.time())}

    def mainnet_live_reporter(
        self,
        env: dict[str, str],
        *,
        actual_chain: str = "main",
        initial_block_download: bool = False,
        peers: int = 2,
    ) -> object:
        reporter = self.self_check.Reporter()

        def fake_qbit_rpc_call(
            rpc_env: dict[str, str],
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockchaininfo":
                info = self.public_blockchain_info(actual_chain)
                info["initialblockdownload"] = initial_block_download
                return info
            if method == "getblocktemplate":
                return self.fresh_template()
            if method == "getblockhash":
                return rpc_env["QBIT_EXPECTED_GENESIS_HASH"]
            if method == "getnetworkinfo":
                return {"connections": peers}
            raise AssertionError(method)

        with patch.object(self.self_check, "qbit_rpc_call", fake_qbit_rpc_call):
            self.self_check.qbit_live_checks(env, reporter)
        return reporter

    def test_parse_host_port_accepts_port_only_and_host_port(self) -> None:
        self.assertEqual(
            self.self_check.parse_host_port("3340", default_host="127.0.0.1", default_port=3340),
            ("127.0.0.1", 3340),
        )
        self.assertEqual(
            self.self_check.parse_host_port("0.0.0.0:43340", default_host="127.0.0.1", default_port=3340),
            ("0.0.0.0", 43340),
        )

    def test_parse_decimal_rejects_non_finite_values(self) -> None:
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "finite decimal number"):
                    self.self_check.parse_decimal(value)

    def test_compose_commands_include_prism_profile(self) -> None:
        command = self.self_check.compose_base_command()

        self.assertIn("--profile", command)
        self.assertEqual(command[command.index("--profile") + 1], "prism")

    def test_deployment_env_replaces_repository_env_in_compose_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "config").mkdir()
            upstream_env = root / "config" / "upstream.env"
            upstream_env.write_text("UPSTREAM=1\n", encoding="utf-8")
            local_env = root / ".env"
            local_env.write_text("LOCAL=1\n", encoding="utf-8")
            deploy_env = root / "mainnet.env"
            deploy_env.write_text("DEPLOYMENT=1\n", encoding="utf-8")

            with (
                patch.object(self.self_check, "ROOT_DIR", root),
                patch.dict(os.environ, {"DEPLOY_ENV_FILE": str(deploy_env)}, clear=False),
            ):
                deploy_args = self.self_check.env_file_args()
            with (
                patch.object(self.self_check, "ROOT_DIR", root),
                patch.dict(os.environ, {"DEPLOY_ENV_FILE": ""}, clear=False),
            ):
                local_args = self.self_check.env_file_args()

        self.assertEqual(deploy_args, ["--env-file", str(upstream_env), "--env-file", str(deploy_env)])
        self.assertNotIn(str(local_env), deploy_args)
        self.assertEqual(local_args, ["--env-file", str(upstream_env), "--env-file", str(local_env)])

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

    def test_missing_launch_readiness_flag_defaults_to_strict(self) -> None:
        self.assertTrue(self.self_check.launch_readiness_checks_enabled({"QBIT_CHAIN": "mainnet"}))

    def test_static_checks_fail_malformed_launch_readiness_flag(self) -> None:
        env = self.valid_env()
        env["QBIT_CHAIN"] = "mainnet"
        env["QBIT_CHAIN_FLAG"] = ""
        env["QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"] = "prelaunch"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertIn(
            ("FAIL", "env.QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_require_full_prelaunch_authorization(self) -> None:
        cases = (
            ("QBIT_PRODUCTION", "0"),
            ("QBIT_TOOLS_PRODUCTION", "0"),
            ("CKPOOL_NON_TEST_READINESS_GATE", "1"),
        )
        for name, value in cases:
            with self.subTest(name=name):
                env = authorize_mainnet_prelaunch(self.valid_env())
                env[name] = value
                reporter = self.self_check.Reporter()

                self.self_check.static_checks(env, reporter)

                self.assertIn(
                    ("FAIL", "launch.readiness"),
                    {(row.status, row.name) for row in reporter.rows},
                )
                launch_failure = next(
                    row for row in reporter.rows if row.name == "launch.readiness"
                )
                self.assertIn("requires QBIT_CHAIN=mainnet", launch_failure.detail)

    def test_whitespace_around_launch_flag_preserves_prelaunch_authorization(self) -> None:
        env = authorize_mainnet_prelaunch(self.valid_env())
        env["QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"] = " 0\t"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        launch_rows = [row for row in reporter.rows if row.name == "launch.readiness"]
        self.assertEqual([row.status for row in launch_rows], ["WARN"])

    def test_static_checks_validate_prelaunch_tip_age(self) -> None:
        env = authorize_mainnet_prelaunch(self.valid_env())
        env["QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"] = "000456789"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        rows = [
            row
            for row in reporter.rows
            if row.name == "env.QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"
        ]
        self.assertEqual([row.status for row in rows], ["PASS"])
        self.assertIn("456789 seconds", rows[0].detail)

    def test_static_checks_reject_invalid_prelaunch_tip_age(self) -> None:
        invalid_values = ("", "0", "-1", "1;echo injected", "9223372036854775808")
        for value in invalid_values:
            with self.subTest(value=value):
                env = authorize_mainnet_prelaunch(self.valid_env())
                env["QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"] = value
                reporter = self.self_check.Reporter()

                self.self_check.static_checks(env, reporter)

                self.assertIn(
                    ("FAIL", "env.QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"),
                    {(row.status, row.name) for row in reporter.rows},
                )

    def test_static_checks_reject_tip_age_for_incompatible_mode(self) -> None:
        cases = (
            {"QBIT_PRODUCTION": "0", "QBIT_CHAIN": "mainnet", "QBIT_CHAIN_FLAG": ""},
            {"QBIT_PRODUCTION": "1", "QBIT_CHAIN": "signet", "QBIT_CHAIN_FLAG": "-signet"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                env = self.valid_env()
                env.update(overrides)
                env["QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"] = "456789"
                reporter = self.self_check.Reporter()

                self.self_check.static_checks(env, reporter)

                self.assertIn(
                    ("FAIL", "env.QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS"),
                    {(row.status, row.name) for row in reporter.rows},
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

    def test_static_checks_fail_bitcoin_chain_flag_mismatch(self) -> None:
        env = self.valid_env()
        env.update({"BITCOIN_CHAIN": "mainnet", "BITCOIN_CHAIN_FLAG": "-server=1"})
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertIn(
            ("FAIL", "bitcoin.chain_flag"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_accept_canonical_bitcoin_chain_flags(self) -> None:
        cases = {
            "regtest": "-regtest",
            "testnet": "-testnet",
            "testnet3": "-testnet",
            "testnet4": "-testnet4",
            "signet": "-signet",
            "mainnet": "-chain=main",
        }
        for bitcoin_chain, chain_flag in cases.items():
            with self.subTest(bitcoin_chain=bitcoin_chain):
                env = self.valid_env()
                env.update({"BITCOIN_CHAIN": bitcoin_chain, "BITCOIN_CHAIN_FLAG": chain_flag})
                reporter = self.self_check.Reporter()

                self.self_check.static_checks(env, reporter)

                self.assertIn(
                    ("PASS", "bitcoin.chain_flag"),
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

    def test_mainnet_implies_production_safeguards(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_CHAIN": "mainnet",
                "QBIT_CHAIN_FLAG": "-chain=main",
                "QBIT_EXPECTED_GENESIS_HASH": "11" * 32,
                "QBIT_GIT_COMMIT": "41" * 20,
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
                "PRISM_ALLOW_TEST_SIGNING_SEEDS": "1",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "env.PRISM_ALLOW_TEST_SIGNING_SEEDS"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_require_mainnet_chain_selector_and_genesis(self) -> None:
        for chain_flag in ("", "-server=1", "-regtest", "-testnet4"):
            with self.subTest(chain_flag=chain_flag):
                env = self.valid_env()
                env.update({"QBIT_CHAIN": "mainnet", "QBIT_CHAIN_FLAG": chain_flag})
                reporter = self.self_check.Reporter()

                self.self_check.static_checks(env, reporter)

                failures = {(row.status, row.name) for row in reporter.rows}
                self.assertIn(("FAIL", "qbit.chain_flag"), failures)
                self.assertIn(("FAIL", "qbit.genesis_config"), failures)

    def test_static_checks_accept_mainnet_chain_selector_and_genesis(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_CHAIN": "mainnet",
                "QBIT_CHAIN_FLAG": "-chain=main",
                "QBIT_EXPECTED_GENESIS_HASH": "AB" * 32,
                "QBIT_GIT_COMMIT": "41" * 20,
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertNotIn(
            ("FAIL", "qbit.chain_flag"),
            {(row.status, row.name) for row in reporter.rows},
        )
        self.assertIn(
            ("PASS", "qbit.genesis_config"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_require_immutable_production_git_commit(self) -> None:
        for commit in ("", "main", "11" * 19, "zz" * 20):
            with self.subTest(commit=commit):
                env = self.valid_env()
                env.update(
                    {
                        "QBIT_PRODUCTION": "1",
                        "QBIT_GIT_COMMIT": commit,
                        "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
                    }
                )
                reporter = self.self_check.Reporter()

                self.self_check.static_checks(env, reporter)

                self.assertIn(
                    ("FAIL", "qbit.source_pin"),
                    {(row.status, row.name) for row in reporter.rows},
                )

    def test_static_checks_accept_immutable_production_git_commit(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_PRODUCTION": "1",
                "QBIT_GIT_COMMIT": "AB" * 20,
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertIn(
            ("PASS", "qbit.source_pin"),
            {(row.status, row.name) for row in reporter.rows},
        )
        self.assertNotIn(
            ("FAIL", "mining.stale_grace"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_do_not_include_capacity_evidence(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_PRODUCTION": "1",
                "QBIT_GIT_COMMIT": "41" * 20,
                "PRISM_POSTGRES_PASSWORD": "not-default",
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
                "PRISM_STRATUM_SHARE_DIFF": "1024",
                "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
                "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
                "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        rows = {(row.status, row.name) for row in reporter.rows}
        self.assertIn(("PASS", "mining.production_difficulty"), rows)
        self.assertFalse(any(name == "capacity.evidence" for _status, name in rows))

    def test_static_checks_mainnet_does_not_require_capacity_evidence(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_CHAIN": "mainnet",
                "QBIT_CHAIN_FLAG": "-chain=main",
                "QBIT_EXPECTED_GENESIS_HASH": "AB" * 32,
                "QBIT_GIT_COMMIT": "41" * 20,
                "QBIT_REQUIRE_RELEASE_PROVENANCE": "0",
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
                "PRISM_STRATUM_SHARE_DIFF": "1024",
                "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
                "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
                "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertFalse(any(row.name == "capacity.evidence" for row in reporter.rows))

    def test_static_checks_reject_lab_production_difficulty(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_PRODUCTION": "1",
                "QBIT_GIT_COMMIT": "41" * 20,
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertIn(
            ("FAIL", "mining.production_difficulty"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_require_zero_mainnet_stale_grace(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_CHAIN": "mainnet",
                "QBIT_CHAIN_FLAG": "-chain=main",
                "QBIT_EXPECTED_GENESIS_HASH": "AB" * 32,
                "QBIT_GIT_COMMIT": "41" * 20,
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "3",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertIn(
            ("FAIL", "mining.stale_grace"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_accept_bounded_stale_grace_off_mainnet(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_PRODUCTION": "1",
                "QBIT_GIT_COMMIT": "41" * 20,
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "2",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        rows = {(row.status, row.name) for row in reporter.rows}
        self.assertIn(("PASS", "mining.stale_grace"), rows)
        self.assertNotIn(("FAIL", "mining.stale_grace"), rows)

    def test_static_checks_require_explicit_production_auxpow_payouts(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_PRODUCTION": "1",
                "QBIT_GIT_COMMIT": "41" * 20,
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
                "BITCOIN_CHAIN": "mainnet",
                "BITCOIN_CHAIN_FLAG": "-chain=main",
                "QBIT_MINER_ADDRESS": "auto",
                "BITCOIN_MINER_ADDRESS": "auto",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        failures = {(row.status, row.name) for row in reporter.rows}
        self.assertIn(("FAIL", "auxpow.qbit_payout"), failures)
        self.assertIn(("FAIL", "auxpow.bitcoin_payout"), failures)

    def test_static_checks_accept_explicit_production_auxpow_payouts(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_PRODUCTION": "1",
                "QBIT_GIT_COMMIT": "41" * 20,
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
                "BITCOIN_CHAIN": "mainnet",
                "BITCOIN_CHAIN_FLAG": "-chain=main",
                "QBIT_MINER_ADDRESS": "qb1explicit",
                "BITCOIN_MINER_ADDRESS": "bc1explicit",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        statuses = {(row.status, row.name) for row in reporter.rows}
        self.assertIn(("PASS", "auxpow.qbit_payout"), statuses)
        self.assertIn(("PASS", "auxpow.bitcoin_payout"), statuses)

    def test_static_checks_accept_positive_explicit_ctv_fee_rate(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "1000",
                "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS": "12000",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        fee_rows = [row for row in reporter.rows if row.name == "ctv.fee_source"]
        self.assertEqual([row.status for row in fee_rows], ["PASS"])
        self.assertIn("explicit rate=1000", fee_rows[0].detail)

    def test_static_checks_reject_invalid_explicit_ctv_fee_rate(self) -> None:
        for fee_rate in ("0", "-1", "1.5", "invalid"):
            with self.subTest(fee_rate=fee_rate):
                env = self.valid_env()
                env.update(
                    {
                        "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                        "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": fee_rate,
                    }
                )
                reporter = self.self_check.Reporter()

                self.self_check.static_checks(env, reporter)

                self.assertIn(
                    ("FAIL", "ctv.fee_source"),
                    {(row.status, row.name) for row in reporter.rows},
                )

    def test_static_checks_warn_when_ctv_requires_live_estimator(self) -> None:
        env = self.valid_env()
        env["PRISM_CTV_SETTLEMENT_ENABLED"] = "1"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        fee_rows = [row for row in reporter.rows if row.name == "ctv.fee_source"]
        self.assertEqual([row.status for row in fee_rows], ["WARN"])
        self.assertIn("live preflight required", fee_rows[0].detail)
        self.assertIn("Fresh chains", fee_rows[0].hint or "")

    def test_highdiff_probe_target_uses_published_host_port_only(self) -> None:
        # The published host mapping is the only valid probe target: falling
        # back to the container listen port could pass while miners cannot
        # reach the listener.
        self.assertEqual(
            self.self_check.highdiff_probe_target({"PRISM_STRATUM_HIGHDIFF_PORT_HOST": "4334"}),
            ("127.0.0.1", 4334),
        )
        self.assertEqual(
            self.self_check.highdiff_probe_target(
                {"PRISM_STRATUM_HIGHDIFF_PORT_HOST": "0.0.0.0:14334"}
            ),
            ("0.0.0.0", 14334),
        )
        # Unset, empty, and the disabled-default ephemeral loopback mapping all
        # mean "not published".
        self.assertIsNone(self.self_check.highdiff_probe_target({}))
        self.assertIsNone(
            self.self_check.highdiff_probe_target({"PRISM_STRATUM_HIGHDIFF_PORT_HOST": ""})
        )
        self.assertIsNone(
            self.self_check.highdiff_probe_target(
                {"PRISM_STRATUM_HIGHDIFF_PORT_HOST": "127.0.0.1:0"}
            )
        )

    def test_static_checks_accept_highdiff_with_empty_share_diff(self) -> None:
        # Compose resolves the fixed difficulty default to an empty string; the
        # coordinator treats that as "track the start difficulty" and the
        # self-check must agree instead of failing mining.highdiff.
        env = self.valid_env()
        env["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
        env["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = ""
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertFalse(reporter.failed)
        self.assertIn(
            "PASS",
            {row.status for row in reporter.rows if row.name == "mining.highdiff"},
        )

    def test_static_checks_fail_highdiff_share_diff_below_floor(self) -> None:
        env = self.valid_env()
        env["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
        env["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = "1000"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "mining.highdiff"),
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

    def test_ready_miner_threshold_is_nonfatal_during_mainnet_prelaunch(self) -> None:
        env = authorize_mainnet_prelaunch(self.valid_env())
        env["PRISM_MIN_READY_MINERS"] = "1"
        reporter = self.self_check.Reporter()

        self.self_check.check_ready_miner_threshold({"ready_miner_count": 0}, env, reporter)

        self.assertFalse(reporter.failed)
        rows = [row for row in reporter.rows if row.name == "coordinator.ready_miners"]
        self.assertEqual([row.status for row in rows], ["WARN"])
        self.assertIn("tolerated only", rows[0].detail)

    def test_ready_miner_threshold_stays_fatal_without_full_prelaunch_authorization(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_CHAIN": "mainnet",
                "PRISM_MIN_READY_MINERS": "1",
                "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "0",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.check_ready_miner_threshold({"ready_miner_count": 0}, env, reporter)

        self.assertIn(
            ("FAIL", "coordinator.ready_miners"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_ready_miner_threshold_is_fatal_after_mainnet_launch(self) -> None:
        env = self.valid_env()
        env.update(
            {
                "QBIT_CHAIN": "mainnet",
                "PRISM_MIN_READY_MINERS": "1",
                "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "1",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.check_ready_miner_threshold({"ready_miner_count": 0}, env, reporter)

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

        def fake_qbit_rpc_call(
            env: dict[str, str],
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockchaininfo":
                return self.public_blockchain_info("signet")
            if method == "getblocktemplate":
                return self.fresh_template()
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

    def test_live_checks_normalize_mainnet_rpc_chain_and_verify_genesis(self) -> None:
        expected_genesis = "11" * 32
        calls: list[tuple[str, list[object] | None]] = []
        reporter = self.self_check.Reporter()

        def fake_qbit_rpc_call(
            env: dict[str, str],
            method: str,
            params: list[object] | None = None,
        ) -> object:
            calls.append((method, params))
            if method == "getblockchaininfo":
                return self.public_blockchain_info("main")
            if method == "getblocktemplate":
                return self.fresh_template()
            if method == "getblockhash":
                return expected_genesis
            if method == "getnetworkinfo":
                return {"connections": 2}
            raise AssertionError(method)

        env = {
            "QBIT_CHAIN": "mainnet",
            "QBIT_EXPECTED_GENESIS_HASH": expected_genesis.upper(),
        }
        with patch.object(self.self_check, "qbit_rpc_call", fake_qbit_rpc_call):
            self.self_check.qbit_live_checks(env, reporter)

        self.assertFalse(reporter.failed)
        self.assertIn(("getblockhash", [0]), calls)
        self.assertIn(
            ("PASS", "qbit.rpc_chain"),
            {(row.status, row.name) for row in reporter.rows},
        )
        self.assertIn(
            ("PASS", "qbit.genesis"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_live_checks_fail_genesis_mismatch(self) -> None:
        reporter = self.self_check.Reporter()

        def fake_qbit_rpc_call(
            env: dict[str, str],
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockchaininfo":
                return self.public_blockchain_info("main")
            if method == "getblocktemplate":
                return self.fresh_template()
            if method == "getblockhash":
                return "22" * 32
            if method == "getnetworkinfo":
                return {"connections": 2}
            raise AssertionError(method)

        env = {"QBIT_CHAIN": "mainnet", "QBIT_EXPECTED_GENESIS_HASH": "11" * 32}
        with patch.object(self.self_check, "qbit_rpc_call", fake_qbit_rpc_call):
            self.self_check.qbit_live_checks(env, reporter)

        self.assertIn(
            ("FAIL", "qbit.genesis"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_live_checks_accept_preflightable_ctv_fee_estimator(self) -> None:
        reporter = self.self_check.Reporter()

        def fake_qbit_rpc_call(
            env: dict[str, str],
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockchaininfo":
                return self.public_blockchain_info("signet")
            if method == "getblocktemplate":
                return self.fresh_template()
            if method == "estimatesmartfee":
                self.assertEqual(params, [2])
                return {"feerate": 0.00001000, "blocks": 2}
            if method == "getnetworkinfo":
                return {"connections": 2}
            raise AssertionError(method)

        env = {"QBIT_CHAIN": "signet", "PRISM_CTV_SETTLEMENT_ENABLED": "1"}
        with patch.object(self.self_check, "qbit_rpc_call", fake_qbit_rpc_call):
            self.self_check.qbit_live_checks(env, reporter)

        self.assertFalse(reporter.failed)
        self.assertIn(
            ("PASS", "ctv.fee_estimator"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_live_checks_fail_ctv_estimator_without_fee_history(self) -> None:
        reporter = self.self_check.Reporter()

        def fake_qbit_rpc_call(
            env: dict[str, str],
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockchaininfo":
                return self.public_blockchain_info("signet")
            if method == "getblocktemplate":
                return self.fresh_template()
            if method == "estimatesmartfee":
                return {"errors": ["Insufficient data or no feerate found"]}
            if method == "getnetworkinfo":
                return {"connections": 2}
            raise AssertionError(method)

        env = {"QBIT_CHAIN": "signet", "PRISM_CTV_SETTLEMENT_ENABLED": "1"}
        with patch.object(self.self_check, "qbit_rpc_call", fake_qbit_rpc_call):
            self.self_check.qbit_live_checks(env, reporter)

        fee_rows = [row for row in reporter.rows if row.name == "ctv.fee_estimator"]
        self.assertEqual([row.status for row in fee_rows], ["FAIL"])
        self.assertIn("Fresh chains", fee_rows[0].hint or "")

    def test_mainnet_rpc_chain_mismatch_remains_fatal(self) -> None:
        reporter = self.mainnet_live_reporter(
            authorize_mainnet_prelaunch({}),
            actual_chain="testnet4",
        )

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "qbit.rpc_chain"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_ibd_is_nonfatal_during_mainnet_prelaunch(self) -> None:
        reporter = self.mainnet_live_reporter(
            authorize_mainnet_prelaunch({}),
            initial_block_download=True,
        )

        self.assertFalse(reporter.failed)
        self.assertIn(
            ("WARN", "qbit.ibd"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_ibd_is_fatal_after_mainnet_launch(self) -> None:
        reporter = self.mainnet_live_reporter(
            {
                "QBIT_CHAIN": "mainnet",
                "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "1",
            },
            initial_block_download=True,
        )

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "qbit.ibd"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_missing_launch_flag_keeps_ibd_fatal(self) -> None:
        reporter = self.mainnet_live_reporter(
            {"QBIT_CHAIN": "mainnet"},
            initial_block_download=True,
        )

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "qbit.ibd"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_unrelated_peer_failure_remains_fatal_during_mainnet_prelaunch(self) -> None:
        reporter = self.mainnet_live_reporter(
            authorize_mainnet_prelaunch({}),
            initial_block_download=True,
            peers=0,
        )

        self.assertTrue(reporter.failed)
        statuses = {(row.status, row.name) for row in reporter.rows}
        self.assertIn(("WARN", "qbit.ibd"), statuses)
        self.assertIn(("FAIL", "qbit.peers"), statuses)


class HighdiffFloorProbeTests(unittest.TestCase):
    """The live high-diff check must judge the first advertised difficulty,
    exactly like a marketplace verification probe, not mere reachability."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.self_check = load_self_check_module()

    def fake_stratum_server(
        self,
        notifications: list[dict[str, object]],
        *,
        reject_authorize: bool = False,
    ) -> int:
        """One-shot stratum server: answers subscribe/authorize, then emits
        the scripted notifications and closes. Returns the listening port."""
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        server_sock.settimeout(5)
        port = server_sock.getsockname()[1]

        def serve() -> None:
            try:
                conn, _ = server_sock.accept()
            except OSError:
                return
            with conn:
                conn.settimeout(5)
                reader = conn.makefile("rb")
                for _ in range(2):  # subscribe + authorize requests
                    reader.readline()
                responses: list[dict[str, object]] = [
                    {"id": 1, "result": [[], "00000001", 8], "error": None}
                ]
                if reject_authorize:
                    responses.append({"id": 2, "result": None, "error": [20, "unauthorized", None]})
                else:
                    responses.append({"id": 2, "result": True, "error": None})
                    responses.extend(notifications)
                for message in responses:
                    conn.sendall((json.dumps(message) + "\n").encode())

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        self.addCleanup(server_sock.close)
        self.addCleanup(lambda: thread.join(timeout=5))
        return port

    def probe_env(self) -> dict[str, str]:
        return {"PRISM_STRATUM_HIGHDIFF_MIN_DIFF": "500000"}

    def floor_rows(self, reporter: object) -> list[object]:
        return [row for row in reporter.rows if row.name == "stratum.highdiff_floor"]

    def test_first_difficulty_at_floor_passes(self) -> None:
        port = self.fake_stratum_server(
            [{"id": None, "method": "mining.set_difficulty", "params": [500000.0]}]
        )
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(self.probe_env(), "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertEqual([row.status for row in rows], ["PASS"])

    def test_first_difficulty_below_floor_fails(self) -> None:
        # The regression this guards: a young chain dragging the first
        # advertised difficulty below the floor. A later compliant value must
        # not rescue the check -- marketplaces judge the first one.
        port = self.fake_stratum_server(
            [
                {"id": None, "method": "mining.set_difficulty", "params": [4.6565423739069247e-10]},
                {"id": None, "method": "mining.set_difficulty", "params": [500000.0]},
            ]
        )
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(self.probe_env(), "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertEqual([row.status for row in rows], ["FAIL"])
        self.assertIn("below", rows[0].detail)

    def test_rejected_authorize_fails_with_handshake_detail(self) -> None:
        port = self.fake_stratum_server([], reject_authorize=True)
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(self.probe_env(), "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertEqual([row.status for row in rows], ["FAIL"])
        self.assertIn("authorize rejected", rows[0].detail)

    def test_connection_closed_without_difficulty_fails(self) -> None:
        port = self.fake_stratum_server([{"id": None, "method": "mining.notify", "params": []}])
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(self.probe_env(), "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertEqual([row.status for row in rows], ["FAIL"])
        self.assertIn("mining.set_difficulty", rows[0].detail)

    def test_missing_difficulty_is_nonfatal_during_mainnet_prelaunch(self) -> None:
        port = self.fake_stratum_server([{"id": None, "method": "mining.notify", "params": []}])
        env = authorize_mainnet_prelaunch(self.probe_env())
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(env, "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertFalse(reporter.failed)
        self.assertEqual([row.status for row in rows], ["WARN"])
        self.assertIn("tolerated only", rows[0].detail)

    def test_missing_difficulty_is_fatal_after_mainnet_launch(self) -> None:
        port = self.fake_stratum_server([{"id": None, "method": "mining.notify", "params": []}])
        env = self.probe_env()
        env.update(
            {
                "QBIT_CHAIN": "mainnet",
                "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "1",
            }
        )
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(env, "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertTrue(reporter.failed)
        self.assertEqual([row.status for row in rows], ["FAIL"])


if __name__ == "__main__":
    unittest.main()
