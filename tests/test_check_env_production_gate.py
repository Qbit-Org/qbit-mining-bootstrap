#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
CHECK_ENV = ROOT_DIR / "scripts" / "check-env.sh"


class CheckEnvProductionGateTests(unittest.TestCase):
    def run_check_env(self, **overrides: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(overrides)
        return subprocess.run(
            ["bash", str(CHECK_ENV)],
            cwd=ROOT_DIR,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )

    def test_production_mode_rejects_regtest_before_docker_check(self) -> None:
        result = self.run_check_env(QBIT_PRODUCTION="1")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_PRODUCTION=1 rejects regtest QBIT_CHAIN", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_production_mainnet_prelaunch_requires_explicit_authorization(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            CKPOOL_NON_TEST_READINESS_GATE="0",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_nonproduction_skips_production_only_policy_checks(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="0",
            QBIT_TOOLS_PRODUCTION="0",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            CKPOOL_PUBLIC_DIFF_POLICY="permissive",
            PRISM_ALLOW_MEMORY_LEDGER="1",
            PRISM_ALLOW_TEST_SIGNING_SEEDS="1",
        )

        self.assertNotIn("mainnet prelaunch requires", result.stderr)
        self.assertNotIn("rejects CKPOOL_PUBLIC_DIFF_POLICY=permissive", result.stderr)
        self.assertNotIn("rejects PRISM_ALLOW_MEMORY_LEDGER=1", result.stderr)
        self.assertNotIn("rejects PRISM_ALLOW_TEST_SIGNING_SEEDS=1", result.stderr)

    def test_production_mainnet_prelaunch_accepts_explicit_authorization(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("mainnet prelaunch requires", result.stderr)
        self.assertIn("requires a non-default PRISM_POSTGRES_PASSWORD", result.stderr)

    def test_production_mainnet_prelaunch_accepts_whitespace_around_launch_flag(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=" 0\t",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("true/false style value", result.stderr)
        self.assertNotIn("mainnet prelaunch requires", result.stderr)
        self.assertIn("requires a non-default PRISM_POSTGRES_PASSWORD", result.stderr)

    def test_production_mainnet_launch_rejects_disabled_readiness(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0", result.stderr)

    def test_production_gate_rejects_malformed_boolean_flags(self) -> None:
        cases = {
            "CKPOOL_NON_TEST_READINESS_GATE": "sometimes",
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "prelaunch",
            "QBIT_TOOLS_PRODUCTION": "maybe",
        }
        for name, value in cases.items():
            with self.subTest(name=name):
                result = self.run_check_env(
                    QBIT_PRODUCTION="1",
                    QBIT_CHAIN="mainnet",
                    **{name: value},
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"{name} must be a true/false style value", result.stderr)

    def test_mainnet_prelaunch_accepts_valid_reviewed_tip_age(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_NODE_EXTRA_ARG="-listen=1",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
            QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS="456789",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS", result.stderr)
        self.assertIn("requires a non-default PRISM_POSTGRES_PASSWORD", result.stderr)

    def test_mainnet_prelaunch_rejects_test_chain_in_qbitd_argv(self) -> None:
        cases = (
            {"QBIT_CHAIN_FLAG": "-regtest", "QBIT_NODE_EXTRA_ARG": "-listen=1"},
            {"QBIT_CHAIN_FLAG": "-chain=main", "QBIT_NODE_EXTRA_ARG": "-signet"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                result = self.run_check_env(
                    QBIT_PRODUCTION="1",
                    QBIT_TOOLS_PRODUCTION="1",
                    QBIT_CHAIN="mainnet",
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                    QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS="456789",
                    **overrides,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("selects a test chain", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_mainnet_prelaunch_rejects_invalid_tip_age_before_docker_check(self) -> None:
        for value in ("", "0", "-1", "1;echo injected", "9223372036854775808"):
            with self.subTest(value=value):
                result = self.run_check_env(
                    QBIT_PRODUCTION="1",
                    QBIT_TOOLS_PRODUCTION="1",
                    QBIT_CHAIN="mainnet",
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                    QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS=value,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_tip_age_rejects_incompatible_chain_or_production_mode(self) -> None:
        cases = (
            {"QBIT_PRODUCTION": "0", "QBIT_CHAIN": "mainnet"},
            {"QBIT_PRODUCTION": "1", "QBIT_CHAIN": "signet", "QBIT_CHAIN_FLAG": "-signet"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                result = self.run_check_env(
                    QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS="456789",
                    **overrides,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_non_mainnet_production_rejects_mainnet_launch_flag(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="testnet4",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("valid only for QBIT_CHAIN=mainnet", result.stderr)

    def test_production_mode_rejects_prism_test_bypass_before_docker_check(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            QBIT_RPC_PASSWORD="not-default",
            BITCOIN_RPC_PASSWORD="not-default",
            PRISM_DATABASE_URL="postgresql://example.invalid/qbit",
            PRISM_POSTGRES_PASSWORD="not-default",
            PRISM_MANIFEST_SIGNING_SEED_HEX="42" * 32,
            PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="43" * 32,
            PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX="44" * 32,
            PRISM_LEDGER_WRITER_ID="managed-writer",
            PRISM_LEDGER_WRITER_EPOCH="7",
            PRISM_AUDIT_DIR="/var/lib/qbit/prism/audit",
            PRISM_EVIDENCE_PATH="/var/lib/qbit/prism/evidence.json",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_REQUIRE_P2MR_PAYOUT="1",
            AUXPOW_STRATUM_HEADER_VARIANT="canonical",
            PRISM_ALLOW_MEMORY_LEDGER="1",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_PRODUCTION=1 rejects PRISM_ALLOW_MEMORY_LEDGER=1", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_production_mode_rejects_default_prism_postgres_password(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            QBIT_RPC_PASSWORD="not-default",
            BITCOIN_RPC_PASSWORD="not-default",
            PRISM_DATABASE_URL="postgresql://example.invalid/qbit",
            PRISM_POSTGRES_PASSWORD="change-this",
            PRISM_MANIFEST_SIGNING_SEED_HEX="42" * 32,
            PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="43" * 32,
            PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX="44" * 32,
            PRISM_LEDGER_WRITER_ID="managed-writer",
            PRISM_LEDGER_WRITER_EPOCH="7",
            PRISM_AUDIT_DIR="/var/lib/qbit/prism/audit",
            PRISM_EVIDENCE_PATH="/var/lib/qbit/prism/evidence.json",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_REQUIRE_P2MR_PAYOUT="1",
            AUXPOW_STRATUM_HEADER_VARIANT="canonical",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_PRODUCTION=1 requires a non-default PRISM_POSTGRES_PASSWORD", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_production_mode_rejects_default_prism_database_url_password(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            QBIT_RPC_PASSWORD="not-default",
            BITCOIN_RPC_PASSWORD="not-default",
            PRISM_DATABASE_URL="postgresql://qbit:change-this@prism-postgres:5432/qbit",
            PRISM_POSTGRES_PASSWORD="not-default",
            PRISM_MANIFEST_SIGNING_SEED_HEX="42" * 32,
            PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX="43" * 32,
            PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX="44" * 32,
            PRISM_LEDGER_WRITER_ID="managed-writer",
            PRISM_LEDGER_WRITER_EPOCH="7",
            PRISM_AUDIT_DIR="/var/lib/qbit/prism/audit",
            PRISM_EVIDENCE_PATH="/var/lib/qbit/prism/evidence.json",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_REQUIRE_P2MR_PAYOUT="1",
            AUXPOW_STRATUM_HEADER_VARIANT="canonical",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_PRODUCTION=1 requires a non-default PRISM_DATABASE_URL", result.stderr)
        self.assertNotIn("docker is required", result.stderr)


if __name__ == "__main__":
    unittest.main()
