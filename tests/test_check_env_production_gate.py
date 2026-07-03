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
