#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


class PrismComposeProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        docker = shutil.which("docker")
        if docker is None:
            raise unittest.SkipTest("docker CLI is not installed")

        version = subprocess.run(
            [docker, "compose", "version"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if version.returncode != 0:
            raise unittest.SkipTest(f"docker compose is unavailable: {version.stderr.strip()}")

        env = os.environ.copy()
        env.update(
            {
                "QBIT_SRC_DIR": str(ROOT),
                "PRISM_STRATUM_PORT": "43340",
                "PRISM_STRATUM_PORT_HOST": "127.0.0.1:43340",
                "PRISM_PUBLIC_STRATUM_URL": "stratum+tcp://public-pool.example:3335",
                "PRISM_PUBLIC_POOL_FEE_BPS": "200",
                "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS": "20971520",
                "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS": "15",
                "PRISM_MAX_DIRECT_COINBASE_OUTPUTS": "7",
                "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION": "999",
                "PRISM_RESERVED_COINBASE_OUTPUTS": "1",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "25",
                "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS": "13000",
                "PRISM_CTV_BROADCASTER_ENABLED": "1",
                "PRISM_CTV_BROADCASTER_WALLET": "fanout-broadcaster",
                "PRISM_CTV_BROADCASTER_FEE_BITS": "0",
                "PRISM_CTV_BROADCASTER_LIMIT": "7",
                "PRISM_CTV_BROADCASTER_INTERVAL_SECONDS": "11",
            }
        )
        completed = subprocess.run(
            [
                docker,
                "compose",
                "--env-file",
                str(ROOT / "config/upstream.env.example"),
                "--env-file",
                str(ROOT / ".env.example"),
                "-f",
                str(ROOT / "compose.yaml"),
                "--project-name",
                "qbit-prism-compose-test",
                "--profile",
                "prism",
                "config",
                "--format",
                "json",
            ],
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise AssertionError(
                "docker compose --profile prism config failed\n"
                f"stdout:\n{completed.stdout}\n"
                f"stderr:\n{completed.stderr}"
            )
        cls.config = json.loads(completed.stdout)

    def test_prism_profile_services_render(self) -> None:
        services = self.config["services"]

        self.assertIn("qbitd", services)
        self.assertIn("prism-postgres", services)
        self.assertIn("prism-coordinator", services)

    def test_prism_coordinator_gets_required_environment(self) -> None:
        env = self._service_environment("prism-coordinator")

        self.assertEqual(env["QBIT_RPC_HOST"], "qbitd")
        self.assertEqual(env["QBIT_PRODUCTION"], "0")
        self.assertEqual(env["QBIT_TOOLS_PRODUCTION"], "0")
        self.assertEqual(env["PRISM_DATABASE_URL"], "postgresql://qbit:change-this@prism-postgres:5432/qbit")
        self.assertEqual(env["PRISM_POSTGRES_INIT_SCHEMA"], "1")
        self.assertEqual(env["PRISM_POSTGRES_READ_CONCURRENCY"], "4")
        self.assertEqual(env["PRISM_LEDGER_LEASE_TTL_SECONDS"], "60")
        self.assertEqual(env["PRISM_WATCHDOG_ENABLED"], "1")
        self.assertEqual(env["PRISM_WATCHDOG_TIMEOUT_SECONDS"], "120")
        self.assertEqual(env["PRISM_WATCHDOG_INTERVAL_SECONDS"], "15")
        self.assertEqual(env["PRISM_STRATUM_BIND"], "0.0.0.0")
        self.assertEqual(env["PRISM_STRATUM_PORT"], "43340")
        self.assertEqual(env["PRISM_PUBLIC_STRATUM_URL"], "stratum+tcp://public-pool.example:3335")
        self.assertEqual(env["PRISM_PUBLIC_POOL_FEE_BPS"], "200")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_ENABLED"], "1")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_TTL_SECONDS"], "5")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_STALE_WHILE_REVALIDATE_SECONDS"], "30")
        self.assertEqual(env["PRISM_PUBLIC_CONFIG_CACHE_TTL_SECONDS"], "300")
        self.assertEqual(env["PRISM_PUBLIC_CONFIG_CACHE_STALE_WHILE_REVALIDATE_SECONDS"], "3600")
        self.assertEqual(env["PRISM_PUBLIC_ARTIFACT_CACHE_TTL_SECONDS"], "86400")
        self.assertEqual(env["PRISM_PUBLIC_ARTIFACT_CACHE_STALE_WHILE_REVALIDATE_SECONDS"], "86400")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_MAX_ENTRIES"], "512")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES"], "1048576")
        self.assertEqual(env["PRISM_PUBLIC_CACHE_DEBUG_HEADERS"], "0")
        self.assertEqual(env["PRISM_AUDIT_BIND"], "127.0.0.1")
        self.assertEqual(env["PRISM_AUDIT_PORT"], "3341")
        self.assertEqual(env["PRISM_STOP_AFTER_BLOCK"], "0")
        self.assertEqual(env["PRISM_CTV_SETTLEMENT_ENABLED"], "1")
        self.assertEqual(env["PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS"], "20971520")
        self.assertEqual(env["PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS"], "15")
        self.assertEqual(env["PRISM_MAX_DIRECT_COINBASE_OUTPUTS"], "7")
        self.assertEqual(env["PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION"], "999")
        self.assertEqual(env["PRISM_RESERVED_COINBASE_OUTPUTS"], "1")
        self.assertEqual(env["PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT"], "25")
        self.assertEqual(env["PRISM_CTV_FANOUT_FEE_PREMIUM_BPS"], "13000")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_ENABLED"], "1")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_WALLET"], "fanout-broadcaster")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_FEE_BITS"], "0")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_LIMIT"], "7")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_INTERVAL_SECONDS"], "11")
        self.assertEqual(env["PRISM_USERNAME_FALLBACK_ADDRESS"], "")
        self.assertEqual(env["PRISM_ALLOW_MEMORY_LEDGER"], "0")
        self.assertEqual(env["PRISM_ALLOW_TEST_SIGNING_SEEDS"], "0")
        self.assertEqual(env["PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY"], "0")

    def test_prism_stratum_port_publish_is_configurable(self) -> None:
        ports = self.config["services"]["prism-coordinator"].get("ports", [])

        self.assertTrue(
            any(
                str(port.get("target")) == "43340"
                and str(port.get("published")) == "43340"
                and port.get("host_ip") == "127.0.0.1"
                for port in ports
                if isinstance(port, dict)
            ),
            f"did not find configurable PRISM Stratum port in {ports!r}",
        )

    def test_prism_audit_directory_is_persistent(self) -> None:
        volumes = self.config["services"]["prism-coordinator"].get("volumes", [])

        self.assertTrue(
            any(
                volume.get("type") == "volume"
                and volume.get("source") == "prism-audit-data"
                and volume.get("target") == "/var/lib/qbit-prism/audit"
                for volume in volumes
                if isinstance(volume, dict)
            ),
            f"did not find persistent PRISM audit volume in {volumes!r}",
        )

    def test_prism_services_restart_for_auto_recovery(self) -> None:
        # The coordinator restarts on crashes and watchdog exits, while clean
        # bounded-run exits (PRISM_STOP_AFTER_BLOCK / PRISM_MAX_BLOCKS) stay
        # stopped. Postgres should still come back after daemon/host restarts.
        self.assertEqual(self.config["services"]["prism-coordinator"].get("restart"), "on-failure")
        self.assertEqual(self.config["services"]["prism-postgres"].get("restart"), "unless-stopped")

    def _service_environment(self, name: str) -> dict[str, str]:
        raw_env = self.config["services"][name]["environment"]
        if isinstance(raw_env, dict):
            return {str(key): str(value) for key, value in raw_env.items()}
        if isinstance(raw_env, list):
            result = {}
            for item in raw_env:
                key, _, value = str(item).partition("=")
                result[key] = value
            return result
        raise TypeError(f"unexpected environment shape for {name}: {type(raw_env).__name__}")


if __name__ == "__main__":
    unittest.main()
