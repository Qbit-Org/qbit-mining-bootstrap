#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from lab.prism import public_api, public_read_service
from lab.prism.coordinator_config import CoordinatorConfig, load_coordinator_config
from lab.prism.prism_coordinator import JsonRpc, PrismCoordinator


ROOT = Path(__file__).resolve().parents[1]


class MiningComposeProfileTests(unittest.TestCase):
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
        cls.docker = docker

    def render_profile(
        self, profile: str, overrides: dict[str, str] | None = None
    ) -> dict[str, Any]:
        inherited_keys = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "XDG_CONFIG_HOME")
        env = {key: os.environ[key] for key in inherited_keys if key in os.environ}
        env["QBIT_SRC_DIR"] = str(ROOT)
        env.update(overrides or {})
        completed = subprocess.run(
            [
                self.docker,
                "compose",
                "--env-file",
                str(ROOT / "config" / "upstream.env.example"),
                "--env-file",
                str(ROOT / ".env.example"),
                "-f",
                str(ROOT / "compose.yaml"),
                "--project-name",
                "qbit-profile-contract",
                "--profile",
                profile,
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
            self.fail(
                f"docker compose --profile {profile} config failed\n"
                f"stdout:\n{completed.stdout}\n"
                f"stderr:\n{completed.stderr}"
            )
        return json.loads(completed.stdout)

    def test_replica_retry_controls_reach_the_container(self) -> None:
        attempts = "PRISM_POSTGRES_REPLICA_BASEBACKUP_ATTEMPTS"
        retry_seconds = "PRISM_POSTGRES_REPLICA_BASEBACKUP_RETRY_SECONDS"
        for overrides, expected in (
            ({}, {attempts: "60", retry_seconds: "5"}),
            ({attempts: "120", retry_seconds: "2"}, {attempts: "120", retry_seconds: "2"}),
        ):
            with self.subTest(overrides=overrides):
                config = self.render_profile("prism", overrides)
                env = config["services"]["prism-postgres-replica"]["environment"]
                for key, value in expected.items():
                    self.assertEqual(env.get(key), value, key)

    def test_public_psql_uses_replica_configuration(self) -> None:
        primary_command = "psql --host=primary.internal --dbname=qbit"
        replica_url = "postgresql://reader@replica.internal:5432/qbit"
        replica_command = "psql --host=replica.internal --dbname=qbit --username=reader"
        for public_command in ("", replica_command):
            with self.subTest(public_command=public_command):
                config = self.render_profile("prism", {
                    "PRISM_POSTGRES_NATIVE_CLIENT": "psql",
                    "PRISM_POSTGRES_PSQL_COMMAND": primary_command,
                    "PRISM_PUBLIC_DATABASE_URL": replica_url,
                    "PRISM_PUBLIC_PSQL_COMMAND": public_command,
                })
                services = config["services"]
                self.assertEqual(
                    services["prism-coordinator"]["environment"]["PRISM_POSTGRES_PSQL_COMMAND"],
                    primary_command,
                )
                env = services["prism-public-api"]["environment"]
                self.assertEqual(env["PRISM_DATABASE_URL"], replica_url)
                env = {key: value for key, value in env.items() if value is not None}
                with patch.dict(os.environ, env, clear=True), patch.object(
                    public_read_service, "build_audit_artifact_store", return_value=None
                ):
                    ledger = public_read_service.build_ledger_from_env(env)
                try:
                    expected = shlex.split(public_command) if public_command else ["psql", replica_url]
                    self.assertEqual(ledger._command, expected)
                    self.assertIsNone(ledger._native)
                    self.assertTrue(ledger._read_only)
                finally:
                    ledger.close()

    def test_public_minimum_payout_preserves_pool_fallbacks(self) -> None:
        for overrides, expected in (
            ({}, 0),
            ({"PRISM_PAYOUT_MIN_OUTPUT_SATS": "500"}, 500),
            ({"PRISM_PAYOUT_MIN_OUTPUT_BITS": "700", "PRISM_PAYOUT_MIN_OUTPUT_SATS": "500"}, 700),
            ({"PRISM_PUBLIC_MINIMUM_PAYOUT_BITS": "900", "PRISM_PAYOUT_MIN_OUTPUT_BITS": "700"}, 900),
            ({"PRISM_PUBLIC_MINIMUM_PAYOUT_BITS": "0", "PRISM_PAYOUT_MIN_OUTPUT_SATS": "500"}, 0),
        ):
            with self.subTest(overrides=overrides):
                services = self.render_profile("prism", overrides)["services"]
                env = services["prism-public-api"]["environment"]
                env = {key: value for key, value in env.items() if value is not None}
                with patch.dict(os.environ, env, clear=True):
                    self.assertEqual(public_api.public_minimum_payout_bits(), expected)

    def coordinator_config(self, overrides: dict[str, str]) -> CoordinatorConfig:
        services = self.render_profile("prism", overrides)["services"]
        env = services["prism-coordinator"]["environment"]
        env = {key: value for key, value in env.items() if value is not None}
        # Supply fixture signing material so configuration validation reaches
        # the runtime knobs without starting a coordinator or opening storage.
        env.update({
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "11" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "22" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "33" * 32,
        })
        return load_coordinator_config(env)

    def test_vardiff_resume_controls_reach_runtime_config(self) -> None:
        for overrides, expected in (
            ({}, (True, 900, 8192, 1024)),
            ({
                "PRISM_STRATUM_VARDIFF_RESUME": "0",
                "PRISM_STRATUM_VARDIFF_RESUME_TTL_SECONDS": "42.5",
                "PRISM_STRATUM_VARDIFF_RESUME_MAX_ENTRIES": "33",
                "PRISM_STRATUM_VARDIFF_RESUME_MAX_START_FACTOR": "8",
            }, (False, 42.5, 33, 8)),
        ):
            with self.subTest(overrides=overrides):
                config = self.coordinator_config(overrides).stratum
                self.assertEqual((
                    config.vardiff_resume_enabled,
                    config.vardiff_resume_ttl_seconds,
                    config.vardiff_resume_max_entries,
                    config.vardiff_resume_max_start_factor,
                ), expected)

    def test_rust_window_switch_reaches_runtime_config(self) -> None:
        for overrides, expected in (
            ({}, False),
            ({"PRISM_WINDOW_PIPELINE_RUST": "1"}, True),
            ({"PRISM_WINDOW_PIPELINE_RUST": "0"}, False),
        ):
            with self.subTest(overrides=overrides):
                config = self.coordinator_config(overrides)
                self.assertEqual(config.jobs.window_pipeline_rust_enabled, expected)
        with self.assertRaisesRegex(SystemExit, "PRISM_WINDOW_PIPELINE_RUST"):
            self.coordinator_config({"PRISM_WINDOW_PIPELINE_RUST": "invalid"})

    def test_hashrate_rollup_controls_reach_coordinator(self) -> None:
        for overrides, expected in (
            ({}, (True, 15.0, 50000)),
            ({
                "PRISM_HASHRATE_ROLLUP_ENABLED": "0",
                "PRISM_HASHRATE_ROLLUP_INTERVAL_SECONDS": "42.5",
                "PRISM_HASHRATE_ROLLUP_BATCH_SHARES": "1000",
            }, (False, 42.5, 1000)),
        ):
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as root:
                services = self.render_profile("prism", overrides)["services"]
                env = services["prism-coordinator"]["environment"]
                env = {key: value for key, value in env.items() if value is not None}
                # The constructor reads these controls from the process
                # environment. Keep its unrelated storage and RPC local.
                config = load_coordinator_config({
                    "QBIT_RPC_HOST": "qbit.example",
                    "QBIT_RPC_USER": "rpc-user",
                    "QBIT_RPC_PASSWORD": "rpc-password",
                    "PRISM_ALLOW_MEMORY_LEDGER": "1",
                    "PRISM_ALLOW_TEST_SIGNING_SEEDS": "1",
                    "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "1",
                    "PRISM_AUDIT_DIR": root,
                    "PRISM_EVIDENCE_PATH": str(Path(root) / "evidence.json"),
                })
                with patch.dict(os.environ, env, clear=True), patch.object(
                    JsonRpc, "call", side_effect=RuntimeError("offline")
                ):
                    coordinator = PrismCoordinator(config)
                self.assertEqual((
                    coordinator.hashrate_rollup_enabled,
                    coordinator.hashrate_rollup_interval_seconds,
                    coordinator.hashrate_rollup_batch_shares,
                ), expected)

    def test_public_read_deadline_reaches_origin_dispatch(self) -> None:
        for overrides, expected in (
            ({}, 20),
            ({"PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS": "7"}, 7),
            ({"PRISM_PUBLIC_READ_STATEMENT_TIMEOUT_SECONDS": "0"}, 0),
        ):
            with self.subTest(overrides=overrides):
                services = self.render_profile("prism", overrides)["services"]
                env = services["prism-public-api"]["environment"]
                env = {key: value for key, value in env.items() if value is not None}
                coordinator = Mock()
                timeout = coordinator.ledger.operation_timeout
                timeout.return_value = nullcontext()
                with patch.dict(os.environ, env, clear=True), patch.object(
                    public_api, "dispatch", return_value=(200, {"ok": True})
                ) as dispatch:
                    result = public_read_service.bounded_public_dispatch(
                        coordinator, "/public/v1/pool", {}, occupies_read_slot=True
                    )
                self.assertEqual(result, (200, {"ok": True}))
                dispatch.assert_called_once_with(coordinator, "/public/v1/pool", {})
                if expected:
                    timeout.assert_called_once_with(float(expected))
                else:
                    timeout.assert_not_called()

    def test_candidate_cleanup_backlog_limit_reaches_runtime_config(self) -> None:
        key = "PRISM_BLOCK_CANDIDATE_CLEANUP_RETRY_BACKLOG_MAX"
        for overrides, expected in (({}, 4096), ({key: "128"}, 128), ({key: "8192"}, 8192)):
            with self.subTest(overrides=overrides):
                config = self.coordinator_config(overrides)
                self.assertEqual(config.block.candidate_cleanup_retry_backlog_max, expected)
        for invalid in ("0", "8193"):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(SystemExit, key):
                self.coordinator_config({key: invalid})

    def test_each_mining_profile_has_an_exact_service_graph(self) -> None:
        expected = {
            "permissionless": {"qbitd", "ckpool", "permissionless-miner"},
            "real-miner-smoke": {"qbitd", "ckpool", "real-miner"},
            "auxpow": {
                "qbitd",
                "bitcoind",
                "auxpow-bridge",
                "auxpow-coordinator",
                "auxpow-stratum",
                "auxpow-real-miner",
            },
            # prism-public-api serves the extracted /public/v1 read tier
            # in its own process (issue #145), against the standby that
            # prism-postgres-replica streams from the coordinator's primary.
            "prism": {
                "qbitd",
                "prism-postgres",
                "prism-postgres-replica",
                "prism-coordinator",
                "prism-public-api",
            },
        }

        for profile, services in expected.items():
            with self.subTest(profile=profile):
                config = self.render_profile(profile)
                self.assertEqual(set(config["services"]), services)

    def test_real_miner_smoke_uses_the_ordinary_pool(self) -> None:
        config = self.render_profile("real-miner-smoke")
        services = config["services"]
        miner = services["real-miner"]

        self.assertEqual(services["qbitd"]["restart"], "unless-stopped")
        self.assertEqual(services["ckpool"]["restart"], "unless-stopped")
        self.assertEqual(miner["environment"]["STRATUM_HOST"], "ckpool")
        self.assertEqual(miner["environment"]["STRATUM_PORT"], "3333")
        self.assertIn("ckpool", miner["depends_on"])

        ports = services["ckpool"]["ports"]
        self.assertEqual(
            {(str(port["published"]), str(port["target"])) for port in ports},
            {("3333", "3333")},
        )

        pool_mount = next(
            volume
            for volume in services["ckpool"].get("volumes", [])
            if volume["target"] == "/run/qbit-real-miner-smoke"
        )
        miner_mount = next(
            volume
            for volume in miner.get("volumes", [])
            if volume["target"] == "/run/qbit-real-miner-smoke"
        )
        self.assertEqual(pool_mount["source"], miner_mount["source"])
        self.assertFalse(
            any(
                volume["target"] == "/var/lib/ckpool"
                for volume in services["ckpool"].get("volumes", [])
            )
        )
        self.assertTrue(miner_mount["read_only"])
        self.assertEqual(
            services["ckpool"]["environment"]["QBIT_MINER_ADDRESS_FILE"],
            "/run/qbit-real-miner-smoke/miner-address.txt",
        )
        self.assertEqual(
            miner["environment"]["MINER_USERNAME_FILE"],
            "/run/qbit-real-miner-smoke/miner-address.txt",
        )


if __name__ == "__main__":
    unittest.main()
