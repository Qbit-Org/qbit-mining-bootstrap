#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "mainnet-compose.env"
OPERATOR_SERVICES = {
    "qbitd",
    "ckpool",
    "bitcoind",
    "auxpow-stratum",
    "prism-postgres",
    "prism-coordinator",
}


class MainnetComposeContractTests(unittest.TestCase):
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

        # Keep developer shell variables from silently overriding the fixture.
        inherited_keys = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "XDG_CONFIG_HOME")
        env = {key: os.environ[key] for key in inherited_keys if key in os.environ}
        completed = subprocess.run(
            [
                docker,
                "compose",
                "--env-file",
                str(ROOT / "config/upstream.env.example"),
                "--env-file",
                str(FIXTURE),
                "-f",
                str(ROOT / "compose.yaml"),
                "-f",
                str(ROOT / "compose.production.yaml"),
                "--project-name",
                "qbit-mainnet-compose-contract",
                "--profile",
                "permissionless",
                "--profile",
                "auxpow",
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
                "mainnet docker compose config failed\n"
                f"stdout:\n{completed.stdout}\n"
                f"stderr:\n{completed.stderr}"
            )
        cls.config = json.loads(completed.stdout)

    def test_profiles_render_expected_service_graph(self) -> None:
        services = set(self.config["services"])
        self.assertEqual(services, OPERATOR_SERVICES)

    def test_operator_service_subset_is_available(self) -> None:
        services = set(self.config["services"])

        self.assertLessEqual(OPERATOR_SERVICES, services)

    def test_node_chain_selectors_are_exact(self) -> None:
        qbit_command = self.config["services"]["qbitd"]["command"]
        bitcoin_command = self.config["services"]["bitcoind"]["command"]

        self.assertEqual(self._network_selectors(qbit_command), ["-chain=main"])
        self.assertEqual(self._network_selectors(bitcoin_command), ["-chain=main"])
        self.assertEqual(
            [argument for argument in bitcoin_command if argument.startswith("-dnsseed")],
            ["-dnsseed=1"],
        )
        self.assertEqual(
            [argument for argument in bitcoin_command if argument.startswith("-discover")],
            ["-discover=1"],
        )

    def test_expected_genesis_reaches_public_pool_runtimes(self) -> None:
        expected = "1" * 64

        for service in (
            "ckpool",
            "auxpow-stratum",
            "prism-coordinator",
        ):
            with self.subTest(service=service):
                self.assertEqual(self._environment(service)["QBIT_EXPECTED_GENESIS_HASH"], expected)

    def test_auxpow_runtimes_receive_parent_chain_identity(self) -> None:
        for service in ("auxpow-stratum",):
            with self.subTest(service=service):
                env = self._environment(service)
                self.assertEqual(env["BITCOIN_CHAIN"], "mainnet")
                self.assertEqual(
                    env["BITCOIN_EXPECTED_GENESIS_HASH"],
                    "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
                )
                self.assertEqual(env["AUXPOW_STRATUM_REFRESH_FAILURE_EXIT_SECONDS"], "120")

    def test_ctv_has_an_explicit_static_fee_rate(self) -> None:
        env = self._environment("prism-coordinator")

        self.assertEqual(env["PRISM_CTV_SETTLEMENT_ENABLED"], "1")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_ENABLED"], "1")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_WALLET"], "")
        self.assertEqual(env["PRISM_CTV_BROADCASTER_FEE_BITS"], "0")
        self.assertEqual(env["PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT"], "1000")
        self.assertEqual(env["PRISM_CTV_FANOUT_FEE_PREMIUM_BPS"], "12000")

    def test_prism_uses_qualified_non_lab_difficulty_profile(self) -> None:
        env = self._environment("prism-coordinator")

        self.assertEqual(env["PRISM_STRATUM_SHARE_DIFF"], "1024")
        self.assertEqual(env["PRISM_STRATUM_VARDIFF_MIN_DIFF"], "1024")
        self.assertEqual(env["PRISM_STRATUM_VARDIFF_START_DIFF"], "4096")
        self.assertEqual(env["PRISM_STRATUM_VARDIFF_MAX_DIFF"], "65536")
        self.assertEqual(env["PRISM_CAPACITY_FORECAST_PEAK_SHARES_PER_SECOND"], "100")
        self.assertEqual(env["PRISM_CAPACITY_ACK_P99_LIMIT_MILLISECONDS"], "50")
        self.assertEqual(env["PRISM_CAPACITY_EVIDENCE_MAX_AGE_SECONDS"], "86400")
        self.assertEqual(env["PRISM_CAPACITY_COORDINATOR_REVISION"], "a" * 40)
        self.assertEqual(env["PRISM_CAPACITY_COORDINATOR_IMAGE_DIGEST"], "sha256:" + "5" * 64)
        self.assertEqual(env["PRISM_CAPACITY_POSTGRES_SERVER_VERSION"], "16.4")
        self.assertEqual(env["PRISM_CAPACITY_DATABASE_PROFILE_SHA256"], "c" * 64)
        self.assertEqual(env["PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS"], "120")

    def test_prism_capacity_evidence_is_a_fixed_read_only_mount(self) -> None:
        env = self._environment("prism-coordinator")
        volumes = self.config["services"]["prism-coordinator"]["volumes"]
        evidence_mounts = [
            volume
            for volume in volumes
            if isinstance(volume, dict)
            and volume.get("target") == "/run/qbit-prism/capacity-evidence.json"
        ]

        self.assertEqual(env["PRISM_CAPACITY_EVIDENCE_FILE"], "/run/qbit-prism/capacity-evidence.json")
        self.assertEqual(len(evidence_mounts), 1)
        self.assertEqual(evidence_mounts[0]["type"], "bind")
        self.assertEqual(
            evidence_mounts[0]["source"],
            "/srv/qbit-mining-bootstrap/mainnet/config/prism-capacity-evidence.json",
        )
        self.assertTrue(evidence_mounts[0]["read_only"])
        self.assertFalse(
            evidence_mounts[0].get("bind", {}).get("create_host_path", False)
        )

    def test_operator_images_are_digest_qualified_release_artifacts(self) -> None:
        artifact_services = OPERATOR_SERVICES

        for service in sorted(artifact_services):
            with self.subTest(service=service):
                image = str(self.config["services"][service]["image"])
                name, separator, digest = image.rpartition("@sha256:")
                self.assertTrue(name and separator, image)
                self.assertEqual(len(digest), 64)
                self.assertTrue(all(character in "0123456789abcdef" for character in digest))
                self.assertEqual(self.config["services"][service]["pull_policy"], "always")

        prism_image_digest = self.config["services"]["prism-coordinator"]["image"].rpartition("@")[2]
        self.assertEqual(
            self._environment("prism-coordinator")["PRISM_CAPACITY_COORDINATOR_IMAGE_DIGEST"],
            prism_image_digest,
        )

    def test_operator_state_uses_explicit_host_bind_mounts(self) -> None:
        expected = {
            ("qbitd", "/var/lib/qbit"): "/srv/qbit-mining-bootstrap/mainnet/qbit",
            ("bitcoind", "/var/lib/bitcoin"): "/srv/qbit-mining-bootstrap/mainnet/bitcoin",
            ("prism-postgres", "/var/lib/postgresql/data"): (
                "/srv/qbit-mining-bootstrap/mainnet/postgres/data"
            ),
            ("prism-postgres", "/var/lib/postgresql/wal"): (
                "/srv/qbit-mining-bootstrap/mainnet/postgres/wal"
            ),
            ("prism-coordinator", "/var/lib/qbit-prism/audit"): (
                "/srv/qbit-mining-bootstrap/mainnet/prism/audit"
            ),
        }

        for (service, target), source in expected.items():
            with self.subTest(service=service, target=target):
                mounts = [
                    volume
                    for volume in self.config["services"][service].get("volumes", [])
                    if isinstance(volume, dict) and volume.get("target") == target
                ]
                self.assertEqual(len(mounts), 1)
                self.assertEqual(mounts[0]["type"], "bind")
                self.assertEqual(mounts[0]["source"], source)
                self.assertTrue(Path(source).is_absolute())
                self.assertFalse(
                    mounts[0].get("bind", {}).get("create_host_path", False)
                )

    def test_postgres_wal_has_a_separate_storage_boundary(self) -> None:
        postgres = self.config["services"]["prism-postgres"]
        mounts = {
            volume["target"]: volume["source"]
            for volume in postgres["volumes"]
            if isinstance(volume, dict)
        }

        self.assertNotEqual(
            mounts["/var/lib/postgresql/data"],
            mounts["/var/lib/postgresql/wal"],
        )
        self.assertEqual(postgres["environment"]["POSTGRES_INITDB_WALDIR"], "/var/lib/postgresql/wal")

    def test_ckpool_runtimes_receive_template_freshness_limit(self) -> None:
        environment = self._environment("ckpool")
        self.assertEqual(environment["CKPOOL_TEMPLATE_MAX_AGE_SECONDS"], "120")
        self.assertEqual(environment["CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS"], "30")
        self.assertEqual(environment["CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS"], "5")
        self.assertEqual(environment["CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS"], "120")

    def test_auxpow_runtime_receives_template_future_time_limit(self) -> None:
        self.assertEqual(
            self._environment("auxpow-stratum")["AUXPOW_TEMPLATE_MAX_FUTURE_SECONDS"],
            "7200",
        )

    def test_ckpool_smoke_address_handoff_is_not_mounted_in_production(self) -> None:
        self.assertEqual(self.config["services"]["ckpool"].get("volumes", []), [])

    def test_operator_services_have_restart_policies(self) -> None:
        expected = {
            "qbitd": "unless-stopped",
            "ckpool": "unless-stopped",
            "bitcoind": "unless-stopped",
            "auxpow-stratum": "unless-stopped",
            "prism-postgres": "unless-stopped",
            "prism-coordinator": "unless-stopped",
        }

        for service, policy in expected.items():
            with self.subTest(service=service):
                self.assertEqual(self.config["services"][service].get("restart"), policy)

    def test_production_profiles_exclude_integration_helpers(self) -> None:
        services = set(self.config["services"])

        self.assertTrue(
            {
                "permissionless-miner",
                "real-miner",
                "auxpow-coordinator",
                "auxpow-bridge",
                "auxpow-real-miner",
            }.isdisjoint(services)
        )

    def test_host_port_bindings_are_explicit(self) -> None:
        expected = {
            "qbitd": {
                ("127.0.0.1", "19552", "19552"),
                ("0.0.0.0", "19555", "19555"),
                ("127.0.0.1", "29532", "28332"),
                ("127.0.0.1", "29533", "28333"),
            },
            "ckpool": {("0.0.0.0", "3333", "3333")},
            "bitcoind": {
                ("127.0.0.1", "8332", "8332"),
                ("0.0.0.0", "8333", "8333"),
            },
            "auxpow-stratum": {("0.0.0.0", "3335", "3335")},
            "prism-coordinator": {
                ("0.0.0.0", "3340", "3340"),
                ("0.0.0.0", "4334", "4334"),
            },
        }

        for service, bindings in expected.items():
            with self.subTest(service=service):
                self.assertEqual(self._port_bindings(service), bindings)

    def test_production_secrets_do_not_use_repository_defaults(self) -> None:
        ckpool_env = self._environment("ckpool")
        bitcoin_command = self.config["services"]["bitcoind"]["command"]
        postgres_env = self._environment("prism-postgres")
        prism_env = self._environment("prism-coordinator")

        self.assertNotIn("change-this", json.dumps(self.config, sort_keys=True))
        self.assertNotEqual(ckpool_env["QBIT_RPC_PASSWORD"], "change-this")
        self.assertNotIn("-rpcpassword=change-this", bitcoin_command)
        self.assertNotEqual(postgres_env["POSTGRES_PASSWORD"], "change-this")
        self.assertNotIn("change-this", prism_env["PRISM_DATABASE_URL"])
        self.assertEqual(prism_env["PRISM_LEDGER_WRITER_SESSION_TOKEN"], "")
        self.assertEqual(len(prism_env["PRISM_MANIFEST_SIGNING_SEED_HEX"]), 64)
        self.assertEqual(len(prism_env["PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX"]), 64)
        self.assertNotEqual(
            prism_env["PRISM_MANIFEST_SIGNING_SEED_HEX"],
            prism_env["PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX"],
        )

    def test_bitcoin_build_uses_architecture_specific_checksums(self) -> None:
        args = self.config["services"]["bitcoind"]["build"]["args"]

        self.assertEqual(args["BITCOIN_RELEASE_SHA256_AMD64"], "a" * 64)
        self.assertEqual(args["BITCOIN_RELEASE_SHA256_ARM64"], "b" * 64)

    def test_mainnet_runbook_pins_every_manual_compose_command(self) -> None:
        text = (ROOT / "docs" / "mainnet-deployment.md").read_text()

        self.assertIn(
            'export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-qbit-mining-bootstrap}"',
            text,
        )
        command_count = text.count("docker compose \\\n")
        pinned_count = text.count(
            'docker compose \\\n  --project-name "$COMPOSE_PROJECT_NAME" \\\n'
        )
        self.assertGreater(command_count, 0)
        self.assertEqual(pinned_count, command_count)
        self.assertEqual(
            text.count("up -d --no-build --pull never"),
            text.count("up -d --no-build"),
        )

    def test_production_config_uses_staged_source_default_when_env_omits_source_path(self) -> None:
        fixture_lines = [
            line
            for line in FIXTURE.read_text(encoding="utf-8").splitlines()
            if not line.startswith("QBIT_SRC_DIR=")
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            deploy_env = Path(temp_dir) / "mainnet.env"
            deploy_env.write_text("\n".join(fixture_lines) + "\n", encoding="utf-8")
            env = {
                key: os.environ[key]
                for key in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "XDG_CONFIG_HOME")
                if key in os.environ
            }
            completed = subprocess.run(
                [
                    self.docker,
                    "compose",
                    "--env-file",
                    str(ROOT / "config/upstream.env.example"),
                    "--env-file",
                    str(deploy_env),
                    "-f",
                    str(ROOT / "compose.yaml"),
                    "-f",
                    str(ROOT / "compose.production.yaml"),
                    "--project-name",
                    "qbit-mainnet-source-default-contract",
                    "config",
                    "--quiet",
                    "qbitd",
                ],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_missing_production_inputs_render_only_failure_sentinels(self) -> None:
        env = {
            key: os.environ[key]
            for key in ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "XDG_CONFIG_HOME")
            if key in os.environ
        }
        completed = subprocess.run(
            [
                self.docker,
                "compose",
                "--env-file",
                str(ROOT / "config/upstream.env.example"),
                "-f",
                str(ROOT / "compose.yaml"),
                "-f",
                str(ROOT / "compose.production.yaml"),
                "--project-name",
                "qbit-mainnet-missing-input-contract",
                "--profile",
                "prism",
                "config",
                "--format",
                "json",
                "qbitd",
                "prism-postgres",
                "prism-coordinator",
            ],
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        config = json.loads(completed.stdout)
        for service in ("qbitd", "prism-postgres", "prism-coordinator"):
            with self.subTest(service=service):
                self.assertTrue(
                    config["services"][service]["image"].startswith("invalid.invalid/"),
                    config["services"][service]["image"],
                )
        evidence_mount = next(
            volume
            for volume in config["services"]["prism-coordinator"]["volumes"]
            if volume.get("target") == "/run/qbit-prism/capacity-evidence.json"
        )
        self.assertEqual(
            evidence_mount["source"],
            "/production-source-not-configured/prism/capacity-evidence.json",
        )
        self.assertFalse(
            evidence_mount.get("bind", {}).get("create_host_path", False)
        )

    @staticmethod
    def _network_selectors(command: list[str]) -> list[str]:
        shorthand = ("-main", "-testnet", "-testnet4", "-signet", "-regtest")
        return [
            str(arg)
            for arg in command
            if str(arg).startswith("-chain=") or str(arg).startswith(shorthand)
        ]

    def _environment(self, service: str) -> dict[str, str]:
        raw_env: Any = self.config["services"][service]["environment"]
        if isinstance(raw_env, dict):
            return {str(key): str(value) for key, value in raw_env.items()}
        if isinstance(raw_env, list):
            return {
                key: value
                for item in raw_env
                for key, _, value in [str(item).partition("=")]
            }
        raise TypeError(f"unexpected environment shape for {service}: {type(raw_env).__name__}")

    def _port_bindings(self, service: str) -> set[tuple[str, str, str]]:
        ports = self.config["services"][service].get("ports", [])
        return {
            (str(port.get("host_ip", "")), str(port["published"]), str(port["target"]))
            for port in ports
            if isinstance(port, dict)
        }


if __name__ == "__main__":
    unittest.main()
