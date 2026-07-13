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

    def render_profile(self, profile: str) -> dict[str, Any]:
        inherited_keys = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "XDG_CONFIG_HOME")
        env = {key: os.environ[key] for key in inherited_keys if key in os.environ}
        env["QBIT_SRC_DIR"] = str(ROOT)
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
            "prism": {"qbitd", "prism-postgres", "prism-coordinator"},
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
