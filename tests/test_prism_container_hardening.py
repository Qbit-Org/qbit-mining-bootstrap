#!/usr/bin/env python3
"""Hold the PRISM image and lab Compose services to the #288 hardening contract."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "lab" / "prism" / "Dockerfile"
NATIVE_SETTINGS = ROOT / "crates" / "qbit-prism-server" / "src" / "config" / "native-settings.txt"
PRISM_SERVICES = ("prism-coordinator", "prism-public-api")


def runtime_stage() -> list[str]:
    """Return the logical instructions of the final stage, continuations joined."""
    text = DOCKERFILE.read_text(encoding="utf-8").replace("\\\n", " ")
    lines = [line.strip() for line in text.splitlines()]
    instructions = [line for line in lines if line and not line.startswith("#")]
    last_from = max(i for i, line in enumerate(instructions) if line.startswith("FROM "))
    return instructions[last_from:]


class PrismDockerfileTests(unittest.TestCase):
    def test_runtime_base_is_digest_pinned(self) -> None:
        self.assertRegex(runtime_stage()[0], r"^FROM debian:bookworm-slim@sha256:[0-9a-f]{64}$")

    def test_runtime_runs_as_numeric_non_root_user_after_setup(self) -> None:
        stage = runtime_stage()
        users = [i for i, line in enumerate(stage) if line.startswith("USER ")]
        self.assertEqual(len(users), 1)
        match = re.fullmatch(r"USER (\d+):(\d+)", stage[users[0]])
        self.assertIsNotNone(match, stage[users[0]])
        self.assertNotEqual(match.group(1), "0")
        self.assertNotEqual(match.group(2), "0")
        setup = [i for i, line in enumerate(stage) if line.startswith(("RUN ", "COPY "))]
        self.assertGreater(users[0], max(setup))
        self.assertTrue(any(f"--uid {match.group(1)}" in line for line in stage))

    def test_entrypoint_disables_core_dumps_and_execs_the_command(self) -> None:
        self.assertIn(
            'ENTRYPOINT ["/bin/sh", "-c", "ulimit -c 0 && exec \\"$@\\"", "qbit-prism"]',
            runtime_stage(),
        )

    def test_healthcheck_probes_the_operator_listener(self) -> None:
        healthchecks = [line for line in runtime_stage() if line.startswith("HEALTHCHECK ")]
        self.assertEqual(len(healthchecks), 1)
        self.assertTrue(
            re.search(r' CMD \["qbit-prism-server", "healthcheck"\]$', healthchecks[0]),
            healthchecks[0],
        )
        self.assertEqual(runtime_stage()[-1], 'CMD ["qbit-prism-server"]')


class PrismImageSmokeWiringTests(unittest.TestCase):
    def test_ci_runs_the_image_smoke_and_settings_guard(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        self.assertIn("run: bash .github/scripts/test-prism-image-runtime.sh", workflow)
        self.assertIn("PRISM_IMAGE: qbit-lab-prism:ci", workflow)
        self.assertIn("run: python3 scripts/check_prism_settings.py", workflow)


class PrismLabComposeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        docker = shutil.which("docker")
        if docker is None:
            raise unittest.SkipTest("docker CLI is not installed")
        inherited = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "XDG_CONFIG_HOME")
        env = {key: os.environ[key] for key in inherited if key in os.environ}
        env["QBIT_SRC_DIR"] = str(ROOT)
        completed = subprocess.run(
            [docker, "compose",
             "--env-file", str(ROOT / "config/upstream.env.example"),
             "--env-file", str(ROOT / ".env.example"),
             "-f", str(ROOT / "compose.yaml"),
             "--project-name", "qbit-prism-hardening-contract",
             "--profile", "prism", "config", "--format", "json"],
            cwd=ROOT, env=env, text=True, capture_output=True, check=False,
        )
        if completed.returncode != 0 and "compose" in completed.stderr and "not a docker command" in completed.stderr:
            raise unittest.SkipTest("docker compose is unavailable")
        if completed.returncode != 0:
            raise AssertionError(f"lab compose render failed: {completed.stderr}")
        cls.services = json.loads(completed.stdout)["services"]

    def test_prism_containers_disable_core_dumps(self) -> None:
        for name in PRISM_SERVICES:
            with self.subTest(service=name):
                core = self.services[name]["ulimits"]["core"]
                # The rendered form omits zero-valued soft and hard limits.
                limits = (
                    (core.get("soft", 0), core.get("hard", 0)) if isinstance(core, dict) else (core, core)
                )
                self.assertEqual(limits, (0, 0))

    def test_role_healthchecks_match_their_listeners(self) -> None:
        self.assertEqual(
            self.services["prism-coordinator"]["healthcheck"]["test"],
            ["CMD", "qbit-prism-server", "healthcheck"],
        )
        self.assertEqual(
            self.services["prism-public-api"]["healthcheck"]["test"],
            ["CMD", "qbit-prism-server", "healthcheck", "--public-api"],
        )

    def test_frontend_receives_mounted_secret_and_operator_token_settings(self) -> None:
        env = self.services["prism-coordinator"]["environment"]
        for name in (
            "PRISM_MANIFEST_SIGNING_SEED_HEX_FILE",
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX_FILE",
            "PRISM_OPERATOR_BEARER_TOKEN",
            "PRISM_OPERATOR_BEARER_TOKEN_FILE",
        ):
            with self.subTest(name=name):
                self.assertEqual(env[name], "")

    def test_public_api_is_seedless(self) -> None:
        env = self.services["prism-public-api"]["environment"]
        self.assertFalse(
            [name for name in env if "SIGNING_SEED" in name or "OPERATOR_BEARER_TOKEN" in name]
        )

    def test_native_processes_receive_only_native_prism_settings(self) -> None:
        # Production check-config rejects an unread PRISM_* name, so Compose-only
        # values such as PRISM_POSTGRES_PASSWORD must stay out of these services.
        native = {
            line for line in NATIVE_SETTINGS.read_text(encoding="utf-8").splitlines()
            if line.startswith("PRISM_")
        }
        for name in PRISM_SERVICES:
            with self.subTest(service=name):
                passed = {key for key in self.services[name]["environment"] if key.startswith("PRISM_")}
                self.assertEqual(passed - native, set())


if __name__ == "__main__":
    unittest.main()
