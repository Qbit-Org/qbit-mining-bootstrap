#!/usr/bin/env python3

from __future__ import annotations

import os
import re
import shlex
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
README = ROOT / "README.md"


class MakefileLifecycleTests(unittest.TestCase):
    def resolve_operator_build_mode(
        self,
        *,
        qbit_production: str,
        qbit_tools_production: str,
        qbit_chain: str,
    ) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_compose = root / "fake-compose"
            fake_compose.write_text(
                textwrap.dedent(
                    """\
                    #!/bin/sh
                    if [ "${1:-}" = config ] && [ "${2:-}" = --environment ]; then
                      printf 'QBIT_PRODUCTION=%s\\n' "$FAKE_QBIT_PRODUCTION"
                      printf 'QBIT_TOOLS_PRODUCTION=%s\\n' "$FAKE_QBIT_TOOLS_PRODUCTION"
                      printf 'QBIT_CHAIN=%s\\n' "$FAKE_QBIT_CHAIN"
                      exit 0
                    fi
                    exit 9
                    """
                ),
                encoding="utf-8",
            )
            fake_compose.chmod(0o755)
            wrapper = root / "Makefile"
            wrapper.write_text(
                f"include {ROOT / 'Makefile'}\n\n"
                "print-operator-build-mode:\n"
                "\t@$(COMPOSE_ENV_HELPERS) operator_build_mode\n",
                encoding="utf-8",
            )
            env = os.environ.copy()
            env.update(
                {
                    "FAKE_QBIT_PRODUCTION": qbit_production,
                    "FAKE_QBIT_TOOLS_PRODUCTION": qbit_tools_production,
                    "FAKE_QBIT_CHAIN": qbit_chain,
                }
            )
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "-s",
                    "-f",
                    str(wrapper),
                    "print-operator-build-mode",
                    f"COMPOSE={fake_compose}",
                ],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            return result.stdout.strip()

    def resolve_compose_env_files(self, *, deploy_env_file: str = "") -> list[str]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config").mkdir()
            (root / "config" / "upstream.env").write_text("UPSTREAM=1\n", encoding="utf-8")
            (root / ".env").write_text("LOCAL=1\n", encoding="utf-8")
            wrapper = root / "Makefile"
            wrapper.write_text(
                f"include {ROOT / 'Makefile'}\n\n"
                "print-compose-env-files:\n"
                "\t@printf '%s\\n' '$(COMPOSE_ENV_FILES)'\n",
                encoding="utf-8",
            )
            env = os.environ.copy()
            env.pop("DEPLOY_ENV_FILE", None)
            if deploy_env_file:
                env["DEPLOY_ENV_FILE"] = deploy_env_file
            result = subprocess.run(
                ["make", "--no-print-directory", "-s", "-f", str(wrapper), "print-compose-env-files"],
                cwd=root,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            tokens = shlex.split(result.stdout.strip())
            return [tokens[index + 1] for index, token in enumerate(tokens) if token == "--env-file"]

    def test_deployment_env_replaces_repository_env_in_make(self) -> None:
        deploy_env = "/etc/qbit-mining-bootstrap/mainnet.env"

        self.assertEqual(
            self.resolve_compose_env_files(deploy_env_file=deploy_env),
            ["config/upstream.env", deploy_env],
        )
        self.assertEqual(
            self.resolve_compose_env_files(),
            ["config/upstream.env", ".env"],
        )

    def test_destructive_test_cleanup_uses_disposable_projects_only(self) -> None:
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        destructive_lines = [line.strip() for line in makefile.splitlines() if "down -v" in line]

        self.assertGreater(len(destructive_lines), 1)
        self.assertEqual(
            [line for line in destructive_lines if "TEST_COMPOSE_ALL_PROFILES" not in line],
            ["$(COMPOSE_ALL_PROFILES) --profile prism down -v --remove-orphans"],
        )

    def test_operator_targets_are_lane_scoped_and_do_not_build_in_production(self) -> None:
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")

        for target, lanes in (
            ("up-permissionless-pool", "ckpool"),
            ("up-prism-pool", "prism"),
            ("up-auxpow-pool", "auxpow"),
            ("up-dual-pools", "ckpool,auxpow"),
        ):
            with self.subTest(target=target):
                self.assertIn(f"{target}: export MINING_LANES={lanes}", makefile)

        self.assertIn("operator_build_mode", makefile)
        self.assertEqual(makefile.count("up -d --no-build --pull never"), 4)
        self.assertNotIn(" up --no-build", makefile)
        self.assertIn("PRODUCTION_COMPOSE ?= $(COMPOSE) -f compose.production.yaml", makefile)
        self.assertEqual(makefile.count("$(PRODUCTION_COMPOSE)"), 4)
        self.assertNotIn("$(PRODUCTION_COMPOSE) --profile permissionless pull", makefile)
        self.assertIn("auxpow lab mode: CPU-solving bridge", makefile)
        self.assertIn("export DEPLOY_ENV_FILE", makefile)
        for target in ("up-permissionless", "up-real-miner", "up-auxpow", "up-auxpow-bridge"):
            self.assertRegex(makefile, rf"{target}[^\n]*: require-lab-mode")
        self.assertRegex(
            makefile,
            re.compile(
                r"^TEST_COMPOSE = .*--project-name qbit-mining-bootstrap-test-\$@-\$\$\$\$$",
                re.MULTILINE,
            ),
        )

    def test_operator_build_mode_normalizes_production_and_main_chain_case(self) -> None:
        cases = (
            ("0", "0", "Mainnet", "no-build"),
            ("0", "0", "MAIN", "no-build"),
            ("TrUe", "0", "regtest", "no-build"),
            ("0", "YeS", "testnet4", "no-build"),
            ("0", "0", "signet", "build"),
        )
        for qbit_production, qbit_tools_production, qbit_chain, expected in cases:
            with self.subTest(
                qbit_production=qbit_production,
                qbit_tools_production=qbit_tools_production,
                qbit_chain=qbit_chain,
            ):
                self.assertEqual(
                    self.resolve_operator_build_mode(
                        qbit_production=qbit_production,
                        qbit_tools_production=qbit_tools_production,
                        qbit_chain=qbit_chain,
                    ),
                    expected,
                )

    def test_host_entrypoint_scripts_avoid_bash_4_case_expansion(self) -> None:
        for relative_path in (
            "scripts/check-env.sh",
            "scripts/prepare-qbit-source.sh",
        ):
            with self.subTest(relative_path=relative_path):
                script = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertNotRegex(script, r"\$\{[^}\n]*(?:,,|\^\^)[^}\n]*\}")

    def test_auxpow_real_miner_smoke_uses_deterministic_lab_difficulty(self) -> None:
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        recipe = makefile.split("test-auxpow-stratum:\n", 1)[1].split("\ntest-auxpow-stratum-bip310:", 1)[0]

        self.assertIn("AUXPOW_STRATUM_VARDIFF_ENABLED=0", recipe)
        self.assertIn("AUXPOW_STRATUM_SHARE_DIFF=0.0001", recipe)
        self.assertIn("AUXPOW_STRATUM_MIN_ADVERTISED_DIFF=0.0001", recipe)

    def run_target(
        self,
        target: str,
        *,
        qbit_production: str = "0",
        qbit_chain: str = "regtest",
        qbit_chain_flag: str = "-regtest",
        bitcoin_chain: str = "regtest",
        bitcoin_chain_flag: str = "-regtest",
        qbit_node_extra_arg: str = "",
        bitcoin_node_extra_args: str = "",
        compose_config_fails: bool = False,
        confirm: bool = False,
    ) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            call_log = tmp_path / "calls.log"
            fake_compose = tmp_path / "fake-compose"
            fake_compose.write_text(
                textwrap.dedent(
                    """\
                    #!/bin/sh
                    if [ "${1:-}" = config ] && [ "${2:-}" = --environment ]; then
                      if [ "$FAKE_COMPOSE_CONFIG_FAILS" = 1 ]; then
                        exit 9
                      fi
                      printf 'QBIT_PRODUCTION=%s\\n' "$FAKE_QBIT_PRODUCTION"
                      printf 'QBIT_CHAIN=%s\\n' "$FAKE_QBIT_CHAIN"
                      printf 'QBIT_CHAIN_FLAG=%s\\n' "$FAKE_QBIT_CHAIN_FLAG"
                      printf 'BITCOIN_CHAIN=%s\\n' "$FAKE_BITCOIN_CHAIN"
                      printf 'BITCOIN_CHAIN_FLAG=%s\\n' "$FAKE_BITCOIN_CHAIN_FLAG"
                      exit 0
                    fi
                    printf '%s\\n' "$*" >> "$FAKE_COMPOSE_CALL_LOG"
                    """
                ),
                encoding="utf-8",
            )
            fake_compose.chmod(0o755)
            env = os.environ.copy()
            env.update(
                {
                    "FAKE_COMPOSE_CALL_LOG": str(call_log),
                    "FAKE_QBIT_PRODUCTION": qbit_production,
                    "FAKE_QBIT_CHAIN": qbit_chain,
                    "FAKE_QBIT_CHAIN_FLAG": qbit_chain_flag,
                    "FAKE_BITCOIN_CHAIN": bitcoin_chain,
                    "FAKE_BITCOIN_CHAIN_FLAG": bitcoin_chain_flag,
                    "FAKE_COMPOSE_CONFIG_FAILS": "1" if compose_config_fails else "0",
                    "QBIT_PRODUCTION": qbit_production,
                    "QBIT_CHAIN": qbit_chain,
                    "QBIT_CHAIN_FLAG": qbit_chain_flag,
                    "BITCOIN_CHAIN": bitcoin_chain,
                    "BITCOIN_CHAIN_FLAG": bitcoin_chain_flag,
                    "QBIT_NODE_EXTRA_ARG": qbit_node_extra_arg,
                    "BITCOIN_NODE_EXTRA_ARGS": bitcoin_node_extra_args,
                }
            )
            if confirm:
                env["PURGE_CONFIRM"] = "delete-all-local-mining-data"
            else:
                env.pop("PURGE_CONFIRM", None)

            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "-s",
                    target,
                    f"COMPOSE={fake_compose}",
                    f"COMPOSE_ALL_PROFILES={fake_compose}",
                ],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            calls = call_log.read_text(encoding="utf-8").splitlines() if call_log.exists() else []
            return result, calls

    def test_down_stops_all_profiles_without_deleting_volumes(self) -> None:
        result, calls = self.run_target("down")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["--profile prism down --remove-orphans"])
        self.assertNotIn("-v", calls[0].split())

    def test_upgrade_notes_remove_orphans_without_deleting_retired_volumes(self) -> None:
        readme = README.read_text(encoding="utf-8")
        upgrade_notes = readme.split("When upgrading an existing project", 1)[1].split(
            "For a fresh public-network deployment", 1
        )[0]

        self.assertIn("`--remove-orphans`", upgrade_notes)
        self.assertIn("without deleting their named volumes", upgrade_notes)
        self.assertIn("Retired volumes stay detached", upgrade_notes)
        self.assertNotIn("down -v", upgrade_notes)

    def test_purge_requires_exact_confirmation(self) -> None:
        result, calls = self.run_target("purge-local-volumes")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("permanently deletes local chain, ledger, audit, and miner state", result.stderr)
        self.assertEqual(calls, [])

    def test_purge_fails_when_compose_environment_cannot_be_resolved(self) -> None:
        result, calls = self.run_target(
            "purge-local-volumes",
            compose_config_fails=True,
            confirm=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, [])

    def test_purge_refuses_production_even_when_confirmed(self) -> None:
        result, calls = self.run_target(
            "purge-local-volumes",
            qbit_production="1",
            confirm=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("REFUSED", result.stderr)
        self.assertIn("QBIT_PRODUCTION", result.stderr)
        self.assertEqual(calls, [])

    def test_purge_refuses_main_chain_name_or_flag(self) -> None:
        cases = (
            ("main", "-server=1"),
            ("mainnet", "-server=1"),
            ("custom", "-server=1 -chain=main"),
            ("custom", "-mainnet"),
        )
        for qbit_chain, qbit_chain_flag in cases:
            with self.subTest(qbit_chain=qbit_chain, qbit_chain_flag=qbit_chain_flag):
                result, calls = self.run_target(
                    "purge-local-volumes",
                    qbit_chain=qbit_chain,
                    qbit_chain_flag=qbit_chain_flag,
                    confirm=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("REFUSED", result.stderr)
                self.assertEqual(calls, [])

    def test_confirmed_regtest_purge_deletes_named_volumes(self) -> None:
        result, calls = self.run_target("purge-local-volumes", confirm=True)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls, ["--profile prism down -v --remove-orphans"])

    def test_purge_refuses_parent_main_chain_flag(self) -> None:
        result, calls = self.run_target(
            "purge-local-volumes",
            bitcoin_chain_flag="-chain=main",
            confirm=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("REFUSED", result.stderr)
        self.assertIn("BITCOIN_CHAIN_FLAG", result.stderr)
        self.assertEqual(calls, [])

    def test_purge_refuses_parent_main_chain_name(self) -> None:
        result, calls = self.run_target(
            "purge-local-volumes",
            bitcoin_chain="mainnet",
            bitcoin_chain_flag="-regtest",
            confirm=True,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("REFUSED", result.stderr)
        self.assertIn("BITCOIN_CHAIN", result.stderr)
        self.assertEqual(calls, [])

    def test_purge_refuses_network_selectors_in_extra_args(self) -> None:
        cases = (
            {"qbit_node_extra_arg": "-chain=main"},
            {"qbit_node_extra_arg": "--main"},
            {"qbit_node_extra_arg": "-regtest=0"},
            {"bitcoin_node_extra_args": "-noregtest"},
            {"bitcoin_node_extra_args": "--testnet4=1"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                result, calls = self.run_target(
                    "purge-local-volumes",
                    confirm=True,
                    **overrides,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("REFUSED", result.stderr)
                self.assertIn("network selector", result.stderr)
                self.assertEqual(calls, [])

    def test_purge_refuses_negated_or_boolean_dedicated_chain_flags(self) -> None:
        cases = (
            {"qbit_chain": "testnet4", "qbit_chain_flag": "-notestnet4"},
            {"qbit_chain": "testnet4", "qbit_chain_flag": "-testnet4=0"},
            {"qbit_chain": "signet", "qbit_chain_flag": "--nosignet"},
            {"bitcoin_chain": "testnet4", "bitcoin_chain_flag": "-notestnet4"},
            {"bitcoin_chain": "signet", "bitcoin_chain_flag": "--nosignet"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                result, calls = self.run_target(
                    "purge-local-volumes",
                    confirm=True,
                    **overrides,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("REFUSED", result.stderr)
                self.assertIn("requires", result.stderr)
                self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
