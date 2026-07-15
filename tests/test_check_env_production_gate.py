#!/usr/bin/env python3

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
CHECK_ENV = ROOT_DIR / "scripts" / "check-env.sh"
class CheckEnvProductionGateTests(unittest.TestCase):
    def production_prism_env(self, root: Path) -> dict[str, str]:
        return {
            "MINING_LANES": "prism",
            "QBIT_PRODUCTION": "1",
            "QBIT_REQUIRE_RELEASE_PROVENANCE": "1",
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
            "QBIT_GIT_COMMIT": "41" * 20,
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "QBIT_RPC_PORT_HOST": "127.0.0.1:19552",
            "QBITD_IMAGE": "registry.example.invalid/qbitd@sha256:" + "d" * 64,
            "QBIT_DATA_SOURCE": str(root / "qbit-data"),
            "PRISM_COORDINATOR_IMAGE": "registry.example.invalid/prism@sha256:" + "b" * 64,
            "PRISM_POSTGRES_IMAGE": "registry.example.invalid/postgres@sha256:" + "e" * 64,
            "PRISM_POSTGRES_DATA_SOURCE": str(root / "postgres-data"),
            "PRISM_POSTGRES_WAL_SOURCE": str(root / "postgres-wal"),
            "PRISM_AUDIT_DATA_SOURCE": str(root / "prism-audit"),
            "PRISM_DATABASE_URL": "postgresql://example.invalid/qbit",
            "PRISM_POSTGRES_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "42" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "43" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "44" * 32,
            "PRISM_LEDGER_WRITER_ID": "managed-writer",
            "PRISM_LEDGER_WRITER_EPOCH": "7",
            "PRISM_AUDIT_DIR": "/var/lib/qbit/prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit/prism/evidence.json",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "PRISM_STRATUM_SHARE_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF": "1",
            "PRISM_STRATUM_VARDIFF_TARGET_SECONDS": "15",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
            "PRISM_STRATUM_VARDIFF_RETARGET_SECONDS": "90",
            "PRISM_STRATUM_VARDIFF_MAX_STEP_UP": "4",
            "PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN": "4",
            "PRISM_STRATUM_VARDIFF_EWMA_ALPHA": "0.4",
            "PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE": "0.25",
            "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS": "15",
            "PRISM_SHARE_COMMIT_BATCH_SIZE": "64",
            "PRISM_SHARE_COMMIT_LINGER_MILLISECONDS": "5",
            "PRISM_SHARE_COMMIT_TIMEOUT_SECONDS": "15",
            "PRISM_STRATUM_SEND_TIMEOUT_SECONDS": "20",
        }

    def write_pinned_qbit_checkout(self, root: Path) -> tuple[Path, str]:
        checkout = root / "qbit"
        checkout.mkdir()
        for relative_path in (
            "CMakeLists.txt",
            "src/CMakeLists.txt",
            "test/functional/test_framework/auxpow.py",
        ):
            path = checkout / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# test\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q", str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(checkout),
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            check=True,
        )
        commit = subprocess.check_output(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            text=True,
        ).strip()
        return checkout, commit

    def write_fake_docker(self, root: Path) -> Path:
        fake_bin = root / "bin"
        fake_bin.mkdir()
        docker = fake_bin / "docker"
        docker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        docker.chmod(0o755)
        return fake_bin

    def production_testnet4_env(self, root: Path) -> dict[str, str]:
        checkout, commit = self.write_pinned_qbit_checkout(root)
        fake_bin = self.write_fake_docker(root)
        return {
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "MINING_LANES": "all",
            "QBIT_PRODUCTION": "1",
            "QBIT_CHAIN": "testnet4",
            "QBIT_CHAIN_FLAG": "-testnet4",
            "BITCOIN_CHAIN": "testnet4",
            "BITCOIN_CHAIN_FLAG": "-testnet4",
            "BITCOIN_DNSSEED": "1",
            "QBIT_GIT_COMMIT": commit,
            "QBIT_SRC_DIR": str(checkout),
            "QBIT_SRC_DIR_OVERRIDE": str(checkout),
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "QBIT_RPC_PORT_HOST": "127.0.0.1:19552",
            "BITCOIN_RPC_USER": "bitcoinrpc",
            "BITCOIN_RPC_PASSWORD": "not-default",
            "BITCOIN_RPC_PORT_HOST": "127.0.0.1:18443",
            "QBIT_MINER_ADDRESS": "tq1explicitminer",
            "BITCOIN_MINER_ADDRESS": "tb1explicitminer",
            "CKPOOL_GIT_REF": "42" * 20,
            "CKPOOL_STRATUM_PORT_HOST": "0.0.0.0:3333",
            "CKPOOL_MINDIFF": "1024",
            "CKPOOL_STARTDIFF": "65536",
            "PRISM_DATABASE_URL": "postgresql://example.invalid/qbit",
            "PRISM_POSTGRES_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "42" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "43" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "44" * 32,
            "PRISM_LEDGER_WRITER_ID": "managed-writer",
            "PRISM_LEDGER_WRITER_EPOCH": "7",
            "PRISM_AUDIT_DIR": "/var/lib/qbit/prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit/prism/evidence.json",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "PRISM_STRATUM_SHARE_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
        }

    def run_check_env(
        self,
        *arguments: str,
        script: Path = CHECK_ENV,
        cwd: Path = ROOT_DIR,
        **overrides: str,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.pop("QBIT_GIT_COMMIT", None)
        env.update(overrides)
        return subprocess.run(
            ["/bin/bash", str(script), *arguments],
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )

    def isolated_check_env_root(self, directory: Path) -> tuple[Path, Path]:
        root = directory / "bootstrap"
        (root / "scripts").mkdir(parents=True)
        (root / "config").mkdir()
        script = root / "scripts" / "check-env.sh"
        shutil.copyfile(CHECK_ENV, script)
        shutil.copyfile(ROOT_DIR / ".env.example", root / ".env.example")
        shutil.copyfile(ROOT_DIR / "config" / "upstream.env.example", root / "config" / "upstream.env")
        return root, script

    def test_deploy_env_file_is_loaded_as_the_final_config_layer(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            deploy_env = Path(temp_dir) / "mainnet.env"
            deploy_env.write_text("MINING_LANES=unsupported\n", encoding="utf-8")

            result = self.run_check_env(DEPLOY_ENV_FILE=str(deploy_env))

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsupported lane 'unsupported'", result.stderr)

    def test_deployment_env_omits_repository_env_from_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root, script = self.isolated_check_env_root(Path(temp_dir))
            (root / ".env").write_text("MINING_LANES=unsupported\n", encoding="utf-8")
            deploy_env = root / "mainnet.env"
            deploy_env.write_text("", encoding="utf-8")

            result = self.run_check_env(
                "--require-lab",
                script=script,
                cwd=root,
                DEPLOY_ENV_FILE=str(deploy_env),
                MINING_LANES="",
                QBIT_PRODUCTION="",
                QBIT_CHAIN="",
                BITCOIN_CHAIN="",
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("lab-only target confirmed", result.stdout)

    def test_external_environment_overrides_deployment_env_during_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            deploy_env = Path(temp_dir) / "mainnet.env"
            deploy_env.write_text("MINING_LANES=unsupported\n", encoding="utf-8")

            result = self.run_check_env(
                "--require-lab",
                DEPLOY_ENV_FILE=str(deploy_env),
                MINING_LANES="ckpool",
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("lab-only target confirmed", result.stdout)

    def test_lab_guard_refuses_production_and_main_chain(self) -> None:
        cases = (
            {"QBIT_PRODUCTION": "1"},
            {"QBIT_CHAIN": "mainnet", "QBIT_CHAIN_FLAG": "-chain=main"},
            {"BITCOIN_CHAIN": "mainnet", "BITCOIN_CHAIN_FLAG": "-chain=main"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                result = self.run_check_env("--require-lab", **overrides)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("lab-only target refuses", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_lab_guard_accepts_default_regtest_without_docker(self) -> None:
        result = self.run_check_env("--require-lab")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("lab-only target confirmed", result.stdout)

    def test_lab_guard_refuses_network_selectors_in_extra_args(self) -> None:
        cases = (
            {"QBIT_NODE_EXTRA_ARG": "-chain=main"},
            {"QBIT_NODE_EXTRA_ARG": "--main"},
            {"QBIT_NODE_EXTRA_ARG": "-regtest=0"},
            {"BITCOIN_NODE_EXTRA_ARGS": "-noregtest"},
            {"BITCOIN_NODE_EXTRA_ARGS": "--testnet4=1"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                result = self.run_check_env("--require-lab", **overrides)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("network selector", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_lab_guard_refuses_negated_or_boolean_dedicated_chain_flags(self) -> None:
        cases = (
            {"QBIT_CHAIN": "testnet4", "QBIT_CHAIN_FLAG": "-notestnet4"},
            {"QBIT_CHAIN": "testnet4", "QBIT_CHAIN_FLAG": "-testnet4=0"},
            {"QBIT_CHAIN": "signet", "QBIT_CHAIN_FLAG": "--nosignet"},
            {"BITCOIN_CHAIN": "testnet4", "BITCOIN_CHAIN_FLAG": "-notestnet4"},
            {"BITCOIN_CHAIN": "signet", "BITCOIN_CHAIN_FLAG": "--nosignet"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                result = self.run_check_env("--require-lab", **overrides)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("requires", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_non_auxpow_lane_ignores_unused_bitcoin_validation_settings(self) -> None:
        result = self.run_check_env(
            MINING_LANES="ckpool",
            BITCOIN_CHAIN="unsupported",
            BITCOIN_CHAIN_FLAG="--invalid",
            BITCOIN_DNSSEED="not-a-boolean",
            BITCOIN_DISCOVER="not-a-boolean",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("BITCOIN_CHAIN must", result.stderr)
        self.assertNotIn("BITCOIN_DNSSEED must", result.stderr)
        self.assertNotIn("BITCOIN_DISCOVER must", result.stderr)

    def test_production_mode_rejects_regtest_before_docker_check(self) -> None:
        result = self.run_check_env(QBIT_PRODUCTION="1")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production mode rejects regtest QBIT_CHAIN", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_production_mainnet_prelaunch_requires_explicit_authorization(self) -> None:
        # PR #21 tests, adapted: this branch also requires the explicit mainnet
        # chain flag and genesis pin, runs the PRISM behavioral block before
        # the ckpool lane, so the ckpool lane is selected directly.
        result = self.run_check_env(
            MINING_LANES="ckpool",
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
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
            MINING_LANES="ckpool",
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("mainnet prelaunch requires", result.stderr)
        self.assertNotIn(
            "requires the explicitly authorized mainnet prelaunch combination",
            result.stderr,
        )
        self.assertIn("production CKPool requires an explicit QBIT_MINER_ADDRESS", result.stderr)

    def test_production_mainnet_prelaunch_accepts_whitespace_around_launch_flag(self) -> None:
        result = self.run_check_env(
            MINING_LANES="ckpool",
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=" 0\t",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("true/false style value", result.stderr)
        self.assertNotIn("mainnet prelaunch requires", result.stderr)
        self.assertIn("production CKPool requires an explicit QBIT_MINER_ADDRESS", result.stderr)

    def test_production_mainnet_prelaunch_requires_both_production_flags(self) -> None:
        for name in ("QBIT_PRODUCTION", "QBIT_TOOLS_PRODUCTION"):
            flags = {
                "QBIT_PRODUCTION": "1",
                "QBIT_TOOLS_PRODUCTION": "1",
            }
            flags[name] = "0"
            with self.subTest(name=name):
                result = self.run_check_env(
                    MINING_LANES="ckpool",
                    QBIT_CHAIN="mainnet",
                    QBIT_CHAIN_FLAG="-chain=main",
                    QBIT_EXPECTED_GENESIS_HASH="11" * 32,
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                    **flags,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "requires the explicitly authorized mainnet prelaunch combination",
                    result.stderr,
                )
                self.assertNotIn("docker is required", result.stderr)

    def test_production_mainnet_launch_rejects_disabled_readiness(self) -> None:
        result = self.run_check_env(
            MINING_LANES="ckpool",
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0", result.stderr)

    def test_production_gate_rejects_malformed_boolean_flags(self) -> None:
        cases = (
            ("CKPOOL_NON_TEST_READINESS_GATE", "sometimes", "mainnet"),
            ("QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED", "prelaunch", "mainnet"),
            ("QBIT_TOOLS_PRODUCTION", "maybe", "mainnet"),
        )
        for name, value, chain in cases:
            with self.subTest(name=name):
                chain_env = (
                    {
                        "QBIT_CHAIN": "mainnet",
                        "QBIT_CHAIN_FLAG": "-chain=main",
                        "QBIT_EXPECTED_GENESIS_HASH": "11" * 32,
                    }
                    if chain == "mainnet"
                    else {"QBIT_CHAIN": "signet", "QBIT_CHAIN_FLAG": "-signet"}
                )
                result = self.run_check_env(
                    MINING_LANES="ckpool",
                    QBIT_PRODUCTION="1",
                    **chain_env,
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
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
            QBIT_NODE_EXTRA_ARG="-listen=1",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
            QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS="456789",
            PRISM_STRATUM_STALE_GRACE_SECONDS="0",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS", result.stderr)
        self.assertIn("requires a non-default PRISM_POSTGRES_PASSWORD", result.stderr)

    def test_launch_rejects_caller_supplied_qbitd_tip_age_before_docker_check(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
            QBIT_NODE_EXTRA_ARG="-maxtipage=9223372036854775807",
            CKPOOL_NON_TEST_READINESS_GATE="1",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("caller-provided -maxtipage", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

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
            MINING_LANES="ckpool",
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="testnet4",
            QBIT_CHAIN_FLAG="-testnet4",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("valid only for QBIT_CHAIN=mainnet", result.stderr)

    def test_prism_lane_validates_shared_launch_flag(self) -> None:
        result = self.run_check_env(
            MINING_LANES="prism",
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            CKPOOL_NON_TEST_READINESS_GATE="sometimes",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="prelaunch",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_lane_selection_rejects_unknown_lane(self) -> None:
        result = self.run_check_env(MINING_LANES="ckpool,unknown")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsupported lane 'unknown'", result.stderr)

    def test_ckpool_lane_does_not_require_prism_configuration(self) -> None:
        result = self.run_check_env(
            MINING_LANES="ckpool",
            QBIT_PRODUCTION="1",
            QBIT_REQUIRE_RELEASE_PROVENANCE="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            QBIT_GIT_COMMIT="41" * 20,
            QBIT_RPC_USER="qbitrpc",
            QBIT_RPC_PASSWORD="not-default",
            QBIT_RPC_PORT_HOST="127.0.0.1:19552",
            QBIT_MINER_ADDRESS="qb1explicit",
            CKPOOL_GIT_REF="42" * 20,
            CKPOOL_STRATUM_PORT_HOST="0.0.0.0:3333",
            PRISM_ALLOW_MEMORY_LEDGER="1",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("PRISM_ALLOW_MEMORY_LEDGER", result.stderr)
        self.assertIn("QBITD_IMAGE", result.stderr)

    def test_production_ckpool_requires_explicit_payout_address(self) -> None:
        common = {
            "MINING_LANES": "ckpool",
            "QBIT_PRODUCTION": "1",
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
        }
        for address in ("", "auto", "AUTO"):
            with self.subTest(address=address):
                result = self.run_check_env(**common, QBIT_MINER_ADDRESS=address)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "production CKPool requires an explicit QBIT_MINER_ADDRESS",
                    result.stderr,
                )
                self.assertNotIn("docker is required", result.stderr)

    def test_production_rejects_public_or_implicit_rpc_bindings(self) -> None:
        ckpool_common = {
            "MINING_LANES": "ckpool",
            "QBIT_PRODUCTION": "1",
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
            "QBIT_MINER_ADDRESS": "qb1explicit",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
        }
        for value in ("19552", "0.0.0.0:19552", "[::]:19552", "203.0.113.8:19552"):
            with self.subTest(name="QBIT_RPC_PORT_HOST", value=value):
                result = self.run_check_env(**ckpool_common, QBIT_RPC_PORT_HOST=value)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("QBIT_RPC_PORT_HOST", result.stderr)
                self.assertIn("loopback or private", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

        auxpow_common = {
            "MINING_LANES": "auxpow",
            "QBIT_PRODUCTION": "1",
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
            "BITCOIN_CHAIN": "signet",
            "BITCOIN_CHAIN_FLAG": "-signet",
            "QBIT_MINER_ADDRESS": "qb1explicit",
            "BITCOIN_MINER_ADDRESS": "bc1explicit",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "QBIT_RPC_PORT_HOST": "127.0.0.1:19552",
            "BITCOIN_RPC_USER": "bitcoinrpc",
            "BITCOIN_RPC_PASSWORD": "not-default",
        }
        for value in ("8332", "0.0.0.0:8332", "[::]:8332", "203.0.113.8:8332"):
            with self.subTest(name="BITCOIN_RPC_PORT_HOST", value=value):
                result = self.run_check_env(**auxpow_common, BITCOIN_RPC_PORT_HOST=value)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("BITCOIN_RPC_PORT_HOST", result.stderr)
                self.assertIn("loopback or private", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_production_rejects_chain_and_discovery_flags_in_extra_args(self) -> None:
        common = {
            "MINING_LANES": "ckpool",
            "QBIT_PRODUCTION": "1",
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
        }
        cases = (
            ("QBIT_NODE_EXTRA_ARG", "-testnet4", "network selector"),
            ("QBIT_NODE_EXTRA_ARG", "--TESTNET4=1", "network selector"),
            ("QBIT_NODE_EXTRA_ARG", "--signet=1", "network selector"),
            ("QBIT_NODE_EXTRA_ARG", "-server=1 -chain=main", "network selector"),
            ("BITCOIN_NODE_EXTRA_ARGS", "-regtest", "network selector"),
            ("BITCOIN_NODE_EXTRA_ARGS", "--regtest=1", "network selector"),
        )
        for name, value, expected in cases:
            with self.subTest(name=name, value=value):
                result = self.run_check_env(**common, **{name: value})

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(name, result.stderr)
                self.assertIn(expected, result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_production_auxpow_rejects_discovery_flags_in_extra_args(self) -> None:
        common = {
            "MINING_LANES": "auxpow",
            "QBIT_PRODUCTION": "1",
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
            "BITCOIN_CHAIN": "signet",
            "BITCOIN_CHAIN_FLAG": "-signet",
        }
        for value, expected in (
            ("-DNSSEED=0", "BITCOIN_DNSSEED"),
            ("--dnsseed=0", "BITCOIN_DNSSEED"),
            ("-discover=0", "BITCOIN_DISCOVER"),
        ):
            with self.subTest(value=value):
                result = self.run_check_env(**common, BITCOIN_NODE_EXTRA_ARGS=value)

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("BITCOIN_NODE_EXTRA_ARGS", result.stderr)
                self.assertIn(expected, result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_production_accepts_explicit_private_qbit_rpc_bindings(self) -> None:
        common = {
            "MINING_LANES": "ckpool",
            "QBIT_PRODUCTION": "1",
            "QBIT_REQUIRE_RELEASE_PROVENANCE": "1",
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
            "QBIT_GIT_COMMIT": "41" * 20,
            "QBIT_MINER_ADDRESS": "qb1explicit",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "CKPOOL_GIT_REF": "42" * 20,
        }
        for value in (
            "127.0.0.1:19552",
            "10.0.0.2:19552",
            "100.64.0.2:19552",
            "172.16.0.2:19552",
            "192.168.1.2:19552",
            "[::1]:19552",
            "[FD00::2]:19552",
        ):
            with self.subTest(value=value):
                result = self.run_check_env(**common, QBIT_RPC_PORT_HOST=value)

                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("QBIT_RPC_PORT_HOST", result.stderr)
                self.assertIn("QBITD_IMAGE", result.stderr)

    def test_production_rejects_non_digest_images_before_docker_check(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env = self.production_prism_env(root)
            env["PRISM_COORDINATOR_IMAGE"] = "prism:latest"

            result = self.run_check_env(**env)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PRISM_COORDINATOR_IMAGE as a digest-qualified image reference", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_production_rejects_reused_state_paths_before_docker_check(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env = self.production_prism_env(root)
            env["PRISM_POSTGRES_WAL_SOURCE"] = env["PRISM_POSTGRES_DATA_SOURCE"]

            result = self.run_check_env(**env)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PRISM_POSTGRES_DATA_SOURCE and PRISM_POSTGRES_WAL_SOURCE must be distinct", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_qbit_mainnet_auxpow_requires_mainnet_parent(self) -> None:
        result = self.run_check_env(
            MINING_LANES="auxpow",
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
            BITCOIN_CHAIN="regtest",
            BITCOIN_CHAIN_FLAG="-regtest",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires BITCOIN_CHAIN=mainnet", result.stderr)

    def test_production_mode_rejects_prism_test_bypass_before_docker_check(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            QBIT_GIT_COMMIT="41" * 20,
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
            PRISM_STRATUM_STALE_GRACE_SECONDS="0",
            PRISM_ALLOW_MEMORY_LEDGER="1",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production mode rejects PRISM_ALLOW_MEMORY_LEDGER=1", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_production_mode_rejects_default_prism_postgres_password(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            QBIT_GIT_COMMIT="41" * 20,
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
            PRISM_STRATUM_STALE_GRACE_SECONDS="0",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production mode requires a non-default PRISM_POSTGRES_PASSWORD", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_production_mode_rejects_default_prism_database_url_password(self) -> None:
        result = self.run_check_env(
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            QBIT_GIT_COMMIT="41" * 20,
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
            PRISM_STRATUM_STALE_GRACE_SECONDS="0",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production mode requires a non-default PRISM_DATABASE_URL", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_mainnet_requires_canonical_explicit_chain_selector(self) -> None:
        for chain_flag in ("", "-server=1", "-regtest", "-testnet4"):
            with self.subTest(chain_flag=chain_flag):
                result = self.run_check_env(
                    QBIT_CHAIN="mainnet",
                    QBIT_CHAIN_FLAG=chain_flag,
                    QBIT_EXPECTED_GENESIS_HASH="11" * 32,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "QBIT_CHAIN=mainnet requires explicit QBIT_CHAIN_FLAG=-chain=main",
                    result.stderr,
                )
                self.assertNotIn("docker is required", result.stderr)

    def test_mainnet_requires_pinned_genesis_hash(self) -> None:
        for genesis_hash in ("", "not-a-hash", "11" * 31):
            with self.subTest(genesis_hash=genesis_hash):
                result = self.run_check_env(
                    QBIT_CHAIN="mainnet",
                    QBIT_CHAIN_FLAG="-chain=main",
                    QBIT_EXPECTED_GENESIS_HASH=genesis_hash,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "QBIT_CHAIN=mainnet requires QBIT_EXPECTED_GENESIS_HASH as 64 hex characters",
                    result.stderr,
                )
                self.assertNotIn("docker is required", result.stderr)

    def test_bitcoin_chain_requires_matching_explicit_selector(self) -> None:
        cases = (
            ("regtest", "-chain=main", "-regtest"),
            ("testnet4", "-testnet", "-testnet4"),
            ("signet", "-regtest", "-signet"),
            ("mainnet", "-server=1", "-chain=main"),
        )
        for bitcoin_chain, chain_flag, expected_flag in cases:
            with self.subTest(bitcoin_chain=bitcoin_chain, chain_flag=chain_flag):
                result = self.run_check_env(
                    BITCOIN_CHAIN=bitcoin_chain,
                    BITCOIN_CHAIN_FLAG=chain_flag,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    f"BITCOIN_CHAIN={bitcoin_chain} requires",
                    result.stderr,
                )
                self.assertIn(expected_flag, result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_bitcoin_mainnet_requires_canonical_genesis_hash(self) -> None:
        for genesis_hash in ("", "11" * 32, "not-a-hash"):
            with self.subTest(genesis_hash=genesis_hash):
                result = self.run_check_env(
                    BITCOIN_CHAIN="mainnet",
                    BITCOIN_CHAIN_FLAG="-chain=main",
                    BITCOIN_EXPECTED_GENESIS_HASH=genesis_hash,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("BITCOIN_EXPECTED_GENESIS_HASH", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_mainnet_implies_production_safeguards(self) -> None:
        result = self.run_check_env(
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
            QBIT_GIT_COMMIT="41" * 20,
            PRISM_ALLOW_MEMORY_LEDGER="1",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("production mode rejects PRISM_ALLOW_MEMORY_LEDGER=1", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_production_auxpow_requires_explicit_payouts(self) -> None:
        common = {
            "MINING_LANES": "auxpow",
            "QBIT_CHAIN": "mainnet",
            "QBIT_CHAIN_FLAG": "-chain=main",
            "QBIT_EXPECTED_GENESIS_HASH": "11" * 32,
            "QBIT_GIT_COMMIT": "41" * 20,
            "BITCOIN_CHAIN": "mainnet",
            "BITCOIN_CHAIN_FLAG": "-chain=main",
            "BITCOIN_EXPECTED_GENESIS_HASH": (
                "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
            ).upper(),
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
        }
        for qbit_address, bitcoin_address, expected_name in (
            ("auto", "bc1explicit", "QBIT_MINER_ADDRESS"),
            ("qb1explicit", "auto", "BITCOIN_MINER_ADDRESS"),
        ):
            with self.subTest(expected_name=expected_name):
                result = self.run_check_env(
                    **common,
                    QBIT_MINER_ADDRESS=qbit_address,
                    BITCOIN_MINER_ADDRESS=bitcoin_address,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"production AuxPoW requires an explicit {expected_name}", result.stderr)
                self.assertNotIn("docker is required", result.stderr)

    def test_production_git_provider_requires_exact_commit_pin(self) -> None:
        for commit in ("", "main", "11" * 19, "zz" * 20):
            with self.subTest(commit=commit):
                overrides = {
                    "MINING_LANES": "prism",
                    "QBIT_PRODUCTION": "1",
                    "QBIT_CHAIN": "signet",
                    "QBIT_CHAIN_FLAG": "-signet",
                    "QBIT_RPC_PASSWORD": "not-default",
                    "BITCOIN_RPC_PASSWORD": "not-default",
                    "PRISM_DATABASE_URL": "postgresql://example.invalid/qbit",
                    "PRISM_POSTGRES_PASSWORD": "not-default",
                    "PRISM_MANIFEST_SIGNING_SEED_HEX": "42" * 32,
                    "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "43" * 32,
                    "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "44" * 32,
                    "PRISM_LEDGER_WRITER_ID": "managed-writer",
                    "PRISM_LEDGER_WRITER_EPOCH": "7",
                    "PRISM_AUDIT_DIR": "/var/lib/qbit/prism/audit",
                    "PRISM_EVIDENCE_PATH": "/var/lib/qbit/prism/evidence.json",
                    "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
                    "CKPOOL_MINDIFF": "1024",
                    "CKPOOL_STARTDIFF": "65536",
                    "CKPOOL_REQUIRE_P2MR_PAYOUT": "1",
                    "AUXPOW_STRATUM_HEADER_VARIANT": "canonical",
                }
                if commit:
                    overrides["QBIT_GIT_COMMIT"] = commit
                    result = self.run_check_env(**overrides)
                else:
                    with tempfile.TemporaryDirectory() as temp_dir:
                        deploy_env = Path(temp_dir) / "mainnet.env"
                        deploy_env.write_text("QBIT_GIT_COMMIT=\n", encoding="utf-8")
                        result = self.run_check_env(
                            DEPLOY_ENV_FILE=str(deploy_env),
                            QBIT_GIT_COMMIT="",
                            **overrides,
                        )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "production mode requires QBIT_GIT_COMMIT as exactly 40 hex characters",
                    result.stderr,
                )
                self.assertNotIn("docker is required", result.stderr)

    def test_mainnet_requires_zero_stale_grace(self) -> None:
        result = self.run_check_env(
            MINING_LANES="prism",
            QBIT_CHAIN="mainnet",
            QBIT_CHAIN_FLAG="-chain=main",
            QBIT_EXPECTED_GENESIS_HASH="11" * 32,
            QBIT_GIT_COMMIT="41" * 20,
            PRISM_STRATUM_STALE_GRACE_SECONDS="3",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mainnet requires PRISM_STRATUM_STALE_GRACE_SECONDS=0", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_non_mainnet_production_accepts_bounded_stale_grace(self) -> None:
        # A public-chain production pool may credit shares that raced a block
        # within a bounded grace window; only mainnet pins the strict zero.
        result = self.run_check_env(
            MINING_LANES="prism",
            QBIT_PRODUCTION="1",
            QBIT_CHAIN="signet",
            QBIT_CHAIN_FLAG="-signet",
            QBIT_GIT_COMMIT="41" * 20,
            PRISM_STRATUM_STALE_GRACE_SECONDS="2",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("PRISM_STRATUM_STALE_GRACE_SECONDS", result.stderr)

    def test_mainnet_provenance_passes_without_capacity_evidence_and_verifies_pin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            checkout = root / "qbit"
            checkout.mkdir()
            for relative_path in (
                "CMakeLists.txt",
                "src/CMakeLists.txt",
                "test/functional/test_framework/auxpow.py",
            ):
                path = checkout / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# test\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q", str(checkout)], check=True)
            subprocess.run(["git", "-C", str(checkout), "add", "."], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(checkout),
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "-qm",
                    "fixture",
                ],
                check=True,
            )
            actual_commit = subprocess.check_output(
                ["git", "-C", str(checkout), "rev-parse", "HEAD"],
                text=True,
            ).strip()
            fake_bin = root / "bin"
            fake_bin.mkdir()
            docker = fake_bin / "docker"
            docker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            docker.chmod(0o755)
            common = self.production_prism_env(root)
            common.update({
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "QBIT_CHAIN": "mainnet",
                "QBIT_CHAIN_FLAG": "-chain=main",
                "QBIT_EXPECTED_GENESIS_HASH": "11" * 32,
                "QBIT_SRC_DIR": str(checkout),
                "QBIT_SRC_DIR_OVERRIDE": str(checkout),
            })

            matching = self.run_check_env(**{**common, "QBIT_GIT_COMMIT": actual_commit.upper()})
            self.assertEqual(matching.returncode, 0, matching.stderr)
            self.assertIn(f"qbit source checkout verified at {actual_commit}", matching.stdout)

            mismatched = self.run_check_env(**{**common, "QBIT_GIT_COMMIT": "ff" * 20})
            self.assertNotEqual(mismatched.returncode, 0)
            self.assertIn("does not match QBIT_GIT_COMMIT", mismatched.stderr)

    def test_production_testnet4_passes_without_release_provenance_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env = self.production_testnet4_env(Path(temp_dir))

            result = self.run_check_env(**env)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "release provenance not enforced for QBIT_CHAIN=testnet4",
            result.stdout,
        )
        self.assertIn("qbit source checkout verified at", result.stdout)

    def test_release_provenance_opt_in_enforces_on_testnet4(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env = self.production_testnet4_env(Path(temp_dir))

            result = self.run_check_env(**env, QBIT_REQUIRE_RELEASE_PROVENANCE="1")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "release provenance requires QBITD_IMAGE as a digest-qualified image reference",
            result.stderr,
        )
        self.assertNotIn("release provenance not enforced", result.stdout)

    def test_mainnet_release_provenance_cannot_be_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env = self.production_testnet4_env(Path(temp_dir))
            env.update(
                {
                    "QBIT_CHAIN": "mainnet",
                    "QBIT_CHAIN_FLAG": "-chain=main",
                    "QBIT_EXPECTED_GENESIS_HASH": "11" * 32,
                    "BITCOIN_CHAIN": "mainnet",
                    "BITCOIN_CHAIN_FLAG": "-chain=main",
                    "BITCOIN_EXPECTED_GENESIS_HASH": (
                        "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
                    ),
                }
            )
            for opt_out in ("", "0", "false"):
                with self.subTest(opt_out=opt_out):
                    result = self.run_check_env(
                        **env,
                        QBIT_REQUIRE_RELEASE_PROVENANCE=opt_out,
                    )

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(
                        "release provenance requires QBITD_IMAGE as a digest-qualified image reference",
                        result.stderr,
                    )
                    self.assertNotIn("release provenance not enforced", result.stdout)

    def test_release_provenance_opt_in_applies_without_production_mode(self) -> None:
        result = self.run_check_env(
            MINING_LANES="ckpool",
            QBIT_PRODUCTION="0",
            QBIT_CHAIN="testnet4",
            QBIT_CHAIN_FLAG="-testnet4",
            QBIT_REQUIRE_RELEASE_PROVENANCE="1",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "release provenance requires QBITD_IMAGE as a digest-qualified image reference",
            result.stderr,
        )
        self.assertNotIn("docker is required", result.stderr)

    def test_production_rejects_unsafe_prism_difficulty_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            base = self.production_prism_env(root)
            cases = (
                ("PRISM_STRATUM_SHARE_DIFF", "", "requires an explicit PRISM_STRATUM_SHARE_DIFF"),
                (
                    "PRISM_STRATUM_VARDIFF_MIN_DIFF",
                    "",
                    "requires an explicit PRISM_STRATUM_VARDIFF_MIN_DIFF",
                ),
                (
                    "PRISM_STRATUM_VARDIFF_START_DIFF",
                    "",
                    "requires an explicit PRISM_STRATUM_VARDIFF_START_DIFF",
                ),
                (
                    "PRISM_STRATUM_VARDIFF_MAX_DIFF",
                    "",
                    "requires an explicit PRISM_STRATUM_VARDIFF_MAX_DIFF",
                ),
                ("PRISM_STRATUM_SHARE_DIFF", "not-a-decimal", "must be a decimal number"),
                ("PRISM_STRATUM_SHARE_DIFF", "NaN", "must be positive"),
                ("PRISM_STRATUM_SHARE_DIFF", "Infinity", "must be positive"),
                ("PRISM_STRATUM_SHARE_DIFF", "0", "must be positive"),
                ("PRISM_STRATUM_SHARE_DIFF", "-1", "must be positive"),
                ("PRISM_STRATUM_SHARE_DIFF", "0.000000001", "lab-only 1e-9 difficulty"),
                (
                    "PRISM_STRATUM_VARDIFF_MIN_DIFF",
                    "8192",
                    "production vardiff minimum exceeds its start difficulty",
                ),
                (
                    "PRISM_STRATUM_VARDIFF_START_DIFF",
                    "131072",
                    "production vardiff start exceeds its maximum difficulty",
                ),
            )
            for name, value, message in cases:
                with self.subTest(name=name, value=value):
                    deploy_env = root / "difficulty.env"
                    deploy_env.write_text(f"{name}={value}\n", encoding="utf-8")
                    env = {
                        **base,
                        "DEPLOY_ENV_FILE": str(deploy_env),
                        # An empty shell value does not override DEPLOY_ENV_FILE,
                        # including when the deployed value itself is empty.
                        name: "",
                    }

                    result = self.run_check_env(**env)

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(message, result.stderr)
                    self.assertNotIn("docker is required", result.stderr)

    def test_production_requires_commit_pinned_mining_sources(self) -> None:
        common = {
            "MINING_LANES": "ckpool",
            "QBIT_PRODUCTION": "1",
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
            "QBIT_GIT_COMMIT": "41" * 20,
            "QBIT_MINER_ADDRESS": "qb1explicit",
            "QBIT_RPC_PORT_HOST": "127.0.0.1:19552",
            "QBIT_RPC_PASSWORD": "not-default",
            "BITCOIN_RPC_PASSWORD": "not-default",
            "PRISM_DATABASE_URL": "postgresql://example.invalid/qbit",
            "PRISM_POSTGRES_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "42" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "43" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "44" * 32,
            "PRISM_LEDGER_WRITER_ID": "managed-writer",
            "PRISM_LEDGER_WRITER_EPOCH": "7",
            "PRISM_AUDIT_DIR": "/var/lib/qbit/prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit/prism/evidence.json",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "CKPOOL_MINDIFF": "1024",
            "CKPOOL_STARTDIFF": "65536",
        }
        result = self.run_check_env(**common, CKPOOL_GIT_REF="main")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "production mode requires CKPOOL_GIT_REF as exactly 40 hex characters",
            result.stderr,
        )
        self.assertNotIn("docker is required", result.stderr)

    def test_mainnet_parent_requires_peer_bootstrap(self) -> None:
        result = self.run_check_env(
            MINING_LANES="auxpow",
            BITCOIN_CHAIN="mainnet",
            BITCOIN_CHAIN_FLAG="-chain=main",
            BITCOIN_EXPECTED_GENESIS_HASH=(
                "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
            ),
            BITCOIN_DNSSEED="0",
            BITCOIN_NODE_EXTRA_ARGS="",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BITCOIN_CHAIN=mainnet needs parent peer bootstrap", result.stderr)
        self.assertNotIn("docker is required", result.stderr)

    def test_ctv_rejects_non_positive_explicit_fee_rate_before_docker_check(self) -> None:
        for fee_rate in ("0", "-1", "1.5", "invalid"):
            with self.subTest(fee_rate=fee_rate):
                result = self.run_check_env(
                    MINING_LANES="prism",
                    PRISM_CTV_SETTLEMENT_ENABLED="1",
                    PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT=fee_rate,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT must be a positive integer",
                    result.stderr,
                )
                self.assertNotIn("docker is required", result.stderr)


if __name__ == "__main__":
    unittest.main()
