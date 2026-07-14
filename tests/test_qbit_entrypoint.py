#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
ENTRYPOINT = ROOT_DIR / "docker" / "qbit" / "qbit-entrypoint.sh"
RELEVANT_ENV = (
    "QBIT_PRODUCTION",
    "QBIT_TOOLS_PRODUCTION",
    "QBIT_CHAIN",
    "CKPOOL_NON_TEST_READINESS_GATE",
    "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED",
    "QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS",
)


class QbitEntrypointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.bin_dir = Path(self.temp_dir.name) / "bin"
        self.bin_dir.mkdir()
        self.argv_path = Path(self.temp_dir.name) / "argv.json"
        self.ready_path = Path(self.temp_dir.name) / "ready"
        fake_qbitd = self.bin_dir / "qbitd"
        fake_qbitd.write_text(
            """#!/usr/bin/env python3
import json
import os
import signal
import sys
import time

with open(os.environ["FAKE_QBITD_ARGV_PATH"], "w", encoding="utf-8") as handle:
    json.dump(sys.argv[1:], handle)

mode = os.environ.get("FAKE_QBITD_MODE", "exit")
if mode == "wait":
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(42))
    with open(os.environ["FAKE_QBITD_READY_PATH"], "w", encoding="utf-8"):
        pass
    while True:
        time.sleep(0.05)
sys.exit(int(os.environ.get("FAKE_QBITD_EXIT_CODE", "0")))
""",
            encoding="utf-8",
        )
        fake_qbitd.chmod(0o755)

    def environment(self, **overrides: str) -> dict[str, str]:
        env = os.environ.copy()
        for name in RELEVANT_ENV:
            env.pop(name, None)
        env.update(
            {
                "PATH": f"{self.bin_dir}{os.pathsep}{env['PATH']}",
                "FAKE_QBITD_ARGV_PATH": str(self.argv_path),
                "FAKE_QBITD_READY_PATH": str(self.ready_path),
            }
        )
        env.update(overrides)
        return env

    def run_entrypoint(
        self,
        *args: str,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(ENTRYPOINT), *args],
            cwd=ROOT_DIR,
            env=env or self.environment(),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            check=False,
        )

    def captured_argv(self) -> list[str]:
        return json.loads(self.argv_path.read_text(encoding="utf-8"))

    def prelaunch_environment(self, duration: str = "456789") -> dict[str, str]:
        return self.environment(
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
            QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS=duration,
        )

    def test_prelaunch_appends_exactly_one_tip_age_argument(self) -> None:
        result = self.run_entrypoint(
            "-printtoconsole",
            "-listen=1",
            env=self.prelaunch_environment("456789"),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self.captured_argv()
        self.assertEqual(argv, ["-printtoconsole", "-listen=1", "-maxtipage=456789"])
        self.assertEqual(argv.count("-maxtipage=456789"), 1)

    def test_prelaunch_accepts_whitespace_around_launch_flag(self) -> None:
        env = self.prelaunch_environment()
        env["QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"] = " 0\t"

        result = self.run_entrypoint("-listen=1", env=env)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.captured_argv(), ["-listen=1", "-maxtipage=456789"])

    def test_prelaunch_rejects_qbitd_test_chain_arguments(self) -> None:
        for chain_arg in ("-regtest", "-testnet4=1", "-chain=signet"):
            with self.subTest(chain_arg=chain_arg):
                if self.argv_path.exists():
                    self.argv_path.unlink()

                result = self.run_entrypoint(chain_arg, env=self.prelaunch_environment())

                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.argv_path.exists(), "qbitd started on a test chain")
                self.assertIn("selects a test chain", result.stderr)

    def test_strict_launch_rejects_caller_tip_age_argument(self) -> None:
        env = self.prelaunch_environment()
        env["CKPOOL_NON_TEST_READINESS_GATE"] = "1"
        env["QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"] = "1"

        result = self.run_entrypoint(
            "-chain=main",
            "-maxtipage=9223372036854775807",
            env=env,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.argv_path.exists())
        self.assertIn("caller-provided -maxtipage", result.stderr)

    def test_absent_duration_rejects_caller_tip_age_argument(self) -> None:
        result = self.run_entrypoint("-maxtipage", "123")

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.argv_path.exists())
        self.assertIn("caller-provided -maxtipage", result.stderr)

    def test_rejects_double_dash_caller_tip_age_argument(self) -> None:
        result = self.run_entrypoint("--maxtipage=123")

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.argv_path.exists())
        self.assertIn("caller-provided --maxtipage=123", result.stderr)

    def test_prelaunch_rejects_caller_tip_age_before_managed_duplicate(self) -> None:
        result = self.run_entrypoint(
            "-maxtipage=123",
            env=self.prelaunch_environment("456"),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.argv_path.exists())
        self.assertIn("caller-provided -maxtipage=123", result.stderr)

    def test_existing_arguments_remain_separate_and_byte_exact(self) -> None:
        original_args = ["-printtoconsole", "-listen=1", "-server=1", "-uacomment=two words"]

        result = self.run_entrypoint(*original_args, env=self.prelaunch_environment())

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.captured_argv(), [*original_args, "-maxtipage=456789"])

    def test_launched_mode_does_not_append_tip_age_argument(self) -> None:
        env = self.prelaunch_environment()
        env["CKPOOL_NON_TEST_READINESS_GATE"] = "1"
        env["QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"] = "1"

        result = self.run_entrypoint("-listen=1", env=env)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.captured_argv(), ["-listen=1"])

    def test_changing_only_launch_flag_removes_tip_age_argument(self) -> None:
        env = self.prelaunch_environment("987654")
        prelaunch = self.run_entrypoint("-listen=1", env=env)
        self.assertEqual(prelaunch.returncode, 0, prelaunch.stderr)
        self.assertEqual(self.captured_argv(), ["-listen=1", "-maxtipage=987654"])

        self.argv_path.unlink()
        env["CKPOOL_NON_TEST_READINESS_GATE"] = "1"
        env["QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"] = "1"
        launched = self.run_entrypoint("-listen=1", env=env)

        self.assertEqual(launched.returncode, 0, launched.stderr)
        self.assertEqual(self.captured_argv(), ["-listen=1"])

    def test_missing_launch_flag_does_not_append_tip_age_argument(self) -> None:
        env = self.prelaunch_environment()
        env.pop("QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED")

        result = self.run_entrypoint("-listen=1", env=env)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.captured_argv(), ["-listen=1"])

    def test_absent_duration_preserves_legacy_arguments(self) -> None:
        env = self.environment(
            QBIT_PRODUCTION="1",
            QBIT_TOOLS_PRODUCTION="1",
            QBIT_CHAIN="mainnet",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
        )

        result = self.run_entrypoint("-listen=1", "-server=1", env=env)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.captured_argv(), ["-listen=1", "-server=1"])

    def test_invalid_duration_fails_before_qbitd_starts(self) -> None:
        invalid_values = (
            "",
            "0",
            "-1",
            "1;touch /tmp/injected",
            "9223372036854775808",
            "18446744073709551616",
        )
        for value in invalid_values:
            with self.subTest(value=value):
                if self.argv_path.exists():
                    self.argv_path.unlink()

                result = self.run_entrypoint("-listen=1", env=self.prelaunch_environment(value))

                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.argv_path.exists(), "qbitd started with invalid configuration")
                self.assertIn("QBIT_MAINNET_PRELAUNCH_MAX_TIP_AGE_SECONDS", result.stderr)

    def test_prelaunch_tip_age_requires_complete_authorization_tuple(self) -> None:
        cases = (
            ("QBIT_PRODUCTION missing", "QBIT_PRODUCTION", None),
            ("QBIT_PRODUCTION disabled", "QBIT_PRODUCTION", "0"),
            ("QBIT_TOOLS_PRODUCTION missing", "QBIT_TOOLS_PRODUCTION", None),
            ("QBIT_TOOLS_PRODUCTION disabled", "QBIT_TOOLS_PRODUCTION", "0"),
            ("QBIT_CHAIN missing", "QBIT_CHAIN", None),
            ("QBIT_CHAIN incorrect", "QBIT_CHAIN", "signet"),
            ("CKPOOL_NON_TEST_READINESS_GATE missing", "CKPOOL_NON_TEST_READINESS_GATE", None),
            ("CKPOOL_NON_TEST_READINESS_GATE enabled", "CKPOOL_NON_TEST_READINESS_GATE", "1"),
        )
        for label, name, value in cases:
            with self.subTest(case=label):
                if self.argv_path.exists():
                    self.argv_path.unlink()
                env = self.prelaunch_environment()
                if value is None:
                    env.pop(name)
                else:
                    env[name] = value

                result = self.run_entrypoint("-listen=1", env=env)

                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.argv_path.exists(), "qbitd started with incompatible configuration")
                self.assertIn(name, result.stderr)

    def test_malformed_launch_flag_fails_before_qbitd_starts(self) -> None:
        env = self.prelaunch_environment()
        env["QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED"] = "prelaunch"

        result = self.run_entrypoint("-listen=1", env=env)

        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.argv_path.exists())
        self.assertIn("must be a true/false style value", result.stderr)

    def test_qbitd_exit_code_is_propagated(self) -> None:
        env = self.environment(FAKE_QBITD_EXIT_CODE="37")

        result = self.run_entrypoint("-printtoconsole", env=env)

        self.assertEqual(result.returncode, 37)

    def test_signal_reaches_execed_qbitd_process(self) -> None:
        env = self.environment(FAKE_QBITD_MODE="wait")
        process = subprocess.Popen(
            ["bash", str(ENTRYPOINT), "-printtoconsole"],
            cwd=ROOT_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)
        deadline = time.monotonic() + 5
        while not self.ready_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(self.ready_path.exists(), "fake qbitd did not start")

        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=5)

        self.assertEqual(process.returncode, 42)


if __name__ == "__main__":
    unittest.main()
