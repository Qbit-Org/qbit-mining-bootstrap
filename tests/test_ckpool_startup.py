#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT_DIR = Path(__file__).resolve().parents[1]
START_CKPOOL = ROOT_DIR / "docker" / "ckpool" / "start-ckpool.sh"
FAKE_QBIT_RPC = ROOT_DIR / "tests" / "fake_qbit_rpc.py"
PREFLIGHT = ROOT_DIR / "docker" / "ckpool" / "qbit-ckpool-preflight.py"
VERSION_MASK = ROOT_DIR / "docker" / "ckpool" / "ckpool-version-mask.py"
REGTEST_ADDRESS = "qbrt1staticqbitaddress"
VERSION_MASK_PROBE_MARKER = "start-ckpool-test: version mask helper invoked"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class FakeRpcServer:
    def __init__(self, *extra_args: str) -> None:
        self.port = free_port()
        self.extra_args = extra_args
        self.process: subprocess.Popen[str] | None = None

    def __enter__(self) -> "FakeRpcServer":
        self.process = subprocess.Popen(
            [
                sys.executable,
                str(FAKE_QBIT_RPC),
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-requests",
                "0",
                *self.extra_args,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert self.process.stdout is not None
        line = self.process.stdout.readline()
        if "fake qbit RPC listening" not in line:
            raise RuntimeError(f"fake RPC did not start: {line}")
        return self

    def __exit__(self, *_exc: object) -> None:
        assert self.process is not None
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        if self.process.stdout is not None:
            self.process.stdout.close()


class CkpoolStartupTests(unittest.TestCase):
    def run_start_ckpool_raw(
        self,
        tmpdir: Path,
        rpc: FakeRpcServer,
        *,
        real_version_mask: bool = False,
        **overrides: str,
    ) -> tuple[subprocess.CompletedProcess[str], Path]:
        config_file = tmpdir / "ckpool.conf"
        state_dir = tmpdir / "state"
        bin_dir = tmpdir / "bin"
        bin_dir.mkdir()
        version_mask_helper = bin_dir / "ckpool-version-mask"
        if real_version_mask:
            shutil.copy(VERSION_MASK, version_mask_helper)
        else:
            # The stub records every invocation on stderr and in a marker file so
            # tests can assert when the mask is resolved relative to the
            # supervisor's readiness gate.
            version_mask_helper.write_text(
                "#!/usr/bin/env bash\n"
                "if [[ \"${1:-}\" == \"--validate-config\" ]]; then\n"
                "  printf 'stub validate-config\\n' >&2\n"
                "  exit 0\n"
                "fi\n"
                f"printf '{VERSION_MASK_PROBE_MARKER}\\n' >&2\n"
                "printf 'probe\\n' >> \"${CKPOOL_VERSION_MASK_PROBE_LOG}\"\n"
                "printf '%s\\n' \"${CKPOOL_VERSION_MASK_TEST_VALUE:-1fffe000}\"\n",
                encoding="utf-8",
            )
        version_mask_helper.chmod(0o755)
        preflight_helper = bin_dir / "qbit-ckpool-preflight"
        shutil.copy(PREFLIGHT, preflight_helper)
        preflight_helper.chmod(0o755)

        env = os.environ.copy()
        env.pop("CKPOOL_BTCSIG", None)
        env.update(
            {
                "PATH": f"{bin_dir}:{env['PATH']}",
                "QBIT_RPC_USER": "qbitrpc",
                "QBIT_RPC_PASSWORD": "change-this",
                "QBIT_RPC_HOST": "127.0.0.1",
                "QBIT_RPC_PORT": str(rpc.port),
                "QBIT_MINER_ADDRESS": REGTEST_ADDRESS,
                "CKPOOL_BIN": shutil.which("true") or "/usr/bin/true",
                "CKPOOL_CONFIG_FILE": str(config_file),
                "CKPOOL_LOG_DIR": str(tmpdir / "logs"),
                "CKPOOL_STATE_DIR": str(state_dir),
                "QBIT_MINER_ADDRESS_FILE": str(state_dir / "miner-address.txt"),
                "CKPOOL_VERSION_MASK_PROBE_LOG": str(tmpdir / "version-mask-probes.log"),
                "CKPOOL_VERSION_MASK_PROBE_ATTEMPTS": "1",
                "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "0",
            }
        )
        env.update(overrides)

        result = subprocess.run(
            ["bash", str(START_CKPOOL)],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
        )
        return result, config_file

    def run_start_ckpool(
        self,
        tmpdir: Path,
        rpc: FakeRpcServer,
        *,
        real_version_mask: bool = False,
        **overrides: str,
    ) -> dict[str, object]:
        result, config_file = self.run_start_ckpool_raw(
            tmpdir, rpc, real_version_mask=real_version_mask, **overrides
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        with config_file.open(encoding="utf-8") as handle:
            return json.load(handle)

    def test_regtest_defaults_render_fractional_difficulty_floor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer() as rpc:
            config = self.run_start_ckpool(Path(tmp), rpc)

        self.assertEqual(config["btcaddress"], REGTEST_ADDRESS)
        self.assertEqual(config["mindiff"], 0.00390625)
        self.assertEqual(config["startdiff"], 0.00390625)
        self.assertEqual(config["version_mask"], "1fffe000")
        self.assertEqual(config["btcsig"], "/qbit-mining-bootstrap/")

    def test_startup_writes_resolved_address_for_real_miner_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer() as rpc:
            root = Path(tmp)
            self.run_start_ckpool(root, rpc)
            address = (root / "state" / "miner-address.txt").read_text(encoding="utf-8").strip()

        self.assertEqual(address, REGTEST_ADDRESS)

    def test_mainnet_rejects_implicit_address_before_wallet_resolution(self) -> None:
        for address in ("", "auto", " AUTO "):
            with self.subTest(address=address), tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
                "--chain", "main"
            ) as rpc:
                root = Path(tmp)
                result, config_file = self.run_start_ckpool_raw(
                    root,
                    rpc,
                    QBIT_CHAIN="mainnet",
                    QBIT_PRODUCTION="0",
                    QBIT_RPC_PASSWORD="not-default",
                    QBIT_MINER_ADDRESS=address,
                    QBIT_EXPECTED_GENESIS_HASH="11" * 32,
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn("requires an explicit QBIT_MINER_ADDRESS", result.stderr)
                self.assertFalse(config_file.exists())
                self.assertFalse((root / "state" / "miner-address.txt").exists())

    def test_mainnet_rejects_invalid_genesis_before_state_creation(self) -> None:
        cases = {
            "": "QBIT_CHAIN=mainnet requires QBIT_EXPECTED_GENESIS_HASH",
            "not-a-hash": "QBIT_EXPECTED_GENESIS_HASH must be 64 lowercase hex characters",
        }
        for value, expected_error in cases.items():
            with self.subTest(value=value), tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
                "--chain", "main"
            ) as rpc:
                root = Path(tmp)
                result, config_file = self.run_start_ckpool_raw(
                    root,
                    rpc,
                    QBIT_CHAIN="mainnet",
                    QBIT_PRODUCTION="0",
                    QBIT_RPC_PASSWORD="not-default",
                    QBIT_MINER_ADDRESS="qb1staticqbitaddress",
                    QBIT_EXPECTED_GENESIS_HASH=value,
                    CKPOOL_MINDIFF="1024",
                    CKPOOL_STARTDIFF="65536",
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)
                self.assertFalse(config_file.exists())
                self.assertFalse((root / "state").exists())

    def test_explicit_public_difficulty_and_knobs_render(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer("--chain", "testnet4") as rpc:
            config = self.run_start_ckpool(
                Path(tmp),
                rpc,
                QBIT_CHAIN="testnet4",
                QBIT_MINER_ADDRESS="tq1staticqbitaddress",
                CKPOOL_MINDIFF="1024",
                CKPOOL_STARTDIFF="65536",
                CKPOOL_MAXDIFF="4294967296",
                CKPOOL_NOTIFY="true",
                CKPOOL_BTCSIG="/qbit-mainnet-pool/",
                CKPOOL_BLOCKPOLL="5",
                CKPOOL_DONATION="0.25",
                CKPOOL_NONCE1LENGTH="5",
                CKPOOL_NONCE2LENGTH="7",
                CKPOOL_UPDATE_INTERVAL="45",
            )

        self.assertEqual(config["btcd"][0]["notify"], True)
        self.assertEqual(config["btcsig"], "/qbit-mainnet-pool/")
        self.assertEqual(config["blockpoll"], 5)
        self.assertEqual(config["donation"], 0.25)
        self.assertEqual(config["nonce1length"], 5)
        self.assertEqual(config["nonce2length"], 7)
        self.assertEqual(config["update_interval"], 45)
        self.assertEqual(config["mindiff"], 1024)
        self.assertEqual(config["startdiff"], 65536)
        self.assertEqual(config["maxdiff"], 4294967296)

    def test_explicit_empty_btcsig_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer() as rpc:
            config = self.run_start_ckpool(
                Path(tmp),
                rpc,
                CKPOOL_BTCSIG="",
            )

        self.assertEqual(config["btcsig"], "")

    def test_default_btcsig_does_not_inherit_empty_parent_value(self) -> None:
        with mock.patch.dict(os.environ, {"CKPOOL_BTCSIG": ""}):
            with tempfile.TemporaryDirectory() as tmp, FakeRpcServer() as rpc:
                config = self.run_start_ckpool(Path(tmp), rpc)

        self.assertEqual(config["btcsig"], "/qbit-mining-bootstrap/")

    def test_compose_preserves_explicit_empty_btcsig(self) -> None:
        compose = (ROOT_DIR / "compose.yaml").read_text(encoding="utf-8")

        self.assertEqual(
            compose.count(
                'CKPOOL_BTCSIG: "${CKPOOL_BTCSIG-/qbit-mining-bootstrap/}"'
            ),
            1,
        )
        self.assertNotIn(
            "CKPOOL_BTCSIG: ${CKPOOL_BTCSIG:-/qbit-mining-bootstrap/}",
            compose,
        )

    def test_compose_passes_mainnet_launch_readiness_flag_to_qbit_services(self) -> None:
        compose = (ROOT_DIR / "compose.yaml").read_text(encoding="utf-8")

        self.assertEqual(
            compose.count(
                "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED: "
                "${QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED:-}"
            ),
            2,
        )

    def test_compose_passes_supervisor_settings_to_ckpool_service(self) -> None:
        compose = (ROOT_DIR / "compose.yaml").read_text(encoding="utf-8")
        ckpool_service = compose.split("\n  ckpool:\n", 1)[1].split(
            "\n  permissionless-miner:\n", 1
        )[0]
        settings = {
            "CKPOOL_TEMPLATE_MAX_AGE_SECONDS": "120",
            "CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS": "30",
            "CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS": "5",
            "CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS": "120",
            "QBIT_EXPECTED_GENESIS_HASH": "",
            "CKPOOL_VERSION_MASK_PROBE_ATTEMPTS": "3",
            "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "2",
        }

        for name, default in settings.items():
            with self.subTest(name=name):
                self.assertEqual(
                    ckpool_service.count(f"{name}: ${{{name}:-{default}}}"),
                    1,
                )

    def test_production_mainnet_prelaunch_starts_with_explicit_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--chain",
            "main",
            "--initialblockdownload",
            "--reject-gbt-during-ibd",
        ) as rpc:
            result, config_file = self.run_start_ckpool_raw(
                Path(tmp),
                rpc,
                real_version_mask=True,
                CKPOOL_VERSION_MASK_MODE="static",
                QBIT_CHAIN="mainnet",
                QBIT_PRODUCTION="1",
                QBIT_TOOLS_PRODUCTION="1",
                QBIT_RPC_PASSWORD="not-default",
                QBIT_MINER_ADDRESS="qb1staticqbitaddress",
                QBIT_EXPECTED_GENESIS_HASH="00" * 32,
                CKPOOL_MINDIFF="1024",
                CKPOOL_STARTDIFF="65536",
                CKPOOL_NON_TEST_READINESS_GATE="0",
                QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                CKPOOL_REQUIRE_P2MR_PAYOUT="1",
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                result.stderr.count("dynamic getblocktemplate validation deferred"),
                1,
            )
            self.assertIn("initial supervisor checks: PASS", result.stderr)
            with config_file.open(encoding="utf-8") as handle:
                config = json.load(handle)
            self.assertEqual(config["btcaddress"], "qb1staticqbitaddress")

    def test_production_mainnet_launch_rejects_disabled_readiness_before_config_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer("--chain", "main") as rpc:
            result, config_file = self.run_start_ckpool_raw(
                Path(tmp),
                rpc,
                QBIT_CHAIN="mainnet",
                QBIT_PRODUCTION="1",
                QBIT_RPC_PASSWORD="not-default",
                QBIT_MINER_ADDRESS="qb1staticqbitaddress",
                QBIT_EXPECTED_GENESIS_HASH="00" * 32,
                CKPOOL_MINDIFF="1024",
                CKPOOL_STARTDIFF="65536",
                CKPOOL_NON_TEST_READINESS_GATE="0",
                QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
                CKPOOL_REQUIRE_P2MR_PAYOUT="1",
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0", result.stderr)
        self.assertFalse(config_file.exists())

    def test_rpc_preflight_failure_happens_before_state_or_config_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--initialblockdownload",
            "--reject-gbt-during-ibd",
        ) as rpc:
            tmpdir = Path(tmp)
            result, config_file = self.run_start_ckpool_raw(tmpdir, rpc)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("getblocktemplate failed: RPC -10", result.stderr)
            self.assertFalse(config_file.exists())
            self.assertFalse((tmpdir / "state" / "miner-address.txt").exists())

    def test_public_chain_missing_explicit_diff_fails_before_config_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer() as rpc:
            result, config_file = self.run_start_ckpool_raw(
                Path(tmp),
                rpc,
                QBIT_CHAIN="signet",
                QBIT_MINER_ADDRESS="tq1staticqbitaddress",
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires explicit CKPOOL_MINDIFF", result.stderr)
        self.assertFalse(config_file.exists())

    def test_invalid_numeric_knob_fails_before_config_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer() as rpc:
            result, config_file = self.run_start_ckpool_raw(
                Path(tmp),
                rpc,
                CKPOOL_STARTDIFF="NaN",
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CKPOOL_STARTDIFF must be finite", result.stderr)
        self.assertFalse(config_file.exists())

    def test_version_mask_is_not_resolved_before_the_readiness_gate(self) -> None:
        """A cold start must not freeze a mask chosen while qbit is unusable.

        The node here rejects getblocktemplate during initial block download,
        which is exactly the window a container restart lands in. Startup has to
        fail on the supervisor's template validation without ever having asked for a mask.
        """
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--initialblockdownload",
            "--reject-gbt-during-ibd",
        ) as rpc:
            tmpdir = Path(tmp)
            result, config_file = self.run_start_ckpool_raw(tmpdir, rpc)

            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(VERSION_MASK_PROBE_MARKER, result.stderr)
            self.assertFalse((tmpdir / "version-mask-probes.log").exists())
            self.assertFalse(config_file.exists())

    def test_version_mask_is_resolved_after_supervisor_checks_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer() as rpc:
            tmpdir = Path(tmp)
            result, config_file = self.run_start_ckpool_raw(tmpdir, rpc)

            self.assertEqual(result.returncode, 0, result.stderr)
            gate_index = result.stderr.index("initial supervisor checks: PASS")
            probe_index = result.stderr.index(VERSION_MASK_PROBE_MARKER)
            self.assertLess(gate_index, probe_index)
            self.assertEqual(
                (tmpdir / "version-mask-probes.log").read_text(encoding="utf-8").count("probe"),
                1,
            )
            self.assertTrue(config_file.exists())

    def test_cold_start_uses_the_mask_the_ready_node_advertises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--versionrollingmask", "00ffe000"
        ) as rpc:
            config = self.run_start_ckpool(
                Path(tmp),
                rpc,
                real_version_mask=True,
                CKPOOL_VERSION_MASK="1fffe000",
                CKPOOL_VERSION_MASK_MODE="dynamic",
            )

        self.assertEqual(config["version_mask"], "00ffe000")

    def test_cold_start_honours_an_advertised_zero_mask(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--versionrollingmask", "00000000"
        ) as rpc:
            config = self.run_start_ckpool(
                Path(tmp),
                rpc,
                real_version_mask=True,
                CKPOOL_VERSION_MASK="1fffe000",
                CKPOOL_VERSION_MASK_MODE="dynamic",
            )

        self.assertEqual(config["version_mask"], "00000000")

    def test_cold_start_keeps_configured_mask_when_node_omits_the_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--versionrollingmask", ""
        ) as rpc:
            config = self.run_start_ckpool(
                Path(tmp),
                rpc,
                real_version_mask=True,
                CKPOOL_VERSION_MASK="0000007f",
                CKPOOL_VERSION_MASK_MODE="dynamic",
            )

        self.assertEqual(config["version_mask"], "0000007f")

    def test_static_mode_renders_the_configured_mask_without_probing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--versionrollingmask", "00ffe000"
        ) as rpc:
            config = self.run_start_ckpool(
                Path(tmp),
                rpc,
                real_version_mask=True,
                CKPOOL_VERSION_MASK="0x7f",
                CKPOOL_VERSION_MASK_MODE="static",
            )

        self.assertEqual(config["version_mask"], "0000007f")

    def test_malformed_mask_configuration_fails_before_state_creation(self) -> None:
        cases = {
            "CKPOOL_VERSION_MASK": ("not-hex", "invalid fallback CKPOOL_VERSION_MASK"),
            "CKPOOL_VERSION_MASK_MODE": (
                "sometimes",
                "CKPOOL_VERSION_MASK_MODE must be one of",
            ),
        }
        for name, (value, expected_error) in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp, FakeRpcServer() as rpc:
                tmpdir = Path(tmp)
                result, config_file = self.run_start_ckpool_raw(
                    tmpdir,
                    rpc,
                    real_version_mask=True,
                    **{name: value},
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stderr)
                self.assertNotIn("initial supervisor checks: PASS", result.stderr)
                self.assertFalse(config_file.exists())
                self.assertFalse((tmpdir / "state").exists())

    def test_mainnet_prelaunch_requires_static_mode_for_the_version_mask(self) -> None:
        """Prelaunch keeps qbit in IBD, so dynamic resolution has to fail closed.

        Operators who want CKPool to start in that window say so explicitly with
        CKPOOL_VERSION_MASK_MODE=static rather than getting a silent fallback.
        """
        prelaunch = {
            "QBIT_CHAIN": "mainnet",
            "QBIT_PRODUCTION": "1",
            "QBIT_TOOLS_PRODUCTION": "1",
            "QBIT_RPC_PASSWORD": "not-default",
            "QBIT_MINER_ADDRESS": "qb1staticqbitaddress",
            "QBIT_EXPECTED_GENESIS_HASH": "00" * 32,
            "CKPOOL_MINDIFF": "1024",
            "CKPOOL_STARTDIFF": "65536",
            "CKPOOL_NON_TEST_READINESS_GATE": "0",
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "0",
            "CKPOOL_REQUIRE_P2MR_PAYOUT": "1",
            "CKPOOL_VERSION_MASK": "1fffe000",
        }

        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--chain",
            "main",
            "--initialblockdownload",
            "--reject-gbt-during-ibd",
        ) as rpc:
            result, config_file = self.run_start_ckpool_raw(
                Path(tmp),
                rpc,
                real_version_mask=True,
                CKPOOL_VERSION_MASK_MODE="dynamic",
                **prelaunch,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("initial supervisor checks: PASS", result.stderr)
            self.assertIn("getblocktemplate probe failed", result.stderr)
            self.assertFalse(config_file.exists())

        with tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
            "--chain",
            "main",
            "--initialblockdownload",
            "--reject-gbt-during-ibd",
        ) as rpc:
            config = self.run_start_ckpool(
                Path(tmp),
                rpc,
                real_version_mask=True,
                CKPOOL_VERSION_MASK_MODE="static",
                **prelaunch,
            )

        self.assertEqual(config["version_mask"], "1fffe000")


if __name__ == "__main__":
    unittest.main()
