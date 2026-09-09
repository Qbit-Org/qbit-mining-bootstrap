#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import http.client
import io
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock
from urllib import error

from tests.test_ckpool_startup import FakeRpcServer, free_port


SCRIPT_PATH = Path(__file__).parents[1] / "docker" / "ckpool" / "ckpool-version-mask.py"
SPEC = importlib.util.spec_from_file_location("ckpool_version_mask", SCRIPT_PATH)
assert SPEC is not None
ckpool_version_mask = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = ckpool_version_mask
SPEC.loader.exec_module(ckpool_version_mask)


class CkpoolVersionMaskTests(unittest.TestCase):
    def test_selects_advertised_qbit_versionrollingmask(self) -> None:
        result = ckpool_version_mask.select_version_mask(
            {"versionrollingmask": "1fffe000"},
            "0000007f",
        )

        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.source, "qbit_getblocktemplate")
        self.assertEqual(result.detail, "advertised")

    def test_missing_advertised_mask_falls_back_to_configured_mask(self) -> None:
        result = ckpool_version_mask.select_version_mask({}, "1fffe000")

        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.source, "fallback")
        self.assertEqual(result.detail, "missing_versionrollingmask")

    def test_zero_advertised_mask_disables_version_rolling(self) -> None:
        result = ckpool_version_mask.select_version_mask(
            {"versionrollingmask": "00000000"},
            "1fffe000",
        )

        self.assertEqual(result.selected_mask, "00000000")
        self.assertEqual(result.source, "qbit_getblocktemplate")
        self.assertEqual(result.detail, "disabled_by_zero_mask")

    def test_invalid_advertised_mask_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid getblocktemplate.versionrollingmask"):
            ckpool_version_mask.select_version_mask(
                {"versionrollingmask": "not-hex"},
                "1fffe000",
            )

    def test_normalizes_integer_and_short_hex_masks(self) -> None:
        self.assertEqual(ckpool_version_mask.normalize_mask(0x1FFFE000, field="mask"), "1fffe000")
        self.assertEqual(ckpool_version_mask.normalize_mask("ff", field="mask"), "000000ff")
        self.assertEqual(ckpool_version_mask.normalize_mask("0x1fffe000", field="mask"), "1fffe000")

    def test_rejects_invalid_fallback_mask(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid fallback CKPOOL_VERSION_MASK"):
            ckpool_version_mask.select_version_mask({}, "not-hex")

    def test_accepts_current_production_fallback_mask(self) -> None:
        result = ckpool_version_mask.select_version_mask({}, "1fffe000")

        self.assertEqual(result.selected_mask, "1fffe000")

    def test_mode_parsing(self) -> None:
        self.assertTrue(ckpool_version_mask.mode_is_dynamic("dynamic"))
        self.assertTrue(ckpool_version_mask.mode_is_dynamic("auto"))
        self.assertFalse(ckpool_version_mask.mode_is_dynamic("static"))
        self.assertFalse(ckpool_version_mask.mode_is_dynamic("off"))
        with self.assertRaisesRegex(ValueError, "CKPOOL_VERSION_MASK_MODE"):
            ckpool_version_mask.mode_is_dynamic("sometimes")

    def test_signet_gbt_rules_include_signet(self) -> None:
        self.assertEqual(ckpool_version_mask.gbt_rules("signet"), ["segwit", "signet"])
        self.assertEqual(ckpool_version_mask.gbt_rules("testnet4"), ["segwit"])

    def test_static_mode_uses_configured_mask_without_probing(self) -> None:
        env = {
            "CKPOOL_VERSION_MASK_MODE": "static",
            "CKPOOL_VERSION_MASK": "0x1fffe000",
        }
        with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
            ckpool_version_mask, "rpc_getblocktemplate"
        ) as probe:
            result = ckpool_version_mask.resolve_from_env()

        probe.assert_not_called()
        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.source, "fallback")
        self.assertEqual(result.detail, "static_mode")

    def test_static_mode_rejects_invalid_configured_mask(self) -> None:
        env = {
            "CKPOOL_VERSION_MASK_MODE": "static",
            "CKPOOL_VERSION_MASK": "not-hex",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            with self.assertRaisesRegex(ValueError, "CKPOOL_VERSION_MASK"):
                ckpool_version_mask.resolve_from_env()

    def test_dynamic_mode_fails_closed_on_permanent_probe_failure(self) -> None:
        env = {
            "CKPOOL_VERSION_MASK_MODE": "dynamic",
            "CKPOOL_VERSION_MASK": "1fffe000",
            "CKPOOL_VERSION_MASK_PROBE_ATTEMPTS": "2",
            "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "0",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "secret",
        }
        sleeps: list[float] = []
        with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
            ckpool_version_mask,
            "rpc_getblocktemplate",
            side_effect=error.URLError("connection refused"),
        ) as probe:
            with self.assertRaisesRegex(
                ckpool_version_mask.ProbeError, "after 2 attempt"
            ):
                ckpool_version_mask.resolve_from_env(sleep=sleeps.append)

        self.assertEqual(probe.call_count, 2)

    def test_dynamic_mode_retries_transient_failure_then_succeeds(self) -> None:
        env = {
            "CKPOOL_VERSION_MASK_MODE": "dynamic",
            "CKPOOL_VERSION_MASK": "0000007f",
            "CKPOOL_VERSION_MASK_PROBE_ATTEMPTS": "3",
            "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "0.25",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "secret",
        }
        attempts = [
            OSError("temporary failure in name resolution"),
            RuntimeError({"code": -10, "message": "still warming up"}),
            {"versionrollingmask": "1fffe000"},
        ]

        def probe(**_kwargs: object) -> dict[str, object]:
            outcome = attempts.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        sleeps: list[float] = []
        with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
            ckpool_version_mask, "rpc_getblocktemplate", side_effect=probe
        ):
            result = ckpool_version_mask.resolve_from_env(sleep=sleeps.append)

        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.source, "qbit_getblocktemplate")
        self.assertEqual(result.detail, "advertised")
        self.assertEqual(sleeps, [0.25, 0.25])

    def test_dynamic_mode_does_not_retry_missing_rpc_credentials(self) -> None:
        env = {
            "CKPOOL_VERSION_MASK_MODE": "dynamic",
            "CKPOOL_VERSION_MASK": "1fffe000",
        }
        with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
            ckpool_version_mask, "rpc_getblocktemplate"
        ) as probe:
            os.environ.pop("QBIT_RPC_USER", None)
            os.environ.pop("QBIT_RPC_PASSWORD", None)
            with self.assertRaisesRegex(ValueError, "QBIT_RPC_USER is required"):
                ckpool_version_mask.resolve_from_env()

        probe.assert_not_called()

    def test_dynamic_mode_does_not_retry_invalid_advertised_mask(self) -> None:
        env = {
            "CKPOOL_VERSION_MASK_MODE": "dynamic",
            "CKPOOL_VERSION_MASK": "1fffe000",
            "CKPOOL_VERSION_MASK_PROBE_ATTEMPTS": "5",
            "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "0",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "secret",
        }
        with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
            ckpool_version_mask,
            "rpc_getblocktemplate",
            return_value={"versionrollingmask": "not-hex"},
        ) as probe:
            with self.assertRaisesRegex(
                ValueError, "invalid getblocktemplate.versionrollingmask"
            ):
                ckpool_version_mask.resolve_from_env()

        self.assertEqual(probe.call_count, 1)

    def test_dynamic_mode_keeps_zero_and_missing_field_after_successful_probe(self) -> None:
        env = {
            "CKPOOL_VERSION_MASK_MODE": "dynamic",
            "CKPOOL_VERSION_MASK": "1fffe000",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "secret",
        }
        with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
            ckpool_version_mask, "rpc_getblocktemplate",
            return_value={"versionrollingmask": "00000000"},
        ):
            result = ckpool_version_mask.resolve_from_env()
        self.assertEqual(result.selected_mask, "00000000")
        self.assertEqual(result.detail, "disabled_by_zero_mask")

        with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
            ckpool_version_mask, "rpc_getblocktemplate", return_value={}
        ):
            result = ckpool_version_mask.resolve_from_env()

        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.source, "fallback")
        self.assertEqual(result.detail, "missing_versionrollingmask")

    def test_probe_settings_reject_invalid_or_unbounded_values_without_rpc(self) -> None:
        cases = {
            "CKPOOL_VERSION_MASK_RPC_TIMEOUT_SECONDS": ["soon", "0", "-1", "inf", "nan", "1000"],
            "CKPOOL_VERSION_MASK_PROBE_ATTEMPTS": ["many", "0", "-1", "1.5", "1000000000"],
            "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": ["later", "-1", "inf", "nan", "1000"],
        }
        for name, values in cases.items():
            for value in values:
                with self.subTest(name=name, value=value), mock.patch.dict(
                    os.environ, {name: value}, clear=True
                ), mock.patch.object(ckpool_version_mask, "rpc_getblocktemplate") as probe:
                    with self.assertRaises(ValueError):
                        ckpool_version_mask.validate_config()
                    probe.assert_not_called()

    def test_probe_settings_default_and_accept_zero_retry_delay(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(ckpool_version_mask.probe_settings(), (5.0, 3, 2.0))
        with mock.patch.dict(os.environ, {"CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "0"}, clear=True):
            self.assertEqual(ckpool_version_mask.probe_settings(), (5.0, 3, 0.0))

    def test_incomplete_http_response_is_retried(self) -> None:
        env = {
            "QBIT_RPC_USER": "test-user", "QBIT_RPC_PASSWORD": "test-password",
            "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "0",
        }
        with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(
            ckpool_version_mask, "rpc_getblocktemplate",
            side_effect=[http.client.IncompleteRead(b"partial"), {"versionrollingmask": "1fffe000"}],
        ) as probe:
            self.assertEqual(ckpool_version_mask.resolve_from_env().selected_mask, "1fffe000")
        self.assertEqual(probe.call_count, 2)

    def test_non_object_rpc_response_fails_cleanly_and_auth_refusal_is_not_retried(self) -> None:
        env = {
            "QBIT_RPC_USER": "test-user", "QBIT_RPC_PASSWORD": "test-password",
            "CKPOOL_VERSION_MASK_PROBE_ATTEMPTS": "2",
            "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "0",
        }
        with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(
            ckpool_version_mask.request, "urlopen", side_effect=lambda *_a, **_kw: io.BytesIO(b"[]")
        ) as request_mock:
            with self.assertRaisesRegex(ckpool_version_mask.ProbeError, "response was not an object"):
                ckpool_version_mask.resolve_from_env()
        self.assertEqual(request_mock.call_count, 2)
        for status in (401, 403):
            with self.subTest(status=status), mock.patch.dict(os.environ, env, clear=True), mock.patch.object(
                ckpool_version_mask.request, "urlopen",
                side_effect=error.HTTPError("http://qbitd", status, "refused", {}, None),
            ) as request_mock:
                with self.assertRaisesRegex(ValueError, "RPC authentication refused"):
                    ckpool_version_mask.resolve_from_env()
            self.assertEqual(request_mock.call_count, 1)

    def test_mainnet_prelaunch_dynamic_mode_still_fails_closed(self) -> None:
        """Explicit static mode, not a hidden fallback, is the prelaunch escape.

        The authorized mainnet prelaunch combination keeps the node in initial
        block download on purpose, so getblocktemplate is unavailable. Dynamic
        mode must refuse to invent a mask there; an operator who wants CKPool to
        start anyway has to say so with CKPOOL_VERSION_MASK_MODE=static.
        """
        env = {
            "QBIT_CHAIN": "mainnet",
            "QBIT_PRODUCTION": "1",
            "QBIT_TOOLS_PRODUCTION": "1",
            "CKPOOL_NON_TEST_READINESS_GATE": "0",
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "0",
            "CKPOOL_VERSION_MASK": "1fffe000",
            "CKPOOL_VERSION_MASK_PROBE_ATTEMPTS": "1",
            "CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS": "0",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "secret",
        }
        gbt_unavailable = RuntimeError(
            {"code": -10, "message": "qbit is in initial sync and waiting for blocks"}
        )

        with mock.patch.dict(
            os.environ, {**env, "CKPOOL_VERSION_MASK_MODE": "dynamic"}, clear=False
        ), mock.patch.object(
            ckpool_version_mask, "rpc_getblocktemplate", side_effect=gbt_unavailable
        ):
            with self.assertRaises(ckpool_version_mask.ProbeError):
                ckpool_version_mask.resolve_from_env()

        with mock.patch.dict(
            os.environ, {**env, "CKPOOL_VERSION_MASK_MODE": "static"}, clear=False
        ), mock.patch.object(
            ckpool_version_mask, "rpc_getblocktemplate", side_effect=gbt_unavailable
        ) as probe:
            result = ckpool_version_mask.resolve_from_env()

        probe.assert_not_called()
        self.assertEqual(result.selected_mask, "1fffe000")
        self.assertEqual(result.detail, "static_mode")


class CkpoolVersionMaskCliTests(unittest.TestCase):
    """Exercise the real helper the way startup and other consumers invoke it."""

    def run_helper(self, *args: str, **overrides: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        for name in (
            "CKPOOL_VERSION_MASK",
            "CKPOOL_VERSION_MASK_MODE",
            "QBIT_RPC_USER",
            "QBIT_RPC_PASSWORD",
            "QBIT_CHAIN",
        ):
            env.pop(name, None)
        env.update(overrides)
        return subprocess.run(
            [sys.executable, str(SCRIPT_PATH), *args],
            env=env,
            text=True,
            capture_output=True,
            timeout=30,
        )

    def test_static_mode_prints_mask_on_stdout_and_exits_zero(self) -> None:
        result = self.run_helper(
            CKPOOL_VERSION_MASK_MODE="static",
            CKPOOL_VERSION_MASK="ff",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "000000ff")
        self.assertIn("selected=000000ff source=fallback detail=static_mode", result.stderr)

    def test_dynamic_mode_unreachable_node_exits_nonzero_without_stdout(self) -> None:
        result = self.run_helper(
            CKPOOL_VERSION_MASK_MODE="dynamic",
            CKPOOL_VERSION_MASK="1fffe000",
            CKPOOL_VERSION_MASK_PROBE_ATTEMPTS="2",
            CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS="0",
            CKPOOL_VERSION_MASK_RPC_TIMEOUT_SECONDS="1",
            QBIT_RPC_USER="qbitrpc",
            QBIT_RPC_PASSWORD="secret",
            QBIT_RPC_HOST="127.0.0.1",
            QBIT_RPC_PORT=str(free_port()),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "")
        self.assertIn("getblocktemplate probe failed after 2 attempt", result.stderr)
        self.assertNotIn("1fffe000\n", result.stdout)

    def test_validate_config_performs_no_rpc_and_reports_configuration_errors(self) -> None:
        ok = self.run_helper(
            "--validate-config",
            CKPOOL_VERSION_MASK_MODE="dynamic",
            CKPOOL_VERSION_MASK="1fffe000",
            QBIT_RPC_HOST="127.0.0.1",
            QBIT_RPC_PORT=str(free_port()),
        )

        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertEqual(ok.stdout.strip(), "")
        self.assertIn("config ok mode=dynamic configured=1fffe000", ok.stderr)

        bad_mask = self.run_helper(
            "--validate-config",
            CKPOOL_VERSION_MASK_MODE="dynamic",
            CKPOOL_VERSION_MASK="not-hex",
        )
        self.assertNotEqual(bad_mask.returncode, 0)
        self.assertIn("invalid fallback CKPOOL_VERSION_MASK", bad_mask.stderr)

        bad_mode = self.run_helper(
            "--validate-config",
            CKPOOL_VERSION_MASK_MODE="sometimes",
            CKPOOL_VERSION_MASK="1fffe000",
        )
        self.assertNotEqual(bad_mode.returncode, 0)
        self.assertIn("CKPOOL_VERSION_MASK_MODE must be one of", bad_mode.stderr)

    def test_dynamic_mode_resolves_advertised_mask_from_a_live_node(self) -> None:
        with FakeRpcServer("--versionrollingmask", "00ffe000") as rpc:
            result = self.run_helper(
                CKPOOL_VERSION_MASK_MODE="dynamic",
                CKPOOL_VERSION_MASK="1fffe000",
                QBIT_RPC_USER="qbitrpc",
                QBIT_RPC_PASSWORD="secret",
                QBIT_RPC_HOST="127.0.0.1",
                QBIT_RPC_PORT=str(rpc.port),
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "00ffe000")
        self.assertIn("source=qbit_getblocktemplate detail=advertised", result.stderr)

    def test_dynamic_mode_honours_a_live_zero_mask(self) -> None:
        with FakeRpcServer("--versionrollingmask", "00000000") as rpc:
            result = self.run_helper(
                CKPOOL_VERSION_MASK_MODE="dynamic",
                CKPOOL_VERSION_MASK="1fffe000",
                QBIT_RPC_USER="qbitrpc",
                QBIT_RPC_PASSWORD="secret",
                QBIT_RPC_HOST="127.0.0.1",
                QBIT_RPC_PORT=str(rpc.port),
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "00000000")
        self.assertIn("detail=disabled_by_zero_mask", result.stderr)

    def test_dynamic_mode_falls_back_only_when_a_live_node_omits_the_field(self) -> None:
        with FakeRpcServer("--versionrollingmask", "") as rpc:
            result = self.run_helper(
                CKPOOL_VERSION_MASK_MODE="dynamic",
                CKPOOL_VERSION_MASK="0000007f",
                QBIT_RPC_USER="qbitrpc",
                QBIT_RPC_PASSWORD="secret",
                QBIT_RPC_HOST="127.0.0.1",
                QBIT_RPC_PORT=str(rpc.port),
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "0000007f")
        self.assertIn("source=fallback detail=missing_versionrollingmask", result.stderr)

    def test_dynamic_mode_fails_closed_while_the_node_rejects_templates(self) -> None:
        with FakeRpcServer(
            "--initialblockdownload", "--reject-gbt-during-ibd"
        ) as rpc:
            result = self.run_helper(
                CKPOOL_VERSION_MASK_MODE="dynamic",
                CKPOOL_VERSION_MASK="1fffe000",
                CKPOOL_VERSION_MASK_PROBE_ATTEMPTS="2",
                CKPOOL_VERSION_MASK_PROBE_RETRY_SECONDS="0",
                QBIT_RPC_USER="qbitrpc",
                QBIT_RPC_PASSWORD="secret",
                QBIT_RPC_HOST="127.0.0.1",
                QBIT_RPC_PORT=str(rpc.port),
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "")
        self.assertIn("initial sync", result.stderr)


if __name__ == "__main__":
    unittest.main()
