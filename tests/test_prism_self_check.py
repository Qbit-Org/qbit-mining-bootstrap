#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import os
import socket
import sys
import threading
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import patch


def load_self_check_module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "prism-self-check.py"
    spec = importlib.util.spec_from_file_location("prism_self_check", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class PrismSelfCheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.self_check = load_self_check_module()

    def valid_env(self) -> dict[str, str]:
        return {
            "QBIT_CHAIN": "signet",
            "QBIT_CHAIN_FLAG": "-signet",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "11" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "22" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "33" * 32,
            "PRISM_LEDGER_WRITER_ID": "prism-coordinator",
            "PRISM_LEDGER_WRITER_EPOCH": "1",
            "PRISM_DATABASE_URL": "postgresql://qbit:secret@prism-postgres:5432/qbit",
            "PRISM_AUDIT_DIR": "/var/lib/qbit-prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit-prism/audit/prism-live-evidence.json",
            "PRISM_BLOCKWAIT_ENABLED": "1",
            "PRISM_BLOCKWAIT_TIMEOUT_SECONDS": "5",
            "PRISM_ALLOW_MEMORY_LEDGER": "0",
            "PRISM_ALLOW_TEST_SIGNING_SEEDS": "0",
            "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "0",
            "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN": "0",
            "PRISM_MIN_READY_MINERS": "3",
            "PRISM_STRATUM_SHARE_DIFF": "0.000000001",
            "PRISM_STRATUM_VARDIFF": "1",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "0.000000001",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "0.000000001",
            "PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS": "15",
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "3",
            "PRISM_WORKER_METRICS_LIMIT": "100",
            "PRISM_POOL_FEE_ENABLED": "0",
        }

    def test_parse_host_port_accepts_port_only_and_host_port(self) -> None:
        self.assertEqual(
            self.self_check.parse_host_port("3340", default_host="127.0.0.1", default_port=3340),
            ("127.0.0.1", 3340),
        )
        self.assertEqual(
            self.self_check.parse_host_port("0.0.0.0:43340", default_host="127.0.0.1", default_port=3340),
            ("0.0.0.0", 43340),
        )

    def test_compose_commands_include_prism_profile(self) -> None:
        command = self.self_check.compose_base_command()

        self.assertIn("--profile", command)
        self.assertEqual(command[command.index("--profile") + 1], "prism")

    def test_env_value_prefers_compose_resolved_value_over_host_env(self) -> None:
        with patch.dict(os.environ, {"QBIT_CHAIN": "mainnet"}):
            self.assertEqual(
                self.self_check.env_value({"QBIT_CHAIN": "signet"}, "QBIT_CHAIN", "regtest"),
                "signet",
            )
            self.assertEqual(
                self.self_check.env_value({}, "QBIT_CHAIN", "regtest"),
                "regtest",
            )

    def test_static_checks_accept_valid_prism_operator_env(self) -> None:
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(self.valid_env(), reporter)

        self.assertFalse(reporter.failed)

    def test_static_checks_fail_testnet_chain_flag_mismatch(self) -> None:
        env = self.valid_env()
        env["QBIT_CHAIN"] = "testnet"
        env["QBIT_CHAIN_FLAG"] = "-regtest"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "qbit.chain_flag"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_static_checks_fail_production_test_bypass(self) -> None:
        env = self.valid_env()
        env["QBIT_PRODUCTION"] = "1"
        env["PRISM_ALLOW_TEST_SIGNING_SEEDS"] = "1"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "env.PRISM_ALLOW_TEST_SIGNING_SEEDS"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_highdiff_probe_target_uses_published_host_port_only(self) -> None:
        # The published host mapping is the only valid probe target: falling
        # back to the container listen port could pass while miners cannot
        # reach the listener.
        self.assertEqual(
            self.self_check.highdiff_probe_target({"PRISM_STRATUM_HIGHDIFF_PORT_HOST": "4334"}),
            ("127.0.0.1", 4334),
        )
        self.assertEqual(
            self.self_check.highdiff_probe_target(
                {"PRISM_STRATUM_HIGHDIFF_PORT_HOST": "0.0.0.0:14334"}
            ),
            ("0.0.0.0", 14334),
        )
        # Unset, empty, and the disabled-default ephemeral loopback mapping all
        # mean "not published".
        self.assertIsNone(self.self_check.highdiff_probe_target({}))
        self.assertIsNone(
            self.self_check.highdiff_probe_target({"PRISM_STRATUM_HIGHDIFF_PORT_HOST": ""})
        )
        self.assertIsNone(
            self.self_check.highdiff_probe_target(
                {"PRISM_STRATUM_HIGHDIFF_PORT_HOST": "127.0.0.1:0"}
            )
        )

    def test_static_checks_accept_highdiff_with_empty_share_diff(self) -> None:
        # Compose resolves the fixed difficulty default to an empty string; the
        # coordinator treats that as "track the start difficulty" and the
        # self-check must agree instead of failing mining.highdiff.
        env = self.valid_env()
        env["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
        env["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = ""
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertFalse(reporter.failed)
        self.assertIn(
            "PASS",
            {row.status for row in reporter.rows if row.name == "mining.highdiff"},
        )

    def test_static_checks_fail_highdiff_share_diff_below_floor(self) -> None:
        env = self.valid_env()
        env["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
        env["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = "1000"
        reporter = self.self_check.Reporter()

        self.self_check.static_checks(env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "mining.highdiff"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_ready_miner_threshold_fails_when_below_minimum(self) -> None:
        env = self.valid_env()
        env["PRISM_MIN_READY_MINERS"] = "3"
        reporter = self.self_check.Reporter()

        self.self_check.check_ready_miner_threshold({"ready_miner_count": 2}, env, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "coordinator.ready_miners"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_ready_miner_threshold_passes_when_minimum_met(self) -> None:
        env = self.valid_env()
        env["PRISM_MIN_READY_MINERS"] = "3"
        reporter = self.self_check.Reporter()

        self.self_check.check_ready_miner_threshold({"ready_miner_count": 3}, env, reporter)

        self.assertFalse(reporter.failed)
        self.assertIn(
            ("PASS", "coordinator.ready_miners"),
            {(row.status, row.name) for row in reporter.rows},
        )

    def test_public_chain_peer_rpc_error_is_hard_failure(self) -> None:
        reporter = self.self_check.Reporter()

        def fake_qbit_rpc_call(env: dict[str, str], method: str) -> object:
            if method == "getblockchaininfo":
                return {"chain": "signet", "initialblockdownload": False}
            if method == "getnetworkinfo":
                raise RuntimeError("rpc unavailable")
            raise AssertionError(method)

        with patch.object(self.self_check, "qbit_rpc_call", fake_qbit_rpc_call):
            self.self_check.qbit_live_checks({"QBIT_CHAIN": "signet"}, reporter)

        self.assertTrue(reporter.failed)
        self.assertIn(
            ("FAIL", "qbit.peers"),
            {(row.status, row.name) for row in reporter.rows},
        )


class HighdiffFloorProbeTests(unittest.TestCase):
    """The live high-diff check must judge the first advertised difficulty,
    exactly like a marketplace verification probe, not mere reachability."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.self_check = load_self_check_module()

    def fake_stratum_server(
        self,
        notifications: list[dict[str, object]],
        *,
        reject_authorize: bool = False,
    ) -> int:
        """One-shot stratum server: answers subscribe/authorize, then emits
        the scripted notifications and closes. Returns the listening port."""
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        server_sock.settimeout(5)
        port = server_sock.getsockname()[1]

        def serve() -> None:
            try:
                conn, _ = server_sock.accept()
            except OSError:
                return
            with conn:
                conn.settimeout(5)
                reader = conn.makefile("rb")
                for _ in range(2):  # subscribe + authorize requests
                    reader.readline()
                responses: list[dict[str, object]] = [
                    {"id": 1, "result": [[], "00000001", 8], "error": None}
                ]
                if reject_authorize:
                    responses.append({"id": 2, "result": None, "error": [20, "unauthorized", None]})
                else:
                    responses.append({"id": 2, "result": True, "error": None})
                    responses.extend(notifications)
                for message in responses:
                    conn.sendall((json.dumps(message) + "\n").encode())

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        self.addCleanup(server_sock.close)
        self.addCleanup(lambda: thread.join(timeout=5))
        return port

    def probe_env(self) -> dict[str, str]:
        return {"PRISM_STRATUM_HIGHDIFF_MIN_DIFF": "500000"}

    def floor_rows(self, reporter: object) -> list[object]:
        return [row for row in reporter.rows if row.name == "stratum.highdiff_floor"]

    def test_first_difficulty_at_floor_passes(self) -> None:
        port = self.fake_stratum_server(
            [{"id": None, "method": "mining.set_difficulty", "params": [500000.0]}]
        )
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(self.probe_env(), "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertEqual([row.status for row in rows], ["PASS"])

    def test_first_difficulty_below_floor_fails(self) -> None:
        # The regression this guards: a young chain dragging the first
        # advertised difficulty below the floor. A later compliant value must
        # not rescue the check -- marketplaces judge the first one.
        port = self.fake_stratum_server(
            [
                {"id": None, "method": "mining.set_difficulty", "params": [4.6565423739069247e-10]},
                {"id": None, "method": "mining.set_difficulty", "params": [500000.0]},
            ]
        )
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(self.probe_env(), "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertEqual([row.status for row in rows], ["FAIL"])
        self.assertIn("below", rows[0].detail)

    def test_rejected_authorize_fails_with_handshake_detail(self) -> None:
        port = self.fake_stratum_server([], reject_authorize=True)
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(self.probe_env(), "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertEqual([row.status for row in rows], ["FAIL"])
        self.assertIn("authorize rejected", rows[0].detail)

    def test_connection_closed_without_difficulty_fails(self) -> None:
        port = self.fake_stratum_server([{"id": None, "method": "mining.notify", "params": []}])
        reporter = self.self_check.Reporter()

        self.self_check.check_highdiff_advertised_floor(self.probe_env(), "127.0.0.1", port, reporter)

        rows = self.floor_rows(reporter)
        self.assertEqual([row.status for row in rows], ["FAIL"])
        self.assertIn("mining.set_difficulty", rows[0].detail)


if __name__ == "__main__":
    unittest.main()
