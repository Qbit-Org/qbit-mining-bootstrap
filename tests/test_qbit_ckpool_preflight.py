#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest import mock


SCRIPT_PATH = Path(__file__).parents[1] / "docker" / "ckpool" / "qbit-ckpool-preflight.py"
SPEC = importlib.util.spec_from_file_location("qbit_ckpool_preflight", SCRIPT_PATH)
assert SPEC is not None
preflight = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = preflight
SPEC.loader.exec_module(preflight)


class FakeRpc:
    def __init__(
        self,
        *,
        ibd: Any = False,
        rpc_chain: str = "regtest",
        connections: int | list[int] = 1,
        weightlimit: int = 2_000_000,
        template_rules: list[str] | None = None,
        address_valid: bool = True,
        iswitness: bool = True,
        witness_version: int | None = 2,
        template_error: Exception | None = None,
        genesis_hash: str = "0" * 64,
        best_block_hash: str = "1" * 64,
        previous_block_hash: str | None = None,
        template_time: int | None = None,
    ) -> None:
        self.ibd = ibd
        self.rpc_chain = rpc_chain
        self.connections = [connections] if isinstance(connections, int) else connections
        self.network_info_calls = 0
        self.weightlimit = weightlimit
        self.template_rules = template_rules if template_rules is not None else ["segwit"]
        self.address_valid = address_valid
        self.iswitness = iswitness
        self.witness_version = witness_version
        self.template_error = template_error
        self.genesis_hash = genesis_hash
        self.best_block_hash = best_block_hash
        self.previous_block_hash = previous_block_hash or best_block_hash
        self.template_time = int(time.time()) if template_time is None else template_time
        self.calls: list[tuple[str, list[Any]]] = []

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        params = params or []
        self.calls.append((method, params))
        if method == "getblockchaininfo":
            return {"chain": self.rpc_chain, "initialblockdownload": self.ibd}
        if method == "getnetworkinfo":
            index = min(self.network_info_calls, len(self.connections) - 1)
            self.network_info_calls += 1
            return {"connections": self.connections[index]}
        if method == "getblocktemplate":
            if self.template_error is not None:
                raise self.template_error
            return {
                "rules": self.template_rules,
                "weightlimit": self.weightlimit,
                "previousblockhash": self.previous_block_hash,
                "curtime": self.template_time,
            }
        if method == "getblockhash":
            return self.genesis_hash
        if method == "getbestblockhash":
            return self.best_block_hash
        if method == "validateaddress":
            return {
                "isvalid": self.address_valid,
                "address": params[0],
                "iswitness": self.iswitness,
                "witness_version": self.witness_version,
            }
        raise AssertionError(f"unexpected RPC method {method}")


def base_env(**overrides: str) -> dict[str, str]:
    env = {
        "QBIT_CHAIN": "regtest",
        "QBIT_RPC_USER": "qbitrpc",
        "QBIT_RPC_PASSWORD": "change-this",
        "QBIT_MINER_ADDRESS": "qbrt1miner",
        "CKPOOL_NOTIFY": "false",
        "CKPOOL_BLOCKPOLL": "2",
        "CKPOOL_DONATION": "0.0",
        "CKPOOL_NONCE1LENGTH": "4",
        "CKPOOL_NONCE2LENGTH": "8",
        "CKPOOL_UPDATE_INTERVAL": "30",
        "CKPOOL_MINDIFF": "0.00390625",
        "CKPOOL_STARTDIFF": "0.00390625",
        "CKPOOL_MAXDIFF": "",
        "CKPOOL_MINDIFF_EXPLICIT": "0",
        "CKPOOL_STARTDIFF_EXPLICIT": "0",
        "CKPOOL_PUBLIC_DIFF_POLICY": "explicit",
    }
    env.update(overrides)
    return env


def production_env(**overrides: str) -> dict[str, str]:
    env = base_env(
        QBIT_PRODUCTION="1",
        QBIT_CHAIN="testnet4",
        QBIT_RPC_PASSWORD="not-default",
        QBIT_MINER_ADDRESS="tq1miner",
        CKPOOL_MINDIFF="1024",
        CKPOOL_STARTDIFF="65536",
        CKPOOL_MINDIFF_EXPLICIT="1",
        CKPOOL_STARTDIFF_EXPLICIT="1",
        CKPOOL_REQUIRE_P2MR_PAYOUT="1",
        CKPOOL_STRATUM_PORT="3333",
    )
    env.update(overrides)
    return env


def mainnet_production_env(**overrides: str) -> dict[str, str]:
    env = production_env(
        QBIT_CHAIN="mainnet",
        QBIT_TOOLS_PRODUCTION="1",
        QBIT_MINER_ADDRESS="qb1miner",
    )
    env.update(overrides)
    return env


class QbitCkpoolPreflightTests(unittest.TestCase):
    def test_no_argument_cli_preserves_full_preflight(self) -> None:
        rpc = FakeRpc()
        stderr = io.StringIO()

        with (
            mock.patch.dict(preflight.os.environ, base_env(), clear=True),
            mock.patch.object(preflight, "build_rpc_client", return_value=rpc),
            contextlib.redirect_stderr(stderr),
        ):
            returncode = preflight.main([])

        self.assertEqual(returncode, 0)
        self.assertIn("getblocktemplate", [method for method, _params in rpc.calls])
        self.assertIn("validateaddress", [method for method, _params in rpc.calls])
        self.assertIn("PASS", stderr.getvalue())

    def test_production_gate_only_makes_zero_rpc_calls(self) -> None:
        stderr = io.StringIO()
        with (
            mock.patch.dict(preflight.os.environ, production_env(), clear=True),
            mock.patch.object(
                preflight,
                "build_rpc_client",
                side_effect=AssertionError("RPC client must not be instantiated"),
            ) as build_rpc,
            contextlib.redirect_stderr(stderr),
        ):
            returncode = preflight.main(["--production-gate-only"])

        self.assertEqual(returncode, 0)
        build_rpc.assert_not_called()
        self.assertIn("production gate", stderr.getvalue())

    def test_production_gate_only_rejects_implicit_public_difficulty(self) -> None:
        env = production_env(CKPOOL_MINDIFF_EXPLICIT="0")

        with self.assertRaisesRegex(preflight.PreflightError, "explicit CKPOOL_MINDIFF"):
            preflight.run_static_preflight(env)

    def test_prelaunch_requires_both_production_flags(self) -> None:
        env = mainnet_production_env(
            QBIT_TOOLS_PRODUCTION="0",
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "QBIT_TOOLS_PRODUCTION=1"):
            preflight.run_static_preflight(env)

    def test_regtest_allows_implicit_regtest_difficulty_floor(self) -> None:
        messages = preflight.run_preflight(base_env(), FakeRpc())

        self.assertTrue(any("readiness gate: skipped" in message for message in messages))
        self.assertTrue(any("mindiff=0.00390625" in message for message in messages))

    def test_public_chain_requires_explicit_mindiff(self) -> None:
        env = base_env(
            QBIT_CHAIN="testnet4",
            QBIT_MINER_ADDRESS="tq1miner",
            CKPOOL_MINDIFF="1",
            CKPOOL_STARTDIFF="42",
            CKPOOL_MINDIFF_EXPLICIT="0",
            CKPOOL_STARTDIFF_EXPLICIT="1",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "requires explicit CKPOOL_MINDIFF"):
            preflight.run_preflight(env, FakeRpc())

    def test_public_chain_requires_explicit_startdiff(self) -> None:
        env = base_env(
            QBIT_CHAIN="signet",
            QBIT_MINER_ADDRESS="tq1miner",
            CKPOOL_MINDIFF="1",
            CKPOOL_STARTDIFF="42",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="0",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "requires explicit CKPOOL_STARTDIFF"):
            preflight.run_preflight(env, FakeRpc())

    def test_production_gate_rejects_unsafe_ckpool_overrides(self) -> None:
        cases = {
            "regtest": {"QBIT_CHAIN": "regtest"},
            "permissive": {"CKPOOL_PUBLIC_DIFF_POLICY": "permissive"},
            "readiness": {"CKPOOL_NON_TEST_READINESS_GATE": "0"},
            "assumptions": {"CKPOOL_VALIDATE_QBIT_ASSUMPTIONS": "0"},
            "p2mr": {"CKPOOL_REQUIRE_P2MR_PAYOUT": "0"},
            "password": {"QBIT_RPC_PASSWORD": "change-this"},
            "port": {"CKPOOL_STRATUM_PORT": ""},
        }
        for name, overrides in cases.items():
            with self.subTest(name=name), self.assertRaises(preflight.PreflightError):
                preflight.run_preflight(production_env(**overrides), FakeRpc(rpc_chain="testnet4"))

    def test_production_gate_accepts_strict_public_ckpool_env(self) -> None:
        messages = preflight.run_preflight(production_env(), FakeRpc(rpc_chain="testnet4"))

        self.assertTrue(any("production gate" in message for message in messages))

    def test_production_mainnet_prelaunch_requires_explicit_authorization(self) -> None:
        with self.assertRaisesRegex(
            preflight.PreflightError,
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0",
        ):
            preflight.run_preflight(
                mainnet_production_env(CKPOOL_NON_TEST_READINESS_GATE="0"),
                FakeRpc(ibd=True, rpc_chain="main"),
            )

    def test_production_mainnet_prelaunch_defers_live_template_validation(self) -> None:
        rpc = FakeRpc(
            ibd=True,
            connections=0,
            rpc_chain="main",
            template_error=preflight.PreflightError(
                "getblocktemplate failed: RPC -10: qbit is in initial sync and waiting for blocks"
            ),
        )
        messages = preflight.run_preflight(
            mainnet_production_env(
                CKPOOL_NON_TEST_READINESS_GATE="0",
                QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
            ),
            rpc,
        )

        self.assertTrue(any("ckpool=mainnet-prelaunch" in message for message in messages))
        self.assertTrue(any("explicitly relaxed" in message for message in messages))
        self.assertTrue(any("dynamic getblocktemplate validation deferred" in message for message in messages))
        self.assertNotIn("getblocktemplate", [method for method, _params in rpc.calls])
        self.assertIn("validateaddress", [method for method, _params in rpc.calls])

    def test_production_mainnet_prelaunch_still_validates_static_assumptions(self) -> None:
        with self.assertRaisesRegex(
            preflight.PreflightError,
            "QBIT_EXPECTED_MAX_BLOCK_WEIGHT must be 2000000",
        ):
            preflight.run_preflight(
                mainnet_production_env(
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                    QBIT_EXPECTED_MAX_BLOCK_WEIGHT="4000000",
                ),
                FakeRpc(ibd=True, rpc_chain="main"),
            )

    def test_production_mainnet_prelaunch_still_validates_payout_address(self) -> None:
        rpc = FakeRpc(ibd=True, rpc_chain="main", address_valid=False)

        with self.assertRaisesRegex(preflight.PreflightError, "is not valid"):
            preflight.run_preflight(
                mainnet_production_env(
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                ),
                rpc,
            )

        self.assertIn("validateaddress", [method for method, _params in rpc.calls])

    def test_launch_enabled_rejects_disabled_readiness(self) -> None:
        with self.assertRaisesRegex(
            preflight.PreflightError,
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED=0",
        ):
            preflight.run_preflight(
                mainnet_production_env(
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
                ),
                FakeRpc(rpc_chain="main"),
            )

    def test_launch_enabled_rejects_initial_block_download(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "initial block download"):
            preflight.run_preflight(
                mainnet_production_env(
                    CKPOOL_NON_TEST_READINESS_GATE="1",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
                ),
                FakeRpc(ibd=True, rpc_chain="main"),
            )

    def test_launch_enabled_requires_boolean_initial_block_download(self) -> None:
        for value in (None, "false", 0):
            with self.subTest(value=value), self.assertRaisesRegex(
                preflight.PreflightError,
                "initialblockdownload was missing or not a boolean",
            ):
                preflight.run_preflight(
                    mainnet_production_env(
                        CKPOOL_NON_TEST_READINESS_GATE="1",
                        QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
                    ),
                    FakeRpc(ibd=value, rpc_chain="main"),
                )

    def test_launch_enabled_accepts_sufficient_readiness(self) -> None:
        messages = preflight.run_preflight(
            mainnet_production_env(
                CKPOOL_NON_TEST_READINESS_GATE="1",
                QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
            ),
            FakeRpc(ibd=False, connections=1, rpc_chain="main"),
        )

        self.assertTrue(any("ckpool=strict" in message for message in messages))
        self.assertTrue(any("ibd=false peers=1" in message for message in messages))

    def test_launch_enabled_requires_live_template_validation(self) -> None:
        with self.assertRaisesRegex(
            preflight.PreflightError,
            "RPC -10: qbit is in initial sync and waiting for blocks",
        ):
            preflight.run_preflight(
                mainnet_production_env(
                    CKPOOL_NON_TEST_READINESS_GATE="1",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
                ),
                FakeRpc(
                    ibd=False,
                    rpc_chain="main",
                    template_error=preflight.PreflightError(
                        "getblocktemplate failed: RPC -10: "
                        "qbit is in initial sync and waiting for blocks"
                    ),
                ),
            )

    def test_mainnet_prelaunch_still_rejects_rpc_chain_mismatch(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "does not match"):
            preflight.run_preflight(
                mainnet_production_env(
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                ),
                FakeRpc(ibd=True, rpc_chain="testnet4"),
            )

    def test_non_mainnet_production_cannot_use_mainnet_prelaunch_authorization(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "valid only"):
            preflight.run_preflight(
                production_env(
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                ),
                FakeRpc(rpc_chain="testnet4"),
            )

    def test_non_mainnet_chains_cannot_disable_readiness_gate(self) -> None:
        for chain in ("regtest", "testnet4", "signet"):
            with self.subTest(chain=chain), self.assertRaisesRegex(
                preflight.PreflightError,
                "explicitly authorized mainnet prelaunch",
            ):
                preflight.run_static_preflight(
                    base_env(
                        QBIT_CHAIN=chain,
                        CKPOOL_NON_TEST_READINESS_GATE="0",
                    )
                )

    def test_malformed_readiness_and_production_flags_fail(self) -> None:
        cases = {
            "CKPOOL_NON_TEST_READINESS_GATE": "sometimes",
            "QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED": "prelaunch",
            "QBIT_TOOLS_PRODUCTION": "maybe",
        }
        for name, value in cases.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                preflight.PreflightError,
                name,
            ):
                preflight.run_preflight(
                    mainnet_production_env(**{name: value}),
                    FakeRpc(rpc_chain="main"),
                )

    def test_public_readiness_rejects_initial_block_download(self) -> None:
        env = base_env(
            QBIT_CHAIN="mainnet",
            QBIT_MINER_ADDRESS="qb1miner",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="1",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "initial block download"):
            preflight.run_preflight(env, FakeRpc(ibd=True, rpc_chain="main"))

    def test_public_readiness_rejects_insufficient_peers(self) -> None:
        env = base_env(
            QBIT_CHAIN="testnet4",
            QBIT_MINER_ADDRESS="tq1miner",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="1",
            CKPOOL_MIN_PEERS="2",
            CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS="0",
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaisesRegex(
            preflight.PreflightError,
            "requires at least 2",
        ):
            preflight.run_preflight(
                env,
                FakeRpc(connections=1, rpc_chain="testnet4"),
            )

    def test_public_readiness_waits_until_peers_appear(self) -> None:
        env = base_env(
            QBIT_CHAIN="testnet4",
            QBIT_MINER_ADDRESS="tq1miner",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="1",
            CKPOOL_MIN_PEERS="2",
            CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS="30",
        )
        rpc = FakeRpc(connections=[0, 1, 2], rpc_chain="testnet4")
        stderr = io.StringIO()

        with (
            mock.patch.object(preflight.time, "sleep", return_value=None),
            contextlib.redirect_stderr(stderr),
        ):
            messages = preflight.run_preflight(env, rpc)

        self.assertEqual(rpc.network_info_calls, 3)
        self.assertIn("readiness wait", stderr.getvalue())
        self.assertTrue(any("peers=2" in message for message in messages))

    def test_public_readiness_fails_after_peer_timeout(self) -> None:
        env = base_env(
            QBIT_CHAIN="testnet4",
            QBIT_MINER_ADDRESS="tq1miner",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="1",
            CKPOOL_MIN_PEERS="1",
            CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS="0",
        )
        stderr = io.StringIO()

        with contextlib.redirect_stderr(stderr), self.assertRaisesRegex(
            preflight.PreflightError,
            "after waiting 0s",
        ):
            preflight.run_preflight(env, FakeRpc(connections=0, rpc_chain="testnet4"))

        self.assertIn("readiness wait timed out", stderr.getvalue())

    def test_public_readiness_rejects_rpc_chain_mismatch(self) -> None:
        env = base_env(
            QBIT_CHAIN="testnet4",
            QBIT_MINER_ADDRESS="tq1miner",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="1",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "does not match"):
            preflight.run_preflight(env, FakeRpc(rpc_chain="signet"))

    def test_public_readiness_requires_positive_min_peers(self) -> None:
        env = base_env(
            QBIT_CHAIN="testnet4",
            QBIT_MINER_ADDRESS="tq1miner",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="1",
            CKPOOL_MIN_PEERS="0",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "at least 1"):
            preflight.run_preflight(env, FakeRpc(connections=0, rpc_chain="testnet4"))

    def test_rejects_wrong_qbit_weightlimit(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "weightlimit"):
            preflight.run_preflight(base_env(), FakeRpc(weightlimit=4_000_000))

    def test_allows_empty_template_rules_when_weightlimit_matches(self) -> None:
        messages = preflight.run_preflight(base_env(), FakeRpc(template_rules=[]))

        self.assertTrue(any("qbit assumptions" in message for message in messages))

    def test_other_chains_do_not_defer_live_template_validation(self) -> None:
        cases = {
            "regtest": (base_env(), "regtest"),
            "testnet4": (production_env(), "testnet4"),
            "signet": (
                production_env(QBIT_CHAIN="signet", QBIT_MINER_ADDRESS="tq1miner"),
                "signet",
            ),
        }
        for name, (env, rpc_chain) in cases.items():
            with self.subTest(chain=name), self.assertRaisesRegex(
                preflight.PreflightError,
                "template unavailable",
            ):
                preflight.run_preflight(
                    env,
                    FakeRpc(
                        rpc_chain=rpc_chain,
                        template_error=preflight.PreflightError("template unavailable"),
                    ),
                )

    def test_http_500_preserves_json_rpc_error_diagnostics(self) -> None:
        body = io.BytesIO(
            b'{"result":null,"error":{"code":-10,"message":'
            b'"qbit is in initial sync and waiting for blocks"},"id":"preflight"}'
        )
        http_error = preflight.error.HTTPError(
            "http://qbitd:8352",
            500,
            "Internal Server Error",
            {},
            body,
        )
        rpc = preflight.HttpRpcClient("qbitd", "8352", "user", "password", 5.0)

        with mock.patch.object(preflight.request, "urlopen", side_effect=http_error), self.assertRaisesRegex(
            preflight.PreflightError,
            "getblocktemplate failed: RPC -10: qbit is in initial sync and waiting for blocks.*HTTP 500",
        ):
            rpc.call("getblocktemplate", [{"rules": ["segwit"]}])

    def test_rejects_wrong_address_hrp(self) -> None:
        env = base_env(
            QBIT_CHAIN="testnet4",
            QBIT_MINER_ADDRESS="qb1wrongnet",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="1",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "HRP"):
            preflight.run_preflight(env, FakeRpc(rpc_chain="testnet4"))

    def test_public_payout_requires_p2mr_witness_version(self) -> None:
        env = base_env(
            QBIT_CHAIN="mainnet",
            QBIT_MINER_ADDRESS="qb1miner",
            CKPOOL_MINDIFF="1024",
            CKPOOL_STARTDIFF="65536",
            CKPOOL_MINDIFF_EXPLICIT="1",
            CKPOOL_STARTDIFF_EXPLICIT="1",
        )

        with self.assertRaisesRegex(preflight.PreflightError, "witness_version"):
            preflight.run_preflight(env, FakeRpc(rpc_chain="main", witness_version=1))

    def test_rejects_invalid_numeric_knobs_before_json_rendering(self) -> None:
        for name, value in {
            "CKPOOL_MINDIFF": "NaN",
            "CKPOOL_STARTDIFF": "Infinity",
            "CKPOOL_MAXDIFF": "-1",
            "CKPOOL_BLOCKPOLL": "0",
            "CKPOOL_NONCE1LENGTH": "1",
            "CKPOOL_NONCE2LENGTH": "9",
            "CKPOOL_UPDATE_INTERVAL": "0",
            "CKPOOL_DONATION": "-0.1",
        }.items():
            with self.subTest(name=name):
                env = base_env(**{name: value})
                with self.assertRaises(preflight.PreflightError):
                    preflight.run_preflight(env, FakeRpc())

    def test_rejects_difficulty_ordering_errors(self) -> None:
        cases = [
            {"CKPOOL_MINDIFF": "10", "CKPOOL_STARTDIFF": "1"},
            {"CKPOOL_MINDIFF": "1", "CKPOOL_STARTDIFF": "10", "CKPOOL_MAXDIFF": "5"},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(preflight.PreflightError):
                    preflight.run_preflight(base_env(**overrides), FakeRpc())


class QbitCkpoolSupervisorTests(unittest.TestCase):
    def supervisor_env(self, **overrides: str) -> dict[str, str]:
        env = base_env(
            CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS="0.01",
            CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS="0.05",
        )
        env.update(overrides)
        return env

    def test_supervisor_launches_child_and_preserves_arguments_without_shell(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "argv.json"
            marker = Path(tmp) / "must-not-exist"
            arguments = ["plain value", f"$(touch {marker})", "; exit 99"]
            code = "import json,sys; open(sys.argv[1],'w').write(json.dumps(sys.argv[2:]))"
            command = [sys.executable, "-c", code, str(output), *arguments]

            returncode = preflight.run_supervisor(
                self.supervisor_env(), FakeRpc(), command
            )

            self.assertEqual(returncode, 0)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), arguments)
            self.assertFalse(marker.exists())

    def test_supervisor_propagates_child_exit_code(self) -> None:
        returncode = preflight.run_supervisor(
            self.supervisor_env(),
            FakeRpc(),
            [sys.executable, "-c", "raise SystemExit(23)"],
        )

        self.assertEqual(returncode, 23)

    def test_supervisor_requires_a_child_command(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            preflight.main(["--supervise"])

        self.assertEqual(error.exception.code, 2)

    def test_initial_preflight_failure_does_not_launch_child(self) -> None:
        with mock.patch.object(preflight.subprocess, "Popen") as popen:
            with self.assertRaises(preflight.PreflightError):
                preflight.run_supervisor(
                    self.supervisor_env(CKPOOL_MINDIFF="0"),
                    FakeRpc(),
                    ["must-not-run"],
                )

        popen.assert_not_called()

    def test_unexpected_supervisor_error_terminates_and_reaps_child(self) -> None:
        class WaitCrashChild:
            pid = 123

            def __init__(self) -> None:
                self.returncode: int | None = None
                self.signals: list[int] = []
                self.wait_calls = 0

            def poll(self) -> int | None:
                return self.returncode

            def send_signal(self, signum: int) -> None:
                self.signals.append(signum)
                self.returncode = -signum

            def wait(self, timeout: float | None = None) -> int:
                self.wait_calls += 1
                if self.wait_calls == 1:
                    raise RuntimeError("unexpected wait failure")
                assert self.returncode is not None
                return self.returncode

            def kill(self) -> None:
                self.returncode = -signal.SIGKILL

        child = WaitCrashChild()
        with (
            mock.patch.object(preflight.subprocess, "Popen", return_value=child),
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaisesRegex(RuntimeError, "unexpected wait failure"),
        ):
            preflight.run_supervisor(
                self.supervisor_env(),
                FakeRpc(),
                ["ckpool"],
            )

        self.assertEqual(child.signals, [signal.SIGTERM])
        self.assertEqual(child.returncode, -signal.SIGTERM)
        self.assertEqual(child.wait_calls, 2)

    def test_strict_watchdog_enforces_ibd_peers_genesis_and_live_template(self) -> None:
        strict_env = mainnet_production_env(
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="1",
            CKPOOL_NON_TEST_READINESS_GATE="1",
            QBIT_EXPECTED_GENESIS_HASH="a" * 64,
        )
        cases = {
            "ibd": (FakeRpc(rpc_chain="main", ibd=True, genesis_hash="a" * 64), "initial block download"),
            "peers": (
                FakeRpc(rpc_chain="main", connections=0, genesis_hash="a" * 64),
                "requires at least 1",
            ),
            "genesis": (FakeRpc(rpc_chain="main", genesis_hash="b" * 64), "does not match"),
            "template": (
                FakeRpc(
                    rpc_chain="main",
                    genesis_hash="a" * 64,
                    template_error=preflight.PreflightError("getblocktemplate unavailable"),
                ),
                "getblocktemplate unavailable",
            ),
        }

        for name, (rpc, error_pattern) in cases.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                preflight.PreflightError, error_pattern
            ):
                preflight.run_watchdog_check(strict_env, rpc)

    def test_strict_watchdog_enforces_template_time_and_active_tip(self) -> None:
        env = self.supervisor_env(
            CKPOOL_TEMPLATE_MAX_AGE_SECONDS="10",
            CKPOOL_TEMPLATE_MAX_FUTURE_SECONDS="5",
        )
        cases = {
            "stale": (FakeRpc(template_time=int(time.time()) - 11), "is .* old"),
            "future": (FakeRpc(template_time=int(time.time()) + 6), "in the future"),
            "wrong-tip": (
                FakeRpc(previous_block_hash="2" * 64),
                "does not build on the active tip",
            ),
        }

        for name, (rpc, error_pattern) in cases.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                preflight.PreflightError, error_pattern
            ):
                preflight.run_watchdog_check(env, rpc)

    def test_prelaunch_watchdog_tolerates_rpc_minus_10_indefinitely(self) -> None:
        rpc = FakeRpc(
            rpc_chain="main",
            ibd=True,
            template_error=preflight.PreflightError(
                "getblocktemplate failed: RPC -10: qbit is in initial sync"
            ),
        )
        env = mainnet_production_env(
            CKPOOL_NON_TEST_READINESS_GATE="0",
            QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
            CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS="0.01",
            CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS="0",
        )

        returncode = preflight.run_supervisor(
            env,
            rpc,
            [sys.executable, "-c", "import time; time.sleep(0.08); raise SystemExit(7)"],
        )

        self.assertEqual(returncode, 7)
        self.assertNotIn("getblocktemplate", [method for method, _params in rpc.calls])
        self.assertGreaterEqual(
            [method for method, _params in rpc.calls].count("validateaddress"), 2
        )

    def test_watchdog_recovers_when_rpc_minus_10_transitions_to_valid_template(self) -> None:
        class RecoveringRpc(FakeRpc):
            def __init__(self) -> None:
                super().__init__()
                self.template_calls = 0

            def call(self, method: str, params: list[Any] | None = None) -> Any:
                if method == "getblocktemplate":
                    self.template_calls += 1
                    if self.template_calls == 3:
                        self.calls.append((method, params or []))
                        raise preflight.PreflightError(
                            "getblocktemplate failed: RPC -10: temporary initial sync"
                        )
                return super().call(method, params)

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            returncode = preflight.run_supervisor(
                self.supervisor_env(CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS="1"),
                RecoveringRpc(),
                [sys.executable, "-c", "import time; time.sleep(0.08); raise SystemExit(9)"],
            )

        self.assertEqual(returncode, 9)
        self.assertIn("watchdog failure", stderr.getvalue())
        self.assertIn("watchdog recovered", stderr.getvalue())

    def test_launched_watchdog_exits_after_failure_grace(self) -> None:
        class FailingRpc(FakeRpc):
            def __init__(self) -> None:
                super().__init__()
                self.template_calls = 0

            def call(self, method: str, params: list[Any] | None = None) -> Any:
                if method == "getblocktemplate":
                    self.template_calls += 1
                    if self.template_calls > 2:
                        self.calls.append((method, params or []))
                        raise preflight.PreflightError("template service unavailable")
                return super().call(method, params)

        started = time.monotonic()
        returncode = preflight.run_supervisor(
            self.supervisor_env(CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS="0.03"),
            FailingRpc(),
            [sys.executable, "-c", "import time; time.sleep(30)"],
        )

        self.assertEqual(returncode, 1)
        self.assertLess(time.monotonic() - started, 2)

    def test_supervisor_forwards_term_and_int_and_reaps_child(self) -> None:
        from tests.test_ckpool_startup import FakeRpcServer

        child_code = (
            "import os,signal,sys,time; ready,out=sys.argv[1:]; "
            "stop=lambda sig,frame:(open(out,'w').write(str(sig)),sys.exit(0)); "
            "signal.signal(signal.SIGTERM,stop); signal.signal(signal.SIGINT,stop); "
            "open(ready,'w').write(str(os.getpid())); "
            "exec('while True:\\n time.sleep(0.02)')"
        )
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signum=signum), tempfile.TemporaryDirectory() as tmp, FakeRpcServer(
                "--chain",
                "main",
                "--initialblockdownload",
                "--reject-gbt-during-ibd",
            ) as rpc:
                ready = Path(tmp) / "ready"
                received = Path(tmp) / "received"
                env = mainnet_production_env(
                    CKPOOL_NON_TEST_READINESS_GATE="0",
                    QBIT_MAINNET_LAUNCH_READINESS_CHECKS_ENABLED="0",
                    QBIT_RPC_HOST="127.0.0.1",
                    QBIT_RPC_PORT=str(rpc.port),
                    CKPOOL_TEMPLATE_WATCHDOG_POLL_SECONDS="0.02",
                    CKPOOL_TEMPLATE_FAILURE_EXIT_SECONDS="0",
                )
                process = subprocess.Popen(
                    [
                        sys.executable,
                        str(SCRIPT_PATH),
                        "--supervise",
                        sys.executable,
                        "-c",
                        child_code,
                        str(ready),
                        str(received),
                    ],
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists() and process.poll() is None:
                        if time.monotonic() >= deadline:
                            self.fail("supervised child did not start")
                        time.sleep(0.01)
                    child_pid = int(ready.read_text(encoding="utf-8"))
                    process.send_signal(signum)
                    _stdout, stderr = process.communicate(timeout=5)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)

                self.assertEqual(process.returncode, 128 + signum, stderr)
                self.assertEqual(received.read_text(encoding="utf-8"), str(signum))
                with self.assertRaises(ProcessLookupError):
                    os.kill(child_pid, 0)


if __name__ == "__main__":
    unittest.main()
