#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import importlib.util
import sys
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
        ibd: bool = False,
        rpc_chain: str = "regtest",
        connections: int | list[int] = 1,
        weightlimit: int = 2_000_000,
        template_rules: list[str] | None = None,
        address_valid: bool = True,
        iswitness: bool = True,
        witness_version: int | None = 2,
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
            return {"rules": self.template_rules, "weightlimit": self.weightlimit}
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


class QbitCkpoolPreflightTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
