#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import importlib.util
import sys
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
        ibd: bool | list[bool] = False,
        rpc_chain: str = "regtest",
        connections: int | list[int] = 1,
        weightlimit: int = 2_000_000,
        template_rules: list[str] | None = None,
        address_valid: bool = True,
        iswitness: bool = True,
        witness_version: int | None = 2,
        genesis_hash: str = "11" * 32,
        blocks: int | list[int] = 100,
        headers: int | list[int] = 100,
        template_time: int | None = None,
        template_previous_hash: str | None = "22" * 32,
    ) -> None:
        self.ibd = [ibd] if isinstance(ibd, bool) else ibd
        self.rpc_chain = rpc_chain
        self.connections = [connections] if isinstance(connections, int) else connections
        self.network_info_calls = 0
        self.weightlimit = weightlimit
        self.template_rules = template_rules if template_rules is not None else ["segwit"]
        self.address_valid = address_valid
        self.iswitness = iswitness
        self.witness_version = witness_version
        self.genesis_hash = genesis_hash
        self.blocks = [blocks] if isinstance(blocks, int) else blocks
        self.headers = [headers] if isinstance(headers, int) else headers
        self.blockchain_info_calls = 0
        self.template_time = int(time.time()) if template_time is None else template_time
        self.template_previous_hash = template_previous_hash
        self.calls: list[tuple[str, list[Any]]] = []

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        params = params or []
        self.calls.append((method, params))
        if method == "getblockchaininfo":
            index = self.blockchain_info_calls
            self.blockchain_info_calls += 1
            return {
                "chain": self.rpc_chain,
                "initialblockdownload": self.sequence_value(self.ibd, index),
                "blocks": self.sequence_value(self.blocks, index),
                "headers": self.sequence_value(self.headers, index),
            }
        if method == "getnetworkinfo":
            index = min(self.network_info_calls, len(self.connections) - 1)
            self.network_info_calls += 1
            return {"connections": self.connections[index]}
        if method == "getblockhash":
            self.assert_genesis_height(params)
            return self.genesis_hash
        if method == "getblocktemplate":
            return {
                "rules": self.template_rules,
                "weightlimit": self.weightlimit,
                "previousblockhash": self.template_previous_hash,
                "curtime": self.template_time,
            }
        if method == "validateaddress":
            return {
                "isvalid": self.address_valid,
                "address": params[0],
                "iswitness": self.iswitness,
                "witness_version": self.witness_version,
            }
        raise AssertionError(f"unexpected RPC method {method}")

    @staticmethod
    def assert_genesis_height(params: list[Any]) -> None:
        if params != [0]:
            raise AssertionError(f"expected getblockhash height 0, got {params!r}")

    @staticmethod
    def sequence_value(values: list[Any], index: int) -> Any:
        return values[min(index, len(values) - 1)]


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


def mainnet_env(**overrides: str) -> dict[str, str]:
    env = production_env(
        QBIT_PRODUCTION="0",
        QBIT_CHAIN="mainnet",
        QBIT_MINER_ADDRESS="qb1miner",
        QBIT_EXPECTED_GENESIS_HASH="11" * 32,
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
            "empty-payout": {"QBIT_MINER_ADDRESS": ""},
            "automatic-payout": {"QBIT_MINER_ADDRESS": "auto"},
            "password": {"QBIT_RPC_PASSWORD": "change-this"},
            "port": {"CKPOOL_STRATUM_PORT": ""},
        }
        for name, overrides in cases.items():
            with self.subTest(name=name), self.assertRaises(preflight.PreflightError):
                preflight.run_preflight(production_env(**overrides), FakeRpc(rpc_chain="testnet4"))

    def test_production_gate_accepts_strict_public_ckpool_env(self) -> None:
        messages = preflight.run_preflight(production_env(), FakeRpc(rpc_chain="testnet4"))

        self.assertTrue(any("production gate" in message for message in messages))

    def test_mainnet_implies_production_gate(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "non-default QBIT_RPC_PASSWORD"):
            preflight.run_preflight(
                mainnet_env(QBIT_RPC_PASSWORD="change-this"),
                FakeRpc(rpc_chain="main"),
            )

    def test_public_readiness_waits_for_initial_block_download_to_finish(self) -> None:
        rpc = FakeRpc(ibd=[True, True, False], rpc_chain="main")
        stderr = io.StringIO()

        with (
            mock.patch.object(preflight.time, "sleep", return_value=None),
            contextlib.redirect_stderr(stderr),
        ):
            messages = preflight.run_preflight(mainnet_env(), rpc)

        self.assertEqual(rpc.blockchain_info_calls, 3)
        self.assertIn("ibd=true", stderr.getvalue())
        self.assertTrue(any("ibd=false" in message for message in messages))

    def test_public_readiness_requires_explicit_false_ibd(self) -> None:
        rpc = FakeRpc(rpc_chain="main")
        original_call = rpc.call

        def call_without_ibd(method: str, params: list[Any] | None = None) -> Any:
            result = original_call(method, params)
            if method == "getblockchaininfo":
                result.pop("initialblockdownload")
            return result

        rpc.call = call_without_ibd  # type: ignore[method-assign]
        with self.assertRaisesRegex(preflight.PreflightError, "initial block download"):
            preflight.run_preflight(mainnet_env(), rpc)

    def test_public_readiness_waits_until_blocks_catch_up_to_headers(self) -> None:
        rpc = FakeRpc(rpc_chain="main", blocks=[99, 99, 100], headers=100)
        stderr = io.StringIO()

        with (
            mock.patch.object(preflight.time, "sleep", return_value=None),
            contextlib.redirect_stderr(stderr),
        ):
            messages = preflight.run_preflight(mainnet_env(), rpc)

        self.assertEqual(rpc.blockchain_info_calls, 3)
        self.assertIn("blocks=99 headers=100", stderr.getvalue())
        self.assertTrue(any("blocks=100 headers=100" in message for message in messages))

    def test_public_readiness_rejects_stale_template(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "template is stale"):
            preflight.run_preflight(
                mainnet_env(CKPOOL_TEMPLATE_MAX_AGE_SECONDS="120"),
                FakeRpc(rpc_chain="main", template_time=int(time.time()) - 121),
            )

    def test_mainnet_requires_expected_genesis_hash(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "requires QBIT_EXPECTED_GENESIS_HASH"):
            preflight.run_preflight(
                mainnet_env(QBIT_EXPECTED_GENESIS_HASH=""),
                FakeRpc(rpc_chain="main"),
            )

    def test_mainnet_rejects_wrong_genesis_hash(self) -> None:
        with self.assertRaisesRegex(preflight.PreflightError, "genesis hash"):
            preflight.run_preflight(
                mainnet_env(),
                FakeRpc(ibd=True, rpc_chain="main", genesis_hash="22" * 32),
            )

    def test_mainnet_accepts_expected_genesis_hash(self) -> None:
        messages = preflight.run_preflight(mainnet_env(), FakeRpc(rpc_chain="main"))

        self.assertTrue(any(f"genesis={'11' * 32}" in message for message in messages))

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

    def test_public_readiness_waits_for_sync_and_peers_together(self) -> None:
        env = mainnet_env(
            CKPOOL_MIN_PEERS="2",
            CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS="30",
        )
        rpc = FakeRpc(
            rpc_chain="main",
            connections=[0, 2, 2],
            blocks=[99, 99, 100],
            headers=100,
        )
        stderr = io.StringIO()

        with (
            mock.patch.object(preflight.time, "sleep", return_value=None),
            contextlib.redirect_stderr(stderr),
        ):
            messages = preflight.run_preflight(env, rpc)

        self.assertEqual(rpc.blockchain_info_calls, 3)
        self.assertEqual(rpc.network_info_calls, 3)
        self.assertIn("blocks=99 headers=100 peers=2", stderr.getvalue())
        self.assertTrue(any("blocks=100 headers=100 peers=2" in message for message in messages))

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

    def test_public_readiness_timeout_reports_all_last_observed_signals(self) -> None:
        env = mainnet_env(
            CKPOOL_MIN_PEERS="2",
            CKPOOL_PREFLIGHT_READINESS_TIMEOUT_SECONDS="0",
        )
        stderr = io.StringIO()

        with contextlib.redirect_stderr(stderr), self.assertRaisesRegex(
            preflight.PreflightError,
            r"initial block download.*not caught up.*requires at least 2",
        ):
            preflight.run_preflight(
                env,
                FakeRpc(
                    ibd=True,
                    rpc_chain="main",
                    blocks=98,
                    headers=100,
                    connections=1,
                ),
            )

        diagnostic = stderr.getvalue()
        self.assertIn("ibd=true", diagnostic)
        self.assertIn("blocks=98 headers=100", diagnostic)
        self.assertIn("peers=1 min_peers=2", diagnostic)
        self.assertIn("timeout=0s attempts=1", diagnostic)

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
        env = mainnet_env()

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
