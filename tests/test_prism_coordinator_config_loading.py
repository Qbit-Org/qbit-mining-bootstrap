#!/usr/bin/env python3
"""Direct tests for the extracted coordinator configuration and RPC seams."""

from __future__ import annotations

import dataclasses
import subprocess
import sys
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism.coordinator_config import (
    CoordinatorConfig,
    LifecycleConfig,
    load_coordinator_config,
)
from lab.prism.prism_coordinator import JsonRpc as CompatibilityJsonRpc
from lab.prism.prism_coordinator import PrismCoordinator
from lab.prism.rpc import JsonRpc
from lab.prism.run_ctv_broadcaster_daemon import JsonRpc as DaemonJsonRpc


def minimal_environment(root: Path) -> dict[str, str]:
    return {
        "QBIT_RPC_HOST": "qbit.example",
        "QBIT_RPC_USER": "rpc-user",
        "QBIT_RPC_PASSWORD": "rpc-password",
        "PRISM_ALLOW_MEMORY_LEDGER": "1",
        "PRISM_ALLOW_TEST_SIGNING_SEEDS": "1",
        "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "1",
        "PRISM_AUDIT_DIR": str(root),
        "PRISM_EVIDENCE_PATH": str(root / "evidence.json"),
    }


class CoordinatorConfigLoadingTests(unittest.TestCase):
    def construct(self, source: dict[str, str]) -> PrismCoordinator:
        config = load_coordinator_config(source)
        with patch.object(JsonRpc, "call", side_effect=RuntimeError("offline")):
            return PrismCoordinator(config)

    def test_loader_accepts_mapping_without_reading_process_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = minimal_environment(Path(temp_dir))
            with patch.dict("os.environ", {}, clear=True):
                config = load_coordinator_config(source)

        self.assertIsInstance(config, CoordinatorConfig)
        self.assertEqual(config.rpc.host, "qbit.example")
        self.assertEqual(config.rpc.port, 18452)
        self.assertEqual(config.stratum.bind, "127.0.0.1")
        self.assertEqual(config.lifecycle.pending_refresh_health_deadline_seconds, 15.0)
        self.assertEqual(config.lifecycle.coherent_tip_poll_health_deadline_seconds, 15.0)

    def test_loader_captures_pr54_health_deadlines_in_frozen_lifecycle_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_HEALTH_REFRESH_SECONDS": "7",
                "PRISM_METRICS_REFRESH_SECONDS": "11",
                "PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS": "23",
                "PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS": "29",
                "PRISM_MINING_HEALTH_STARTUP_GRACE_SECONDS": "31",
            }
            config = load_coordinator_config(source)

        self.assertEqual(
            config.lifecycle,
            replace(
                config.lifecycle,
                health_refresh_seconds=7.0,
                metrics_refresh_seconds=11.0,
                pending_refresh_health_deadline_seconds=23.0,
                coherent_tip_poll_health_deadline_seconds=29.0,
                mining_health_startup_grace_seconds=31.0,
            ),
        )
        self.assertIsInstance(config.lifecycle, LifecycleConfig)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            config.lifecycle.health_refresh_seconds = 1.0  # type: ignore[misc]

    def test_loader_owns_public_reward_window_cache_interval(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS": "47",
            }
            config = load_coordinator_config(source)

        self.assertEqual(config.ledger.reward_window_cache_seconds, 47.0)

    def test_coordinator_uses_supplied_snapshot_without_reloading_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = load_coordinator_config(minimal_environment(Path(temp_dir)))
            with (
                patch(
                    "lab.prism.prism_coordinator.load_coordinator_config",
                    side_effect=AssertionError("unexpected environment reload"),
                ),
                patch.object(JsonRpc, "call", side_effect=RuntimeError("offline")),
            ):
                coordinator = PrismCoordinator(config)

        self.assertIs(coordinator.config, config)
        self.assertEqual(coordinator.bind, config.stratum.bind)
        self.assertEqual(
            coordinator.health_pending_refresh_max_age_seconds,
            config.lifecycle.pending_refresh_health_deadline_seconds,
        )
        self.assertEqual(
            coordinator.metrics_refresh_seconds,
            config.lifecycle.metrics_refresh_seconds,
        )

    def test_metrics_refresh_interval_must_be_positive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_METRICS_REFRESH_SECONDS": "0",
            }
            with self.assertRaisesRegex(
                SystemExit,
                "PRISM_METRICS_REFRESH_SECONDS must be positive",
            ):
                load_coordinator_config(source)

    def test_zero_argument_coordinator_still_loads_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = minimal_environment(Path(temp_dir))
            with (
                patch.dict("os.environ", source, clear=True),
                patch.object(JsonRpc, "call", side_effect=RuntimeError("offline")),
            ):
                coordinator = PrismCoordinator()

        self.assertEqual(coordinator.rpc.host, "qbit.example")
        self.assertEqual(coordinator.config.rpc.user, "rpc-user")

    def test_dormant_live_chain_validation_is_deferred_until_use(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_MIN_PEERS": "0",
                "PRISM_TEMPLATE_MAX_AGE_SECONDS": "not-an-int",
            }
            coordinator = self.construct(source)

        class PublicChainRpc:
            def call(self, method: str, params: list[object] | None = None) -> object:
                if method == "getblockchaininfo":
                    return {
                        "chain": "testnet4",
                        "initialblockdownload": False,
                        "blocks": 10,
                        "headers": 10,
                    }
                if method == "getnetworkinfo":
                    return {"connections": 1}
                raise RuntimeError(method)

        coordinator.qbit_chain = "testnet4"
        coordinator.rpc = PublicChainRpc()  # type: ignore[assignment]
        with self.assertRaisesRegex(SystemExit, "PRISM_MIN_PEERS must be positive"):
            coordinator.validate_live_chain_identity()

        coordinator.current_template_artifacts = lambda: SimpleNamespace(  # type: ignore[method-assign]
            template={"curtime": int(time.time())},
            previousblockhash="11" * 32,
        )
        with self.assertRaisesRegex(
            SystemExit, "PRISM_TEMPLATE_MAX_AGE_SECONDS must be an integer"
        ):
            coordinator.validate_live_template_and_fee_policy()

    def test_disabled_ctv_settlement_ignores_invalid_settlement_only_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_CTV_SETTLEMENT_ENABLED": "0",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS": "not-an-int",
                "PRISM_RESERVED_COINBASE_OUTPUTS": "also-not-an-int",
            }
            coordinator = self.construct(source)

        self.assertIsNone(coordinator.prism_ctv_settlement_config())

    def test_enabled_ctv_settlement_validates_at_consuming_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS": "not-an-int",
            }
            coordinator = self.construct(source)

        with self.assertRaisesRegex(
            SystemExit, "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS must be an integer"
        ):
            coordinator.prism_ctv_settlement_config()

    def test_payout_validation_is_deferred_until_policy_is_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES": "not-an-int",
                "PRISM_POOL_FEE_ENABLED": "",
            }
            coordinator = self.construct(source)

        with self.assertRaisesRegex(
            SystemExit, "PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES must be an integer"
        ):
            coordinator.prism_payout_policy()

    def test_explicit_disabled_broadcaster_defers_empty_settlement_flag(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_CTV_BROADCASTER_ENABLED": "0",
                "PRISM_CTV_SETTLEMENT_ENABLED": "",
            }
            coordinator = self.construct(source)

        self.assertFalse(coordinator.ctv_broadcaster_enabled)
        with self.assertRaisesRegex(
            SystemExit, "PRISM_CTV_SETTLEMENT_ENABLED is required"
        ):
            coordinator.prism_ctv_settlement_config()

    def test_broadcaster_default_still_consumes_settlement_flag_during_construction(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source = {
                **minimal_environment(Path(temp_dir)),
                "PRISM_CTV_SETTLEMENT_ENABLED": "",
            }
            with self.assertRaisesRegex(
                SystemExit, "PRISM_CTV_SETTLEMENT_ENABLED is required"
            ):
                load_coordinator_config(source)

    def test_rpc_compatibility_reexport_points_to_leaf_class(self) -> None:
        self.assertIs(CompatibilityJsonRpc, JsonRpc)
        self.assertIs(DaemonJsonRpc, JsonRpc)

    def test_importing_rpc_leaf_does_not_import_coordinator(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys; import lab.prism.rpc; "
                    "assert 'lab.prism.prism_coordinator' not in sys.modules"
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
