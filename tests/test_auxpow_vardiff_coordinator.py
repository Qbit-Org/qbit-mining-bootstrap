#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import hashlib
import importlib
import io
import os
import sys
import threading
import time
import types
import unittest
from decimal import Decimal
from unittest.mock import Mock, patch

from lab.auxpow import vardiff


def install_test_framework_stubs() -> None:
    if "test_framework.messages" in sys.modules:
        return

    framework = types.ModuleType("test_framework")
    auxpow = types.ModuleType("test_framework.auxpow")
    blocktools = types.ModuleType("test_framework.blocktools")
    messages = types.ModuleType("test_framework.messages")
    script = types.ModuleType("test_framework.script")

    class AuxPowPayload:
        pass

    class CBlockHeader:
        pass

    class CTransaction:
        def deserialize(self, stream: object) -> None:
            return None

        def serialize(self) -> bytes:
            return b""

    class CScript(bytes):
        def __new__(cls, value: object = b"") -> "CScript":
            return bytes.__new__(cls, b"")

    def uint256_from_compact(compact: int) -> int:
        size = compact >> 24
        word = compact & 0x007FFFFF
        if size <= 3:
            return word >> (8 * (3 - size))
        return word << (8 * (size - 3))

    def hash256(data: bytes) -> bytes:
        return hashlib.sha256(hashlib.sha256(data).digest()).digest()

    auxpow.AuxPowPayload = AuxPowPayload
    def check_merkle_branch(*, leaf: int, branch: list[int], index: int) -> int:
        if branch:
            raise NotImplementedError("test stub only supports empty chain merkle branches")
        return leaf

    auxpow.MERGED_MINING_HEADER = bytes.fromhex("fabe6d6d")
    auxpow.check_merkle_branch = check_merkle_branch
    auxpow.get_expected_index = lambda *args, **kwargs: 0
    blocktools.add_witness_commitment = lambda *args, **kwargs: None
    blocktools.create_block = lambda *args, **kwargs: object()
    blocktools.create_coinbase = lambda *args, **kwargs: CTransaction()
    messages.CBlockHeader = CBlockHeader
    messages.CTransaction = CTransaction
    messages.hash256 = hash256
    messages.ser_uint256 = lambda value: int(value).to_bytes(32, "little")
    messages.uint256_from_compact = uint256_from_compact
    messages.uint256_from_str = lambda value: int.from_bytes(value, "little")
    script.CScript = CScript

    sys.modules["test_framework"] = framework
    sys.modules["test_framework.auxpow"] = auxpow
    sys.modules["test_framework.blocktools"] = blocktools
    sys.modules["test_framework.messages"] = messages
    sys.modules["test_framework.script"] = script


install_test_framework_stubs()
from lab.auxpow import auxpow_coordinator as coordinator  # noqa: E402


class AuxPowVardiffCoordinatorTests(unittest.TestCase):
    def test_build_chain_commitment_uses_display_order_root_bytes(self) -> None:
        root_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        aux_template = {"hash": root_hex, "chainid": 31430, "commitmentorder": "display"}

        commitment, chain_index = coordinator.build_chain_commitment(aux_template, chain_nonce=0x11223344)

        self.assertEqual(chain_index, 0)
        self.assertEqual(commitment[:4], coordinator.MERGED_MINING_HEADER)
        self.assertEqual(commitment[4:36], bytes.fromhex(root_hex))
        self.assertEqual(commitment[36:40], (1).to_bytes(4, "little"))
        self.assertEqual(commitment[40:44], (0x11223344).to_bytes(4, "little"))

    def test_build_chain_commitment_uses_internal_order_when_requested(self) -> None:
        root_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        aux_template = {"hash": root_hex, "chainid": 31430, "commitmentorder": "internal"}

        commitment, _ = coordinator.build_chain_commitment(aux_template)

        self.assertEqual(commitment[4:36], int(root_hex, 16).to_bytes(32, "little"))
        self.assertNotEqual(commitment[4:36], bytes.fromhex(root_hex))

    def test_build_chain_commitment_defaults_to_internal_for_legacy_templates(self) -> None:
        root_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        aux_template = {"hash": root_hex, "chainid": 31430}

        commitment, _ = coordinator.build_chain_commitment(aux_template)

        self.assertEqual(commitment[4:36], int(root_hex, 16).to_bytes(32, "little"))

    def test_build_chain_commitment_rejects_unknown_commitment_order(self) -> None:
        root_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        aux_template = {"hash": root_hex, "chainid": 31430, "commitmentorder": "sideways"}

        with self.assertRaisesRegex(RuntimeError, "unsupported commitmentorder"):
            coordinator.build_chain_commitment(aux_template)

    def test_package_import_uses_package_vardiff_module(self) -> None:
        import lab.auxpow as auxpow_package

        existing_module = sys.modules.pop("lab.auxpow.auxpow_coordinator", None)
        had_package_attr = hasattr(auxpow_package, "auxpow_coordinator")
        existing_package_attr = getattr(auxpow_package, "auxpow_coordinator", None)
        existing_top_level_vardiff = sys.modules.get("vardiff")
        sys.modules["vardiff"] = types.ModuleType("vardiff")
        try:
            imported = importlib.import_module("lab.auxpow.auxpow_coordinator")
            self.assertIs(imported.vardiff, vardiff)
        finally:
            sys.modules.pop("lab.auxpow.auxpow_coordinator", None)
            if existing_module is not None:
                sys.modules["lab.auxpow.auxpow_coordinator"] = existing_module
            if had_package_attr:
                setattr(auxpow_package, "auxpow_coordinator", existing_package_attr)
            elif hasattr(auxpow_package, "auxpow_coordinator"):
                delattr(auxpow_package, "auxpow_coordinator")
            if existing_top_level_vardiff is None:
                sys.modules.pop("vardiff", None)
            else:
                sys.modules["vardiff"] = existing_top_level_vardiff

    def test_load_vardiff_config_defaults_match_documented_public_defaults(self) -> None:
        vardiff_env_names = [name for name in os.environ if name.startswith("AUXPOW_STRATUM_VARDIFF_")]
        with patch.dict(os.environ, {}, clear=False):
            for name in vardiff_env_names:
                os.environ.pop(name, None)
            config = coordinator.load_vardiff_config()

        self.assertTrue(config.enabled)
        self.assertEqual(config.target_share_interval_seconds, Decimal("5"))
        self.assertEqual(config.min_difficulty, Decimal("1024"))
        self.assertEqual(config.max_difficulty, Decimal("4294967296"))
        self.assertEqual(config.retarget_interval_seconds, Decimal("120"))
        self.assertEqual(config.max_step_factor, Decimal("4"))
        self.assertEqual(config.max_step_down_factor, Decimal("2"))
        self.assertEqual(config.ewma_alpha, Decimal("0.4"))
        self.assertEqual(config.retarget_tolerance, Decimal("0.25"))
        self.assertEqual(config.startup_difficulty, Decimal("8192"))

    def test_vardiff_minimum_sets_advertised_floor_even_below_qbit_difficulty(self) -> None:
        server = self.server()
        server.vardiff_config = vardiff.VardiffConfig(
            enabled=True,
            target_share_interval_seconds=Decimal("5"),
            min_difficulty=Decimal("1024"),
            max_difficulty=Decimal("4096"),
            retarget_interval_seconds=Decimal("120"),
            max_step_factor=Decimal("4"),
            startup_difficulty=Decimal("1024"),
        )
        qbit_target = coordinator.difficulty_target(Decimal("1"))

        with patch.object(coordinator, "AUXPOW_STRATUM_MIN_ADVERTISED_DIFF", Decimal("0")):
            share_target = server.effective_share_target(Decimal("1024"), qbit_target)

        self.assertLess(abs(coordinator.target_difficulty(share_target) - Decimal("1024")), Decimal("1e-30"))

    def config(self) -> vardiff.VardiffConfig:
        return vardiff.VardiffConfig(
            enabled=True,
            target_share_interval_seconds=Decimal("15"),
            min_difficulty=Decimal("0.01"),
            max_difficulty=Decimal("1024"),
            retarget_interval_seconds=Decimal("90"),
            max_step_factor=Decimal("4"),
            startup_difficulty=Decimal("1"),
            max_step_down_factor=Decimal("4"),
            ewma_alpha=Decimal("1"),
            retarget_tolerance=Decimal("0"),
        )

    def server(self) -> coordinator.AuxPowStratumServer:
        server = coordinator.AuxPowStratumServer.__new__(coordinator.AuxPowStratumServer)
        server.fixed_share_difficulty = Decimal("1")
        server.vardiff_config = self.config()
        server.clients = set()
        server.jobs = {}
        server.current_job = None
        server.tip_snapshot = None
        server.qbit_miner_address = "qbit-address"
        server.lock = threading.RLock()
        server.clients_lock = threading.Lock()
        server.job_counter = 0
        server.worker_stats = {}
        server.log_event = lambda *args, **kwargs: None
        return server

    def client(self) -> coordinator.StratumClientState:
        client = coordinator.StratumClientState(
            sock=object(),
            address=("127.0.0.1", 3335),
            extranonce1_hex="00000001",
            connection_id=1,
        )
        client.subscribed = True
        client.authorized = True
        return client

    def job(self, job_id: str = "base", difficulty: Decimal = Decimal("1")) -> coordinator.AuxPowStratumJob:
        return coordinator.AuxPowStratumJob(
            job_id=job_id,
            aux_template={},
            btc_template={},
            bitcoin_script_pubkey_hex="",
            chain_nonce=0,
            chain_index=0,
            share_target=coordinator.difficulty_target(difficulty),
            qbit_target=0,
            parent_target=coordinator.difficulty_target(Decimal("1024")),
            share_difficulty=difficulty,
            coinbase_merkle_branch=[],
            prevhash="00" * 32,
            coinb1="",
            coinb2="",
            version="20000000",
            nbits="1d00ffff",
            ntime="00000000",
        )

    def test_accepted_share_retarget_uses_current_client_difficulty(self) -> None:
        server = self.server()
        client = self.client()
        client.share_difficulty = Decimal("16")
        client.vardiff_window_started_monotonic = time.monotonic() - 120
        stale_job = self.job(difficulty=Decimal("1"))
        captured: dict[str, object] = {}

        def capture_retarget(*args: object, **kwargs: object) -> None:
            captured.update(kwargs)

        server.retarget_client = capture_retarget

        server.note_vardiff_accepted_share(client, stale_job, "worker")

        self.assertEqual(captured["current_difficulty"], Decimal("16"))

    def test_idle_retarget_closes_elapsed_nonempty_window(self) -> None:
        server = self.server()
        client = self.client()
        client.vardiff_window_started_monotonic = time.monotonic() - 120
        client.vardiff_window_accepted = 2
        server.clients = {client}
        captured: dict[str, object] = {}

        def capture_retarget(*args: object, **kwargs: object) -> None:
            captured.update(kwargs)

        server.retarget_client = capture_retarget

        server.maybe_retarget_idle_clients()

        self.assertEqual(captured["accepted_shares"], 2)
        self.assertEqual(client.vardiff_window_accepted, 0)

    def test_retarget_defers_difficulty_until_next_job(self) -> None:
        server = self.server()
        client = self.client()
        server.current_job = self.job()
        captured: dict[str, object] = {"sent_jobs": 0}

        server.send_difficulty_value = lambda client, difficulty: captured.update({"difficulty": difficulty})
        server.send_job_to_client = lambda *args, **kwargs: captured.update({"sent_jobs": captured["sent_jobs"] + 1})

        with contextlib.redirect_stdout(io.StringIO()):
            server.retarget_client(
                client,
                current_difficulty=Decimal("1"),
                accepted_shares=24,
                submitted_shares=24,
                accepted_difficulty=Decimal("24"),
                elapsed_seconds=Decimal("90"),
                worker="worker",
                reason="test",
            )

        self.assertEqual(captured["difficulty"], Decimal("4"))
        self.assertEqual(client.pending_share_difficulty, Decimal("4"))
        self.assertEqual(client.share_difficulty, Decimal("1"))
        self.assertEqual(captured["sent_jobs"], 0)

    def test_idle_zero_share_windows_keep_stepping_down_pending_difficulty(self) -> None:
        server = self.server()
        client = self.client()
        client.share_difficulty = Decimal("16")
        client.vardiff_window_started_monotonic = time.monotonic() - 120
        server.clients = {client}
        captured: dict[str, object] = {}

        server.send_difficulty_value = lambda client, difficulty: captured.update({"difficulty": difficulty})

        with contextlib.redirect_stdout(io.StringIO()):
            server.maybe_retarget_idle_clients()

        self.assertEqual(captured["difficulty"], Decimal("4"))
        self.assertEqual(client.pending_share_difficulty, Decimal("4"))
        self.assertEqual(client.share_difficulty, Decimal("16"))

        client.vardiff_window_started_monotonic = time.monotonic() - 120
        captured.clear()

        with contextlib.redirect_stdout(io.StringIO()):
            server.maybe_retarget_idle_clients()

        self.assertEqual(captured["difficulty"], Decimal("1"))
        self.assertEqual(client.pending_share_difficulty, Decimal("1"))
        self.assertEqual(client.share_difficulty, Decimal("16"))

    def test_zero_share_window_resets_stale_vardiff_estimate(self) -> None:
        server = self.server()
        client = self.client()
        client.share_difficulty = Decimal("16")
        client.vardiff_difficulty_estimate = Decimal("128")
        server.send_difficulty_value = lambda *args, **kwargs: None

        with contextlib.redirect_stdout(io.StringIO()):
            server.retarget_client(
                client,
                current_difficulty=Decimal("16"),
                accepted_shares=0,
                submitted_shares=0,
                accepted_difficulty=Decimal("0"),
                elapsed_seconds=Decimal("90"),
                worker="worker",
                reason="test",
            )

        self.assertIsNone(client.vardiff_difficulty_estimate)
        self.assertEqual(client.pending_share_difficulty, Decimal("4"))

    def test_retarget_does_not_overwrite_newer_client_difficulty(self) -> None:
        server = self.server()
        client = self.client()
        client.share_difficulty = Decimal("8")
        server.current_job = self.job()
        sent_jobs = []

        def capture_send_job(*args: object, **kwargs: object) -> coordinator.AuxPowStratumJob:
            sent_jobs.append((args, kwargs))
            return self.job()

        server.send_job_to_client = capture_send_job

        server.retarget_client(
            client,
            current_difficulty=Decimal("1"),
            accepted_shares=24,
            submitted_shares=24,
            accepted_difficulty=Decimal("24"),
            elapsed_seconds=Decimal("90"),
            worker="worker",
            reason="test",
        )

        self.assertEqual(client.share_difficulty, Decimal("8"))
        self.assertEqual(sent_jobs, [])

    def test_refresh_job_does_not_register_base_job_when_vardiff_is_enabled(self) -> None:
        server = self.server()
        server.qbit_rpc = types.SimpleNamespace(
            call=lambda method, params=None, **kwargs: {
                "getbestblockhash": "qbit-tip",
                "createauxblock": {"height": 1, "hash": "aux-hash", "bits": "1d00ffff"},
            }[method]
        )
        server.bitcoin_rpc = types.SimpleNamespace(
            call=lambda method, params=None, **kwargs: {
                "getbestblockhash": "bitcoin-tip",
                "getblocktemplate": {
                    "height": 1,
                    "previousblockhash": "00" * 32,
                    "version": 0x20000000,
                    "bits": "1d00ffff",
                    "curtime": 1,
                },
            }[method]
        )
        server.make_job = lambda **kwargs: self.job(
            job_id=kwargs["job_id"],
            difficulty=kwargs["desired_share_difficulty"],
        )
        server.broadcast_job = lambda job: None

        with contextlib.redirect_stdout(io.StringIO()):
            refreshed = server.refresh_job(force=True)

        self.assertTrue(refreshed)
        self.assertEqual(server.jobs, {})
        self.assertIsNotNone(server.current_job)

    def test_pending_difficulty_applies_to_next_vardiff_job(self) -> None:
        server = self.server()
        client = self.client()
        client.share_difficulty = Decimal("1")
        client.pending_share_difficulty = Decimal("8")
        server.send_difficulty = lambda *args, **kwargs: None
        server.send_job = lambda *args, **kwargs: None

        job = server.send_job_to_client(client, self.job(), clean_jobs=True)

        self.assertLess(abs(job.share_difficulty - Decimal("8")), Decimal("1e-30"))
        self.assertLess(abs(client.share_difficulty - Decimal("8")), Decimal("1e-30"))
        self.assertIsNone(client.pending_share_difficulty)

    def test_send_job_keeps_pending_difficulty_set_after_job_build(self) -> None:
        server = self.server()
        client = self.client()
        client.share_difficulty = Decimal("1")
        server.send_difficulty = lambda *args, **kwargs: None
        server.send_job = lambda *args, **kwargs: None

        def build_old_difficulty_job(
            client: coordinator.StratumClientState,
            base_job: coordinator.AuxPowStratumJob,
            *,
            clean_jobs: bool,
        ) -> coordinator.AuxPowStratumJob:
            job = self.job(difficulty=Decimal("1"))
            job.clean_jobs = clean_jobs
            client.pending_share_difficulty = Decimal("8")
            return job

        server.job_for_client = build_old_difficulty_job

        job = server.send_job_to_client(client, self.job(), clean_jobs=True)

        self.assertEqual(job.share_difficulty, Decimal("1"))
        self.assertEqual(client.share_difficulty, Decimal("1"))
        self.assertEqual(client.pending_share_difficulty, Decimal("8"))

    def test_clean_vardiff_job_invalidates_previous_client_jobs(self) -> None:
        server = self.server()
        client = self.client()
        base_job = self.job()
        server.send_difficulty = lambda *args, **kwargs: None
        server.send_job = lambda *args, **kwargs: None

        first_job = server.send_job_to_client(client, base_job, clean_jobs=True)
        second_job = server.send_job_to_client(client, base_job, clean_jobs=True)

        self.assertNotIn(first_job.job_id, server.jobs)
        self.assertIn(second_job.job_id, server.jobs)
        self.assertNotIn(first_job.job_id, client.active_job_ids)
        self.assertEqual(client.active_job_ids, {second_job.job_id})

        with self.assertRaises(coordinator.StratumError) as raised:
            server.handle_submit(
                client,
                [
                    "worker",
                    first_job.job_id,
                    "00" * coordinator.AUXPOW_STRATUM_EXTRANONCE2_SIZE,
                    "00000000",
                    "00000000",
                ],
            )

        self.assertEqual(raised.exception.code, 21)

    def test_disconnect_prunes_active_vardiff_jobs(self) -> None:
        server = self.server()
        client = self.client()
        client.sock = Mock()
        server.clients = {client}
        server.send_difficulty = lambda *args, **kwargs: None
        server.send_job = lambda *args, **kwargs: None

        job = server.send_job_to_client(client, self.job(), clean_jobs=True)

        self.assertIn(job.job_id, server.jobs)

        server.disconnect_client(client)

        self.assertNotIn(job.job_id, server.jobs)
        self.assertEqual(client.active_job_ids, set())


if __name__ == "__main__":
    unittest.main()
