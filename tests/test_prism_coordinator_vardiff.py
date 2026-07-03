#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.prism_coordinator import (
    ClientState,
    DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS,
    MAX_ACTIVE_PRISM_JOBS_PER_CLIENT,
    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
    PRISM_REJECTION_REASON_IDS,
    PRISM_REJECTION_STALE_JOB,
    PRISM_REJECTION_SUBMITBLOCK_REJECTED,
    PRISM_REJECTION_UNKNOWN_JOB,
    QbitTipTemplateSnapshot,
    StratumError,
    PrismCoordinator,
    WorkerIdentity,
    default_prism_coinbase_tag_hex,
    default_prism_username_fallback_address,
    load_prism_vardiff_config,
    qbit_template_fingerprint,
    qbit_gbt_rules,
    env_positive_float,
    scaled_target_difficulty,
    target_from_compact,
    validate_prism_production_gate,
)

PAYOUT_ADDRESS = "tq1z70ukpvs96kye6jmgvl3nttevtkrq8uu89snkpm6m8gwqukw8u5dsz32kwa"


def tx_output(value_sats: int, script_hex: str) -> str:
    return value_sats.to_bytes(8, "little").hex() + direct_stratum.compact_size(len(bytes.fromhex(script_hex))).hex() + script_hex


def synthetic_witness_transaction(seed: str) -> str:
    script = seed * 3
    witness_item = seed * 5
    return (
        "01000000"
        + "0001"
        + "01"
        + (seed * 32)
        + "00000000"
        + direct_stratum.compact_size(len(bytes.fromhex(script))).hex()
        + script
        + "ffffffff"
        + "01"
        + tx_output(1, "51")
        + "01"
        + direct_stratum.compact_size(len(bytes.fromhex(witness_item))).hex()
        + witness_item
        + "00000000"
    )


class FakeJob:
    def __init__(self, difficulty: Decimal) -> None:
        self.share_difficulty = difficulty


class FakeLedger:
    backend_name = "fake"

    def __init__(self, shares: int = 0, prior_balances: list[dict[str, object]] | None = None) -> None:
        self.shares = shares
        self.prior_balances = prior_balances or []

    def all_shares(self) -> list[object]:
        return [object()] * self.shares

    def current_prior_balances(self) -> list[dict[str, object]]:
        return [dict(balance) for balance in self.prior_balances]

    def metrics(self) -> dict[str, int]:
        return {"blocks": 2, "owed_accounts": 3}

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        return []

    def mark_pool_block_inactive(self, *, block_hash: str, active_tip_height: int) -> dict[str, object]:
        return {"backend": "fake", "inactive_count": 0}

    def reject_prepared_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, object]:
        return {"backend": "fake", "rejected_count": 0}

    def reactivate_pool_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, object]:
        return {"backend": "fake", "reactivated_count": 0}

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, object]:
        return {"backend": "fake", "matured_count": 0}


class RecordingLedger(FakeLedger):
    def __init__(self) -> None:
        super().__init__(shares=0)
        self.pending: list[object] = []
        self.persisted: list[dict[str, object]] = []
        self.confirmed: list[dict[str, object]] = []
        self.reversed: list[dict[str, object]] = []
        self.rejected: list[dict[str, object]] = []
        self.submit_seen = False

    def append(self, pending: object) -> object:
        self.pending.append(pending)
        self.shares += 1
        return SimpleNamespace(share_seq=self.shares, miner_id=getattr(pending, "miner_id", "miner-a"))

    def persist_accepted_block(self, **kwargs: object) -> dict[str, object]:
        self.persisted.append({**kwargs, "submit_seen_at_persist": self.submit_seen})
        return {
            "backend": "fake",
            "share_count": self.shares,
            "block_count": 1,
            "bundle_count": 1,
            "payout_entry_count": 0,
            "carry_forward_count": 0,
            "onchain_output_count": 1,
        }

    def reverse_immature_block(self, **kwargs: object) -> dict[str, object]:
        self.reversed.append(kwargs)
        return {"backend": "fake", "reversed_count": 1}

    def reject_prepared_block(self, **kwargs: object) -> dict[str, object]:
        self.rejected.append(kwargs)
        return {"backend": "fake", "rejected_count": 1}

    def confirm_accepted_block(self, **kwargs: object) -> dict[str, object]:
        self.confirmed.append({**kwargs, "submit_seen_at_confirm": self.submit_seen})
        return {"backend": "fake", "confirmed_count": 1}

    def all_shares(self) -> list[object]:
        return [
            SimpleNamespace(miner_id=getattr(pending, "miner_id", "miner-a"))
            for pending in self.pending
        ]


class FakeRpc:
    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getblockchaininfo":
            return {"initialblockdownload": False}
        if method == "getnetworkinfo":
            return {"connections": 4}
        raise RuntimeError(method)


class FeeEstimateRpc(FakeRpc):
    def __init__(self, estimate: object) -> None:
        self.estimate = estimate
        self.calls: list[tuple[str, list[object] | None]] = []

    def call(self, method: str, params: list[object] | None = None) -> object:
        self.calls.append((method, params))
        if method == "estimatesmartfee":
            return self.estimate
        return super().call(method, params)


class TemplateRpc(FakeRpc):
    def __init__(self, template: object) -> None:
        self.template = template
        self.calls: list[tuple[str, list[object] | None]] = []

    def call(self, method: str, params: list[object] | None = None) -> object:
        self.calls.append((method, params))
        if method == "getblocktemplate":
            return self.template
        return super().call(method, params)


class AddressValidationRpc(FakeRpc):
    def __init__(
        self,
        *,
        valid_address: str = PAYOUT_ADDRESS,
        script_byte: str = "11",
        p2mr: bool = True,
    ) -> None:
        self.valid_address = valid_address
        self.script_byte = script_byte
        self.p2mr = p2mr
        self.validated: list[str] = []

    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "validateaddress":
            address = str((params or [""])[0])
            self.validated.append(address)
            script = "5220" + self.script_byte * 32 if self.p2mr else "51"
            return {"isvalid": address == self.valid_address, "scriptPubKey": script}
        return super().call(method, params)


AddressRpc = AddressValidationRpc


class TipRpc(FakeRpc):
    def __init__(self, tip: str) -> None:
        self.tip = tip

    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getbestblockhash":
            return self.tip
        return super().call(method, params)


class TipTemplateRpc(FakeRpc):
    def __init__(self, *, tip: str, template: dict[str, object]) -> None:
        self.tip = tip
        self.template = template
        self.calls: list[tuple[str, list[object] | None]] = []

    def call(self, method: str, params: list[object] | None = None) -> object:
        self.calls.append((method, params))
        if method == "getbestblockhash":
            return self.tip
        if method == "getblocktemplate":
            return self.template
        return super().call(method, params)


class ReorgRpc(TipTemplateRpc):
    def __init__(
        self,
        *,
        tip: str,
        template: dict[str, object],
        height: int,
        block_hashes: dict[int, str],
        initialblockdownload: bool = False,
        headers: int | None = None,
    ) -> None:
        super().__init__(tip=tip, template=template)
        self.height = height
        self.block_hashes = block_hashes
        self.initialblockdownload = initialblockdownload
        self.headers = headers if headers is not None else height

    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getblockchaininfo":
            return {
                "initialblockdownload": self.initialblockdownload,
                "blocks": self.height,
                "headers": self.headers,
            }
        if method == "getblockcount":
            return self.height
        if method == "getblockhash":
            height = int((params or [0])[0])
            try:
                return self.block_hashes[height]
            except KeyError as exc:
                raise RuntimeError(f"unknown height {height}") from exc
        return super().call(method, params)


class ReorgLedger(FakeLedger):
    def __init__(self, rows: list[dict[str, object]]) -> None:
        super().__init__(shares=0)
        self.rows = [dict(row) for row in rows]
        self.events: list[object] = []

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        self.events.append(("watch", active_tip_height))
        return [dict(row) for row in self.rows]

    def mark_pool_block_inactive(self, *, block_hash: str, active_tip_height: int) -> dict[str, object]:
        self.events.append(("inactive", block_hash, active_tip_height))
        for row in self.rows:
            if str(row.get("block_hash", "")).lower() == block_hash.lower():
                row["chain_state"] = "inactive"
                return {"backend": "fake", "inactive_count": 1}
        return {"backend": "fake", "inactive_count": 0}

    def reactivate_pool_block(self, *, block_hash: str, active_tip_height: int) -> dict[str, object]:
        self.events.append(("reactivate", block_hash, active_tip_height))
        for row in self.rows:
            if str(row.get("block_hash", "")).lower() == block_hash.lower():
                row["chain_state"] = "confirmed"
                return {"backend": "fake", "reactivated_count": 1}
        return {"backend": "fake", "reactivated_count": 0}

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, object]:
        self.events.append(("mature", active_tip_height))
        return {"backend": "fake", "matured_count": 0}


class SubmitRpc(FakeRpc):
    def __init__(
        self,
        *,
        tip: str,
        block_hash: str,
        submit_result: object = None,
        ledger: RecordingLedger | None = None,
    ) -> None:
        self.tip = tip
        self.block_hash = block_hash
        self.submit_result = submit_result
        self.ledger = ledger
        self.height = 9
        self.submitted = False

    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getbestblockhash":
            return self.tip
        if method == "getblockcount":
            return self.height
        if method == "submitblock":
            self.submitted = True
            if self.ledger is not None:
                self.ledger.submit_seen = True
            if self.submit_result is None:
                self.height += 1
            return self.submit_result
        if method == "getblockhash":
            return self.block_hash
        return super().call(method, params)


class SubmitAcceptingTemplateRpc(FakeRpc):
    def __init__(
        self,
        *,
        old_tip: str,
        block_hash: str,
        fail_template_after_submit: bool = False,
        ledger: RecordingLedger | None = None,
    ) -> None:
        self.old_tip = old_tip
        self.block_hash = block_hash
        self.fail_template_after_submit = fail_template_after_submit
        self.ledger = ledger
        self.height = 9
        self.submitted = False

    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getbestblockhash":
            return self.block_hash if self.submitted else self.old_tip
        if method == "getblockcount":
            return self.height
        if method == "submitblock":
            self.submitted = True
            self.height += 1
            if self.ledger is not None:
                self.ledger.submit_seen = True
            return None
        if method == "getblockhash":
            return self.block_hash
        if method == "getblocktemplate":
            if self.submitted and self.fail_template_after_submit:
                raise RuntimeError("transient getblocktemplate failure after submitblock")
            previousblockhash = self.block_hash if self.submitted else self.old_tip
            return gbt_template(previousblockhash, height=self.height + 1)
        return super().call(method, params)


def client() -> ClientState:
    state = ClientState(sock=object(), address=("127.0.0.1", 1), connection_id=1, extranonce1_hex="00000001")
    state.subscribed = True
    state.authorized = True
    return state


def gbt_template(
    previousblockhash: str,
    *,
    height: int = 10,
    coinbasevalue: int = 50_00000000,
    curtime: int = 1_700_000_000,
    transactions: list[str] | None = None,
) -> dict[str, object]:
    return {
        "previousblockhash": previousblockhash,
        "version": 0x20000000,
        "bits": "207fffff",
        "curtime": curtime,
        "height": height,
        "coinbasevalue": coinbasevalue,
        "transactions": [{"data": tx_hex} for tx_hex in transactions or []],
    }


def worker_identity(username: str = "miner-a") -> WorkerIdentity:
    return WorkerIdentity(
        username=username,
        payout_address=username,
        worker_name=None,
        script_pubkey_hex="5220" + "11" * 32,
        p2mr_program_hex="11" * 32,
    )


def stratum_job(
    job_id: str,
    *,
    difficulty: Decimal = Decimal("1"),
    clean_jobs: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        job_id=job_id,
        share_difficulty=difficulty,
        share_target=target_from_compact("207fffff"),
        prevhash="00" * 32,
        coinb1="",
        coinb2="",
        merkle_branch=(),
        version="20000000",
        nbits="207fffff",
        ntime="6553f100",
        clean_jobs=clean_jobs,
        transaction_hexes=(),
    )


def prism_context(
    job_id: str,
    previousblockhash: str,
    *,
    worker: WorkerIdentity | None = None,
    difficulty: Decimal = Decimal("1"),
    clean_jobs: bool = True,
) -> SimpleNamespace:
    return SimpleNamespace(
        job=stratum_job(job_id, difficulty=difficulty, clean_jobs=clean_jobs),
        template=gbt_template(previousblockhash),
        found_block={"network_difficulty": 1},
        issued_at_ms=12345,
        collection_only=False,
        worker=worker or worker_identity(),
        shares_json=[],
        prior_balances=[],
    )


def verified_block_bundle(coinbase_tx_hex: str = "c0ffee") -> dict[str, object]:
    return {
        "found_block": {"coinbase_value_sats": 50_00000000},
        "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
        "payout_policy_manifest": {"accounts": []},
        "signed_coinbase_manifest": {
            "manifest": {
                "coinbase_tx_hex": coinbase_tx_hex,
                "payout_count": 1,
            }
        },
    }


def verified_audit_report(coinbase_tx_hex: str = "c0ffee") -> dict[str, object]:
    return {
        "coinbase_txid": "11" * 32,
        "coinbase_manifest_sha256_hex": "22" * 32,
        "audit_bundle_sha256_hex": "33" * 32,
        "coinbase_tx_hex": coinbase_tx_hex,
    }


def coordinator() -> PrismCoordinator:
    server = PrismCoordinator.__new__(PrismCoordinator)
    server.vardiff_config = vardiff.VardiffConfig(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("0.000000001"),
        max_difficulty=Decimal("1024"),
        retarget_interval_seconds=Decimal("1"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("0.000000001"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("1"),
        retarget_tolerance=Decimal("0"),
    )
    server.share_difficulty = Decimal("0.000000001")
    server.lock = threading.RLock()
    server.stop_event = threading.Event()
    server.clients = set()
    server.submitted_share_count = 0
    server.stale_share_count = 0
    server.duplicate_share_count = 0
    server.low_difficulty_share_count = 0
    server.rejection_counts_by_reason = {reason: 0 for reason in PRISM_REJECTION_REASON_IDS}
    server.job_build_failure_count = 0
    server.tip_refresh_job_count = 0
    server.post_accept_refresh_failure_count = 0
    server.reorg_reconciler_enabled = False
    server.reorg_inactive_block_count = 0
    server.reorg_reactivated_block_count = 0
    server.reorg_reconcile_skip_count = 0
    server.reorg_reconcile_error_count = 0
    server.matured_payout_count = 0
    server.last_reorg_reconciled_tip_hash = None
    server.last_reorg_reconciled_trusted = False
    server.accepted_block_count = 1
    server.started_monotonic = time.monotonic() - 10
    server.ledger = FakeLedger(shares=5)
    server.latest_bundle = {
        "signed_coinbase_manifest": {
            "manifest": {
                "coinbase_tx_hex": "00" * 250,
            }
        }
    }
    server.rpc = FakeRpc()
    server.qbit_chain = "regtest"
    server.blockpoll_seconds = 2.0
    server.ctv_broadcaster_enabled = False
    server.ctv_broadcaster_wallet = None
    server.ctv_broadcaster_fee_sats = 0
    server.ctv_broadcaster_limit = 100
    server.ctv_broadcaster_interval_seconds = 30.0
    server.ctv_fanout_broadcast_daemon = None
    server._ctv_fanout_market_fee_rate_cache = {}
    server.tip_template_snapshot = None
    server.extranonce2_size = 8
    server.coinbase_tag_hex = default_prism_coinbase_tag_hex()
    server.version_mask = direct_stratum.QBIT_VERSION_ROLLING_MASK
    server.version_mask_selection = direct_stratum.VersionRollingMaskSelection(
        direct_stratum.QBIT_VERSION_ROLLING_MASK,
        "fallback",
        "test",
    )
    return server


def submit_coordinator(tip: str = "00" * 32) -> tuple[PrismCoordinator, ClientState, RecordingLedger]:
    server = coordinator()
    server.vardiff_config = SimpleNamespace(enabled=False)
    server.rpc = TipRpc(tip)
    server.jobs = {}
    server.recent_share_keys = set()
    server.accepted_block_count = 0
    server.max_blocks = 1
    server.stop_after_block = True
    server.extranonce2_size = 8
    server.share_weights_by_username = {"miner-a": 7}
    ledger = RecordingLedger()
    server.ledger = ledger
    worker = WorkerIdentity(
        username="miner-a",
        payout_address="miner-a",
        worker_name=None,
        script_pubkey_hex="5220" + "11" * 32,
        p2mr_program_hex="11" * 32,
    )
    context = SimpleNamespace(
        job=SimpleNamespace(
            job_id="job-1",
            share_target=target_from_compact("207fffff"),
            share_difficulty=Decimal("1"),
            transaction_hexes=(),
        ),
        template={"previousblockhash": tip, "height": 10, "coinbasevalue": 50_00000000},
        found_block={"network_difficulty": 1},
        issued_at_ms=12345,
        collection_only=False,
        worker=worker,
        shares_json=[],
        prior_balances=[],
    )
    state = client()
    state.username = "miner-a"
    state.worker = worker
    state.active_job_ids = {"job-1"}
    server.jobs["job-1"] = context
    return server, state, ledger


class PrismCoordinatorVardiffTests(unittest.TestCase):
    def test_load_prism_vardiff_config_defaults_to_small_miner_vardiff(self) -> None:
        names = [name for name in os.environ if name.startswith("PRISM_STRATUM_VARDIFF")]
        with patch.dict(os.environ, {}, clear=False):
            for name in names:
                os.environ.pop(name, None)
            config = load_prism_vardiff_config(Decimal("0.000000001"))

        self.assertTrue(config.enabled)
        self.assertEqual(config.target_share_interval_seconds, Decimal("15"))
        self.assertEqual(config.min_difficulty, Decimal("1E-9"))
        self.assertEqual(config.startup_difficulty, Decimal("1E-9"))
        self.assertEqual(config.max_step_factor, Decimal("4"))
        self.assertEqual(config.max_step_down_factor, Decimal("4"))

    def test_vardiff_retarget_sends_new_difficulty_and_clean_job(self) -> None:
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("1")
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        sent: dict[str, object] = {"jobs": 0}

        def fake_send_job(client: object, clean_jobs: bool) -> bool:
            sent.update({"jobs": sent["jobs"] + 1, "clean": clean_jobs})
            return True

        server.maybe_send_job = fake_send_job  # type: ignore[method-assign]

        server.note_vardiff_submitted_share(state)
        server.note_vardiff_accepted_share(state, FakeJob(Decimal("1")))  # type: ignore[arg-type]

        # Difficulty is now advertised by maybe_send_job alongside the job (gated on
        # a successful build), so the retarget commits the pending difficulty and
        # requests a single clean job.
        self.assertEqual(state.pending_share_difficulty, Decimal("4"))
        self.assertEqual(sent["jobs"], 1)
        self.assertTrue(sent["clean"])

    def test_vardiff_retarget_build_failure_keeps_consistent_difficulty_and_job(self) -> None:
        # If the job build is skipped during a retarget, the client must stay on its
        # existing job at its existing difficulty -- never advertise a new difficulty
        # for a job it never received. Otherwise its easier shares miss the old
        # target, nothing is accepted, and (since retargets only fire on accepted
        # shares) it cannot self-heal without reconnecting.
        server = coordinator()
        server.jobs = {"old-job": SimpleNamespace(job=SimpleNamespace(job_id="old-job"))}
        server.extranonce2_size = 8
        state = client()
        state.username = "miner-a"
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        state.share_difficulty = Decimal("1")
        state.active_job_ids = {"old-job"}
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        advertised: list[object] = []
        state.send = lambda payload: advertised.append(payload)  # type: ignore[method-assign]

        def failing_build(client: ClientState, *, clean_jobs: bool) -> object:
            raise ValueError("transient getblocktemplate failure")

        server.build_job_for_client = failing_build  # type: ignore[method-assign]

        server.note_vardiff_submitted_share(state)
        server.note_vardiff_accepted_share(state, FakeJob(Decimal("1")))  # type: ignore[arg-type]

        self.assertEqual(server.job_build_failure_count, 1)
        self.assertIsNone(state.pending_share_difficulty)  # rolled back, not left at the new value
        self.assertEqual(state.share_difficulty, Decimal("1"))  # unchanged
        self.assertEqual(state.active_job_ids, {"old-job"})  # old job retained, still submittable
        self.assertEqual(set(server.jobs), {"old-job"})
        self.assertEqual(advertised, [])  # no set_difficulty / notify advertised for the skipped build

    def test_maybe_send_job_isolates_build_failure_and_keeps_client_connected(self) -> None:
        server = coordinator()
        server.jobs = {}
        server.extranonce2_size = 8
        state = client()
        state.username = "miner-a"
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def boom(client: ClientState, *, clean_jobs: bool) -> None:
            raise ValueError(
                "full coinbase transaction does not end its coinbase scriptSig "
                "with the extranonce placeholder"
            )

        server.build_job_for_client = boom  # type: ignore[method-assign]

        # The bug: this used to propagate out of handle_client and drop the miner.
        # It must now be swallowed so the client thread survives a single bad template.
        server.maybe_send_job(state, clean_jobs=True)

        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(state.active_job_ids, set())
        self.assertEqual(server.jobs, {})
        self.assertEqual(sent, [])  # no difficulty / mining.notify pushed for the failed build

        # A subsequent good template still issues a job (skip, do not permanently break).
        server.build_job_for_client = lambda client, *, clean_jobs: SimpleNamespace(  # type: ignore[method-assign]
            job=SimpleNamespace(
                job_id="job-ok",
                share_difficulty=Decimal("1"),
                share_target=target_from_compact("207fffff"),
            ),
            collection_only=False,
        )
        server.send_difficulty = lambda client, job: None  # type: ignore[method-assign]
        server.send_job = lambda client, job: sent.append("notify")  # type: ignore[method-assign]
        server.apply_job_difficulty = lambda client, job: None  # type: ignore[method-assign]

        server.maybe_send_job(state, clean_jobs=True)

        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(state.active_job_ids, {"job-ok"})
        self.assertEqual(sent, ["notify"])

    def test_maybe_send_job_does_not_swallow_send_failures_as_build_failures(self) -> None:
        # Only the job build is isolated. A Stratum send failure (a dead socket)
        # must propagate so handle_client disconnects and cleans up, rather than
        # being miscounted as a build failure or leaving the client wedged.
        server = coordinator()
        server.jobs = {}
        server.extranonce2_size = 8
        state = client()
        state.username = "miner-a"
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )

        server.build_job_for_client = lambda client, *, clean_jobs: SimpleNamespace(  # type: ignore[method-assign]
            job=SimpleNamespace(
                job_id="job-dead",
                share_difficulty=Decimal("1"),
                share_target=target_from_compact("207fffff"),
            ),
            collection_only=False,
        )
        server.send_difficulty = lambda client, job: None  # type: ignore[method-assign]

        def dead_socket(client: ClientState, job: object) -> None:
            raise OSError("broken pipe")

        server.send_job = dead_socket  # type: ignore[method-assign]

        with self.assertRaises(OSError):
            server.maybe_send_job(state, clean_jobs=True)

        # The send failure is not a build failure, and handle_client (not us) owns
        # the disconnect/cleanup of the registered job for the dead connection.
        self.assertEqual(server.job_build_failure_count, 0)

    def test_metrics_include_issue_scope_operational_gauges(self) -> None:
        server = coordinator()
        server.submitted_share_count = 10
        server.stale_share_count = 2
        server.duplicate_share_count = 1
        server.low_difficulty_share_count = 3
        server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB] = 2
        server.rejection_counts_by_reason["duplicate-share"] = 1
        server.rejection_counts_by_reason["low-difficulty"] = 3
        server.tip_refresh_job_count = 4
        server.post_accept_refresh_failure_count = 5

        metrics = server.metrics_payload()

        self.assertIn("qbit_prism_submitted_shares_total 10", metrics)
        self.assertIn("qbit_prism_stale_shares_total 2", metrics)
        self.assertIn("qbit_prism_duplicate_shares_total 1", metrics)
        self.assertIn("qbit_prism_low_difficulty_shares_total 3", metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="stale-job"} 2', metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="duplicate-share"} 1', metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="low-difficulty"} 3', metrics)
        self.assertIn("qbit_prism_tip_refresh_jobs_total 4", metrics)
        self.assertIn("qbit_prism_post_accept_refresh_failures_total 5", metrics)
        self.assertIn("qbit_prism_stale_share_percent 20", metrics)
        self.assertIn("qbit_prism_coinbase_weight_headroom_bytes 1999750", metrics)
        self.assertIn("qbit_prism_vardiff_enabled 1", metrics)
        self.assertIn("qbit_prism_qbitd_initial_block_download 0", metrics)
        self.assertIn("qbit_prism_qbitd_peers 4", metrics)

    def test_send_error_includes_canonical_reason_id_data(self) -> None:
        server = coordinator()
        sent: list[dict[str, object]] = []
        client = SimpleNamespace(send=lambda payload: sent.append(payload))

        server.send_error(client, "submit-1", 21, "stale job", reason=PRISM_REJECTION_STALE_JOB)  # type: ignore[arg-type]

        self.assertEqual(
            sent,
            [
                {
                    "id": "submit-1",
                    "result": None,
                    "error": [21, "stale job", {"reason_id": PRISM_REJECTION_STALE_JOB}],
                }
            ],
        )

    def test_scaled_target_difficulty_uses_pow_limit_units(self) -> None:
        pow_limit = target_from_compact("207fffff")

        self.assertEqual(scaled_target_difficulty(pow_limit), 1_000_000)
        self.assertEqual(scaled_target_difficulty(pow_limit // 4), 4_000_000)

    def test_qbit_gbt_rules_include_signet_rule_only_for_signet(self) -> None:
        self.assertEqual(qbit_gbt_rules("regtest"), ["segwit"])
        self.assertEqual(qbit_gbt_rules("testnet4"), ["segwit"])
        self.assertEqual(qbit_gbt_rules("signet"), ["segwit", "signet"])

    def test_qbit_template_fingerprint_ignores_clock_only_fields(self) -> None:
        base = gbt_template("00" * 32, curtime=1)
        base["longpollid"] = "10:0"
        base["mintime"] = 1
        clock_only = gbt_template("00" * 32, curtime=2)
        clock_only["longpollid"] = "10:1"
        clock_only["mintime"] = 2
        changed_value = dict(clock_only)
        changed_value["coinbasevalue"] = 49_99999999

        self.assertEqual(qbit_template_fingerprint(base), qbit_template_fingerprint(clock_only))
        self.assertNotEqual(qbit_template_fingerprint(base), qbit_template_fingerprint(changed_value))

    def test_resolve_version_mask_uses_gbt_versionrollingmask(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = TemplateRpc({"versionrollingmask": "1fffe000"})
        server.rpc = rpc
        server.qbit_chain = "signet"

        selection = server.resolve_version_rolling_mask(0x000000FF)

        self.assertEqual(selection.selected_mask, 0x1FFFE000)
        self.assertEqual(selection.source, "qbit_getblocktemplate")
        self.assertEqual(rpc.calls, [("getblocktemplate", [{"rules": ["segwit", "signet"]}])])

    def test_resolve_version_mask_falls_back_only_when_gbt_missing_or_unavailable(self) -> None:
        missing = PrismCoordinator.__new__(PrismCoordinator)
        missing.rpc = TemplateRpc({})
        missing.qbit_chain = "regtest"

        missing_selection = missing.resolve_version_rolling_mask(direct_stratum.QBIT_VERSION_ROLLING_MASK)

        self.assertEqual(missing_selection.selected_mask, direct_stratum.QBIT_VERSION_ROLLING_MASK)
        self.assertEqual(missing_selection.source, "fallback")
        self.assertEqual(missing_selection.detail, "missing_versionrollingmask")

        unavailable = PrismCoordinator.__new__(PrismCoordinator)
        unavailable.rpc = FakeRpc()
        unavailable.qbit_chain = "regtest"

        unavailable_selection = unavailable.resolve_version_rolling_mask(direct_stratum.QBIT_VERSION_ROLLING_MASK)

        self.assertEqual(unavailable_selection.selected_mask, direct_stratum.QBIT_VERSION_ROLLING_MASK)
        self.assertEqual(unavailable_selection.source, "fallback")
        self.assertTrue(unavailable_selection.detail.startswith("probe_error:"))

    def test_resolve_version_mask_disables_only_on_gbt_zero_mask(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.rpc = TemplateRpc({"versionrollingmask": "00000000"})
        server.qbit_chain = "regtest"

        selection = server.resolve_version_rolling_mask(direct_stratum.QBIT_VERSION_ROLLING_MASK)

        self.assertEqual(selection.selected_mask, 0)
        self.assertEqual(selection.source, "qbit_getblocktemplate")
        self.assertEqual(selection.detail, "disabled_by_zero_mask")

    def test_resolve_version_mask_rejects_invalid_gbt_mask(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.rpc = TemplateRpc({"versionrollingmask": "not-hex"})
        server.qbit_chain = "regtest"

        with self.assertRaisesRegex(SystemExit, "invalid getblocktemplate.versionrollingmask"):
            server.resolve_version_rolling_mask(direct_stratum.QBIT_VERSION_ROLLING_MASK)

    def test_configure_negotiates_requested_mask_with_gbt_server_mask(self) -> None:
        server = coordinator()
        server.version_mask = 0x1FFFE000
        state = client()
        captured: dict[str, object] = {}
        server.send_result = lambda _client, request_id, result: captured.update(  # type: ignore[method-assign]
            {"request_id": request_id, "result": result}
        )

        server.handle_configure(
            state,
            "configure-1",
            [
                ["version-rolling"],
                {"version-rolling.mask": "0000f000"},
            ],
        )

        self.assertEqual(captured["request_id"], "configure-1")
        self.assertEqual(
            captured["result"],
            {
                "version-rolling": True,
                "version-rolling.mask": "0000e000",
            },
        )
        self.assertEqual(state.version_mask, 0x0000E000)

    def test_configure_disables_version_rolling_when_gbt_mask_is_zero(self) -> None:
        server = coordinator()
        server.version_mask = 0
        state = client()
        captured: dict[str, object] = {}
        server.send_result = lambda _client, request_id, result: captured.update(  # type: ignore[method-assign]
            {"request_id": request_id, "result": result}
        )

        server.handle_configure(
            state,
            "configure-1",
            [
                ["version-rolling"],
                {"version-rolling.mask": "ffffffff"},
            ],
        )

        self.assertEqual(
            captured["result"],
            {
                "version-rolling": False,
                "version-rolling.mask": "00000000",
            },
        )
        self.assertEqual(state.version_mask, 0)

    def test_accepted_share_difficulty_uses_actual_target_unless_overridden(self) -> None:
        server = coordinator()
        worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        context = SimpleNamespace(
            worker=worker,
            job=SimpleNamespace(share_target=target_from_compact("207fffff") // 2),
        )
        server.share_weights_by_username = {}

        self.assertEqual(server.accepted_share_difficulty(context), 2_000_000)

        server.share_weights_by_username = {"miner-a": 7}
        self.assertEqual(server.accepted_share_difficulty(context), 7)

    def test_resolve_worker_accepts_bare_payout_address(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(script_byte="22")
        server.rpc = rpc

        worker = server.resolve_worker(PAYOUT_ADDRESS)

        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS])
        self.assertEqual(worker.username, PAYOUT_ADDRESS)
        self.assertEqual(worker.payout_address, PAYOUT_ADDRESS)
        self.assertIsNone(worker.worker_name)
        self.assertEqual(worker.p2mr_program_hex, "22" * 32)

    def test_resolve_worker_accepts_address_worker_username(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(script_byte="33")
        server.rpc = rpc

        worker = server.resolve_worker(f"{PAYOUT_ADDRESS}.rig-a")

        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS])
        self.assertEqual(worker.username, f"{PAYOUT_ADDRESS}.rig-a")
        self.assertEqual(worker.payout_address, PAYOUT_ADDRESS)
        self.assertEqual(worker.worker_name, "rig-a")
        self.assertEqual(worker.p2mr_program_hex, "33" * 32)

    def test_resolve_worker_rejects_invalid_base_address_with_worker_suffix(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc()
        server.rpc = rpc
        server.username_fallback_address = None

        with self.assertRaises(StratumError) as raised:
            server.resolve_worker("not-a-qbit-address.rig-a")

        self.assertEqual(raised.exception.code, 20)
        self.assertEqual(rpc.validated, ["not-a-qbit-address"])

    def test_resolve_worker_uses_configured_fallback_for_invalid_username(self) -> None:
        fallback_address = "tq1fallback"
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(valid_address=fallback_address, script_byte="44")
        server.rpc = rpc
        server.username_fallback_address = fallback_address

        worker = server.resolve_worker("not-a-qbit-address.rig-a")

        self.assertEqual(rpc.validated, ["not-a-qbit-address", fallback_address])
        self.assertEqual(worker.username, "not-a-qbit-address.rig-a")
        self.assertEqual(worker.payout_address, fallback_address)
        self.assertEqual(worker.worker_name, "rig-a")
        self.assertEqual(worker.p2mr_program_hex, "44" * 32)

    def test_resolve_worker_uses_testnet_default_fallback_for_invalid_username(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(valid_address=DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS, script_byte="55")
        server.rpc = rpc

        with patch.dict(os.environ, {"QBIT_CHAIN": "testnet4"}, clear=True):
            worker = server.resolve_worker("not-a-qbit-address")

        self.assertEqual(rpc.validated, ["not-a-qbit-address", DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS])
        self.assertEqual(worker.username, "not-a-qbit-address")
        self.assertEqual(worker.payout_address, DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS)
        self.assertIsNone(worker.worker_name)
        self.assertEqual(worker.p2mr_program_hex, "55" * 32)

    def test_default_username_fallback_is_testnet_only_unless_configured(self) -> None:
        with patch.dict(os.environ, {"QBIT_CHAIN": "testnet4"}, clear=True):
            self.assertEqual(default_prism_username_fallback_address(), DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS)
        with patch.dict(os.environ, {"QBIT_CHAIN": "regtest"}, clear=True):
            self.assertIsNone(default_prism_username_fallback_address())
        with patch.dict(
            os.environ,
            {"QBIT_CHAIN": "regtest", "PRISM_USERNAME_FALLBACK_ADDRESS": "qbrt1fallback"},
            clear=True,
        ):
            self.assertEqual(default_prism_username_fallback_address(), "qbrt1fallback")

    def test_prism_payout_policy_defaults_to_no_pool_fee(self) -> None:
        server = coordinator()

        with patch.dict(os.environ, {}, clear=True):
            policy = server.prism_payout_policy()

        self.assertEqual(
            policy,
            {
                "p2mr_spend_input_bytes": 3_680,
                "target_feerate_sats_per_byte": 1,
                "safety_multiplier": 4,
            },
        )

    def test_prism_payout_policy_allows_fixed_min_output_bits_override(self) -> None:
        server = coordinator()

        with patch.dict(os.environ, {"PRISM_PAYOUT_MIN_OUTPUT_BITS": "10000"}, clear=True):
            policy = server.prism_payout_policy()

        self.assertEqual(
            policy,
            {
                "p2mr_spend_input_bytes": 3_680,
                "target_feerate_sats_per_byte": 1,
                "safety_multiplier": 4,
                "min_output_sats": 10_000,
            },
        )

    def test_prism_payout_policy_falls_back_to_legacy_min_output_sats_override(self) -> None:
        server = coordinator()

        with patch.dict(os.environ, {"PRISM_PAYOUT_MIN_OUTPUT_SATS": "10000"}, clear=True):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["min_output_sats"], 10_000)

    def test_prism_payout_policy_min_output_bits_overrides_legacy_sats_override(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_PAYOUT_MIN_OUTPUT_BITS": "11000",
                "PRISM_PAYOUT_MIN_OUTPUT_SATS": "10000",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["min_output_sats"], 11_000)

    def test_prism_coinbase_tag_defaults_to_prism(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(default_prism_coinbase_tag_hex(), "/PRISM/".encode("ascii").hex())

    def test_prism_coinbase_tag_is_configurable_and_can_be_disabled(self) -> None:
        with patch.dict(os.environ, {"PRISM_COINBASE_TAG": "/CUSTOM/"}, clear=True):
            self.assertEqual(default_prism_coinbase_tag_hex(), "/CUSTOM/".encode("ascii").hex())
        with patch.dict(os.environ, {"PRISM_COINBASE_TAG": ""}, clear=True):
            self.assertEqual(default_prism_coinbase_tag_hex(), "")

    def test_prism_coinbase_tag_rejects_non_printable_or_long_values(self) -> None:
        for tag, message in (
            ("PRISM\n", "printable ASCII"),
            ("P" * 41, "at most 40 bytes"),
            ("PRISM-π", "ASCII"),
        ):
            with self.subTest(tag=tag), patch.dict(
                os.environ, {"PRISM_COINBASE_TAG": tag}, clear=True
            ):
                with self.assertRaisesRegex(SystemExit, message):
                    default_prism_coinbase_tag_hex()

    def test_coinbase_script_sig_suffix_places_pool_tag_before_extranonce(self) -> None:
        server = coordinator()
        server.coinbase_tag_hex = "/PRISM/".encode("ascii").hex()

        suffix = server.coinbase_script_sig_suffix_hex("aabbccdd", "00" * 8)

        self.assertEqual(suffix, "/PRISM/".encode("ascii").hex() + "aabbccdd" + "00" * 8)
        self.assertTrue(suffix.endswith("aabbccdd" + "00" * 8))

    def test_prism_payout_policy_allows_formula_overrides(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_PAYOUT_P2MR_SPEND_INPUT_BYTES": "2500",
                "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE": "2",
                "PRISM_PAYOUT_SAFETY_MULTIPLIER": "3",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(
            policy,
            {
                "p2mr_spend_input_bytes": 2_500,
                "target_feerate_sats_per_byte": 2,
                "safety_multiplier": 3,
            },
        )

    def test_prism_payout_policy_formula_uses_legacy_feerate_alias(self) -> None:
        server = coordinator()

        with patch.dict(os.environ, {"PRISM_PAYOUT_TARGET_FEERATE_SATS_PER_BYTE": "2"}, clear=True):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["target_feerate_sats_per_byte"], 2)

    def test_prism_payout_policy_formula_bits_feerate_overrides_legacy_alias(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE": "3",
                "PRISM_PAYOUT_TARGET_FEERATE_SATS_PER_BYTE": "2",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["target_feerate_sats_per_byte"], 3)

    def test_prism_payout_policy_rejects_invalid_floor_settings(self) -> None:
        cases = [
            ({"PRISM_PAYOUT_MIN_OUTPUT_BITS": "0"}, "PRISM_PAYOUT_MIN_OUTPUT_BITS must be positive"),
            (
                {"PRISM_PAYOUT_MIN_OUTPUT_BITS": "not-int"},
                "PRISM_PAYOUT_MIN_OUTPUT_BITS must be an integer",
            ),
            ({"PRISM_PAYOUT_MIN_OUTPUT_SATS": "0"}, "PRISM_PAYOUT_MIN_OUTPUT_SATS must be positive"),
            (
                {"PRISM_PAYOUT_MIN_OUTPUT_SATS": "not-int"},
                "PRISM_PAYOUT_MIN_OUTPUT_SATS must be an integer",
            ),
            (
                {"PRISM_PAYOUT_SAFETY_MULTIPLIER": "0"},
                "PRISM_PAYOUT_SAFETY_MULTIPLIER must be positive",
            ),
            (
                {"PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE": "0"},
                "PRISM_PAYOUT_TARGET_FEERATE_BITS_PER_BYTE must be positive",
            ),
        ]
        for env_vars, expected in cases:
            with self.subTest(env_vars=env_vars), patch.dict(os.environ, env_vars, clear=True):
                server = coordinator()
                with self.assertRaisesRegex(SystemExit, expected):
                    server.prism_payout_policy()

    def test_prism_pool_fee_address_config_validates_p2mr_address(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="88")

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(server.rpc.validated, ["tq1fee"])
        self.assertEqual(
            policy["pool_fee_policy"],
            {
                "fee_bps": 125,
                "recipient_id": "tq1fee",
                "order_key": "tq1fee",
                "p2mr_program_hex": "88" * 32,
            },
        )

    def test_prism_pool_fee_enabled_allows_zero_bps_policy(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="66")

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "0",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["pool_fee_policy"]["fee_bps"], 0)
        self.assertEqual(policy["pool_fee_policy"]["p2mr_program_hex"], "66" * 32)

    def test_prism_pool_fee_program_config_requires_recipient_identity(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_P2MR_PROGRAM_HEX": "55" * 32,
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "PRISM_POOL_FEE_RECIPIENT_ID"):
                server.prism_payout_policy()

    def test_prism_pool_fee_program_config_uses_explicit_order_key(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_P2MR_PROGRAM_HEX": "55" * 32,
                "PRISM_POOL_FEE_RECIPIENT_ID": "pool-fee",
                "PRISM_POOL_FEE_ORDER_KEY": "000-pool-fee",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(
            policy["pool_fee_policy"],
            {
                "fee_bps": 125,
                "recipient_id": "pool-fee",
                "order_key": "000-pool-fee",
                "p2mr_program_hex": "55" * 32,
            },
        )

    def test_prism_pool_fee_config_rejects_ambiguous_or_invalid_settings(self) -> None:
        cases = [
            (
                {"PRISM_POOL_FEE_ENABLED": "1", "PRISM_POOL_FEE_ADDRESS": "tq1fee"},
                "PRISM_POOL_FEE_BPS",
            ),
            (
                {
                    "PRISM_POOL_FEE_ENABLED": "1",
                    "PRISM_POOL_FEE_BPS": "10001",
                    "PRISM_POOL_FEE_ADDRESS": "tq1fee",
                },
                "between 0 and 10000",
            ),
            (
                {
                    "PRISM_POOL_FEE_ENABLED": "1",
                    "PRISM_POOL_FEE_BPS": "125",
                    "PRISM_POOL_FEE_ADDRESS": "tq1fee",
                    "PRISM_POOL_FEE_P2MR_PROGRAM_HEX": "55" * 32,
                },
                "exactly one",
            ),
        ]
        for env_vars, expected in cases:
            with self.subTest(env_vars=env_vars), patch.dict(os.environ, env_vars, clear=True):
                server = coordinator()
                server.rpc = AddressRpc(valid_address="tq1fee")
                with self.assertRaisesRegex(SystemExit, expected):
                    server.prism_payout_policy()

    def test_prism_pool_fee_config_rejects_disabled_fee_settings(self) -> None:
        cases = [
            {"PRISM_POOL_FEE_BPS": "125"},
            {"PRISM_POOL_FEE_ADDRESS": "tq1fee"},
            {"PRISM_POOL_FEE_P2MR_PROGRAM_HEX": "55" * 32},
            {"PRISM_POOL_FEE_RECIPIENT_ID": "pool-fee"},
            {
                "PRISM_POOL_FEE_ENABLED": "0",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
        ]
        for env_vars in cases:
            with self.subTest(env_vars=env_vars), patch.dict(os.environ, env_vars, clear=True):
                server = coordinator()
                server.rpc = AddressRpc(valid_address="tq1fee")
                with self.assertRaisesRegex(SystemExit, "PRISM_POOL_FEE_ENABLED=1"):
                    server.prism_payout_policy()

    def test_prism_pool_fee_config_rejects_non_p2mr_fee_address(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", p2mr=False)

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "P2MR"):
                server.prism_payout_policy()

    def test_build_audit_bundle_passes_pool_fee_policy_to_cli_payload(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="99")
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        def fake_run(cmd: list[str], **kwargs: object) -> object:
            captured["cmd"] = cmd
            captured["payload"] = json.loads(str(kwargs["input"]))
            return SimpleNamespace(returncode=0, stdout='{"ok": true}', stderr="")

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ), patch("lab.prism.prism_coordinator.subprocess.run", side_effect=fake_run):
            bundle = server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
            )

        self.assertEqual(bundle, {"ok": True})
        self.assertEqual(captured["payload"]["payout_policy"]["pool_fee_policy"]["fee_bps"], 125)
        self.assertEqual(
            captured["payload"]["payout_policy"]["pool_fee_policy"]["p2mr_program_hex"],
            "99" * 32,
        )

    def test_build_audit_bundle_passes_ctv_settlement_config_to_cli_payload(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        def fake_run(cmd: list[str], **kwargs: object) -> object:
            captured["payload"] = json.loads(str(kwargs["input"]))
            return SimpleNamespace(returncode=0, stdout='{"ok": true}', stderr="")

        with patch.dict(
            os.environ,
            {
                "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_BITS": "10485760",
                "PRISM_MAX_COINBASE_SETTLEMENT_OUTPUTS": "16",
                "PRISM_MAX_DIRECT_COINBASE_OUTPUTS": "12",
                "PRISM_MAX_CTV_FANOUT_RECIPIENTS_PER_TRANSACTION": "1000",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "25",
                "PRISM_CTV_FANOUT_FEE_PREMIUM_BPS": "12000",
            },
            clear=True,
        ), patch("lab.prism.prism_coordinator.subprocess.run", side_effect=fake_run):
            bundle = server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
            )

        self.assertEqual(bundle, {"ok": True})
        self.assertEqual(
            captured["payload"]["ctv_settlement"],
            {
                "direct_floor_sats": 10_485_760,
                "config": {
                    "max_coinbase_settlement_outputs": 16,
                    "max_direct_coinbase_outputs": 12,
                    "max_fanout_recipients_per_transaction": 1000,
                    "reserved_coinbase_outputs": 0,
                },
                "fanout_fee_rate_policy": {
                    "market_fee_rate_sats_per_1000_weight": 25,
                    "premium_bps": 12_000,
                },
            },
        )

    def test_prism_ctv_settlement_config_uses_legacy_unit_aliases(self) -> None:
        server = coordinator()

        with patch.dict(
            os.environ,
            {
                "PRISM_CTV_SETTLEMENT_ENABLED": "1",
                "PRISM_DIRECT_COINBASE_PAYOUT_FLOOR_SATS": "10485760",
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_SATS_PER_1000_WEIGHT": "25",
            },
            clear=True,
        ):
            config = server.prism_ctv_settlement_config()

        self.assertIsNotNone(config)
        assert config is not None
        self.assertEqual(config["direct_floor_sats"], 10_485_760)
        self.assertEqual(
            config["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 25, "premium_bps": 12_000},
        )

    def test_prism_ctv_settlement_config_uses_node_fee_estimate_by_default(self) -> None:
        server = coordinator()
        rpc = FeeEstimateRpc({"feerate": "0.00001001"})
        server.rpc = rpc

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            config = server.prism_ctv_settlement_config()

        self.assertIsNotNone(config)
        assert config is not None
        self.assertEqual(
            config["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 1001, "premium_bps": 12_000},
        )
        self.assertIn(("estimatesmartfee", [2]), rpc.calls)

    def test_prism_ctv_settlement_config_caches_node_fee_estimate_per_block_height(self) -> None:
        server = coordinator()
        rpc = FeeEstimateRpc({"feerate": "0.00001001"})
        server.rpc = rpc

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            first = server.prism_ctv_settlement_config(block_height=10)
            rpc.estimate = {"feerate": "0.00002000"}
            second = server.prism_ctv_settlement_config(block_height=10)
            next_height = server.prism_ctv_settlement_config(block_height=11)

        assert first is not None
        assert second is not None
        assert next_height is not None
        self.assertEqual(
            first["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 1001, "premium_bps": 12_000},
        )
        self.assertEqual(second["fanout_fee_rate_policy"], first["fanout_fee_rate_policy"])
        self.assertEqual(
            next_height["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 2000, "premium_bps": 12_000},
        )
        self.assertEqual(
            [call for call in rpc.calls if call[0] == "estimatesmartfee"],
            [("estimatesmartfee", [2]), ("estimatesmartfee", [2])],
        )

    def test_prism_ctv_settlement_config_separates_fee_cache_by_parent_hash(self) -> None:
        server = coordinator()
        rpc = FeeEstimateRpc({"feerate": "0.00001001"})
        server.rpc = rpc

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            first = server.prism_ctv_settlement_config(block_height=10, parent_hash="aa" * 32)
            rpc.estimate = {"feerate": "0.00002000"}
            same_parent = server.prism_ctv_settlement_config(block_height=10, parent_hash="aa" * 32)
            reorg_parent = server.prism_ctv_settlement_config(block_height=10, parent_hash="bb" * 32)

        assert first is not None
        assert same_parent is not None
        assert reorg_parent is not None
        self.assertEqual(same_parent["fanout_fee_rate_policy"], first["fanout_fee_rate_policy"])
        self.assertEqual(
            reorg_parent["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 2000, "premium_bps": 12_000},
        )
        self.assertEqual(
            [call for call in rpc.calls if call[0] == "estimatesmartfee"],
            [("estimatesmartfee", [2]), ("estimatesmartfee", [2])],
        )

    def test_prism_ctv_settlement_config_fails_closed_when_fee_estimate_unavailable(self) -> None:
        server = coordinator()
        server.rpc = FeeEstimateRpc({"errors": ["insufficient data"]})

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "unable to compute PRISM CTV fanout fee rate"):
                server.prism_ctv_settlement_config()

    def test_prism_ctv_settlement_config_retries_after_fee_estimate_failure(self) -> None:
        server = coordinator()
        rpc = FeeEstimateRpc({"errors": ["insufficient data"]})
        server.rpc = rpc

        with patch.dict(os.environ, {"PRISM_CTV_SETTLEMENT_ENABLED": "1"}, clear=True):
            with self.assertRaises(RuntimeError):
                server.prism_ctv_settlement_config(block_height=10)
            rpc.estimate = {"feerate": "0.00002000"}
            recovered = server.prism_ctv_settlement_config(block_height=10)

        assert recovered is not None
        self.assertEqual(
            recovered["fanout_fee_rate_policy"],
            {"market_fee_rate_sats_per_1000_weight": 2000, "premium_bps": 12_000},
        )
        self.assertEqual(
            [call for call in rpc.calls if call[0] == "estimatesmartfee"],
            [("estimatesmartfee", [2]), ("estimatesmartfee", [2])],
        )

    def test_ctv_broadcaster_daemon_uses_coordinator_ledger_and_config(self) -> None:
        server = coordinator()
        server.ctv_broadcaster_wallet = None
        server.ctv_broadcaster_fee_sats = 0
        server.ctv_broadcaster_limit = 7
        captured: dict[str, object] = {}

        class FakeDaemon:
            def __init__(self, ledger: object, broadcaster: object, *, fee_sats: int) -> None:
                captured["ledger"] = ledger
                captured["broadcaster"] = broadcaster
                captured["fee_sats"] = fee_sats

            def run_once(self, *, limit: int) -> object:
                captured["limit"] = limit
                return SimpleNamespace(
                    scanned_count=1,
                    submitted_count=0,
                    updated_count=1,
                    failed_count=0,
                )

        with patch("lab.prism.prism_coordinator.CtvFanoutBroadcastDaemon", FakeDaemon):
            result = server.run_ctv_fanout_broadcaster_once()

        self.assertIs(captured["ledger"], server.ledger)
        self.assertEqual(captured["fee_sats"], 0)
        self.assertEqual(captured["limit"], 7)
        self.assertEqual(result.updated_count, 1)
        self.assertIsNotNone(captured["broadcaster"])

    def test_ctv_broadcaster_daemon_requires_wallet_for_cpfp_fee(self) -> None:
        server = coordinator()
        server.ctv_broadcaster_wallet = None
        server.ctv_broadcaster_fee_sats = 1

        with self.assertRaisesRegex(ValueError, "ctv_broadcaster_wallet is required"):
            server.make_ctv_fanout_broadcast_daemon()

    def test_clean_job_prunes_previous_active_prism_job(self) -> None:
        server = coordinator()
        state = client()
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        server.jobs = {}
        counter = {"value": 0}

        def build_context(client: ClientState, *, clean_jobs: bool) -> object:
            counter["value"] += 1
            return SimpleNamespace(
                job=SimpleNamespace(job_id=f"job-{counter['value']}", share_difficulty=Decimal("1")),
                collection_only=False,
            )

        server.build_job_for_client = build_context  # type: ignore[method-assign]
        server.send_difficulty = lambda *args, **kwargs: None
        server.send_job = lambda *args, **kwargs: None
        server.apply_job_difficulty = lambda *args, **kwargs: None

        server.maybe_send_job(state, clean_jobs=True)
        first_job_id = next(iter(state.active_job_ids))
        server.maybe_send_job(state, clean_jobs=True)
        second_job_id = next(iter(state.active_job_ids))

        self.assertNotEqual(first_job_id, second_job_id)
        self.assertNotIn(first_job_id, server.jobs)
        self.assertIn(second_job_id, server.jobs)

        state.sock = SimpleNamespace(shutdown=lambda *_args: None, close=lambda: None)
        server.disconnect_client(state)
        self.assertNotIn(second_job_id, server.jobs)
        self.assertEqual(state.active_job_ids, set())

    def test_non_clean_job_retention_caps_previous_active_prism_jobs(self) -> None:
        server = coordinator()
        state = client()
        state.worker = WorkerIdentity(
            username="miner-a",
            payout_address="miner-a",
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )
        server.jobs = {}
        counter = {"value": 0}

        def build_context(client: ClientState, *, clean_jobs: bool) -> object:
            counter["value"] += 1
            return SimpleNamespace(
                job=SimpleNamespace(job_id=f"job-{counter['value']}", share_difficulty=Decimal("1")),
                collection_only=False,
            )

        server.build_job_for_client = build_context  # type: ignore[method-assign]
        server.send_difficulty = lambda *args, **kwargs: None
        server.send_job = lambda *args, **kwargs: None
        server.apply_job_difficulty = lambda *args, **kwargs: None

        total_jobs = MAX_ACTIVE_PRISM_JOBS_PER_CLIENT + 3
        for _ in range(total_jobs):
            server.maybe_send_job(state, clean_jobs=False)

        retained_ids = {
            f"job-{index}"
            for index in range(4, total_jobs + 1)
        }
        self.assertEqual(state.active_job_ids, retained_ids)
        self.assertEqual(set(server.jobs), retained_ids)
        self.assertNotIn("job-1", server.jobs)
        self.assertEqual(state.active_job.job.job_id, f"job-{total_jobs}")

    def test_tip_change_refreshes_clean_job_and_old_job_becomes_stale_without_reconnect(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        server.accepted_block_count = 0
        server.max_blocks = 1
        server.stop_after_block = True
        server.jobs = {}
        server.recent_share_keys = set()
        server.share_weights_by_username = {}
        ledger = RecordingLedger()
        server.ledger = ledger
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}

        old_context = prism_context("old-job", old_tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        server.rpc = TipTemplateRpc(tip=new_tip, template=gbt_template(new_tip, height=11))

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIs(client, state)
            return prism_context(
                "fresh-job",
                new_tip,
                worker=worker,
                difficulty=client.pending_share_difficulty or client.share_difficulty,
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertEqual(server.tip_refresh_job_count, 1)
        self.assertIn(state, server.clients)
        self.assertNotIn("old-job", server.jobs)
        self.assertEqual(state.active_job_ids, {"fresh-job"})
        self.assertIn("fresh-job", server.jobs)
        self.assertEqual([payload["method"] for payload in sent], ["mining.set_difficulty", "mining.notify"])
        self.assertEqual(sent[1]["params"][0], "fresh-job")
        self.assertTrue(sent[1]["params"][8])

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "old-job", "00" * 8, "00000001", "00000002"],
            )
        self.assertEqual(raised.exception.code, 21)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)
        self.assertEqual(server.stale_share_count, 1)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_UNKNOWN_JOB], 1)
        self.assertEqual(len(ledger.pending), 0)

        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=True,
            block_pass=False,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "fresh-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].job_id, "fresh-job")
        self.assertIn(state, server.clients)

    def test_tip_refresh_rpc_race_uses_clean_job_when_template_parent_changed(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}

        old_context = prism_context("old-job", old_tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        server.rpc = TipTemplateRpc(tip=old_tip, template=gbt_template(new_tip, height=11))

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertTrue(clean_jobs)
            return prism_context("fresh-job", new_tip, worker=worker, clean_jobs=clean_jobs)

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertNotIn("old-job", server.jobs)
        self.assertEqual(state.active_job_ids, {"fresh-job"})
        self.assertEqual(sent[1]["params"][0], "fresh-job")
        self.assertTrue(sent[1]["params"][8])

    def test_same_tip_template_refresh_sends_non_clean_job_and_keeps_old_job_submittable(self) -> None:
        tip = "00" * 32
        server = coordinator()
        server.accepted_block_count = 0
        server.max_blocks = 1
        server.stop_after_block = True
        server.jobs = {}
        server.recent_share_keys = set()
        server.share_weights_by_username = {}
        ledger = RecordingLedger()
        server.ledger = ledger
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}

        old_context = prism_context("old-job", tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=tip,
            previousblockhash=tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        refreshed_template = gbt_template(tip, height=10, coinbasevalue=50_00000001)
        server.rpc = TipTemplateRpc(tip=tip, template=refreshed_template)

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIs(client, state)
            self.assertFalse(clean_jobs)
            fresh_context = prism_context(
                "fresh-job",
                tip,
                worker=worker,
                difficulty=client.pending_share_difficulty or client.share_difficulty,
                clean_jobs=clean_jobs,
            )
            fresh_context.template["coinbasevalue"] = refreshed_template["coinbasevalue"]
            fresh_context.template_fingerprint = qbit_template_fingerprint(fresh_context.template)
            return fresh_context

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertEqual(server.tip_refresh_job_count, 1)
        self.assertIn(state, server.clients)
        self.assertIn("old-job", server.jobs)
        self.assertIn("fresh-job", server.jobs)
        self.assertEqual(state.active_job_ids, {"old-job", "fresh-job"})
        self.assertEqual([payload["method"] for payload in sent], ["mining.set_difficulty", "mining.notify"])
        self.assertEqual(sent[1]["params"][0], "fresh-job")
        self.assertFalse(sent[1]["params"][8])
        self.assertIn("qbit_prism_active_job_contexts 2", server.metrics_payload())

        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=True,
            block_pass=False,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "old-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].job_id, "old-job")
        self.assertEqual(server.stale_share_count, 0)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_UNKNOWN_JOB], 0)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB], 0)

    def test_tip_refresh_uses_pending_vardiff_difficulty_for_consistent_pair(self) -> None:
        old_tip = "00" * 32
        new_tip = "22" * 32
        server = coordinator()
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        state.pending_share_difficulty = Decimal("8")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}
        old_context = prism_context("old-job", old_tip, worker=worker, difficulty=Decimal("1"))
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        server.rpc = TipTemplateRpc(tip=new_tip, template=gbt_template(new_tip, height=11))

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            return prism_context(
                "fresh-vardiff-job",
                new_tip,
                worker=worker,
                difficulty=server.desired_client_share_difficulty(client),
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertEqual(sent[0]["method"], "mining.set_difficulty")
        self.assertEqual(sent[0]["params"], [8.0])
        self.assertEqual(sent[1]["method"], "mining.notify")
        self.assertEqual(sent[1]["params"][0], "fresh-vardiff-job")
        self.assertTrue(sent[1]["params"][8])
        self.assertEqual(state.share_difficulty, Decimal("8"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(server.jobs["fresh-vardiff-job"].job.share_difficulty, Decimal("8"))

    def test_tip_refresh_build_failure_keeps_client_connected_and_old_job_registered(self) -> None:
        old_tip = "00" * 32
        new_tip = "33" * 32
        server = coordinator()
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}
        old_context = prism_context("old-job", old_tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        server.rpc = TipTemplateRpc(tip=new_tip, template=gbt_template(new_tip, height=11))

        def failing_build(client: ClientState, *, clean_jobs: bool) -> object:
            raise RuntimeError("transient getblocktemplate failure")

        server.build_job_for_client = failing_build  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 0)
        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(server.tip_refresh_job_count, 0)
        self.assertIn(state, server.clients)
        self.assertEqual(state.active_job_ids, {"old-job"})
        self.assertIn("old-job", server.jobs)
        self.assertEqual(sent, [])

    def test_tip_reconciliation_quarantines_disconnected_block_before_refresh_job(self) -> None:
        old_tip = "00" * 32
        new_tip = "44" * 32
        pool_block_hash = "aa" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}
        old_context = prism_context("old-job", old_tip, worker=worker)
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )
        ledger = ReorgLedger(
            [
                {
                    "block_hash": pool_block_hash,
                    "block_height": 10,
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            ]
        )
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=new_tip,
            template=gbt_template(new_tip, height=11),
            height=10,
            block_hashes={10: "bb" * 32},
        )

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIn(("inactive", pool_block_hash, 10), ledger.events)
            ledger.events.append(("build", client.connection_id))
            return prism_context("fresh-job", new_tip, worker=worker, clean_jobs=clean_jobs)

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 1)
        self.assertLess(
            ledger.events.index(("inactive", pool_block_hash, 10)),
            ledger.events.index(("build", state.connection_id)),
        )
        self.assertEqual(server.reorg_inactive_block_count, 1)
        self.assertEqual(ledger.rows[0]["chain_state"], "inactive")
        self.assertEqual(sent[1]["params"][0], "fresh-job")

    def test_reconciliation_quarantines_confirmed_block_above_shortened_tip(self) -> None:
        pool_block_hash = "af" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        ledger = ReorgLedger(
            [
                {
                    "block_hash": pool_block_hash,
                    "block_height": 12,
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            ]
        )
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip="77" * 32,
            template=gbt_template("77" * 32, height=11),
            height=10,
            block_hashes={},
        )

        summary = server.reconcile_prism_pool_blocks_once(tip_hash="77" * 32)

        self.assertEqual(summary["inactive_blocks"], 1)
        self.assertEqual(server.reorg_inactive_block_count, 1)
        self.assertEqual(ledger.rows[0]["chain_state"], "inactive")

    def test_tip_reconciliation_skips_jobs_when_qbit_chain_view_is_untrusted(self) -> None:
        old_tip = "00" * 32
        new_tip = "55" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.active_job = prism_context("old-job", old_tip, worker=worker)
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = state.active_job
        server.clients = {state}
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(state.active_job.template),
        )
        ledger = ReorgLedger([])
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=new_tip,
            template=gbt_template(new_tip, height=11),
            height=10,
            block_hashes={10: new_tip},
            initialblockdownload=True,
        )

        def unexpected_build(client: ClientState, *, clean_jobs: bool) -> object:
            raise AssertionError("job build should be skipped while qbitd is in IBD")

        server.build_job_for_client = unexpected_build  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(refreshed, 0)
        self.assertEqual(server.reorg_reconcile_skip_count, 1)
        self.assertEqual(ledger.events, [])
        self.assertEqual(state.active_job_ids, {"old-job"})
        self.assertEqual(server.tip_template_snapshot.bestblockhash, old_tip)

    def test_reconciliation_error_before_job_build_is_not_counted_as_build_failure(self) -> None:
        tip = "59" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        state = client()
        state.username = "miner-a"
        state.worker = worker_identity()
        state.share_difficulty = Decimal("1")
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )

        class FailingReorgLedger(FakeLedger):
            def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
                raise RuntimeError("ledger unavailable")

        server.ledger = FailingReorgLedger()
        server.build_job_for_client = lambda _client, *, clean_jobs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("job build should not run after reconcile failure")
        )

        sent_job = server.maybe_send_job(state, clean_jobs=True)

        self.assertFalse(sent_job)
        self.assertEqual(server.reorg_reconcile_error_count, 1)
        self.assertEqual(server.job_build_failure_count, 0)

    def test_reconciliation_runs_again_for_same_tip_hash(self) -> None:
        tip = "5a" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        ledger = ReorgLedger([])
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )

        self.assertTrue(server.ensure_reorg_reconciled_for_tip(tip))
        self.assertTrue(server.ensure_reorg_reconciled_for_tip(tip))

        self.assertEqual(ledger.events, [("watch", 10), ("mature", 10), ("watch", 10), ("mature", 10)])

    def test_reconciliation_reactivates_inactive_block_that_returns_to_active_chain(self) -> None:
        pool_block_hash = "cc" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        ledger = ReorgLedger(
            [
                {
                    "block_hash": pool_block_hash,
                    "block_height": 12,
                    "chain_state": "inactive",
                    "maturity_state": "immature",
                }
            ]
        )
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=pool_block_hash,
            template=gbt_template(pool_block_hash, height=13),
            height=12,
            block_hashes={12: pool_block_hash},
        )

        summary = server.reconcile_prism_pool_blocks_once(tip_hash=pool_block_hash)

        self.assertEqual(summary["reactivated_blocks"], 1)
        self.assertEqual(server.reorg_reactivated_block_count, 1)
        self.assertEqual(ledger.rows[0]["chain_state"], "confirmed")
        self.assertIn(("mature", 12), ledger.events)

    def test_maybe_send_job_reconciles_before_direct_job_build(self) -> None:
        tip = "66" * 32
        pool_block_hash = "dd" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.jobs = {}
        worker = worker_identity()
        state = client()
        state.username = worker.username
        state.worker = worker
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        ledger = ReorgLedger(
            [
                {
                    "block_hash": pool_block_hash,
                    "block_height": 20,
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            ]
        )
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=21),
            height=20,
            block_hashes={20: "ee" * 32},
        )

        def build_direct_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIn(("inactive", pool_block_hash, 20), ledger.events)
            ledger.events.append(("build", client.connection_id))
            return prism_context("direct-job", tip, worker=worker, clean_jobs=clean_jobs)

        server.build_job_for_client = build_direct_job  # type: ignore[method-assign]

        sent_job = server.maybe_send_job(state, clean_jobs=True)

        self.assertTrue(sent_job)
        self.assertLess(
            ledger.events.index(("inactive", pool_block_hash, 20)),
            ledger.events.index(("build", state.connection_id)),
        )
        self.assertEqual(sent[1]["params"][0], "direct-job")

    def test_make_ledger_requires_explicit_memory_opt_in(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit):
                server.make_ledger()

        with patch.dict(os.environ, {"PRISM_ALLOW_MEMORY_LEDGER": "1"}, clear=True):
            ledger = server.make_ledger()

        self.assertEqual(ledger.backend_name, "memory")

    def test_trusted_ledger_key_must_be_configured_or_explicitly_test_mode(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)

        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit):
                server.load_trusted_ledger_writer_public_key()

        with patch.dict(os.environ, {"PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "aa" * 32}, clear=True):
            self.assertEqual(server.load_trusted_ledger_writer_public_key(), "aa" * 32)

        with patch.dict(os.environ, {"PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY": "1"}, clear=True):
            self.assertIsNone(server.load_trusted_ledger_writer_public_key())

    def test_fixed_ledger_session_token_requires_explicit_opt_in(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        env = {
            "PRISM_POSTGRES_PSQL_COMMAND": "psql postgresql://example.invalid/qbit",
            "PRISM_LEDGER_WRITER_SESSION_TOKEN": "fixed-session",
        }

        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(SystemExit):
                server.make_ledger()

        with patch.dict(os.environ, {**env, "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN": "1"}, clear=True):
            with patch("lab.prism.prism_coordinator.PsqlShareLedger") as fake_ledger:
                fake_ledger.return_value = SimpleNamespace(backend_name="postgres-psql")
                ledger = server.make_ledger()

        self.assertEqual(ledger.backend_name, "postgres-psql")
        self.assertEqual(fake_ledger.call_args.kwargs["writer_session_token"], "fixed-session")

    def test_production_gate_rejects_prism_test_bypasses(self) -> None:
        base = {
            "QBIT_PRODUCTION": "1",
            "QBIT_RPC_USER": "qbitrpc",
            "QBIT_RPC_PASSWORD": "not-default",
            "PRISM_POSTGRES_PSQL_COMMAND": "psql postgresql://example.invalid/qbit",
            "PRISM_POSTGRES_PASSWORD": "not-default",
            "PRISM_MANIFEST_SIGNING_SEED_HEX": "42" * 32,
            "PRISM_LEDGER_ATTESTATION_SIGNING_SEED_HEX": "43" * 32,
            "PRISM_LEDGER_WRITER_PUBLIC_KEY_HEX": "44" * 32,
            "PRISM_LEDGER_WRITER_ID": "managed-writer",
            "PRISM_LEDGER_WRITER_EPOCH": "7",
            "PRISM_AUDIT_DIR": "/var/lib/qbit/prism/audit",
            "PRISM_EVIDENCE_PATH": "/var/lib/qbit/prism/evidence.json",
        }
        for name in (
            "PRISM_ALLOW_MEMORY_LEDGER",
            "PRISM_ALLOW_TEST_SIGNING_SEEDS",
            "PRISM_ALLOW_BUNDLE_EMBEDDED_LEDGER_KEY",
            "PRISM_ALLOW_FIXED_LEDGER_SESSION_TOKEN",
        ):
            with self.subTest(name=name), patch.dict(os.environ, {**base, name: "1"}, clear=True):
                with self.assertRaisesRegex(SystemExit, name):
                    validate_prism_production_gate()

        with patch.dict(os.environ, base, clear=True):
            validate_prism_production_gate()

        with patch.dict(os.environ, {**base, "PRISM_POSTGRES_PASSWORD": "change-this"}, clear=True):
            with self.assertRaisesRegex(SystemExit, "PRISM_POSTGRES_PASSWORD"):
                validate_prism_production_gate()

        with patch.dict(
            os.environ,
            {
                **base,
                "PRISM_POSTGRES_PASSWORD": "not-default",
                "PRISM_POSTGRES_PSQL_COMMAND": "",
                "PRISM_DATABASE_URL": "postgresql://qbit:change-this@prism-postgres:5432/qbit",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "PRISM_DATABASE_URL"):
                validate_prism_production_gate()

    def test_normal_accepted_share_does_not_close_client(self) -> None:
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].share_id, "miner-a:" + "bb" * 32)

    def test_submit_passes_negotiated_version_bits_and_mask_to_stratum_assembly(self) -> None:
        server, state, _ledger = submit_coordinator()
        state.version_mask = 0x1FFFE000
        submission = SimpleNamespace(
            header_hex="ac" * 80,
            block_hash_hex="ba" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ) as assemble_submission:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002", "00002000"],
            )

        self.assertEqual(assemble_submission.call_args.kwargs["version_bits_hex"], "00002000")
        self.assertEqual(assemble_submission.call_args.kwargs["version_mask"], 0x1FFFE000)

    def test_address_worker_submit_accrues_to_base_payout_address(self) -> None:
        server, state, ledger = submit_coordinator()
        username = f"{PAYOUT_ADDRESS}.rig-a"
        worker = WorkerIdentity(
            username=username,
            payout_address=PAYOUT_ADDRESS,
            worker_name="rig-a",
            script_pubkey_hex="5220" + "44" * 32,
            p2mr_program_hex="44" * 32,
        )
        state.username = username
        state.worker = worker
        server.jobs["job-1"].worker = worker
        server.share_weights_by_username = {PAYOUT_ADDRESS: 9}
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bc" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                [username, "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].share_id, username + ":" + "bc" * 32)
        self.assertEqual(ledger.pending[0].miner_id, PAYOUT_ADDRESS)
        self.assertEqual(ledger.pending[0].order_key, PAYOUT_ADDRESS)
        self.assertEqual(ledger.pending[0].share_difficulty, 9)

    def test_address_worker_submit_still_requires_authorized_full_username(self) -> None:
        server, state, ledger = submit_coordinator()
        username = f"{PAYOUT_ADDRESS}.rig-a"
        worker = WorkerIdentity(
            username=username,
            payout_address=PAYOUT_ADDRESS,
            worker_name="rig-a",
            script_pubkey_hex="5220" + "44" * 32,
            p2mr_program_hex="44" * 32,
        )
        state.username = username
        state.worker = worker
        server.jobs["job-1"].worker = worker

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                [PAYOUT_ADDRESS, "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.code, 20)
        self.assertEqual(len(ledger.pending), 0)

    def test_block_submit_rejects_job_when_prior_balances_changed_before_persist(self) -> None:
        server, state, ledger = submit_coordinator()
        ledger.prior_balances = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("audit bundle should not be rebuilt from stale prior balances")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ef" * 32,
            block_hex="00",
        )

        with self.assertRaises(StratumError) as raised:
            server.submit_block_candidate(
                server.jobs["job-1"],
                submission,
                state.extranonce1_hex,
                "00" * 8,
                pending_share=SimpleNamespace(share_id="miner-a:" + "ef" * 32),
                client=state,
            )

        self.assertEqual(raised.exception.code, 21)
        self.assertEqual(server.stale_share_count, 1)
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(ledger.pending, [])

    def test_block_submit_reconciliation_error_is_structured_rejection(self) -> None:
        tip = "f0" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.reorg_reconciler_enabled = True
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=10),
            height=9,
            block_hashes={9: tip},
        )

        class FailingSubmitReorgLedger(RecordingLedger):
            def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
                raise RuntimeError("ledger unavailable")

        ledger = FailingSubmitReorgLedger()
        server.ledger = ledger
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("audit bundle should not be rebuilt after reconcile failure")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="f1" * 32,
            block_hex="00",
        )

        with self.assertRaises(StratumError) as raised:
            server.submit_block_candidate(
                server.jobs["job-1"],
                submission,
                state.extranonce1_hex,
                "00" * 8,
                pending_share=SimpleNamespace(share_id="miner-a:" + "f1" * 32),
                client=state,
            )

        self.assertEqual(raised.exception.code, 20)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE], 1)
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(ledger.pending, [])

    def test_address_worker_suffixes_share_one_payout_account(self) -> None:
        server, state_a, ledger = submit_coordinator()
        usernames = [f"{PAYOUT_ADDRESS}.rig-a", f"{PAYOUT_ADDRESS}.rig-b"]
        submissions = [
            SimpleNamespace(
                header_hex="aa" * 80,
                block_hash_hex="bd" * 32,
                share_pass=True,
                block_pass=False,
            ),
            SimpleNamespace(
                header_hex="bb" * 80,
                block_hash_hex="be" * 32,
                share_pass=True,
                block_pass=False,
            ),
        ]
        states = [state_a, client()]
        states[1].active_job_ids = {"job-1"}
        for state, username in zip(states, usernames, strict=True):
            worker = WorkerIdentity(
                username=username,
                payout_address=PAYOUT_ADDRESS,
                worker_name=username.rsplit(".", 1)[1],
                script_pubkey_hex="5220" + "55" * 32,
                p2mr_program_hex="55" * 32,
            )
            state.username = username
            state.worker = worker
            state.subscribed = True
            state.authorized = True
            server.jobs["job-1"].worker = worker
            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submissions.pop(0),
            ):
                server.handle_submit(
                    state,
                    [username, "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual([pending.miner_id for pending in ledger.pending], [PAYOUT_ADDRESS, PAYOUT_ADDRESS])
        self.assertEqual([pending.order_key for pending in ledger.pending], [PAYOUT_ADDRESS, PAYOUT_ADDRESS])

    def test_stale_tip_rejects_without_appending_share(self) -> None:
        server, state, ledger = submit_coordinator(tip="00" * 32)
        server.rpc = TipRpc("11" * 32)

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.code, 21)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_STALE_JOB)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(server.stale_share_count, 1)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB], 1)

    def test_block_candidate_is_not_appended_before_submit_path(self) -> None:
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=True,
        )
        captured: dict[str, object] = {}

        def fake_submit(*args: object, **kwargs: object) -> bool:
            self.assertEqual(len(ledger.pending), 0)
            captured["pending"] = kwargs["pending_share"]
            return False

        server.submit_block_candidate = fake_submit  # type: ignore[method-assign]

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertIn("pending", captured)
        self.assertEqual(len(ledger.pending), 0)

    def test_block_candidate_persists_verified_bundle_before_submitblock(self) -> None:
        server, state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "cc" * 32
            rpc = SubmitRpc(tip="00" * 32, block_hash=block_hash, ledger=ledger)
            server.rpc = rpc
            witness_tx = synthetic_witness_transaction("44")
            server.jobs["job-1"].job.transaction_hexes = (witness_tx,)
            build_kwargs: list[dict[str, object]] = []

            def fake_build_audit_bundle(**kwargs: object) -> dict[str, object]:
                build_kwargs.append(kwargs)
                return {
                "found_block": {"coinbase_value_sats": 50_00000000},
                "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
                "payout_policy_manifest": {"accounts": []},
                "signed_coinbase_manifest": {
                    "manifest": {
                        "coinbase_tx_hex": "c0ffee",
                        "payout_count": 1,
                    }
                },
                }

            server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
            server.verify_bundle = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
                "coinbase_txid": "11" * 32,
                "coinbase_manifest_sha256_hex": "22" * 32,
                "audit_bundle_sha256_hex": "33" * 32,
                "coinbase_tx_hex": "c0ffee",
            }
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            pending = SimpleNamespace(share_id="miner-a:" + block_hash)

            server.submit_block_candidate(
                server.jobs["job-1"],
                submission,
                state.extranonce1_hex,
                "00" * 8,
                pending_share=pending,
                client=state,
            )

        self.assertTrue(rpc.submitted)
        self.assertEqual(
            build_kwargs[0]["witness_merkle_leaves_hex"],
            direct_stratum.witness_merkle_leaves_hex((witness_tx,)),
        )
        self.assertEqual(
            build_kwargs[0]["coinbase_script_sig_suffix_hex"],
            server.coinbase_tag_hex + state.extranonce1_hex + "00" * 8,
        )
        self.assertEqual(ledger.persisted[0]["block_hash"], block_hash)
        self.assertEqual(ledger.persisted[0]["block_height"], 10)
        self.assertFalse(ledger.persisted[0]["submit_seen_at_persist"])
        self.assertEqual(ledger.confirmed[0]["block_hash"], block_hash)
        self.assertTrue(ledger.confirmed[0]["submit_seen_at_confirm"])
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(server.latest_evidence["persistence"]["block_count"], 1)
        self.assertEqual(server.latest_evidence["confirmation"]["confirmed_count"], 1)

    def test_accepted_direct_block_refreshes_clean_job_after_submit_response(self) -> None:
        old_tip = "00" * 32
        block_hash = "ab" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        state.share_difficulty = Decimal("1")
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        old_context = server.jobs["job-1"]
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(old_context.template),
        )

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            self.assertIs(client, state)
            return prism_context(
                "fresh-job",
                block_hash,
                worker=state.worker,
                difficulty=server.desired_client_share_difficulty(client),
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitAcceptingTemplateRpc(old_tip=old_tip, block_hash=block_hash, ledger=ledger)
            server.build_audit_bundle = lambda **_kwargs: verified_block_bundle()  # type: ignore[method-assign]
            server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
            submission = SimpleNamespace(
                header_hex="aa" * 80,
                share_pass=True,
                block_pass=True,
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_request(
                    state,
                    {
                        "id": "submit-1",
                        "method": "mining.submit",
                        "params": ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                    },
                )

        self.assertEqual(sent[0], {"id": "submit-1", "result": True, "error": None})
        self.assertEqual([payload.get("method") for payload in sent[1:]], ["mining.set_difficulty", "mining.notify"])
        self.assertEqual(sent[2]["params"][0], "fresh-job")
        self.assertTrue(sent[2]["params"][8])
        self.assertEqual(server.tip_refresh_job_count, 1)
        self.assertEqual(server.post_accept_refresh_failure_count, 0)
        self.assertEqual(server.accepted_block_count, 1)
        self.assertNotIn("job-1", server.jobs)
        self.assertIn("fresh-job", server.jobs)
        self.assertEqual(state.active_job_ids, {"fresh-job"})
        self.assertIn(state, server.clients)

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )
        self.assertEqual(raised.exception.code, 21)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)

        fresh_submission = SimpleNamespace(
            header_hex="bb" * 80,
            block_hash_hex="bc" * 32,
            share_pass=True,
            block_pass=False,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=fresh_submission,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "fresh-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(ledger.pending[-1].job_id, "fresh-job")

    def test_post_accept_refresh_failure_does_not_fail_accepted_direct_block(self) -> None:
        old_tip = "00" * 32
        block_hash = "ad" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def unexpected_build(client: ClientState, *, clean_jobs: bool) -> object:
            raise AssertionError("job build should not run when template refresh fails")

        server.build_job_for_client = unexpected_build  # type: ignore[method-assign]

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitAcceptingTemplateRpc(
                old_tip=old_tip,
                block_hash=block_hash,
                fail_template_after_submit=True,
                ledger=ledger,
            )
            server.build_audit_bundle = lambda **_kwargs: verified_block_bundle()  # type: ignore[method-assign]
            server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
            submission = SimpleNamespace(
                header_hex="aa" * 80,
                share_pass=True,
                block_pass=True,
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_request(
                    state,
                    {
                        "id": "submit-1",
                        "method": "mining.submit",
                        "params": ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                    },
                )

        self.assertEqual(sent, [{"id": "submit-1", "result": True, "error": None}])
        self.assertEqual(server.accepted_block_count, 1)
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(server.tip_refresh_job_count, 0)
        self.assertEqual(server.post_accept_refresh_failure_count, 1)
        self.assertEqual(state.active_job_ids, {"job-1"})
        self.assertIn("job-1", server.jobs)
        self.assertIn("qbit_prism_post_accept_refresh_failures_total 1", server.metrics_payload())

    def test_post_accept_refresh_preserves_pending_vardiff_difficulty_pair(self) -> None:
        old_tip = "00" * 32
        block_hash = "ae" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.vardiff_config = vardiff.VardiffConfig(
            enabled=True,
            target_share_interval_seconds=Decimal("15"),
            min_difficulty=Decimal("1"),
            max_difficulty=Decimal("1024"),
            retarget_interval_seconds=Decimal("90"),
            max_step_factor=Decimal("4"),
            startup_difficulty=Decimal("1"),
            max_step_down_factor=Decimal("4"),
            ewma_alpha=Decimal("1"),
            retarget_tolerance=Decimal("0"),
        )
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        state.share_difficulty = Decimal("1")
        state.pending_share_difficulty = Decimal("8")
        state.vardiff_window_started_monotonic = time.monotonic()
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            return prism_context(
                "fresh-vardiff-job",
                block_hash,
                worker=state.worker,
                difficulty=server.desired_client_share_difficulty(client),
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitAcceptingTemplateRpc(old_tip=old_tip, block_hash=block_hash, ledger=ledger)
            server.build_audit_bundle = lambda **_kwargs: verified_block_bundle()  # type: ignore[method-assign]
            server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
            submission = SimpleNamespace(
                header_hex="aa" * 80,
                share_pass=True,
                block_pass=True,
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_request(
                    state,
                    {
                        "id": "submit-1",
                        "method": "mining.submit",
                        "params": ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                    },
                )

        self.assertEqual(sent[0], {"id": "submit-1", "result": True, "error": None})
        self.assertEqual(sent[1]["method"], "mining.set_difficulty")
        self.assertEqual(sent[1]["params"], [8.0])
        self.assertEqual(sent[2]["method"], "mining.notify")
        self.assertEqual(sent[2]["params"][0], "fresh-vardiff-job")
        self.assertTrue(sent[2]["params"][8])
        self.assertEqual(state.share_difficulty, Decimal("8"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.vardiff_window_submitted, 1)
        self.assertEqual(state.vardiff_window_accepted, 1)

    def test_rejected_prepersisted_candidate_is_marked_rejected_not_reorged(self) -> None:
        server, state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "dd" * 32
            server.rpc = SubmitRpc(tip="00" * 32, block_hash=block_hash, submit_result="rejected")
            server.build_audit_bundle = lambda **_kwargs: {  # type: ignore[method-assign]
                "found_block": {"coinbase_value_sats": 50_00000000},
                "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
                "payout_policy_manifest": {"accounts": []},
                "signed_coinbase_manifest": {
                    "manifest": {
                        "coinbase_tx_hex": "c0ffee",
                        "payout_count": 1,
                    }
                },
            }
            server.verify_bundle = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
                "coinbase_txid": "11" * 32,
                "coinbase_manifest_sha256_hex": "22" * 32,
                "audit_bundle_sha256_hex": "33" * 32,
                "coinbase_tx_hex": "c0ffee",
            }
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            with self.assertRaises(StratumError) as raised:
                server.submit_block_candidate(
                    server.jobs["job-1"],
                    submission,
                    state.extranonce1_hex,
                    "00" * 8,
                    pending_share=SimpleNamespace(share_id="miner-a:" + block_hash),
                    client=state,
                )

        self.assertEqual(ledger.persisted[0]["block_hash"], block_hash)
        self.assertEqual(ledger.rejected[0]["block_hash"], block_hash)
        self.assertEqual(ledger.rejected[0]["active_tip_height"], 9)
        self.assertEqual(ledger.reversed, [])
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_SUBMITBLOCK_REJECTED)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_SUBMITBLOCK_REJECTED], 1)


class PrismCoordinatorReliabilityTests(unittest.TestCase):
    def _bare_coordinator(self) -> PrismCoordinator:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.stop_event = threading.Event()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 120.0
        server.watchdog_interval_seconds = 15.0
        return server

    def test_positive_float_env_rejects_non_finite_values(self) -> None:
        for raw in ("nan", "inf", "-inf"):
            with self.subTest(raw=raw), patch.dict(
                os.environ, {"PRISM_WATCHDOG_TIMEOUT_SECONDS": raw}, clear=True
            ):
                with self.assertRaisesRegex(SystemExit, "PRISM_WATCHDOG_TIMEOUT_SECONDS must be finite"):
                    env_positive_float("PRISM_WATCHDOG_TIMEOUT_SECONDS", 120.0)

    def test_overdue_heartbeats_flags_only_stale_subsystems(self) -> None:
        server = self._bare_coordinator()
        server._record_heartbeat("stratum_accept")
        server._record_heartbeat("qbit_blockpoll")
        now = time.monotonic()

        self.assertEqual(server._overdue_heartbeats(now), [])

        with server._heartbeats_lock:
            server._heartbeats["qbit_blockpoll"] = now - 1_000.0

        self.assertEqual(server._overdue_heartbeats(now), ["qbit_blockpoll"])

    def test_watchdog_pause_suppresses_known_long_critical_section(self) -> None:
        server = self._bare_coordinator()
        server._record_heartbeat("stratum_accept")
        server._record_heartbeat("qbit_blockpoll")
        now = time.monotonic()
        with server._heartbeats_lock:
            server._heartbeats["stratum_accept"] = now - 1_000.0
            server._heartbeats["qbit_blockpoll"] = now - 1_000.0

        self.assertEqual(server._overdue_heartbeats(now), ["qbit_blockpoll", "stratum_accept"])

        with server._watchdog_paused("qbit_blockpoll", "stratum_accept"):
            self.assertEqual(server._overdue_heartbeats(now + 1_000.0), [])

        self.assertEqual(server._overdue_heartbeats(time.monotonic()), [])

    def test_release_ledger_lease_is_noop_without_lease_support(self) -> None:
        server = self._bare_coordinator()
        server.ledger = SimpleNamespace()

        # In-memory/regtest ledgers have no release_writer_lease; must not raise.
        server.release_ledger_lease()

    def test_release_ledger_lease_invokes_ledger_release(self) -> None:
        server = self._bare_coordinator()
        calls: list[bool] = []
        server.ledger = SimpleNamespace(
            release_writer_lease=lambda: (calls.append(True), True)[1]
        )

        server.release_ledger_lease()

        self.assertEqual(calls, [True])

    def test_release_ledger_lease_swallows_release_errors(self) -> None:
        server = self._bare_coordinator()

        def _boom() -> bool:
            raise RuntimeError("db unreachable during shutdown")

        server.ledger = SimpleNamespace(release_writer_lease=_boom)

        # Shutdown must not raise even if the lease release fails.
        server.release_ledger_lease()


if __name__ == "__main__":
    unittest.main()
