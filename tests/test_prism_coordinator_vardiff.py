#!/usr/bin/env python3

from __future__ import annotations

import errno
import json
import os
import queue
import socket
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
from lab.prism.share_ledger import PendingShare, SingleWriterShareLedger
from lab.prism.prism_coordinator import (
    CachedJobBundle,
    ClientState,
    DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS,
    MAX_ACTIVE_PRISM_JOBS_PER_CLIENT,
    MAX_PENDING_SHARE_APPENDS,
    PRISM_CREDIT_POLICY_STALE_GRACE,
    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
    PRISM_REJECTION_DUPLICATE_SHARE,
    PRISM_REJECTION_INVALID_NTIME_OR_NONCE,
    PRISM_REJECTION_LOW_DIFFICULTY,
    PendingShareAppend,
    PrismBlockCandidate,
    PRISM_REJECTION_POOL_CLOSED,
    PRISM_REJECTION_REASON_IDS,
    PRISM_REJECTION_STALE_JOB,
    PRISM_REJECTION_SUBMITBLOCK_REJECTED,
    PRISM_REJECTION_UNAUTHORIZED_WORKER,
    PRISM_REJECTION_UNKNOWN_JOB,
    PRISM_WORKER_METRICS_OVERFLOW_LABEL,
    QbitTipTemplateSnapshot,
    StratumError,
    StratumListenerProfile,
    TemplateRefreshBlocked,
    PrismCoordinator,
    WorkerIdentity,
    default_prism_coinbase_tag_hex,
    default_prism_username_fallback_address,
    load_prism_highdiff_listener,
    load_prism_vardiff_config,
    parse_stratum_password_options,
    qbit_template_fingerprint,
    qbit_gbt_rules,
    env_positive_float,
    scaled_target_difficulty,
    target_from_compact,
    validate_prism_production_gate,
    validate_same_tip_job_retention_limits,
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
        if method == "getblockheader":
            raise RuntimeError("qbit RPC getblockheader failed: -5 Block not found")
        return super().call(method, params)


class ParentTipRpc(TipRpc):
    def __init__(self, *, tip: str, parent: str) -> None:
        super().__init__(tip)
        self.parent = parent
        self.submitblock_calls = 0

    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getblock":
            self.assert_tip_param(params)
            return {"hash": self.tip, "previousblockhash": self.parent}
        if method == "submitblock":
            self.submitblock_calls += 1
            return None
        return super().call(method, params)

    def assert_tip_param(self, params: list[object] | None) -> None:
        if not params or str(params[0]) != self.tip:
            raise AssertionError(f"expected getblock current tip {self.tip}, got {params!r}")


class UnsupportedBlockwaitRpc(TipRpc):
    def call(
        self,
        method: str,
        params: list[object] | None = None,
        *,
        timeout: float | None = None,
    ) -> object:
        if method == "waitfornewblock":
            raise RuntimeError("Method not found")
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
    server.collection_block_submission_count = 0
    server._pool_ready_latched = False
    server.grace_credited_share_count = 0
    server.idle_retarget_count = 0
    server.rejection_counts_by_reason = {reason: 0 for reason in PRISM_REJECTION_REASON_IDS}
    server.worker_metrics_limit = 100
    server.worker_metrics_lock = threading.Lock()
    server.worker_share_counts = {}
    server.worker_rejection_counts = {}
    server.evicted_job_graveyard = {}
    server.block_candidate_queue = queue.Queue(maxsize=8)
    server.block_candidates_dropped = 0
    server.block_candidate_abandoned_counts = {}
    server.share_append_queue = queue.Queue(maxsize=8)
    server.share_writer_active = False
    server.share_append_failure_count = 0
    server.share_recovery_path = None
    server.share_recovery_lock = threading.Lock()
    server.shares_recovered_to_disk = 0
    server.shares_replayed = 0
    server.current_tip_first_seen = None
    server.current_tip_parent = None
    server.stale_grace_seconds = 3.0
    server.blockwait_enabled = True
    server.blockwait_timeout_seconds = 5.0
    server.vardiff_idle_sweep_seconds = 15.0
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
    server._tip_refresh_lock = threading.Lock()
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


def block_candidate(
    server: PrismCoordinator,
    state: ClientState,
    submission: object,
    *,
    job_id: str = "job-1",
    pending_share: object | None = None,
    credit_share_on_accept: bool = False,
) -> PrismBlockCandidate:
    return PrismBlockCandidate(
        context=server.jobs[job_id],
        submission=submission,
        extranonce1_hex=state.extranonce1_hex,
        extranonce2_hex="00" * 8,
        pending_share=pending_share
        or SimpleNamespace(share_id="miner-a:" + submission.block_hash_hex),
        client=state,
        credit_share_on_accept=credit_share_on_accept,
    )


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

    def test_idle_vardiff_sweep_steps_down_zero_share_window(self) -> None:
        server = coordinator()
        state = client()
        state.worker = worker_identity()
        state.active_job = prism_context("job-1", "00" * 32, worker=state.worker)
        state.share_difficulty = Decimal("16")
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        server.clients = {state}
        sent: dict[str, object] = {}

        def fake_send_job(client: ClientState, *, clean_jobs: bool) -> bool:
            sent.update(
                {
                    "client": client,
                    "clean_jobs": clean_jobs,
                    "difficulty": client.pending_share_difficulty,
                }
            )
            return True

        server.maybe_send_job = fake_send_job  # type: ignore[method-assign]

        retargeted = server.vardiff_idle_sweep_once()

        self.assertEqual(retargeted, 1)
        self.assertEqual(server.idle_retarget_count, 1)
        self.assertEqual(sent["client"], state)
        self.assertTrue(sent["clean_jobs"])
        self.assertEqual(sent["difficulty"], Decimal("4"))
        self.assertEqual(state.pending_share_difficulty, Decimal("4"))
        self.assertEqual(state.vardiff_window_submitted, 0)

    def test_idle_vardiff_sweep_skips_submitted_reject_storm_window(self) -> None:
        server = coordinator()
        state = client()
        state.worker = worker_identity()
        state.active_job = prism_context("job-1", "00" * 32, worker=state.worker)
        state.share_difficulty = Decimal("16")
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        state.vardiff_window_submitted = 3
        server.clients = {state}

        def fail_send_job(client: ClientState, *, clean_jobs: bool) -> bool:
            raise AssertionError("reject-storm windows must not idle-retarget")

        server.maybe_send_job = fail_send_job  # type: ignore[method-assign]

        retargeted = server.vardiff_idle_sweep_once()

        self.assertEqual(retargeted, 0)
        self.assertEqual(state.pending_share_difficulty, None)
        self.assertEqual(state.vardiff_window_submitted, 3)

    def test_idle_vardiff_sweep_aborts_step_down_when_share_accepted_mid_retarget(self) -> None:
        # The sweep snapshots an idle window, then computes the retarget outside
        # the lock. If a concurrent handle_submit accepts a share in that gap, the
        # require_idle commit re-check must abort the speculative step-down rather
        # than down-diffing a client that just resumed submitting.
        server = coordinator()
        state = client()
        state.worker = worker_identity()
        state.active_job = prism_context("job-1", "00" * 32, worker=state.worker)
        state.share_difficulty = Decimal("16")
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        server.clients = {state}

        def fail_send_job(client: ClientState, *, clean_jobs: bool) -> bool:
            raise AssertionError("a client that resumed submitting must not idle-retarget")

        server.maybe_send_job = fail_send_job  # type: ignore[method-assign]

        real_calc = vardiff.calculate_next_difficulty

        def racing_calc(**kwargs: object) -> Decimal:
            # Simulate a share accepted on the fresh window between the idle
            # snapshot and the step-down commit.
            state.vardiff_window_accepted = 1
            return real_calc(**kwargs)

        with patch.object(vardiff, "calculate_next_difficulty", side_effect=racing_calc):
            retargeted = server.vardiff_idle_sweep_once()

        self.assertEqual(retargeted, 0)
        self.assertIsNone(state.pending_share_difficulty)
        # The accept path owns the window now; the sweep must not have reset it.
        self.assertEqual(state.vardiff_window_accepted, 1)

    def test_idle_vardiff_sweep_disconnects_send_failure_and_rolls_back_pending_difficulty(self) -> None:
        server = coordinator()
        state = client()
        state.worker = worker_identity()
        state.active_job = prism_context("job-1", "00" * 32, worker=state.worker)
        state.share_difficulty = Decimal("16")
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        server.clients = {state}
        disconnected: list[ClientState] = []

        def failing_send_job(client: ClientState, *, clean_jobs: bool) -> bool:
            self.assertEqual(client.pending_share_difficulty, Decimal("4"))
            raise OSError("socket send failed")

        def fake_disconnect(client: ClientState) -> None:
            disconnected.append(client)
            server.clients.discard(client)

        server.maybe_send_job = failing_send_job  # type: ignore[method-assign]
        server.disconnect_client = fake_disconnect  # type: ignore[method-assign]

        retargeted = server.vardiff_idle_sweep_once()

        self.assertEqual(retargeted, 0)
        self.assertEqual(disconnected, [state])
        self.assertIsNone(state.pending_share_difficulty)

    def test_idle_vardiff_sweep_skipped_send_does_not_restart_idle_window_clock(self) -> None:
        # A step-down whose job build/send is skipped (maybe_send_job False)
        # rolls back the pending difficulty; the idle window clock must roll
        # back too, so the next sweep retries immediately instead of waiting
        # out another full retarget interval with the miner still over-diffed.
        server = coordinator()
        state = client()
        state.worker = worker_identity()
        state.active_job = prism_context("job-1", "00" * 32, worker=state.worker)
        state.share_difficulty = Decimal("16")
        window_started = time.monotonic() - 2
        state.vardiff_window_started_monotonic = window_started
        server.clients = {state}

        def skipped_send_job(client: ClientState, *, clean_jobs: bool) -> bool:
            return False

        server.maybe_send_job = skipped_send_job  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 0)
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.vardiff_window_started_monotonic, window_started)

        sent: dict[str, object] = {}

        def working_send_job(client: ClientState, *, clean_jobs: bool) -> bool:
            sent["difficulty"] = client.pending_share_difficulty
            return True

        server.maybe_send_job = working_send_job  # type: ignore[method-assign]

        # The very next sweep can step down; without the clock rollback the
        # restarted window would gate this behind another full interval.
        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertEqual(sent["difficulty"], Decimal("4"))
        self.assertEqual(state.pending_share_difficulty, Decimal("4"))

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
            template={"previousblockhash": "00" * 32},
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
        server.grace_credited_share_count = 6
        server.idle_retarget_count = 7
        server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB] = 2
        server.rejection_counts_by_reason["duplicate-share"] = 1
        server.rejection_counts_by_reason["low-difficulty"] = 3
        server.tip_refresh_job_count = 4
        server.post_accept_refresh_failure_count = 5
        server.connection_limit_rejection_counts = {"global": 2, "username": 3}
        server.accept_resource_exhaustion_count = 4
        server.connection_setup_failure_count = 5

        metrics = server.metrics_payload()

        self.assertIn("qbit_prism_submitted_shares_total 10", metrics)
        self.assertIn("qbit_prism_stale_shares_total 2", metrics)
        self.assertIn("qbit_prism_duplicate_shares_total 1", metrics)
        self.assertIn("qbit_prism_low_difficulty_shares_total 3", metrics)
        self.assertIn("qbit_prism_grace_credited_shares_total 6", metrics)
        self.assertIn("qbit_prism_stratum_active_connections 0", metrics)
        self.assertIn(
            'qbit_prism_stratum_connection_limit_rejections_total{scope="global"} 2',
            metrics,
        )
        self.assertIn(
            'qbit_prism_stratum_connection_limit_rejections_total{scope="username"} 3',
            metrics,
        )
        self.assertIn("qbit_prism_stratum_accept_resource_exhaustions_total 4", metrics)
        self.assertIn("qbit_prism_stratum_connection_setup_failures_total 5", metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="stale-job"} 2', metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="duplicate-share"} 1', metrics)
        self.assertIn('qbit_prism_rejections_total{reason_id="low-difficulty"} 3', metrics)
        self.assertIn("qbit_prism_tip_refresh_jobs_total 4", metrics)
        self.assertIn("qbit_prism_post_accept_refresh_failures_total 5", metrics)
        self.assertIn("qbit_prism_vardiff_idle_retargets_total 7", metrics)
        self.assertIn("qbit_prism_stale_share_percent 20", metrics)
        self.assertIn("qbit_prism_coinbase_weight_headroom_bytes 1999750", metrics)
        self.assertIn("qbit_prism_vardiff_enabled 1", metrics)
        self.assertIn("qbit_prism_qbitd_initial_block_download 0", metrics)
        self.assertIn("qbit_prism_qbitd_peers 4", metrics)

    def test_metrics_include_bounded_worker_share_and_rejection_counters(self) -> None:
        server = coordinator()
        server.worker_metrics_limit = 1

        server.note_worker_submitted_share("miner-a")
        server.note_worker_accepted_share("miner-a", PRISM_CREDIT_POLICY_STALE_GRACE)
        server.note_worker_submitted_share("miner-b")
        server.record_rejection(PRISM_REJECTION_LOW_DIFFICULTY, worker="miner-b")

        metrics = server.metrics_payload()

        self.assertIn('qbit_prism_worker_submitted_shares_total{worker="miner-a"} 1', metrics)
        self.assertIn('qbit_prism_worker_accepted_shares_total{worker="miner-a"} 1', metrics)
        self.assertIn('qbit_prism_worker_grace_credited_shares_total{worker="miner-a"} 1', metrics)
        self.assertIn('qbit_prism_worker_submitted_shares_total{worker="_other"} 1', metrics)
        self.assertIn(
            'qbit_prism_worker_rejections_total{worker="_other",reason_id="low-difficulty"} 1',
            metrics,
        )

    def test_metrics_include_ctv_broadcaster_progress_and_pass_duration(self) -> None:
        server = coordinator()
        server._record_ctv_fanout_broadcaster_progress()
        server._record_ctv_fanout_broadcaster_progress()
        server.observe_ctv_fanout_broadcaster_pass(102.0)

        metrics = server.metrics_payload()

        self.assertIn("qbit_prism_ctv_fanout_broadcaster_processed_rows_total 2", metrics)
        self.assertIn(
            'qbit_prism_ctv_fanout_broadcaster_pass_seconds_bucket{le="60"} 0',
            metrics,
        )
        self.assertIn(
            'qbit_prism_ctv_fanout_broadcaster_pass_seconds_bucket{le="120"} 1',
            metrics,
        )
        self.assertIn("qbit_prism_ctv_fanout_broadcaster_pass_seconds_sum 102.000000", metrics)
        self.assertIn("qbit_prism_ctv_fanout_broadcaster_pass_seconds_count 1", metrics)

    def test_zero_worker_metric_limit_uses_overflow_bucket(self) -> None:
        server = coordinator()
        server.worker_metrics_limit = 0

        server.note_worker_submitted_share("miner-a")

        self.assertEqual(set(server.worker_share_counts), {"_other"})
        self.assertEqual(server.worker_share_counts["_other"]["submitted"], 1)

    def test_unauthorized_submit_does_not_admit_payload_worker_metric_label(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.worker_metrics_limit = 1

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["spoofed-miner", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNAUTHORIZED_WORKER)
        self.assertNotIn("spoofed-miner", server.worker_share_counts)
        self.assertEqual(server.worker_share_counts["miner-a"]["submitted"], 0)
        self.assertEqual(
            server.worker_rejection_counts[("miner-a", PRISM_REJECTION_UNAUTHORIZED_WORKER)],
            1,
        )

        server.note_worker_submitted_share("miner-a")

        self.assertNotIn(PRISM_WORKER_METRICS_OVERFLOW_LABEL, server.worker_share_counts)
        self.assertEqual(server.worker_share_counts["miner-a"]["submitted"], 1)

    def test_metrics_include_audit_artifact_storage_gauges(self) -> None:
        server = coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            files = {
                f"prism-audit-bundle-body-{'aa' * 32}-{'bb' * 32}.json": b"abc",
                f"prism-audit-share-segment-1-1-{'cc' * 32}.json": b"defg",
                f"prism-live-audit-bundle-1-{'dd' * 32}.json": b"hi",
                f"prism-live-audit-bundle-candidate-{'ee' * 32}.json": b"j",
                f".prism-live-audit-bundle-candidate-{'ff' * 32}.json.tmp": b"klmno",
                "operator-note.txt": b"pqrstu",
            }
            for name, body in files.items():
                (Path(tempdir) / name).write_bytes(body)

            metrics = server.metrics_payload()

        self.assertIn('qbit_prism_audit_artifact_bytes{kind="body"} 3', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="body"} 1', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="share_segment"} 4', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="share_segment"} 1', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="live_bundle"} 2', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="live_bundle"} 1', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="candidate"} 6', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="candidate"} 2', metrics)
        self.assertIn('qbit_prism_audit_artifact_bytes{kind="other"} 6', metrics)
        self.assertIn('qbit_prism_audit_artifact_files{kind="other"} 1', metrics)
        self.assertIn("qbit_prism_audit_artifact_scan_error 0", metrics)

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

    def test_resolve_worker_caches_successful_address_validation(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        rpc = AddressValidationRpc(script_byte="33")
        server.rpc = rpc

        first = server.resolve_worker(f"{PAYOUT_ADDRESS}.rig-a")
        second = server.resolve_worker(f"{PAYOUT_ADDRESS}.rig-b")

        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS])
        self.assertEqual(first.p2mr_program_hex, second.p2mr_program_hex)

    def test_payout_address_cache_evicts_least_recently_used_entry(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.payout_address_cache_max_entries = 2
        server.payout_address_cache_ttl_seconds = 60

        class AnyAddressRpc:
            def __init__(self) -> None:
                self.validated: list[str] = []

            def call(self, method: str, params: list[object] | None = None) -> object:
                address = str((params or [""])[0])
                self.validated.append(address)
                return {"isvalid": True, "scriptPubKey": "5220" + "33" * 32}

        rpc = AnyAddressRpc()
        server.rpc = rpc

        server.validate_p2mr_address("address-a", label="test")
        server.validate_p2mr_address("address-b", label="test")
        server.validate_p2mr_address("address-a", label="test")
        server.validate_p2mr_address("address-c", label="test")

        self.assertEqual(rpc.validated, ["address-a", "address-b", "address-c"])
        self.assertEqual(list(server._p2mr_address_cache), ["address-a", "address-c"])

        server.validate_p2mr_address("address-b", label="test")
        self.assertEqual(rpc.validated[-1], "address-b")
        self.assertEqual(len(server._p2mr_address_cache), 2)

    def test_payout_address_cache_revalidates_expired_entry(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.payout_address_cache_max_entries = 2
        server.payout_address_cache_ttl_seconds = 5
        rpc = AddressValidationRpc(script_byte="33")
        server.rpc = rpc
        now = 100.0

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: now,
        ):
            server.validate_p2mr_address(PAYOUT_ADDRESS, label="test")
            now = 104.0
            server.validate_p2mr_address(PAYOUT_ADDRESS, label="test")
            now = 106.0
            server.validate_p2mr_address(PAYOUT_ADDRESS, label="test")

        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS, PAYOUT_ADDRESS])

    def test_concurrent_worker_resolution_singleflights_address_validation(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        entered = threading.Event()
        release = threading.Event()

        class BlockingAddressRpc(AddressValidationRpc):
            def call(self, method: str, params: list[object] | None = None) -> object:
                if method == "validateaddress":
                    entered.set()
                    if not release.wait(timeout=5):
                        raise TimeoutError("test did not release validateaddress")
                return super().call(method, params)

        rpc = BlockingAddressRpc(script_byte="33")
        server.rpc = rpc
        server._ensure_p2mr_address_cache_state()
        workers: list[WorkerIdentity] = []
        errors: list[BaseException] = []

        def resolve(index: int) -> None:
            try:
                workers.append(server.resolve_worker(f"{PAYOUT_ADDRESS}.rig-{index}"))
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=resolve, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        self.assertTrue(entered.wait(timeout=5))
        release.set()
        for thread in threads:
            thread.join(timeout=5)

        self.assertFalse(errors)
        self.assertEqual(len(workers), 8)
        self.assertEqual(rpc.validated, [PAYOUT_ADDRESS])

    def test_concurrent_failed_worker_resolution_shares_singleflight_error(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        entered = threading.Event()
        release = threading.Event()

        class FailingAddressRpc:
            def __init__(self) -> None:
                self.calls = 0

            def call(self, method: str, params: list[object] | None = None) -> object:
                self.calls += 1
                entered.set()
                if not release.wait(timeout=5):
                    raise TimeoutError("test did not release validateaddress")
                raise RuntimeError("qbitd unavailable")

        rpc = FailingAddressRpc()
        server.rpc = rpc
        server._ensure_p2mr_address_cache_state()
        errors: list[BaseException] = []

        def resolve() -> None:
            try:
                server.resolve_worker(PAYOUT_ADDRESS)
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=resolve) for _ in range(8)]
        for thread in threads:
            thread.start()
        self.assertTrue(entered.wait(timeout=5))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with server._p2mr_address_cache_lock:
                pending = server._p2mr_address_validation_inflight[PAYOUT_ADDRESS]
                if pending.waiters == 7:
                    break
            time.sleep(0.001)
        self.assertEqual(pending.waiters, 7)
        release.set()
        for thread in threads:
            thread.join(timeout=5)

        self.assertEqual(rpc.calls, 1)
        self.assertEqual(len(errors), 8)
        self.assertTrue(all("qbitd unavailable" in str(exc) for exc in errors))

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
                template={"previousblockhash": "00" * 32},
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
                template={"previousblockhash": "00" * 32},
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
        server.stale_grace_seconds = 0
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

    def test_tip_refresh_rpc_race_blocks_mismatched_tip_template_snapshot(self) -> None:
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

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "tip changed while fetching block template",
        ):
            server.poll_qbit_tip_template_once()

        self.assertIs(state.active_job, old_context)
        self.assertEqual(server.jobs, {"old-job": old_context})
        self.assertEqual(state.active_job_ids, {"old-job"})
        self.assertEqual(sent, [])
        self.assertIsNone(server.current_tip_first_seen)
        self.assertIsNone(server._template_artifacts)

    def test_slow_tip_poll_cannot_regress_newer_blockwait_observation(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        old_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint="22" * 32,
        )

        def overtake_poll() -> QbitTipTemplateSnapshot:
            self.assertTrue(server.observe_tip_first_seen(new_tip))
            return old_snapshot

        server.fetch_qbit_tip_template_snapshot = overtake_poll  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "superseded by a newer tip observation",
        ):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.current_tip_first_seen[0], new_tip)
        self.assertIsNone(server.tip_template_snapshot)

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

        with self.assertRaisesRegex(TemplateRefreshBlocked, "no refreshed work was issued"):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(server.tip_refresh_job_count, 0)
        self.assertIn(state, server.clients)
        self.assertEqual(state.active_job_ids, {"old-job"})
        self.assertIn("old-job", server.jobs)
        self.assertEqual(sent, [])

    def test_tip_refresh_build_failure_is_not_masked_by_disconnected_client(self) -> None:
        old_tip = "00" * 32
        new_tip = "33" * 32
        server = coordinator()
        server.jobs = {}

        build_failed = client()
        build_failed.worker = worker_identity("miner-build-failed")
        build_failed.username = build_failed.worker.username
        build_failed.active_job = prism_context(
            "old-build-failed-job", old_tip, worker=build_failed.worker
        )
        build_failed.active_job_ids = {"old-build-failed-job"}

        disconnected = client()
        disconnected.connection_id = 2
        disconnected.worker = worker_identity("miner-disconnected")
        disconnected.username = disconnected.worker.username
        disconnected.active_job = prism_context(
            "old-disconnected-job", old_tip, worker=disconnected.worker
        )
        disconnected.active_job_ids = {"old-disconnected-job"}

        def disconnect_on_send(_payload: object) -> None:
            raise OSError("socket closed")

        disconnected.send = disconnect_on_send  # type: ignore[method-assign]
        server.clients = {build_failed, disconnected}
        server.jobs = {
            "old-build-failed-job": build_failed.active_job,
            "old-disconnected-job": disconnected.active_job,
        }
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(build_failed.active_job.template),
        )
        server.rpc = TipTemplateRpc(tip=new_tip, template=gbt_template(new_tip, height=11))

        def mixed_build(state: ClientState, *, clean_jobs: bool) -> object:
            if state is build_failed:
                raise RuntimeError("template build unavailable")
            return prism_context(
                "disconnected-fresh-job",
                new_tip,
                worker=state.worker,
                clean_jobs=clean_jobs,
            )

        disconnected_clients: list[ClientState] = []
        server.build_job_for_client = mixed_build  # type: ignore[method-assign]
        server.disconnect_client = disconnected_clients.append  # type: ignore[method-assign]

        with self.assertRaisesRegex(TemplateRefreshBlocked, "no refreshed work was issued"):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(disconnected_clients, [disconnected])
        self.assertIn(build_failed, server.clients)

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

        with self.assertRaisesRegex(TemplateRefreshBlocked, "chain view remained untrusted"):
            server.poll_qbit_tip_template_once()

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

    def test_same_tip_retention_requires_connection_derived_production_bound(self) -> None:
        with self.assertRaisesRegex(
            SystemExit,
            "PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION",
        ):
            validate_same_tip_job_retention_limits(
                retention_seconds=30,
                per_connection=0,
                max_connections=0,
                production=False,
            )
        with self.assertRaisesRegex(SystemExit, "PRISM_STRATUM_MAX_CONNECTIONS"):
            validate_same_tip_job_retention_limits(
                retention_seconds=30,
                per_connection=64,
                max_connections=0,
                production=True,
            )

        validate_same_tip_job_retention_limits(
            retention_seconds=30,
            per_connection=64,
            max_connections=1_900,
            production=True,
        )
        validate_same_tip_job_retention_limits(
            retention_seconds=30,
            per_connection=64,
            max_connections=0,
            production=False,
        )
        validate_same_tip_job_retention_limits(
            retention_seconds=0,
            per_connection=0,
            max_connections=0,
            production=True,
        )

    def test_production_gate_rejects_prism_test_bypasses_without_capacity_evidence(self) -> None:
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
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "PRISM_STRATUM_SHARE_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
            "PRISM_STRATUM_MAX_CONNECTIONS": "1900",
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

        with patch.dict(
            os.environ,
            {**base, "PRISM_STRATUM_MAX_CONNECTIONS": "0"},
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "PRISM_STRATUM_MAX_CONNECTIONS"):
                validate_prism_production_gate()

        with patch.dict(
            os.environ,
            {**base, "QBIT_CHAIN": "mainnet", "PRISM_STRATUM_STALE_GRACE_SECONDS": "3"},
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "mainnet requires PRISM_STRATUM_STALE_GRACE_SECONDS=0"):
                validate_prism_production_gate()

        # Off mainnet, production mode accepts a bounded grace window.
        with patch.dict(
            os.environ,
            {**base, "PRISM_STRATUM_STALE_GRACE_SECONDS": "2"},
            clear=True,
        ):
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

    def test_production_gate_rejects_unsafe_difficulty_without_capacity_gate(self) -> None:
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
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "PRISM_STRATUM_SHARE_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
        }
        cases = (
            ({"PRISM_STRATUM_SHARE_DIFF": ""}, "requires PRISM_STRATUM_SHARE_DIFF"),
            ({"PRISM_STRATUM_SHARE_DIFF": "not-a-decimal"}, "must be a decimal number"),
            ({"PRISM_STRATUM_SHARE_DIFF": "NaN"}, "PRISM_STRATUM_SHARE_DIFF must be positive"),
            ({"PRISM_STRATUM_SHARE_DIFF": "0"}, "PRISM_STRATUM_SHARE_DIFF must be positive"),
            ({"PRISM_STRATUM_SHARE_DIFF": "1e-9"}, "lab-only 1e-9 difficulty"),
            ({"PRISM_STRATUM_VARDIFF_MIN_DIFF": "8192"}, "minimum exceeds its start"),
            ({"PRISM_STRATUM_VARDIFF_START_DIFF": "131072"}, "start exceeds its maximum"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides), patch.dict(
                os.environ,
                {**base, **overrides},
                clear=True,
            ):
                with self.assertRaisesRegex(SystemExit, message):
                    validate_prism_production_gate()

    def test_mainnet_implies_production_gate(self) -> None:
        with patch.dict(
            os.environ,
            {
                "QBIT_CHAIN": "mainnet",
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "requires PRISM_STRATUM_SHARE_DIFF"):
                validate_prism_production_gate()

    def test_compatibility_production_flag_implies_production_gate(self) -> None:
        with patch.dict(
            os.environ,
            {
                "QBIT_CHAIN": "testnet4",
                "QBIT_TOOLS_PRODUCTION": "1",
                "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(SystemExit, "requires PRISM_STRATUM_SHARE_DIFF"):
                validate_prism_production_gate()

    def test_mainnet_ctv_requires_static_fee_rate_before_runtime_startup(self) -> None:
        env = {
            "QBIT_CHAIN": "mainnet",
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
            "PRISM_STRATUM_STALE_GRACE_SECONDS": "0",
            "PRISM_STRATUM_SHARE_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_MIN_DIFF": "1024",
            "PRISM_STRATUM_VARDIFF_START_DIFF": "4096",
            "PRISM_STRATUM_VARDIFF_MAX_DIFF": "65536",
            "PRISM_CTV_SETTLEMENT_ENABLED": "1",
        }
        with (
            patch.dict(os.environ, env, clear=True),
            self.assertRaisesRegex(
                SystemExit,
                "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT",
            ),
        ):
            validate_prism_production_gate()

    def test_live_chain_identity_accepts_main_alias_and_pinned_genesis(self) -> None:
        genesis = "12" * 32
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.qbit_chain = "mainnet"

        class Rpc:
            def call(self, method: str, params: object = None) -> object:
                if method == "getblockchaininfo":
                    return {
                        "chain": "main",
                        "initialblockdownload": False,
                        "blocks": 100,
                        "headers": 100,
                    }
                if method == "getnetworkinfo":
                    return {"connections": 2}
                if method == "getblockhash" and params == [0]:
                    return genesis
                raise RuntimeError(method)

        server.rpc = Rpc()
        with patch.dict(os.environ, {"QBIT_EXPECTED_GENESIS_HASH": genesis}, clear=True):
            server.validate_live_chain_identity()

    def test_live_chain_identity_rejects_incomplete_public_readiness(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.qbit_chain = "mainnet"
        genesis = "12" * 32

        cases = (
            ({"chain": "main", "blocks": 10, "headers": 10}, {"connections": 1}, "initial block"),
            (
                {"chain": "main", "initialblockdownload": False, "blocks": 9, "headers": 10},
                {"connections": 1},
                "not caught up",
            ),
            (
                {"chain": "main", "initialblockdownload": False, "blocks": 10, "headers": 10},
                {"connections": 0},
                "requires at least 1",
            ),
        )
        for blockchain_info, network_info, message in cases:
            with self.subTest(message=message):
                server.rpc = SimpleNamespace(
                    call=lambda method, params=None: (
                        blockchain_info
                        if method == "getblockchaininfo"
                        else network_info
                        if method == "getnetworkinfo"
                        else genesis
                    )
                )
                with (
                    patch.dict(os.environ, {"QBIT_EXPECTED_GENESIS_HASH": genesis}, clear=True),
                    self.assertRaisesRegex(RuntimeError, message),
                ):
                    server.validate_live_chain_identity()

    def test_live_template_preflight_enforces_freshness_and_relay_fee_floor(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        previous_hash = "34" * 32
        template = {"height": 1, "curtime": int(time.time()), "previousblockhash": previous_hash}
        server.current_template_artifacts = lambda: SimpleNamespace(
            template=template,
            previousblockhash=previous_hash,
        )
        server.rpc = SimpleNamespace(
            call=lambda method, params=None: {
                "minrelaytxfee": "0.00001000",
                "mempoolminfee": "0.00001000",
            }
        )
        server._ctv_fanout_market_fee_rate_cache = {}

        enabled = {
            "PRISM_CTV_SETTLEMENT_ENABLED": "1",
            "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "1000",
            "PRISM_TEMPLATE_MAX_AGE_SECONDS": "120",
        }
        with patch.dict(os.environ, enabled, clear=True):
            server.validate_live_template_and_fee_policy()

        server._ctv_fanout_market_fee_rate_cache = {}
        with (
            patch.dict(
                os.environ,
                {**enabled, "PRISM_CTV_FANOUT_FEE_MARKET_RATE_BITS_PER_1000_WEIGHT": "1"},
                clear=True,
            ),
            self.assertRaisesRegex(RuntimeError, "below the connected node relay floor"),
        ):
            server.validate_live_template_and_fee_policy()

        template["curtime"] = int(time.time()) - 121
        with (
            patch.dict(os.environ, enabled, clear=True),
            self.assertRaisesRegex(RuntimeError, "block template is stale"),
        ):
            server.validate_live_template_and_fee_policy()

    def test_template_refresh_failure_budget_distinguishes_transient_and_sustained_outage(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.template_refresh_failure_exit_seconds = 120
        server.last_successful_template_refresh_monotonic = 100.0

        self.assertFalse(server.template_refresh_failure_expired(219.999))
        self.assertTrue(server.template_refresh_failure_expired(220.0))

        server.last_successful_template_refresh_monotonic = 219.0
        self.assertFalse(server.template_refresh_failure_expired(220.0))

    def test_healthy_noop_template_poll_resets_refresh_failure_clock(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]

        def trusted_chain_view(_tip: str) -> bool:
            server.stop_event.set()
            return True

        server.ensure_reorg_reconciled_for_tip = trusted_chain_view  # type: ignore[method-assign]
        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=200.0):
            server.blockpoll_loop()

        self.assertEqual(server.last_successful_template_refresh_monotonic, 200.0)

    def test_shared_template_poll_records_success_for_blockwait_callers(self) -> None:
        server = coordinator()
        server.last_successful_template_refresh_monotonic = 100.0
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]

        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=200.0):
            refreshed = server.poll_qbit_tip_template_once(heartbeat_name="qbit_blockwait")

        self.assertEqual(refreshed, 0)
        self.assertEqual(server.last_successful_template_refresh_monotonic, 200.0)

    def test_untrusted_reconciliation_exhausts_template_refresh_failure_budget(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_tip = lambda _tip: False  # type: ignore[method-assign]

        with (
            patch("lab.prism.prism_coordinator.time.monotonic", return_value=110.0),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
            self.assertRaises(SystemExit),
        ):
            server.blockpoll_loop()

        exit_process.assert_called_once_with(1)
        self.assertEqual(server.last_successful_template_refresh_monotonic, 100.0)

    def test_all_refresh_job_builds_failing_exhausts_failure_budget(self) -> None:
        old_tip = "00" * 32
        new_tip = "33" * 32
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        state = client()
        state.username = "miner-a"
        state.worker = worker_identity()
        state.active_job = prism_context("old-job", old_tip, worker=state.worker)
        state.active_job_ids = {"old-job"}
        server.clients = {state}
        server.jobs = {"old-job": state.active_job}
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=qbit_template_fingerprint(state.active_job.template),
        )
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash=new_tip,
            previousblockhash=new_tip,
            template_fingerprint="44" * 32,
        )
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]
        server.build_job_for_client = lambda *_args, **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("template build unavailable")
        )

        with (
            patch("lab.prism.prism_coordinator.time.monotonic", return_value=110.0),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
            self.assertRaises(SystemExit),
        ):
            server.blockpoll_loop()

        exit_process.assert_called_once_with(1)
        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(server.last_successful_template_refresh_monotonic, 100.0)

    def test_transient_template_refresh_failure_recovers_on_healthy_noop(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        poll_count = 0

        def fail_then_noop() -> int:
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                raise RuntimeError("transient RPC failure")
            server.last_successful_template_refresh_monotonic = time.monotonic()
            server.stop_event.set()
            return 0

        server.poll_qbit_tip_template_once = fail_then_noop  # type: ignore[method-assign]
        with (
            patch("lab.prism.prism_coordinator.time.monotonic", side_effect=[105.0, 106.0]),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
        ):
            server.blockpoll_loop()

        self.assertEqual(poll_count, 2)
        self.assertEqual(server.last_successful_template_refresh_monotonic, 106.0)

    def test_live_chain_identity_rejects_wrong_chain_or_genesis(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.qbit_chain = "mainnet"
        server.rpc = SimpleNamespace(
            call=lambda method, params=None: (
                {"chain": "regtest"} if method == "getblockchaininfo" else "34" * 32
            )
        )
        with patch.dict(os.environ, {"QBIT_EXPECTED_GENESIS_HASH": "12" * 32}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "does not match RPC chain"):
                server.validate_live_chain_identity()

        server.rpc = SimpleNamespace(
            call=lambda method, params=None: (
                {"chain": "main"} if method == "getblockchaininfo" else "34" * 32
            )
        )
        with patch.dict(os.environ, {"QBIT_EXPECTED_GENESIS_HASH": "12" * 32}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "does not match the connected"):
                server.validate_live_chain_identity()

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

    def test_prior_tip_share_inside_grace_is_credited_without_submitblock(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.current_tip_first_seen = (new_tip, time.monotonic())
        server.stale_grace_seconds = 3
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=True,
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
        self.assertEqual(rpc.submitblock_calls, 0)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)
        self.assertEqual(server.grace_credited_share_count, 1)
        self.assertEqual(server.worker_share_counts["miner-a"]["grace"], 1)

    def test_evicted_prior_tip_share_inside_grace_is_credited(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.current_tip_first_seen = (new_tip, time.monotonic())
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="ad" * 80,
            block_hash_hex="cd" * 32,
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
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)

    def test_evicted_same_tip_share_is_credited_without_stale_grace_policy(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="af" * 80,
            block_hash_hex="cf" * 32,
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
        self.assertIsNone(ledger.pending[0].credit_policy)

    def test_retained_share_dedup_uses_original_worker_after_reauthorization(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="af" * 80,
            block_hash_hex="cf" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )
            state.username = "miner-b"
            state.worker = worker_identity("miner-b")
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-b", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_DUPLICATE_SHARE)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].share_id, "miner-a:" + "cf" * 32)
        self.assertEqual(server.worker_share_counts["miner-a"]["accepted"], 1)
        self.assertEqual(server.worker_share_counts["miner-b"]["accepted"], 0)

    def test_evicted_same_tip_share_survives_beyond_legacy_one_second_floor(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.same_tip_job_retention_seconds = 30
        server.bury_evicted_job(state, "job-1", now=100.0)
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="a1" * 80,
            block_hash_hex="c1" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            return_value=102.0,
        ), patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )

        self.assertEqual(len(ledger.pending), 1)
        self.assertIsNone(ledger.pending[0].credit_policy)
        self.assertIn("job-1", server.evicted_job_graveyard)
        self.assertEqual(server.evicted_job_submit_counts["accepted_same_tip"], 1)

    def test_evicted_same_tip_submit_uses_original_job_difficulty(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        original_context = server.jobs["job-1"]
        original_context.job.share_difficulty = Decimal("2")
        state.share_difficulty = Decimal("32")
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="a2" * 80,
            block_hash_hex="c2" * 32,
            share_pass=True,
            block_pass=False,
        )

        def assemble(job: object, **_kwargs: object) -> object:
            self.assertIs(job, original_context.job)
            self.assertEqual(job.share_difficulty, Decimal("2"))
            return submission

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            side_effect=assemble,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )

        self.assertEqual(len(ledger.pending), 1)
        self.assertIsNone(ledger.pending[0].credit_policy)

    def test_same_tip_retention_ttl_and_capacity_are_bounded(self) -> None:
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.same_tip_job_retention_seconds = 30
        server.same_tip_job_retention_per_connection = 2
        identity = state.worker
        for index in range(3):
            job_id = f"job-{index + 1}"
            server.jobs[job_id] = prism_context(job_id, tip, worker=identity)
            server.bury_evicted_job(state, job_id, now=100.0 + index)

        self.assertNotIn("job-1", server.evicted_job_graveyard)
        self.assertEqual(
            list(server.evicted_job_graveyard),
            ["job-2", "job-3"],
        )
        self.assertEqual(server.evicted_job_capacity_eviction_counts["connection"], 1)

        server.prune_evicted_job_graveyard(now=133.1)
        self.assertEqual(server.evicted_job_graveyard, {})
        self.assertEqual(server.evicted_job_expiration_counts["same_tip"], 2)

    def test_evicted_job_hit_is_constant_work_in_large_graveyard(self) -> None:
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.same_tip_job_retention_seconds = 30
        server.same_tip_job_retention_per_connection = 4_096
        for index in range(4_096):
            job_id = f"retained-{index}"
            server.jobs[job_id] = prism_context(job_id, tip, worker=state.worker)
            server.bury_evicted_job(state, job_id, now=100.0, prune=False)

        self.assertEqual(len(server.evicted_job_graveyard), 4_096)
        self.assertEqual(len(server.evicted_same_tip_job_ids), 4_096)
        classify_calls = 0
        original_classify = server._evicted_job_class_locked

        def counted_classify(entry: object) -> str:
            nonlocal classify_calls
            classify_calls += 1
            return original_classify(entry)

        server._evicted_job_class_locked = counted_classify  # type: ignore[method-assign]
        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=101.0):
            for _ in range(100):
                self.assertIsNotNone(
                    server.evicted_job_entry(state, "retained-2048")
                )

        self.assertEqual(classify_calls, 100)

    def test_pool_width_does_not_evict_other_connections_retained_jobs(self) -> None:
        tip = "00" * 32
        server, _state, _ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.same_tip_job_retention_seconds = 30
        server.same_tip_job_retention_per_connection = 1
        clients: list[ClientState] = []
        for index in range(4_097):
            state = client()
            state.connection_id = index + 1
            state.worker = worker_identity(f"miner-{index}")
            clients.append(state)
            job_id = f"wide-{index}"
            server.jobs[job_id] = prism_context(job_id, tip, worker=state.worker)
            server.bury_evicted_job(state, job_id, now=100.0, prune=False)

        self.assertEqual(len(server.evicted_job_graveyard), 4_097)
        self.assertIn("wide-0", server.evicted_job_graveyard)
        self.assertIn("wide-4096", server.evicted_job_graveyard)

        replacement_id = "wide-0-replacement"
        server.jobs[replacement_id] = prism_context(
            replacement_id,
            tip,
            worker=clients[0].worker,
        )
        server.bury_evicted_job(
            clients[0],
            replacement_id,
            now=101.0,
            prune=False,
        )

        self.assertNotIn("wide-0", server.evicted_job_graveyard)
        self.assertIn("wide-1", server.evicted_job_graveyard)
        self.assertIn(replacement_id, server.evicted_job_graveyard)
        self.assertEqual(len(server.evicted_job_graveyard), 4_097)
        self.assertEqual(server.evicted_job_capacity_eviction_counts["connection"], 1)

    def test_tip_change_and_disconnect_remove_retained_contexts(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _ledger = submit_coordinator(tip=old_tip)
        server.current_tip_first_seen = (old_tip, None)
        server.stale_grace_seconds = 0
        server.bury_evicted_job(state, "job-1")
        self.assertIn("job-1", server.evicted_job_graveyard)

        server.observe_tip_first_seen(new_tip)
        self.assertNotIn("job-1", server.evicted_job_graveyard)

        server.current_tip_first_seen = (new_tip, None)
        server.jobs["job-2"] = prism_context("job-2", new_tip, worker=state.worker)
        server.bury_evicted_job(state, "job-2")
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]
        server.disconnect_client(state)
        self.assertEqual(server.evicted_job_graveyard, {})

    def test_tip_flip_reanchors_retained_job_grace_to_client_delivery(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _ledger = submit_coordinator(tip=old_tip)
        server.clients = {state}
        server.current_tip_first_seen = (old_tip, None)
        server.stale_grace_seconds = 3
        server.bury_evicted_job(state, "job-1", now=100.0)

        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=120.0):
            server.observe_tip_first_seen(new_tip)

        # Burial predates the flip by twenty seconds, but grace does not begin
        # until this connection actually receives replacement work.
        server.prune_evicted_job_graveyard(now=130.0)
        self.assertIn("job-1", server.evicted_job_graveyard)

        state.tip_work_delivered = (new_tip, 130.0)
        server.prune_evicted_job_graveyard(now=132.9)
        self.assertIn("job-1", server.evicted_job_graveyard)
        server.prune_evicted_job_graveyard(now=133.1)
        self.assertNotIn("job-1", server.evicted_job_graveyard)
        self.assertEqual(server.evicted_job_expiration_counts["stale_grace"], 1)

    def test_tip_flip_prunes_by_chain_parent_when_poller_skips_observed_tip(self) -> None:
        observed_tip = "00" * 32
        intermediate_tip = "11" * 32
        current_tip = "22" * 32
        server, state, _ledger = submit_coordinator(tip=intermediate_tip)
        server.clients = {state}
        server.current_tip_first_seen = (observed_tip, None)
        server.stale_grace_seconds = 3
        server.jobs["older-job"] = prism_context(
            "older-job",
            observed_tip,
            worker=state.worker,
        )
        server.bury_evicted_job(state, "older-job", now=100.0, prune=False)
        server.bury_evicted_job(state, "job-1", now=110.0, prune=False)
        server.rpc = ParentTipRpc(tip=current_tip, parent=intermediate_tip)

        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=120.0):
            server.observe_tip_first_seen(current_tip)

        # The poller's previous observation is not authoritative. Tip
        # observation proactively loads the actual parent, drops older work,
        # and preserves the intermediate-tip context that submit can credit.
        self.assertNotIn("older-job", server.evicted_job_graveyard)
        self.assertIn("job-1", server.evicted_job_graveyard)
        self.assertEqual(
            server.current_tip_parent_hash(current_tip),
            intermediate_tip,
        )
        entry = server.evicted_job_entry(state, "job-1")
        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(
            server.evicted_submit_context(state, entry, current_tip),
            (entry.context, PRISM_CREDIT_POLICY_STALE_GRACE),
        )

    def test_slow_parent_lookup_cannot_overwrite_newer_tip_parent_cache(self) -> None:
        old_tip = "00" * 32
        old_parent = "ff" * 32
        new_tip = "11" * 32
        new_parent = old_tip
        server = coordinator()
        server.current_tip_first_seen = (old_tip, None)
        server.current_tip_observation_sequence = 1
        server.current_tip_parent = None

        def overtake_parent_lookup(tip_hash: str) -> str:
            self.assertEqual(tip_hash, old_tip)
            with server.lock:
                server.current_tip_first_seen = (new_tip, 100.0)
                server.current_tip_observation_sequence = 2
                server.current_tip_parent = (new_tip, new_parent)
            return old_parent

        server._fetch_tip_parent_hash = overtake_parent_lookup  # type: ignore[method-assign]

        self.assertEqual(server.current_tip_parent_hash(old_tip), old_parent)
        self.assertEqual(server.current_tip_parent, (new_tip, new_parent))

    def test_retained_same_tip_duplicate_remains_duplicate_share(self) -> None:
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.current_tip_first_seen = (tip, None)
        server.bury_evicted_job(state, "job-1")
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="a3" * 80,
            block_hash_hex="c3" * 32,
            share_pass=True,
            block_pass=False,
        )
        params = ["miner-a", "job-1", "00" * 8, "00000001", "00000002"]

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(server.handle_submit(state, params))
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(state, params)

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_DUPLICATE_SHARE)
        self.assertEqual(len(ledger.pending), 1)
        self.assertIn("job-1", server.evicted_job_graveyard)
        metrics = server.metrics_payload()
        self.assertIn('qbit_prism_evicted_job_contexts{class="same_tip"} 1', metrics)
        self.assertIn(
            'qbit_prism_evicted_job_submits_total{outcome="accepted_same_tip"} 1',
            metrics,
        )

    def test_pool_closed_submit_rejects_before_any_share_accounting(self) -> None:
        # Post-close submits must not inflate submitted totals (the
        # stale-percent denominator), per-worker submitted counters, or the
        # vardiff window; only the pool-closed rejection itself is recorded.
        server, state, ledger = submit_coordinator()
        server.accepted_block_count = 1
        server.max_blocks = 1
        state.vardiff_config = SimpleNamespace(enabled=True)
        submitted_before = server.submitted_share_count

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_POOL_CLOSED)
        self.assertEqual(server.submitted_share_count, submitted_before)
        # The rejection itself may admit the label, but no submission counted.
        self.assertEqual(server.worker_share_counts["miner-a"]["submitted"], 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(
            server.worker_rejection_counts[("miner-a", PRISM_REJECTION_POOL_CLOSED)], 1
        )

    def test_malformed_submit_does_not_diverge_worker_and_aggregate_submitted(self) -> None:
        # A malformed-ntime submit must count identically in the per-worker and
        # aggregate submitted counters (i.e. not at all) so the two never drift.
        server, state, _ledger = submit_coordinator()
        submitted_before = server.submitted_share_count

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "bad-ntime", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_INVALID_NTIME_OR_NONCE)
        self.assertEqual(server.submitted_share_count, submitted_before)
        self.assertEqual(server.worker_share_counts["miner-a"]["submitted"], 0)
        self.assertEqual(
            server.worker_rejection_counts[("miner-a", PRISM_REJECTION_INVALID_NTIME_OR_NONCE)],
            1,
        )

    def test_stale_grace_closed_when_refresh_path_has_not_observed_tip(self) -> None:
        # Only blockpoll/blockwait may open the grace window. If the refresh path
        # has not anchored the new tip (current_tip_first_seen is None) and only
        # this submit's getbestblockhash sees it, the prior-tip share must reject
        # as stale-job -- not get credited from a submit-anchored window.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.current_tip_first_seen = None
        server.stale_grace_seconds = 3
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_STALE_JOB)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(rpc.submitblock_calls, 0)
        # The submit must not have anchored the window either.
        self.assertIsNone(server.current_tip_first_seen)

    def test_stale_grace_rejected_after_window_expires(self) -> None:
        # This connection received current-tip work well outside the grace
        # window; a prior-tip share arriving now must reject rather than be
        # credited late.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.stale_grace_seconds = 3
        server.current_tip_first_seen = (new_tip, time.monotonic() - 10)
        state.tip_work_delivered = (new_tip, time.monotonic() - 10)
        submission = SimpleNamespace(
            header_hex="ab" * 80,
            block_hash_hex="ce" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_STALE_JOB)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(rpc.submitblock_calls, 0)

    def test_stale_grace_open_until_connection_receives_new_tip_work(self) -> None:
        # The refresh pass may be slow or aborted (reorg reconcile failure,
        # transient build errors). Until THIS connection is sent current-tip
        # work, its prior-tip shares are still in flight and must stay
        # creditable even after the global first-seen stamp ages past the
        # grace window.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.stale_grace_seconds = 3
        server.current_tip_first_seen = (new_tip, time.monotonic() - 10)
        state.tip_work_delivered = (old_tip, time.monotonic() - 60)
        submission = SimpleNamespace(
            header_hex="ac" * 80,
            block_hash_hex="cd" * 32,
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
        self.assertEqual(rpc.submitblock_calls, 0)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)

    def test_stale_grace_window_runs_from_per_connection_delivery_not_first_seen(self) -> None:
        # A slow refresh pass can deliver current-tip work to a connection
        # after the global first-seen stamp has already aged past the grace
        # window. The window for that connection runs from ITS delivery.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.stale_grace_seconds = 3
        server.current_tip_first_seen = (new_tip, time.monotonic() - 10)
        state.tip_work_delivered = (new_tip, time.monotonic() - 1)
        submission = SimpleNamespace(
            header_hex="ae" * 80,
            block_hash_hex="cb" * 32,
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
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)

    def test_startup_baseline_tip_does_not_open_stale_grace_window(self) -> None:
        # The first tip observed after process start is a baseline, not a tip
        # flip: it must not open the grace window. A later real flip must.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _ledger = submit_coordinator(tip=old_tip)
        server.stale_grace_seconds = 3
        server.current_tip_first_seen = None

        server.observe_tip_first_seen(new_tip)
        self.assertEqual(server.current_tip_first_seen, (new_tip, None))
        self.assertFalse(server.stale_grace_deadline_open(state, new_tip))

        # A change away from the observed baseline is a real flip and opens
        # the window for connections that have not yet received the new work.
        flip_tip = "22" * 32
        server.observe_tip_first_seen(flip_tip)
        self.assertIsNotNone(server.current_tip_first_seen[1])
        self.assertTrue(server.stale_grace_deadline_open(state, flip_tip))

    def test_note_tip_work_delivered_keeps_first_delivery_per_tip(self) -> None:
        # Same-tip template refreshes must not slide the grace anchor forward.
        server, state, _ledger = submit_coordinator()
        tip = "11" * 32

        server.note_tip_work_delivered(state, tip)
        first = state.tip_work_delivered
        self.assertEqual(first[0], tip)
        server.note_tip_work_delivered(state, tip)
        self.assertEqual(state.tip_work_delivered, first)

        # A new tip re-anchors.
        server.note_tip_work_delivered(state, "22" * 32)
        self.assertEqual(state.tip_work_delivered[0], "22" * 32)
        self.assertGreaterEqual(state.tip_work_delivered[1], first[1])

    def test_evicted_graveyard_keeps_unexpired_entries_above_previous_cap_for_grace_credit(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        rpc = ParentTipRpc(tip=new_tip, parent=old_tip)
        server.rpc = rpc
        server.current_tip_first_seen = (new_tip, time.monotonic())
        context = server.jobs["job-1"]
        evicted_at = time.monotonic()
        server.evicted_job_graveyard = {
            "job-1": (context, state.connection_id, evicted_at),
        }
        previous_hard_cap = 512
        for index in range(previous_hard_cap):
            server.evicted_job_graveyard[f"filler-{index}"] = (
                context,
                state.connection_id,
                evicted_at + 0.001 + (index / 1_000_000),
            )
        server.prune_evicted_job_graveyard(now=evicted_at + 0.5)
        self.assertIn("job-1", server.evicted_job_graveyard)
        server.jobs.pop("job-1")
        state.active_job_ids.clear()
        submission = SimpleNamespace(
            header_hex="ae" * 80,
            block_hash_hex="ce" * 32,
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
        self.assertEqual(ledger.pending[0].credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)

    def test_stale_grace_parent_rpc_failure_rejects_as_backend_unavailable(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _ledger = submit_coordinator(tip=old_tip)
        server.rpc = TipRpc(new_tip)
        server.current_tip_first_seen = (new_tip, time.monotonic())

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE)

    def test_unknown_job_rejects_before_getbestblockhash_rpc(self) -> None:
        class CountingTipRpc(TipRpc):
            def __init__(self, tip: str) -> None:
                super().__init__(tip)
                self.getbest_calls = 0

            def call(self, method: str, params: list[object] | None = None) -> object:
                if method == "getbestblockhash":
                    self.getbest_calls += 1
                return super().call(method, params)

        server, state, _ledger = submit_coordinator()
        rpc = CountingTipRpc("00" * 32)
        server.rpc = rpc

        with self.assertRaises(StratumError) as raised:
            server.handle_submit(
                state,
                ["miner-a", "garbage-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)
        self.assertEqual(rpc.getbest_calls, 0)

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

        accepted = server.submit_block_candidate(block_candidate(server, state, submission))

        self.assertFalse(accepted)
        # The share was already accepted at submit time, so a lost block is a
        # block-abandonment, not a stale share rejection.
        self.assertEqual(server.stale_share_count, 0)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
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

        with patch("builtins.print") as printed:
            accepted = server.submit_block_candidate(block_candidate(server, state, submission))

        self.assertFalse(accepted)
        self.assertEqual(
            server.block_candidate_abandoned_counts.get(PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE, 0),
            0,
        )
        messages = [str(call.args[0]) for call in printed.call_args_list if call.args]
        self.assertTrue(any("block candidate deferred" in message for message in messages))
        self.assertFalse(any("block candidate abandoned" in message for message in messages))
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE], 0)
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
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.rpc = ParentTipRpc(tip=new_tip, parent="22" * 32)

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

    def test_share_ack_and_counters_wait_for_group_commit(self) -> None:
        server, state, ledger = submit_coordinator()
        server.share_writer_active = True
        server.share_commit_timeout_seconds = 2.0
        server.share_commit_linger_seconds = 0.0
        commit_started = threading.Event()
        release_commit = threading.Event()

        class BlockingBatchLedger(type(ledger)):
            def append_batch(self, entries: object) -> list[object]:
                commit_started.set()
                release_commit.wait(timeout=2)
                return [self.append(pending) for pending, _candidate in entries]

        ledger = BlockingBatchLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=False,
        )

        writer = threading.Thread(target=server.share_append_loop, daemon=True)
        writer.start()
        outcome: list[object] = []

        def submit() -> None:
            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                outcome.append(
                    server.handle_submit(
                        state,
                        ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                    )
                )

        submitter = threading.Thread(target=submit)
        submitter.start()
        self.assertTrue(commit_started.wait(timeout=1))
        self.assertTrue(submitter.is_alive())
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(server.worker_share_counts["miner-a"]["accepted"], 0)

        release_commit.set()
        submitter.join(timeout=2)
        self.assertFalse(submitter.is_alive())
        self.assertEqual(outcome, [False])
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].share_id, "miner-a:" + "cc" * 32)
        self.assertEqual(server.worker_share_counts["miner-a"]["accepted"], 1)
        server.stop_event.set()
        writer.join(timeout=2)

    def test_failed_commit_releases_duplicate_key_for_exact_retry(self) -> None:
        server, state, healthy = submit_coordinator()
        server.share_writer_active = True
        server.share_commit_linger_seconds = 0.0
        server.share_commit_timeout_seconds = 1.0

        class FailedLedger:
            def append_batch(self, _entries: object) -> list[object]:
                raise RuntimeError("postgres unavailable")

        server.ledger = FailedLedger()
        writer = threading.Thread(target=server.share_append_loop, daemon=True)
        writer.start()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="dd" * 32,
            share_pass=True,
            block_pass=False,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaisesRegex(StratumError, "commit failed"):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        server.share_writer_active = False
        server.ledger = healthy
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )
        self.assertEqual(len(healthy.pending), 1)
        self.assertEqual(server.worker_share_counts["miner-a"]["accepted"], 1)
        server.stop_event.set()
        writer.join(timeout=2)

    def test_share_writer_retries_failed_append_until_it_lands(self) -> None:
        server, state, ledger = submit_coordinator()

        class FlakyLedger(type(ledger)):
            def __init__(self) -> None:
                super().__init__()
                self.failures_remaining = 2

            def append(self, pending: object) -> object:
                if self.failures_remaining > 0:
                    self.failures_remaining -= 1
                    raise RuntimeError("ledger briefly unavailable")
                return super().append(pending)

        flaky = FlakyLedger()
        server.ledger = flaky
        entry = PendingShareAppend(
            pending_share=SimpleNamespace(share_id="miner-a:" + "ee" * 32),
            username="miner-a",
            job_id="job-1",
            block_hash_hex="ee" * 32,
            collection_only=False,
            credit_policy=None,
        )

        with patch.object(server.stop_event, "wait", return_value=False) as waited:
            server._append_share_entry(entry, retry_until_stopped=True)

        self.assertEqual(len(flaky.pending), 1)
        self.assertEqual(server.share_append_failure_count, 2)
        self.assertEqual(waited.call_count, 2)

    def _pending_append(self, tag: str, accepted_at_ms: int = 2) -> PendingShareAppend:
        from lab.prism.share_ledger import PendingShare

        return PendingShareAppend(
            pending_share=PendingShare(
                share_id=f"miner-a:{tag}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=10,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=accepted_at_ms,
                ntime=1_700_000_000,
            ),
            username="miner-a",
            job_id="job-1",
            block_hash_hex=tag * 32,
            collection_only=False,
            credit_policy=None,
        )

    def test_share_append_backlog_overflow_never_reports_success(self) -> None:
        server, _state, _ledger = submit_coordinator()
        server.share_append_queue = queue.Queue(maxsize=2)
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"

            server.enqueue_share_append(self._pending_append("aa"))
            server.enqueue_share_append(self._pending_append("bb"))
            with self.assertRaisesRegex(StratumError, "queue is full"):
                server.enqueue_share_append(self._pending_append("cc"))

            self.assertEqual(server.shares_recovered_to_disk, 0)
            remaining = [
                server.share_append_queue.get_nowait().pending_share.share_id
                for _ in range(2)
            ]
            self.assertEqual(remaining, ["miner-a:aa", "miner-a:bb"])
            self.assertFalse(server.share_recovery_path.exists())

    def test_writer_recovers_acked_share_on_shutdown_during_outage(self) -> None:
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"

            class DownLedger(type(ledger)):
                def append(self, pending: object) -> object:
                    raise RuntimeError("postgres unavailable")

            server.ledger = DownLedger()
            entry = self._pending_append("ee")
            # First backoff wait returns True (stop requested): the share must
            # be recovered, not silently dropped.
            with patch.object(server.stop_event, "wait", return_value=True):
                server._append_share_entry(entry, retry_until_stopped=True)

            self.assertEqual(server.shares_recovered_to_disk, 1)
            recovered = json.loads(server.share_recovery_path.read_text().strip())
            self.assertEqual(recovered["share_id"], "miner-a:ee")

    def test_replay_recovered_shares_appends_and_clears_file(self) -> None:
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("f1"), "test")
            server._recover_share_to_disk(self._pending_append("f2"), "test")

            replayed = server.replay_recovered_shares()

            self.assertEqual(replayed, 2)
            self.assertEqual(server.shares_replayed, 2)
            self.assertEqual(
                [p.share_id for p in ledger.pending], ["miner-a:f1", "miner-a:f2"]
            )
            # File is cleared after a clean replay so shares are not re-added.
            self.assertFalse(server.share_recovery_path.exists())
            self.assertEqual(server.replay_recovered_shares(), 0)

    def test_replay_recovered_shares_orders_by_accepted_at(self) -> None:
        # A share can be recovered out of FIFO order (overflow of the newest, or
        # a ledger flap during the shutdown drain). Replay must reorder by
        # accepted_at_ms so share_seq reflects acceptance order, keeping the
        # reward window correctly ordered.
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("late", accepted_at_ms=300), "test")
            server._recover_share_to_disk(self._pending_append("early", accepted_at_ms=100), "test")
            server._recover_share_to_disk(self._pending_append("mid", accepted_at_ms=200), "test")

            replayed = server.replay_recovered_shares()

            self.assertEqual(replayed, 3)
            self.assertEqual(
                [p.share_id for p in ledger.pending],
                ["miner-a:early", "miner-a:mid", "miner-a:late"],
            )

    def test_replay_skips_torn_line_and_keeps_file(self) -> None:
        # A crash mid-append can leave the last line torn. That one line must
        # not block the intact shares before it, and the file is kept so the
        # torn line is preserved rather than silently discarded.
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("g1", accepted_at_ms=100), "test")
            server._recover_share_to_disk(self._pending_append("g2", accepted_at_ms=200), "test")
            with open(server.share_recovery_path, "a", encoding="utf-8") as handle:
                handle.write('{"share_id": "miner-a:torn", "miner_i')  # truncated, no newline

            replayed = server.replay_recovered_shares()

            self.assertEqual(replayed, 2)
            self.assertEqual(
                [p.share_id for p in ledger.pending], ["miner-a:g1", "miner-a:g2"]
            )
            # File kept because a line could not be parsed.
            self.assertTrue(server.share_recovery_path.exists())
            # Re-running dedups the good shares (ledger is idempotent by id).
            self.assertEqual(server.replay_recovered_shares(), 2)

    def test_replay_is_idempotent_across_partial_replay(self) -> None:
        # Finding: a partial replay (A commits, B fails transiently) kept the
        # whole file; on retry, replay hit A's duplicate and stopped, stranding
        # B forever. Replay must skip the already-committed A and reach B.
        server, _state, _ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("A", accepted_at_ms=100), "test")
            server._recover_share_to_disk(self._pending_append("B", accepted_at_ms=200), "test")

            class DedupLedger:
                def __init__(self) -> None:
                    self.ids: list[str] = []
                    self.fail_b_once = True

                def append(self, pending: object) -> object:
                    if pending.share_id == "miner-a:B" and self.fail_b_once:
                        self.fail_b_once = False
                        raise RuntimeError("postgres unavailable")
                    if pending.share_id in self.ids:
                        raise RuntimeError("duplicate share_id")
                    self.ids.append(pending.share_id)
                    return SimpleNamespace(share_seq=len(self.ids))

            server.ledger = DedupLedger()

            # Pass 1: A commits, B raises a transient (non-duplicate) error, so
            # the pass stops and keeps the file.
            self.assertEqual(server.replay_recovered_shares(), 1)
            self.assertTrue(server.share_recovery_path.exists())
            self.assertEqual(server.ledger.ids, ["miner-a:A"])

            # Pass 2: A is now a duplicate (skipped, not fatal); B commits and
            # the file is cleared.
            self.assertEqual(server.replay_recovered_shares(), 1)
            self.assertEqual(server.ledger.ids, ["miner-a:A", "miner-a:B"])
            self.assertFalse(server.share_recovery_path.exists())

    def test_append_share_entry_reports_persisted_vs_recovered(self) -> None:
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            # Healthy ledger: reports persisted.
            self.assertTrue(
                server._append_share_entry(self._pending_append("ok"), retry_until_stopped=True)
            )

            class DownLedger(type(ledger)):
                def append(self, pending: object) -> object:
                    raise RuntimeError("postgres unavailable")

            server.ledger = DownLedger()
            with patch.object(server.stop_event, "wait", return_value=True):
                # Shutdown mid-outage: reports recovered, not persisted.
                self.assertFalse(
                    server._append_share_entry(self._pending_append("down"), retry_until_stopped=True)
                )

    def test_group_commit_failure_releases_all_waiters_without_recovery_file(self) -> None:
        server, _state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server.share_append_queue = queue.Queue(maxsize=MAX_PENDING_SHARE_APPENDS)

            append_calls: list[str] = []

            class DownLedger(type(ledger)):
                def append(self, pending: object) -> object:
                    append_calls.append(pending.share_id)
                    raise RuntimeError("postgres unavailable")

            server.ledger = DownLedger()
            server.stop_event.set()
            entries = []
            for tag in ("s1", "s2", "s3"):
                entry = self._pending_append(tag)
                entries.append(entry)
                server.enqueue_share_append(entry)

            server.share_append_loop()

            # The compatibility ledger fails on the first row; the whole batch
            # is reported failed and no uncommitted share is called durable.
            self.assertEqual(append_calls, ["miner-a:s1"])
            self.assertEqual(ledger.pending, [])
            self.assertTrue(all(entry.committed.is_set() for entry in entries))
            self.assertTrue(all(entry.error is not None for entry in entries))
            self.assertFalse(server.share_recovery_path.exists())

    def test_orphaned_block_candidate_keeps_share_credit(self) -> None:
        # Option-A semantics: a share that met its target stays credited even
        # when its block candidate loses the tip race in the submitter.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=True,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            server.handle_submit(
                state,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertEqual(len(ledger.pending), 1)
        # The tip moves before the submitter drains the candidate.
        server.rpc = TipRpc(new_tip)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(server.accepted_block_count, 0)
        # Counted as a block abandonment, not a share rejection.
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(server.stale_share_count, 0)
        # The credited share survives the lost block race.
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.persisted, [])

    def test_block_candidate_queue_overflow_coalesces_wakeup_without_drop(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.block_candidate_queue = queue.Queue(maxsize=2)

        def candidate(tag: str) -> PrismBlockCandidate:
            return block_candidate(
                server,
                state,
                SimpleNamespace(block_hash_hex=tag * 32, share_pass=True, block_pass=True),
            )

        server.enqueue_block_candidate(candidate("aa"))
        server.enqueue_block_candidate(candidate("bb"))
        server.enqueue_block_candidate(candidate("cc"))

        self.assertEqual(server.block_candidates_dropped, 0)
        self.assertEqual(server.block_candidate_queue.qsize(), 2)
        remaining = [
            server.block_candidate_queue.get_nowait().submission.block_hash_hex
            for _ in range(2)
        ]
        # Existing wakeups remain ordered; the third candidate remains durable
        # in the outbox and will be re-read after the queue drains.
        self.assertEqual(remaining, ["aa" * 32, "bb" * 32])
        self.assertIn(
            "qbit_prism_block_candidates_dropped_total 0", server.metrics_payload()
        )
        self.assertIn(
            "qbit_prism_block_candidate_wakeups_coalesced_total 1",
            server.metrics_payload(),
        )

    def test_durable_block_candidates_replay_after_queue_drains(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.block_candidate_queue = queue.Queue(maxsize=2)

        for index, tag in enumerate(("aa", "bb", "cc"), start=1):
            pending = PendingShare(
                share_id=f"miner-a:{tag * 32}",
                miner_id="miner-a",
                order_key="miner-a",
                p2mr_program_hex="11" * 32,
                share_difficulty=1,
                network_difficulty=1,
                template_height=9,
                job_id="job-1",
                job_issued_at_ms=1,
                accepted_at_ms=index,
                ntime=1,
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=tag * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            )
            candidate = block_candidate(
                server, state, submission, pending_share=pending
            )
            ledger.append_batch([(pending, server.block_candidate_intent(candidate))])

        self.assertEqual(server.replay_pending_block_candidates(), 2)
        first = server.block_candidate_queue.get_nowait()
        second = server.block_candidate_queue.get_nowait()
        self.assertEqual(
            [first.submission.block_hash_hex, second.submission.block_hash_hex],
            ["aa" * 32, "bb" * 32],
        )
        ledger.mark_block_candidate_submitted(block_hash="aa" * 32)
        ledger.mark_block_candidate_abandoned(block_hash="bb" * 32, error="stale")

        self.assertEqual(server.replay_pending_block_candidates(), 1)
        replayed = server.block_candidate_queue.get_nowait()
        self.assertEqual(replayed.submission.block_hash_hex, "cc" * 32)
        self.assertEqual(replayed.pending_share.share_id, "miner-a:" + "cc" * 32)

    def test_candidate_intent_avoids_duplicate_template_transaction_bodies(self) -> None:
        server, state, _ledger = submit_coordinator()
        witness_tx = synthetic_witness_transaction("55")
        server.jobs["job-1"].template["transactions"] = [{"data": witness_tx}]
        server.jobs["job-1"].job.transaction_hexes = (witness_tx,)
        pending = self._pending_append("ca").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="ca" * 32,
                block_hex="00" + witness_tx,
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )

        intent = server.block_candidate_intent(candidate)

        self.assertEqual(
            set(intent["template"]),
            {"previousblockhash", "height", "coinbasevalue"},
        )
        self.assertNotIn("transaction_hexes", intent)
        self.assertEqual(
            intent["witness_merkle_leaves_hex"],
            direct_stratum.witness_merkle_leaves_hex((witness_tx,)),
        )

    def test_transient_candidate_failure_remains_pending_for_retry(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = PendingShare(
            share_id="miner-a:" + "aa" * 32,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=2,
            ntime=1,
        )
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="aa" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        intent = server.block_candidate_intent(candidate)
        ledger.append_batch([(pending, intent)])
        server.enqueue_block_candidate(candidate)
        server.submit_block_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(RuntimeError("rpc unavailable"))
        )

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(ledger.pending_block_candidates(), [intent])
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertIn(
            "qbit_prism_block_candidate_retries_total 1",
            server.metrics_payload(),
        )

    def test_candidate_retry_backoff_is_capped_and_cleared_on_success(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("retry-success").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a1" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server.block_candidate_retry_initial_seconds = 0.1
        server.block_candidate_retry_max_seconds = 0.4
        attempts = 0

        def retry_then_succeed(_candidate: PrismBlockCandidate) -> bool:
            nonlocal attempts
            attempts += 1
            if attempts <= 4:
                server._defer_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "temporary RPC outage",
                    worker="miner-a",
                )
                return False
            return True

        server.submit_block_candidate = retry_then_succeed  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            for _attempt in range(5):
                server.enqueue_block_candidate(candidate)
                self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(waits, [0.1, 0.2, 0.4, 0.4])
        self.assertNotIn(candidate.submission.block_hash_hex, server.block_candidate_retry_delays)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_candidate_retry_state_is_cleared_on_terminal_abandonment(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("retry-terminal").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a2" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server.block_candidate_retry_delays = {candidate.submission.block_hash_hex: 2.0}

        def terminal(_candidate: PrismBlockCandidate) -> bool:
            server._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                "tip moved",
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = terminal  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertNotIn(candidate.submission.block_hash_hex, server.block_candidate_retry_delays)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_invalid_durable_candidate_is_quarantined_by_outbox_row_key(self) -> None:
        for payload_hash in (None, "ff" * 32):
            with self.subTest(payload_hash=payload_hash):
                server, _state, _recording = submit_coordinator()
                ledger = SingleWriterShareLedger()
                server.ledger = ledger
                durable_hash = "de" * 32
                invalid = {
                    "schema": "unsupported",
                    "block_hash_hex": durable_hash,
                    "block_hex": "00",
                }
                ledger.persist_block_candidate_intent(invalid)
                stored = ledger._block_candidate_outbox[durable_hash]["candidate"]
                if payload_hash is None:
                    stored.pop("block_hash_hex")
                else:
                    stored["block_hash_hex"] = payload_hash
                server.block_candidate_retry_delays = {durable_hash: 1.0}

                self.assertEqual(server.replay_pending_block_candidates(), 0)

                self.assertEqual(ledger.pending_block_candidates(), [])
                self.assertNotIn(durable_hash, server.block_candidate_retry_delays)
                self.assertIn(
                    "qbit_prism_block_candidate_poisoned_total 1",
                    server.metrics_payload(),
                )

    def test_block_submitter_drops_candidate_when_pool_closed(self) -> None:
        server, state, ledger = submit_coordinator()
        server.accepted_block_count = 1
        server.max_blocks = 1
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dd" * 32,
            block_hex="00",
        )

        accepted = server.submit_block_candidate(block_candidate(server, state, submission))

        self.assertFalse(accepted)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_POOL_CLOSED], 1)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_POOL_CLOSED], 0)
        self.assertEqual(ledger.persisted, [])

    def test_block_worthy_share_is_credited_and_enqueued_before_block_submission(self) -> None:
        # The share ack must never wait on the block path: a block-worthy
        # share that met its target is credited immediately and the candidate
        # is queued for the submitter thread. Nothing submits synchronously
        # (the fixture RPC would raise on an unexpected submitblock call).
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=True,
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
        self.assertIsNone(ledger.pending[0].credit_policy)
        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        queued = server.block_candidate_queue.get_nowait()
        self.assertIs(queued.submission, submission)
        self.assertFalse(queued.credit_share_on_accept)

    def test_block_candidate_submits_before_full_audit_persistence(self) -> None:
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

            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission, pending_share=pending)
            )
            self.assertTrue(accepted)

            live_files = sorted(Path(tempdir).glob("prism-live-audit-bundle-[0-9]*.json"))
            self.assertEqual(len(live_files), 1)
            self.assertEqual(list(Path(tempdir).glob("prism-live-audit-bundle-candidate-*.json")), [])
            self.assertEqual(list(Path(tempdir).glob(".prism-live-audit-bundle-candidate-*.tmp")), [])
            envelope = json.loads(live_files[0].read_text(encoding="utf-8"))
            self.assertEqual(envelope["schema"], "qbit.prism.live-audit-bundle-envelope.v1")
            self.assertEqual(envelope["block_hash"], block_hash)
            self.assertEqual(envelope["block_height"], 10)
            self.assertEqual(envelope["audit_bundle_sha256"], "33" * 32)
            self.assertNotIn("signed_coinbase_manifest", envelope)
            self.assertEqual(server.latest_evidence["audit_bundle_path"], str(live_files[0]))

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
        self.assertTrue(ledger.persisted[0]["submit_seen_at_persist"])

    def test_active_ancestor_candidate_resumes_full_finalization_without_resubmit(self) -> None:
        server, state, ledger = submit_coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "ac" * 32

            class ActiveAncestorRpc:
                def call(self, method: str, params: object = None) -> object:
                    if method == "getbestblockhash":
                        return "ef" * 32
                    if method == "getblockheader":
                        self.assert_candidate(params)
                        return {"height": 10, "confirmations": 2}
                    if method == "getblockcount":
                        return 11
                    if method == "submitblock":
                        raise AssertionError("active ancestor must not be resubmitted")
                    raise RuntimeError(method)

                @staticmethod
                def assert_candidate(params: object) -> None:
                    if params != [block_hash]:
                        raise AssertionError(params)

            server.rpc = ActiveAncestorRpc()
            server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
            server.build_audit_bundle = lambda **_kwargs: {  # type: ignore[method-assign]
                "found_block": {"coinbase_value_sats": 50_00000000},
                "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
                "payout_policy_manifest": {"accounts": []},
                "signed_coinbase_manifest": {
                    "manifest": {"coinbase_tx_hex": "c0ffee", "payout_count": 1}
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

            self.assertTrue(
                server.submit_block_candidate(block_candidate(server, state, submission))
            )

        self.assertEqual(ledger.persisted[0]["block_hash"], block_hash)
        self.assertEqual(ledger.confirmed[0]["active_tip_height"], 11)
        self.assertEqual(ledger.confirmed[0]["block_hash"], block_hash)
        self.assertFalse(ledger.confirmed[0]["submit_seen_at_confirm"])
        # The share credit happens on the client thread at submit time now;
        # the block path itself appends nothing.
        self.assertEqual(len(ledger.pending), 0)
        # stop_after_block fires from the submitter once the block confirms.
        self.assertTrue(server.stop_event.is_set())
        self.assertEqual(server.latest_evidence["persistence"]["block_count"], 1)
        self.assertEqual(server.latest_evidence["confirmation"]["confirmed_count"], 1)
        # Evidence carries an aggregate miner count, not a materialized list of
        # every miner id (which scanned the whole ledger twice under the lock).
        self.assertEqual(server.latest_evidence["accepted_share_count"], 0)
        self.assertEqual(server.latest_evidence["distinct_miner_count"], 0)
        self.assertNotIn("distinct_miners", server.latest_evidence)

    def test_audit_retention_prunes_only_live_and_candidate_files(self) -> None:
        server = coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.audit_live_bundle_retention = 2
            server.audit_candidate_retention_seconds = 0
            for index in range(4):
                path = Path(tempdir) / f"prism-live-audit-bundle-{index + 1}-{'aa' * 32}.json"
                path.write_text("{}", encoding="utf-8")
                os.utime(path, (100 + index, 100 + index))
            candidate = Path(tempdir) / f"prism-live-audit-bundle-candidate-{'bb' * 32}.json"
            candidate.write_text("{}", encoding="utf-8")
            temp_candidate = Path(tempdir) / f".prism-live-audit-bundle-candidate-{'bb' * 32}.json.tmp"
            temp_candidate.write_text("{}", encoding="utf-8")
            body = Path(tempdir) / f"prism-audit-bundle-body-{'cc' * 32}-{'dd' * 32}.json"
            body.write_text("{}", encoding="utf-8")
            segment = Path(tempdir) / f"prism-audit-share-segment-1-2-{'ee' * 32}.json"
            segment.write_text("{}", encoding="utf-8")

            server.prune_audit_artifacts()

            live_names = sorted(path.name for path in Path(tempdir).glob("prism-live-audit-bundle-[0-9]*.json"))
            self.assertEqual(
                live_names,
                [
                    f"prism-live-audit-bundle-3-{'aa' * 32}.json",
                    f"prism-live-audit-bundle-4-{'aa' * 32}.json",
                ],
            )
            self.assertFalse(candidate.exists())
            self.assertFalse(temp_candidate.exists())
            self.assertTrue(body.exists())
            self.assertTrue(segment.exists())

    def test_audit_retention_zero_preserves_current_live_envelope(self) -> None:
        server = coordinator()
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.audit_live_bundle_retention = 0
            old = Path(tempdir) / f"prism-live-audit-bundle-1-{'aa' * 32}.json"
            old.write_text("{}", encoding="utf-8")
            current = Path(tempdir) / f"prism-live-audit-bundle-2-{'bb' * 32}.json"
            current.write_text("{}", encoding="utf-8")

            server.prune_audit_artifacts(keep_live_path=current)

            self.assertFalse(old.exists())
            self.assertTrue(current.exists())

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
                # The ack goes out before the block path runs; draining the
                # submitter queue lands the block and pushes fresh work.
                self.assertEqual(sent, [{"id": "submit-1", "result": True, "error": None}])
                self.assertTrue(server.submit_next_block_candidate())

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
        server.stale_grace_seconds = 0

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
                self.assertTrue(server.submit_next_block_candidate())

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
                self.assertTrue(server.submit_next_block_candidate())

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

    def test_rejected_candidate_never_creates_prepared_payout_state(self) -> None:
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

            accepted = server.submit_block_candidate(block_candidate(server, state, submission))

        self.assertFalse(accepted)
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(ledger.rejected, [])
        self.assertEqual(ledger.reversed, [])
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(
            server.block_candidate_abandoned_counts[PRISM_REJECTION_SUBMITBLOCK_REJECTED], 1
        )


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

    def test_progressing_ctv_pass_longer_than_watchdog_timeout_stays_healthy(self) -> None:
        server = self._bare_coordinator()
        server.ctv_broadcaster_limit = 200
        server.ctv_broadcaster_interval_seconds = 30.0
        clock = {"now": 0.0}
        overdue_samples: list[list[str]] = []
        seen_limits: list[int] = []

        class StopAfterOnePass:
            def is_set(self) -> bool:
                return False

            def wait(self, timeout: float) -> bool:
                return True

        class ProgressingDaemon:
            def run_once(self, *, limit: int, progress_callback: object) -> object:
                seen_limits.append(limit)
                assert callable(progress_callback)
                for _ in range(3):
                    clock["now"] += 80.0
                    overdue_samples.append(server._overdue_heartbeats(clock["now"]))
                    progress_callback()
                return SimpleNamespace(
                    scanned_count=3,
                    submitted_count=0,
                    updated_count=3,
                    failed_count=0,
                )

        server.stop_event = StopAfterOnePass()  # type: ignore[assignment]
        server.ctv_fanout_broadcast_daemon = ProgressingDaemon()

        with patch("lab.prism.prism_coordinator.time.monotonic", side_effect=lambda: clock["now"]), patch(
            "builtins.print"
        ):
            server.ctv_fanout_broadcaster_loop()

        self.assertGreater(clock["now"], server.watchdog_timeout_seconds)
        self.assertEqual(seen_limits, [200])
        self.assertEqual(overdue_samples, [[], [], []])
        self.assertEqual(server.ctv_broadcaster_processed_rows_total, 3)
        self.assertEqual(server.ctv_broadcaster_pass_count, 1)

    def test_ctv_pass_completion_heartbeat_precedes_interval_wait(self) -> None:
        server = self._bare_coordinator()
        server.ctv_broadcaster_limit = 200
        server.ctv_broadcaster_interval_seconds = 30.0
        clock = {"now": 0.0}
        wait_observation: dict[str, object] = {}

        class StopAfterIntervalWait:
            def is_set(self) -> bool:
                return False

            def wait(self, timeout: float) -> bool:
                wait_observation["timeout"] = timeout
                wait_observation["heartbeat"] = server._heartbeats["ctv_fanout_broadcaster"]
                wait_observation["overdue_after_wait"] = server._overdue_heartbeats(
                    clock["now"] + timeout
                )
                return True

        class IncidentDurationDaemon:
            def run_once(self, *, limit: int, progress_callback: object) -> object:
                clock["now"] += 102.0
                return SimpleNamespace(
                    scanned_count=200,
                    submitted_count=0,
                    updated_count=200,
                    failed_count=0,
                )

        server.stop_event = StopAfterIntervalWait()  # type: ignore[assignment]
        server.ctv_fanout_broadcast_daemon = IncidentDurationDaemon()

        with patch("lab.prism.prism_coordinator.time.monotonic", side_effect=lambda: clock["now"]), patch(
            "builtins.print"
        ):
            server.ctv_fanout_broadcaster_loop()

        self.assertEqual(wait_observation["timeout"], 30.0)
        self.assertEqual(wait_observation["heartbeat"], 102.0)
        self.assertEqual(wait_observation["overdue_after_wait"], [])

    def test_ctv_pass_without_progress_remains_watchdog_eligible(self) -> None:
        server = self._bare_coordinator()
        server.ctv_broadcaster_limit = 200
        server.ctv_broadcaster_interval_seconds = 30.0
        clock = {"now": 0.0}
        entered_row = threading.Event()
        release_row = threading.Event()

        class BlockingDaemon:
            def run_once(self, *, limit: int, progress_callback: object) -> object:
                entered_row.set()
                release_row.wait()
                return SimpleNamespace(
                    scanned_count=1,
                    submitted_count=0,
                    updated_count=1,
                    failed_count=0,
                )

        server.ctv_fanout_broadcast_daemon = BlockingDaemon()
        broadcaster_thread = threading.Thread(target=server.ctv_fanout_broadcaster_loop)
        with patch("lab.prism.prism_coordinator.time.monotonic", side_effect=lambda: clock["now"]), patch(
            "builtins.print"
        ):
            broadcaster_thread.start()
            self.assertTrue(entered_row.wait(timeout=1.0))
            clock["now"] = server.watchdog_timeout_seconds + 1.0
            self.assertEqual(
                server._overdue_heartbeats(clock["now"]),
                ["ctv_fanout_broadcaster"],
            )
            server.stop_event.set()
            release_row.set()
            broadcaster_thread.join(timeout=1.0)

        self.assertFalse(broadcaster_thread.is_alive())

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

    def test_block_submit_pause_names_cover_registered_refresh_and_idle_threads(self) -> None:
        server = self._bare_coordinator()
        for name in ("stratum_accept", "qbit_blockpoll", "qbit_blockwait", "vardiff_idle_sweep"):
            server._record_heartbeat(name)
        now = time.monotonic()
        with server._heartbeats_lock:
            for name in server._heartbeats:
                server._heartbeats[name] = now - 1_000.0

        pause_names = server._registered_watchdog_heartbeat_names(
            "qbit_blockpoll",
            "qbit_blockwait",
            "vardiff_idle_sweep",
            "stratum_accept",
        )

        with server._watchdog_paused(*pause_names):
            self.assertEqual(server._overdue_heartbeats(now + 1_000.0), [])

    def test_pause_names_skip_removed_blockwait_without_resurrecting_heartbeat(self) -> None:
        server = self._bare_coordinator()
        server._record_heartbeat("qbit_blockpoll")
        server._record_heartbeat("qbit_blockwait")
        server._remove_watchdog_heartbeat("qbit_blockwait")

        pause_names = server._registered_watchdog_heartbeat_names("qbit_blockpoll", "qbit_blockwait")

        self.assertEqual(pause_names, ("qbit_blockpoll",))
        with server._watchdog_paused(*pause_names):
            pass
        self.assertNotIn("qbit_blockwait", server._heartbeats)

    def test_blockwait_parameter_mismatch_is_treated_as_unsupported(self) -> None:
        self.assertTrue(
            PrismCoordinator._blockwait_unsupported(
                RuntimeError("RPC error -32602: invalid params: wrong number of parameters")
            )
        )

    def test_blockwait_unsupported_removes_watchdog_heartbeat(self) -> None:
        server = coordinator()
        server.rpc = UnsupportedBlockwaitRpc("00" * 32)
        server._record_heartbeat("qbit_blockwait")

        server.blockwait_loop()

        self.assertNotIn("qbit_blockwait", server._heartbeats)
        self.assertNotIn("qbit_blockwait", server._watchdog_pauses)

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


def highdiff_vardiff_config(**overrides: object) -> vardiff.VardiffConfig:
    values: dict[str, object] = dict(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("500000"),
        max_difficulty=Decimal("4294967296"),
        retarget_interval_seconds=Decimal("1"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("500000"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("1"),
        retarget_tolerance=Decimal("0"),
    )
    values.update(overrides)
    return vardiff.VardiffConfig(**values)  # type: ignore[arg-type]


def clear_stratum_diff_env() -> None:
    for name in [
        name
        for name in os.environ
        if name.startswith("PRISM_STRATUM_HIGHDIFF") or name.startswith("PRISM_STRATUM_VARDIFF")
    ]:
        os.environ.pop(name, None)


class PrismListenerProfileTests(unittest.TestCase):
    def test_highdiff_listener_disabled_without_port(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            base = load_prism_vardiff_config(Decimal("0.000000001"))
            self.assertIsNone(load_prism_highdiff_listener("0.0.0.0", base))

    def test_highdiff_listener_defaults_to_nicehash_floor(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            base = load_prism_vardiff_config(Decimal("0.000000001"))
            profile = load_prism_highdiff_listener("10.0.0.1", base)

        assert profile is not None
        self.assertEqual(profile.name, "highdiff")
        self.assertEqual(profile.bind, "10.0.0.1")
        self.assertEqual(profile.port, 4334)
        self.assertEqual(profile.heartbeat_name, "stratum_accept_highdiff")
        self.assertEqual(profile.share_difficulty, Decimal("500000"))
        self.assertEqual(profile.minimum_advertised_difficulty, Decimal("500000"))
        self.assertEqual(profile.vardiff_config.min_difficulty, Decimal("500000"))
        self.assertEqual(profile.vardiff_config.startup_difficulty, Decimal("500000"))
        self.assertEqual(profile.vardiff_config.max_difficulty, Decimal("4294967296"))
        # Everything but the difficulty bounds is inherited from the base config.
        self.assertEqual(profile.vardiff_config.enabled, base.enabled)
        self.assertEqual(
            profile.vardiff_config.target_share_interval_seconds,
            base.target_share_interval_seconds,
        )
        self.assertEqual(
            profile.vardiff_config.retarget_interval_seconds,
            base.retarget_interval_seconds,
        )
        self.assertEqual(profile.vardiff_config.max_step_factor, base.max_step_factor)

    def test_highdiff_listener_env_overrides(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4335"
            os.environ["PRISM_STRATUM_HIGHDIFF_BIND"] = "127.0.0.2"
            os.environ["PRISM_STRATUM_HIGHDIFF_MIN_DIFF"] = "600000"
            os.environ["PRISM_STRATUM_HIGHDIFF_START_DIFF"] = "1000000"
            os.environ["PRISM_STRATUM_HIGHDIFF_MAX_DIFF"] = "8000000"
            os.environ["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = "700000"
            base = load_prism_vardiff_config(Decimal("0.000000001"))
            profile = load_prism_highdiff_listener("0.0.0.0", base)

        assert profile is not None
        self.assertEqual(profile.bind, "127.0.0.2")
        self.assertEqual(profile.port, 4335)
        self.assertEqual(profile.share_difficulty, Decimal("700000"))
        self.assertEqual(profile.minimum_advertised_difficulty, Decimal("600000"))
        self.assertEqual(profile.vardiff_config.min_difficulty, Decimal("600000"))
        self.assertEqual(profile.vardiff_config.startup_difficulty, Decimal("1000000"))
        self.assertEqual(profile.vardiff_config.max_difficulty, Decimal("8000000"))

    def test_highdiff_listener_rejects_inconsistent_bounds(self) -> None:
        base = load_prism_vardiff_config(Decimal("0.000000001"))
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_MIN_DIFF"] = "1000000"
            with self.assertRaises(SystemExit):
                load_prism_highdiff_listener("0.0.0.0", base)
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_MAX_DIFF"] = "400000"
            with self.assertRaises(SystemExit):
                load_prism_highdiff_listener("0.0.0.0", base)
        for bad_port in ("not-a-port", "0", "70000"):
            with patch.dict(os.environ, {}, clear=False):
                clear_stratum_diff_env()
                os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = bad_port
                with self.assertRaises(SystemExit):
                    load_prism_highdiff_listener("0.0.0.0", base)

    def test_client_startup_difficulty_uses_listener_profile(self) -> None:
        server = coordinator()
        profile = StratumListenerProfile(
            name="highdiff",
            bind="0.0.0.0",
            port=4334,
            share_difficulty=Decimal("700000"),
            vardiff_config=highdiff_vardiff_config(),
            heartbeat_name="stratum_accept_highdiff",
        )
        self.assertEqual(server.client_startup_difficulty(profile), Decimal("500000"))
        # Without a profile the default listener behavior is unchanged.
        self.assertEqual(server.client_startup_difficulty(), Decimal("0.000000001"))
        # With vardiff disabled the listener's fixed share difficulty applies.
        fixed_profile = StratumListenerProfile(
            name="highdiff",
            bind="0.0.0.0",
            port=4334,
            share_difficulty=Decimal("700000"),
            vardiff_config=highdiff_vardiff_config(enabled=False),
            heartbeat_name="stratum_accept_highdiff",
        )
        self.assertEqual(server.client_startup_difficulty(fixed_profile), Decimal("700000"))

    def test_stratum_accept_heartbeat_names(self) -> None:
        server = coordinator()
        # Coordinators built without listener profiles (tests, legacy) keep the
        # historical single heartbeat name.
        self.assertEqual(server.stratum_accept_heartbeat_names(), ("stratum_accept",))
        server.listener_profiles = [
            StratumListenerProfile(
                name="default",
                bind="0.0.0.0",
                port=3340,
                share_difficulty=Decimal("1"),
                vardiff_config=server.vardiff_config,
                heartbeat_name="stratum_accept",
            ),
            StratumListenerProfile(
                name="highdiff",
                bind="0.0.0.0",
                port=4334,
                share_difficulty=Decimal("500000"),
                vardiff_config=highdiff_vardiff_config(),
                heartbeat_name="stratum_accept_highdiff",
            ),
        ]
        self.assertEqual(
            server.stratum_accept_heartbeat_names(),
            ("stratum_accept", "stratum_accept_highdiff"),
        )

    def test_parse_stratum_password_options(self) -> None:
        self.assertEqual(parse_stratum_password_options(""), (None, None))
        self.assertEqual(parse_stratum_password_options("x"), (None, None))
        self.assertEqual(parse_stratum_password_options("d=8192"), (Decimal("8192"), None))
        self.assertEqual(
            parse_stratum_password_options("md=4096,d=8192"),
            (Decimal("8192"), Decimal("4096")),
        )
        self.assertEqual(
            parse_stratum_password_options("D=500000, MD=500000"),
            (Decimal("500000"), Decimal("500000")),
        )
        self.assertEqual(parse_stratum_password_options("d=abc"), (None, None))
        self.assertEqual(parse_stratum_password_options("d=-5,md=0"), (None, None))
        self.assertEqual(parse_stratum_password_options("foo=1,md=2048"), (None, Decimal("2048")))

    def test_password_d_below_highdiff_floor_is_clamped(self) -> None:
        server = coordinator()
        state = client()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.share_difficulty = Decimal("500000")
        state.requested_difficulty = Decimal("1000")

        target = server.apply_client_difficulty_requests(state)

        self.assertEqual(target, Decimal("500000"))
        assert state.vardiff_config is not None
        self.assertEqual(state.vardiff_config.min_difficulty, Decimal("500000"))
        self.assertEqual(state.vardiff_config.startup_difficulty, Decimal("500000"))

    def test_password_md_raises_personal_floor_and_retarget_respects_it(self) -> None:
        server = coordinator()
        state = client()
        state.requested_min_difficulty = Decimal("256")
        state.share_difficulty = Decimal("1")

        target = server.apply_client_difficulty_requests(state)

        self.assertEqual(target, Decimal("256"))
        assert state.vardiff_config is not None
        self.assertEqual(state.vardiff_config.min_difficulty, Decimal("256"))
        self.assertEqual(state.vardiff_config.max_difficulty, Decimal("1024"))

        # A zero-share retarget window wants to step down 4x; the personal
        # floor must hold it at 256.
        state.share_difficulty = Decimal("256")
        server.retarget_client(
            state,
            current_difficulty=Decimal("256"),
            accepted_shares=0,
            submitted_shares=0,
            accepted_difficulty=Decimal("0"),
            elapsed_seconds=Decimal("2"),
        )
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("256"))

    def test_apply_requests_is_stable_across_reapplication(self) -> None:
        server = coordinator()
        state = client()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.requested_min_difficulty = Decimal("600000")
        state.requested_difficulty = Decimal("700000")

        first = server.apply_client_difficulty_requests(state)
        second = server.apply_client_difficulty_requests(state)

        self.assertEqual(first, Decimal("700000"))
        self.assertEqual(second, Decimal("700000"))
        assert state.vardiff_config is not None
        # Recomputed from the pristine listener config: the floor is the md=
        # value, not a compounded one.
        self.assertEqual(state.vardiff_config.min_difficulty, Decimal("600000"))

    def test_suggest_difficulty_before_subscribe_applies_directly(self) -> None:
        server = coordinator()
        state = ClientState(sock=object(), address=("127.0.0.1", 1), connection_id=2, extranonce1_hex="00000002")
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 7, [512])

        self.assertEqual(state.suggested_difficulty, Decimal("512"))
        self.assertEqual(state.share_difficulty, Decimal("512"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(sent, [{"id": 7, "result": True, "error": None}])

    def test_suggest_difficulty_post_authorize_advertises_with_job(self) -> None:
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("1")
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        jobs: dict[str, object] = {"count": 0}

        def fake_send_job(client: object, clean_jobs: bool) -> bool:
            jobs.update({"count": jobs["count"] + 1, "clean": clean_jobs})
            return True

        server.maybe_send_job = fake_send_job  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 8, [512])

        self.assertEqual(state.pending_share_difficulty, Decimal("512"))
        self.assertEqual(jobs["count"], 1)
        self.assertTrue(jobs["clean"])
        self.assertEqual(sent, [{"id": 8, "result": True, "error": None}])

    def test_suggest_difficulty_rolls_back_pending_on_build_failure(self) -> None:
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("1")
        state.send = lambda payload: None  # type: ignore[method-assign]
        server.maybe_send_job = lambda client, *, clean_jobs: False  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 9, [512])

        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("1"))

    def test_suggest_difficulty_yields_to_password_d_option(self) -> None:
        server = coordinator()
        state = client()
        state.requested_difficulty = Decimal("512")
        state.share_difficulty = Decimal("512")
        state.send = lambda payload: None  # type: ignore[method-assign]
        server.maybe_send_job = lambda client, *, clean_jobs: True  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 10, [128])

        # d= wins: the suggestion is recorded but the resolved target stays at
        # the explicit password difficulty, so nothing is re-advertised.
        self.assertEqual(state.suggested_difficulty, Decimal("128"))
        self.assertEqual(state.share_difficulty, Decimal("512"))
        self.assertIsNone(state.pending_share_difficulty)

    def test_suggest_difficulty_ignores_junk_values(self) -> None:
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("1")
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        for junk in ([], ["nan"], ["-4"], ["0"], [None]):
            server.handle_suggest_difficulty(state, 11, junk)  # type: ignore[arg-type]

        self.assertIsNone(state.vardiff_config)
        self.assertEqual(state.share_difficulty, Decimal("1"))
        self.assertEqual(len(sent), 5)

    def test_highdiff_share_diff_tracks_start_and_validates_bounds(self) -> None:
        base = load_prism_vardiff_config(Decimal("0.000000001"))
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_MIN_DIFF"] = "1000000"
            os.environ["PRISM_STRATUM_HIGHDIFF_START_DIFF"] = "1000000"
            profile = load_prism_highdiff_listener("0.0.0.0", base)
        assert profile is not None
        # Unset fixed difficulty tracks the start difficulty instead of a
        # constant that could fall below a raised floor.
        self.assertEqual(profile.share_difficulty, Decimal("1000000"))

        # An explicit fixed difficulty outside the listener bounds must fail
        # startup: advertising below the floor is exactly what the high-diff
        # listener exists to prevent.
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = "1000"
            with self.assertRaises(SystemExit):
                load_prism_highdiff_listener("0.0.0.0", base)
        with patch.dict(os.environ, {}, clear=False):
            clear_stratum_diff_env()
            os.environ["PRISM_STRATUM_HIGHDIFF_PORT"] = "4334"
            os.environ["PRISM_STRATUM_HIGHDIFF_SHARE_DIFF"] = "8589934592"
            with self.assertRaises(SystemExit):
                load_prism_highdiff_listener("0.0.0.0", base)

    def authorize_server_and_client(self) -> tuple[PrismCoordinator, ClientState, list[object]]:
        server = coordinator()
        server.rpc = AddressValidationRpc()
        server.username_fallback_address = None
        server.maybe_send_job = lambda client, *, clean_jobs: True  # type: ignore[method-assign]
        state = ClientState(sock=object(), address=("127.0.0.1", 1), connection_id=3, extranonce1_hex="00000003")
        state.subscribed = True
        sent: list[object] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        return server, state, sent

    def test_authorize_password_applies_before_first_job(self) -> None:
        server, state, sent = self.authorize_server_and_client()

        server.handle_request(
            state,
            {"id": 5, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "d=0.5,md=0.25"]},
        )

        self.assertTrue(state.authorized)
        self.assertEqual(state.requested_difficulty, Decimal("0.5"))
        self.assertEqual(state.requested_min_difficulty, Decimal("0.25"))
        # Applied directly (no job exists yet), so the first
        # set_difficulty/notify pair advertises the requested value.
        self.assertEqual(state.share_difficulty, Decimal("0.5"))
        self.assertIsNone(state.pending_share_difficulty)
        assert state.vardiff_config is not None
        self.assertEqual(state.vardiff_config.min_difficulty, Decimal("0.25"))
        self.assertEqual(sent, [{"id": 5, "result": True, "error": None}])

    def test_reauthorize_with_plain_password_clears_stale_overrides(self) -> None:
        server, state, _ = self.authorize_server_and_client()
        server.handle_request(
            state,
            {"id": 5, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "d=0.5,md=0.25"]},
        )
        assert state.vardiff_config is not None

        server.handle_request(
            state,
            {"id": 6, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "x"]},
        )

        # The new password carries no options: prior overrides are dropped and
        # the client falls back to the pristine listener policy (its current
        # difficulty is left alone; vardiff drifts it under listener bounds).
        self.assertIsNone(state.requested_difficulty)
        self.assertIsNone(state.requested_min_difficulty)
        self.assertIsNone(state.vardiff_config)
        self.assertEqual(state.share_difficulty, Decimal("0.5"))

    def test_reauthorize_with_new_difficulty_sends_single_job_pair(self) -> None:
        server, state, _ = self.authorize_server_and_client()
        send_job_calls: list[bool] = []

        def counting_send_job(client: object, *, clean_jobs: bool) -> bool:
            send_job_calls.append(clean_jobs)
            return True

        server.maybe_send_job = counting_send_job  # type: ignore[method-assign]

        server.handle_request(
            state,
            {"id": 5, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "x"]},
        )
        self.assertEqual(len(send_job_calls), 1)

        # A re-authorize whose new d= advertises a fresh difficulty/job pair
        # must not be followed by a second back-to-back pair.
        server.handle_request(
            state,
            {"id": 6, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "d=0.5"]},
        )
        self.assertEqual(len(send_job_calls), 2)
        self.assertEqual(state.pending_share_difficulty, Decimal("0.5"))

    def test_authorize_rejects_and_disconnects_above_username_connection_limit(self) -> None:
        server, first, _ = self.authorize_server_and_client()
        server.stratum_max_connections_per_username = 1
        second = ClientState(
            sock=object(),
            address=("127.0.0.1", 2),
            connection_id=4,
            extranonce1_hex="00000004",
        )
        second.send = lambda payload: None  # type: ignore[method-assign]
        server.clients.update({first, second})

        server.handle_request(
            first,
            {"id": 5, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "x"]},
        )
        with self.assertRaises(StratumError) as raised:
            server.handle_request(
                second,
                {"id": 6, "method": "mining.authorize", "params": [PAYOUT_ADDRESS, "x"]},
            )

        self.assertTrue(raised.exception.disconnect)
        self.assertEqual(raised.exception.message, "too many connections for username")
        self.assertEqual(server.connection_limit_rejection_counts["username"], 1)
        self.assertFalse(second.authorized)

    def test_reauthorize_limit_error_preserves_live_session(self) -> None:
        server, live, _ = self.authorize_server_and_client()
        server.stratum_max_connections_per_username = 1
        occupant = ClientState(
            sock=object(),
            address=("127.0.0.1", 2),
            connection_id=4,
            extranonce1_hex="00000004",
        )
        occupant.send = lambda payload: None  # type: ignore[method-assign]
        server.clients.update({live, occupant})

        server.handle_request(
            live,
            {
                "id": 5,
                "method": "mining.authorize",
                "params": [f"{PAYOUT_ADDRESS}.original", "x"],
            },
        )
        server.handle_request(
            occupant,
            {
                "id": 6,
                "method": "mining.authorize",
                "params": [f"{PAYOUT_ADDRESS}.full", "x"],
            },
        )
        original_worker = live.worker

        with self.assertRaises(StratumError) as raised:
            server.handle_request(
                live,
                {
                    "id": 7,
                    "method": "mining.authorize",
                    "params": [f"{PAYOUT_ADDRESS}.full", "x"],
                },
            )

        self.assertFalse(raised.exception.disconnect)
        self.assertTrue(live.authorized)
        self.assertIs(live.worker, original_worker)
        self.assertEqual(live.username, f"{PAYOUT_ADDRESS}.original")

    def test_username_connection_limit_is_disabled_by_default(self) -> None:
        server = coordinator()
        first = client()
        second = client()
        server.clients.update({first, second})
        worker = WorkerIdentity(
            username=PAYOUT_ADDRESS,
            payout_address=PAYOUT_ADDRESS,
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )

        self.assertTrue(server.reserve_client_username(first, worker))
        self.assertTrue(server.reserve_client_username(second, worker))
        self.assertEqual(server.connection_limit_rejection_counts["username"], 0)

    def test_username_limit_does_not_count_idle_clients_for_empty_username(self) -> None:
        server = coordinator()
        server.stratum_max_connections_per_username = 1
        idle = client()
        first = client()
        second = client()
        server.clients.update({idle, first, second})
        worker = WorkerIdentity(
            username="",
            payout_address=PAYOUT_ADDRESS,
            worker_name=None,
            script_pubkey_hex="5220" + "11" * 32,
            p2mr_program_hex="11" * 32,
        )

        self.assertTrue(server.reserve_client_username(first, worker))
        self.assertFalse(server.reserve_client_username(second, worker))
        self.assertEqual(server.connection_limit_rejection_counts["username"], 1)

    def test_accept_loop_rejects_above_global_connection_limit(self) -> None:
        server = coordinator()
        server.stratum_max_connections = 1
        server.clients.add(client())

        class AcceptedSocket:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        accepted = AcceptedSocket()

        class OneConnectionListener:
            def __init__(self) -> None:
                self.calls = 0

            def accept(self) -> tuple[object, tuple[str, int]]:
                self.calls += 1
                if self.calls == 1:
                    return accepted, ("127.0.0.1", 1000)
                server.stop_event.set()
                raise socket.timeout()

        profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=3340,
            share_difficulty=server.share_difficulty,
            vardiff_config=server.vardiff_config,
            heartbeat_name="stratum_accept",
        )

        server.accept_loop(OneConnectionListener(), profile)  # type: ignore[arg-type]

        self.assertTrue(accepted.closed)
        self.assertEqual(server.connection_limit_rejection_counts["global"], 1)

    def test_accept_loop_recovers_from_descriptor_exhaustion(self) -> None:
        server = coordinator()
        server.stratum_accept_resource_exhaustion_backoff_seconds = 0

        class ExhaustedListener:
            def __init__(self) -> None:
                self.calls = 0

            def accept(self) -> tuple[object, tuple[str, int]]:
                self.calls += 1
                if self.calls == 1:
                    raise OSError(errno.EMFILE, "too many open files")
                server.stop_event.set()
                raise socket.timeout()

        listener = ExhaustedListener()
        profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=3340,
            share_difficulty=server.share_difficulty,
            vardiff_config=server.vardiff_config,
            heartbeat_name="stratum_accept",
        )

        server.accept_loop(listener, profile)  # type: ignore[arg-type]

        self.assertEqual(listener.calls, 2)
        self.assertEqual(server.accept_resource_exhaustion_count, 1)

    def test_resource_backoff_keeps_accept_watchdog_heartbeat_fresh(self) -> None:
        server = coordinator()
        server.stratum_accept_resource_exhaustion_backoff_seconds = 0.03
        server.watchdog_timeout_seconds = 0.01

        server._wait_after_stratum_resource_failure("stratum_accept")

        self.assertFalse(server._overdue_heartbeats(time.monotonic()))

    def test_accept_loop_recovers_when_handler_thread_cannot_start(self) -> None:
        server = coordinator()
        server.connection_counter = 0
        server.stratum_send_timeout_seconds = 0
        server.stratum_accept_resource_exhaustion_backoff_seconds = 0

        class AcceptedSocket:
            def __init__(self) -> None:
                self.closed = False

            def settimeout(self, timeout: object) -> None:
                pass

            def shutdown(self, how: int) -> None:
                pass

            def close(self) -> None:
                self.closed = True

        accepted = AcceptedSocket()

        class OneConnectionListener:
            def __init__(self) -> None:
                self.calls = 0

            def accept(self) -> tuple[object, tuple[str, int]]:
                self.calls += 1
                if self.calls == 1:
                    return accepted, ("127.0.0.1", 1000)
                server.stop_event.set()
                raise socket.timeout()

        listener = OneConnectionListener()
        profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=3340,
            share_difficulty=server.share_difficulty,
            vardiff_config=server.vardiff_config,
            heartbeat_name="stratum_accept",
        )

        with patch.object(threading.Thread, "start", side_effect=RuntimeError("can't start new thread")):
            server.accept_loop(listener, profile)  # type: ignore[arg-type]

        self.assertEqual(listener.calls, 2)
        self.assertTrue(accepted.closed)
        self.assertFalse(server.clients)
        self.assertEqual(server.connection_setup_failure_count, 1)

    def test_handle_client_cleans_up_when_makefile_hits_descriptor_limit(self) -> None:
        server = coordinator()

        class MakefileFailureSocket:
            def __init__(self) -> None:
                self.closed = False

            def makefile(self, *args: object, **kwargs: object) -> object:
                raise OSError(errno.EMFILE, "too many open files")

            def shutdown(self, how: int) -> None:
                pass

            def close(self) -> None:
                self.closed = True

        sock = MakefileFailureSocket()
        state = ClientState(
            sock=sock,  # type: ignore[arg-type]
            address=("127.0.0.1", 1000),
            connection_id=1,
            extranonce1_hex="00000001",
        )
        server.clients.add(state)

        server.handle_client(state)

        self.assertTrue(sock.closed)
        self.assertNotIn(state, server.clients)
        self.assertEqual(server.accept_resource_exhaustion_count, 1)

    def test_accept_loop_assigns_listener_profiles_and_unique_extranonce(self) -> None:
        server = coordinator()
        server.connection_counter = 0
        server.stratum_send_timeout_seconds = 0.0

        def listening_socket() -> socket.socket:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.settimeout(0.1)
            return listener

        default_listener = listening_socket()
        highdiff_listener = listening_socket()
        default_profile = StratumListenerProfile(
            name="default",
            bind="127.0.0.1",
            port=default_listener.getsockname()[1],
            share_difficulty=server.share_difficulty,
            vardiff_config=server.vardiff_config,
            heartbeat_name="stratum_accept",
        )
        highdiff_profile = StratumListenerProfile(
            name="highdiff",
            bind="127.0.0.1",
            port=highdiff_listener.getsockname()[1],
            share_difficulty=Decimal("500000"),
            vardiff_config=highdiff_vardiff_config(),
            heartbeat_name="stratum_accept_highdiff",
            minimum_advertised_difficulty=Decimal("500000"),
        )
        threads = [
            threading.Thread(target=server.accept_loop, args=(default_listener, default_profile), daemon=True),
            threading.Thread(target=server.accept_loop, args=(highdiff_listener, highdiff_profile), daemon=True),
        ]
        for thread in threads:
            thread.start()
        connections = []
        try:
            connections.append(socket.create_connection(("127.0.0.1", default_profile.port), timeout=5))
            connections.append(socket.create_connection(("127.0.0.1", highdiff_profile.port), timeout=5))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with server.lock:
                    if len(server.clients) == 2:
                        break
                time.sleep(0.01)
            with server.lock:
                clients_by_listener = {c.listener_name: c for c in server.clients}
            self.assertEqual(set(clients_by_listener), {"default", "highdiff"})
            self.assertEqual(
                clients_by_listener["default"].share_difficulty,
                Decimal("0.000000001"),
            )
            self.assertEqual(
                clients_by_listener["highdiff"].share_difficulty,
                Decimal("500000"),
            )
            self.assertIs(
                clients_by_listener["highdiff"].listener_vardiff_config,
                highdiff_profile.vardiff_config,
            )
            self.assertEqual(
                clients_by_listener["highdiff"].minimum_advertised_difficulty,
                Decimal("500000"),
            )
            self.assertEqual(
                clients_by_listener["default"].minimum_advertised_difficulty,
                Decimal("0"),
            )
            extranonces = {c.extranonce1_hex for c in clients_by_listener.values()}
            self.assertEqual(extranonces, {"00000001", "00000002"})
            self.assertIn("stratum_accept", server._heartbeats)
            self.assertIn("stratum_accept_highdiff", server._heartbeats)
        finally:
            server.stop_event.set()
            for connection in connections:
                connection.close()
            default_listener.close()
            highdiff_listener.close()
            for thread in threads:
                thread.join(timeout=5)


class PrismStampedJobFloorTests(unittest.TestCase):
    """The listener floor must hold on the wire, not just in vardiff policy.

    Stamped jobs are the single choke point for every mining.set_difficulty
    the coordinator sends, and marketplace verification judges the first one.
    The regression here is a young chain: qbit network difficulty below the
    high-diff floor used to drag the advertised difficulty down with it.
    """

    def stamp_coordinator(self) -> PrismCoordinator:
        server = coordinator()
        server.job_counter = 0
        server.share_weights_by_username = {}
        server.default_share_weight = 1
        return server

    def cached_bundle(self) -> CachedJobBundle:
        # bits 207fffff: regtest-grade network difficulty (~4.7e-10), far
        # below the 500k marketplace floor.
        qbit_target = target_from_compact("207fffff")
        base_job = direct_stratum.DirectQbitStratumJob(
            job_id="prism-template-base",
            previousblockhash_display="00" * 32,
            prevhash="00" * 32,
            coinb1="",
            coinb2="",
            full_coinbase_prefix="",
            full_coinbase_suffix="",
            merkle_branch=(),
            transaction_hexes=(),
            version="20000000",
            nbits="207fffff",
            ntime="6553f100",
            qbit_target=qbit_target,
            share_target=qbit_target,
            share_difficulty=Decimal("1"),
            extranonce1_hex="ffffffff",
            extranonce2_size=8,
            clean_jobs=True,
        )
        return CachedJobBundle(
            key=("test",),
            template=gbt_template("00" * 32),
            template_fingerprint="fp",
            bundle={},
            shares_json=[],
            prior_balances=[],
            found_block={"network_difficulty": 1},
            collection_only=False,
            issued_at_ms=12345,
            base_job=base_job,
            built_monotonic=time.monotonic(),
        )

    def highdiff_client(self) -> ClientState:
        state = client()
        state.worker = worker_identity()
        state.listener_vardiff_config = highdiff_vardiff_config()
        state.minimum_advertised_difficulty = Decimal("500000")
        state.share_difficulty = Decimal("500000")
        return state

    def test_stamped_job_enforces_floor_below_network_difficulty(self) -> None:
        server = self.stamp_coordinator()
        state = self.highdiff_client()

        context = server.stamp_job_for_client(state, self.cached_bundle(), clean_jobs=True)

        self.assertEqual(
            context.job.share_target,
            direct_stratum.difficulty_target(Decimal("500000")),
        )
        # Decimal round-tripping can land within 1e-27 of the floor; the wire
        # value is float(difficulty), which is what marketplaces judge.
        self.assertGreaterEqual(float(context.job.share_difficulty), 500000.0)

    def test_stamped_job_keeps_network_cap_without_listener_floor(self) -> None:
        server = self.stamp_coordinator()
        state = client()
        state.worker = worker_identity()
        # Even an absurd desired difficulty stays capped at the network
        # target on the default listener: shares are never required to be
        # harder than blocks there.
        state.share_difficulty = Decimal("500000")

        context = server.stamp_job_for_client(state, self.cached_bundle(), clean_jobs=True)

        self.assertEqual(context.job.share_target, target_from_compact("207fffff"))
        self.assertLess(context.job.share_difficulty, Decimal("1"))

    def test_stamped_job_honors_md_raised_floor_on_highdiff_listener(self) -> None:
        server = self.stamp_coordinator()
        state = self.highdiff_client()
        state.requested_min_difficulty = Decimal("2000000")
        server.apply_client_difficulty_requests(state)

        context = server.stamp_job_for_client(state, self.cached_bundle(), clean_jobs=True)

        self.assertEqual(
            context.job.share_target,
            direct_stratum.difficulty_target(Decimal("2000000")),
        )
        self.assertGreaterEqual(float(context.job.share_difficulty), 2000000.0)

    def test_block_worthy_submission_below_share_target_submits_synchronously(self) -> None:
        # With the floor above network difficulty a hash can solve a block
        # while missing the advertised share target. It is a valid share only
        # if the block lands, so it submits synchronously (not via the async
        # queue) and the share credit lands with it.
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=False,
            block_pass=True,
        )
        submitted: list[object] = []

        def fake_submit(candidate: object) -> bool:
            submitted.append(candidate)
            server.append_accepted_share(
                candidate.client, candidate.context, candidate.submission, candidate.pending_share
            )
            return True

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
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 0)
        self.assertEqual(len(submitted), 1)
        self.assertTrue(submitted[0].credit_share_on_accept)
        # Nothing was queued to the async submitter; it landed inline.
        self.assertEqual(server.block_candidate_queue.qsize(), 0)
        self.assertEqual(len(ledger.pending), 1)

    def test_below_target_block_intent_is_durable_before_synchronous_submit(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="bc" * 32,
            share_pass=False,
            block_pass=True,
        )

        def fake_submit(candidate: PrismBlockCandidate) -> bool:
            pending = ledger.pending_block_candidates()
            self.assertEqual(len(pending), 1)
            self.assertTrue(pending[0]["credit_share_on_accept"])
            server.append_accepted_share(
                candidate.client,
                candidate.context,
                candidate.submission,
                candidate.pending_share,
                candidate_intent=server.block_candidate_intent(candidate),
            )
            return True

        server.submit_block_candidate = fake_submit  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )

        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_block_worthy_below_target_rejects_low_difficulty_when_block_fails(self) -> None:
        # If the block does not land, the below-share-target hash earns nothing
        # and the miner is rejected as low-difficulty -- never acked accepted
        # with no ledger row.
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=False,
            block_pass=True,
        )
        def reject_candidate(_candidate: PrismBlockCandidate) -> bool:
            server._abandon_block_candidate(
                PRISM_REJECTION_SUBMITBLOCK_REJECTED,
                "rejected",
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = reject_candidate  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_LOW_DIFFICULTY)
        self.assertEqual(len(ledger.pending), 0)
        self.assertEqual(server.block_candidate_queue.qsize(), 0)
        # The reject is counted (globally and for the worker), not just the
        # block-abandonment reason -- this synchronous path used to skip it.
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 1)
        self.assertEqual(server.low_difficulty_share_count, 1)

    def test_below_target_transient_outcome_closes_without_definitive_reject(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="bd" * 32,
            share_pass=False,
            block_pass=True,
        )
        server.submit_block_candidate = lambda _candidate: False  # type: ignore[method-assign]

        submit_params = ["miner-a", "job-1", "00" * 8, "00000001", "00000002"]
        # Each retry rebuilds its candidate intent with a fresh acknowledgment
        # stamp. Force every call onto a new millisecond so the durable-outbox
        # idempotency is exercised across acknowledgment-stamp drift instead of
        # depending on both attempts landing within the same millisecond.
        clock_ms = iter(range(1_700_000_000_000, 1_700_000_070_000, 7))
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ), patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=clock_ms.__next__,
        ):
            for _attempt in range(2):
                with self.assertRaisesRegex(RuntimeError, "pending durable retry"):
                    server.handle_submit(state, submit_params)
                self.assertEqual(server.recent_share_keys, set())

        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 0)
        self.assertEqual(server.duplicate_share_count, 0)
        self.assertEqual(len(ledger), 0)
        self.assertEqual(len(ledger.pending_block_candidates()), 1)

    def test_post_block_refresh_stamps_block_submitter_heartbeat(self) -> None:
        # The post-block job push runs on the block-submitter thread, so it must
        # stamp block_submitter (not the poller heartbeat) through the refresh,
        # or a long multi-client push trips a false liveness-watchdog exit.
        server, _state, _ledger = submit_coordinator()
        seen: list[str] = []

        def fake_poll(*, heartbeat_name: str = "qbit_blockpoll") -> int:
            seen.append(heartbeat_name)
            return 0

        server.poll_qbit_tip_template_once = fake_poll  # type: ignore[method-assign]
        server.refresh_jobs_after_accepted_block(
            block_height=10, block_hash="bb" * 32, heartbeat_name="block_submitter"
        )
        self.assertEqual(seen, ["block_submitter"])

        # The client-thread pending refresh keeps the default poller heartbeat.
        seen.clear()
        server.refresh_jobs_after_accepted_block(block_height=11, block_hash="cc" * 32)
        self.assertEqual(seen, ["qbit_blockpoll"])

    def test_low_difficulty_submission_without_block_solve_is_rejected(self) -> None:
        server, state, ledger = submit_coordinator()
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=False,
            block_pass=False,
        )
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.code, 23)
        self.assertEqual(raised.exception.reason, PRISM_REJECTION_LOW_DIFFICULTY)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 1)
        self.assertEqual(len(ledger.pending), 0)

    def test_collection_only_below_target_block_solve_submits_solver_pays_all(self) -> None:
        # A collection job's signed bootstrap manifest already commits the
        # whole coinbase to the submitting worker, so a solved block on a
        # collection job is submitted (synchronously here, since the share
        # missed its target) instead of being withheld -- the first block on a
        # fresh ledger must never be silently ledgered away.
        server, state, ledger = submit_coordinator()
        server.jobs["job-1"].collection_only = True
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="bb" * 32,
            share_pass=False,
            block_pass=True,
        )
        submitted: list[object] = []

        def fake_submit(candidate: object) -> bool:
            submitted.append(candidate)
            server.append_accepted_share(
                candidate.client, candidate.context, candidate.submission, candidate.pending_share
            )
            return True

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
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 0)
        self.assertEqual(len(submitted), 1)
        self.assertTrue(submitted[0].credit_share_on_accept)
        self.assertTrue(submitted[0].context.collection_only)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(server.collection_block_submission_count, 1)

    def test_collection_job_block_solve_is_credited_and_enqueued_not_withheld(self) -> None:
        # A solved block that also met its share target on a collection job is
        # credited immediately and queued for the submitter thread, exactly
        # like a ready-window candidate.
        server, state, ledger = submit_coordinator()
        server.jobs["job-1"].collection_only = True
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            share_pass=True,
            block_pass=True,
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
        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        queued = server.block_candidate_queue.get_nowait()
        self.assertTrue(queued.context.collection_only)
        self.assertFalse(queued.credit_share_on_accept)
        self.assertEqual(server.collection_block_submission_count, 1)

    def test_block_candidate_intent_round_trips_collection_flag(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.jobs["job-1"].collection_only = True
        pending = PendingShare(
            share_id="miner-a:" + "dd" * 32,
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=2,
            ntime=1,
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="00",
            block_hash_hex="dd" * 32,
            block_hex="00",
            share_pass=True,
            block_pass=True,
        )
        candidate = block_candidate(server, state, submission, pending_share=pending)

        intent = server.block_candidate_intent(candidate)
        self.assertIs(intent["collection_only"], True)
        self.assertTrue(server.block_candidate_from_intent(intent).context.collection_only)

        # Intents persisted before the flag existed replay as ready-window
        # candidates, which is all the outbox could ever have contained then.
        intent.pop("collection_only")
        self.assertFalse(server.block_candidate_from_intent(intent).context.collection_only)

    def test_ready_pool_refreshes_clients_left_on_collection_jobs(self) -> None:
        # Once the pool crosses min_ready_miners, the poller must replace
        # collection jobs with windowed work even when the template snapshot
        # is otherwise unchanged -- readiness itself is invisible to the
        # template fingerprint.
        server, state, _ledger = submit_coordinator()
        tip = "00" * 32
        snapshot = SimpleNamespace(
            bestblockhash=tip,
            previousblockhash=tip,
            template_fingerprint="fp",
        )
        context = SimpleNamespace(
            template={"previousblockhash": tip},
            template_fingerprint="fp",
            collection_only=True,
        )
        state.active_job = context

        server.min_ready_miners = 1
        server.accepted_share_stats = lambda: (0, 0)  # type: ignore[method-assign]
        self.assertFalse(server.pool_readiness_latched())
        self.assertFalse(server.client_needs_tip_template_refresh(state, snapshot))

        server.accepted_share_stats = lambda: (1, 1)  # type: ignore[method-assign]
        self.assertTrue(server.pool_readiness_latched())
        self.assertTrue(server.client_needs_tip_template_refresh(state, snapshot))

        # Readiness is monotonic: once latched the ledger is never consulted
        # again, and ready (non-collection) jobs still need no refresh.
        server.accepted_share_stats = None  # type: ignore[assignment]
        self.assertTrue(server.pool_readiness_latched())
        context.collection_only = False
        self.assertFalse(server.client_needs_tip_template_refresh(state, snapshot))


if __name__ == "__main__":
    unittest.main()
