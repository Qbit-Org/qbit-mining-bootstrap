#!/usr/bin/env python3
# ruff: noqa: F401
"""Shared no-environment fixtures for coordinator domain tests."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import queue
import socket
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from dataclasses import replace as dataclass_replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.share_ledger import PendingShare, SingleWriterShareLedger
from lab.prism.prism_coordinator import (
    CachedJobBundle,
    CachedTemplateArtifacts,
    ClientState,
    DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS,
    MAX_ACTIVE_PRISM_JOBS_PER_CLIENT,
    MAX_PENDING_SHARE_APPENDS,
    PRISM_CREDIT_POLICY_STALE_GRACE,
    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
    PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
    PRISM_REJECTION_DUPLICATE_SHARE,
    PRISM_REJECTION_INVALID_NTIME_OR_NONCE,
    PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
    PRISM_REJECTION_LOW_DIFFICULTY,
    PendingShareAppend,
    PayoutStateArtifact,
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
    JobBuildCancelled,
    JobBuildSuperseded,
    TemplateRefreshBlocked,
    TemplateRefreshSuperseded,
    PrismCoordinator,
    WorkerIdentity,
    _FanoutCancellation,
    _ObservedRLock,
    _PayoutStatePublicationBlocked,
    _JobBuildCancellation,
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


def fake_audit_bundle_popen(
    captured: dict[str, object],
    *,
    output_text: str = '{"ok":true}',
    returncode: int = 0,
    stderr_text: str = "",
) -> type:
    class FakeStdin:
        def __init__(self) -> None:
            self.parts: list[str] = []

        def write(self, value: str) -> int:
            self.parts.append(value)
            return len(value)

        def close(self) -> None:
            return None

    class FakePopen:
        def __init__(self, cmd: list[str], **kwargs: object) -> None:
            captured["cmd"] = cmd
            self.stdin = FakeStdin()
            self.stdout = kwargs["stdout"]
            self.stderr = kwargs["stderr"]

        def wait(self, timeout: float | None = None) -> int:
            captured["timeout"] = timeout
            captured["payload"] = json.loads("".join(self.stdin.parts))
            self.stdout.write(output_text)
            self.stderr.write(stderr_text)
            return returncode

    return FakePopen


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


def prepare_idle_client(
    server: PrismCoordinator,
    state: ClientState,
    *,
    connection_id: int = 1,
    difficulty: Decimal = Decimal("16"),
    tip: str = "00" * 32,
) -> None:
    if not hasattr(server, "jobs"):
        server.jobs = {}
    state.connection_id = connection_id
    state.extranonce1_hex = f"{connection_id:08x}"
    state.username = f"miner-{connection_id}"
    state.worker = worker_identity(state.username)
    job_id = f"job-{connection_id}"
    state.active_job = prism_context(
        job_id,
        tip,
        worker=state.worker,
        difficulty=difficulty,
    )
    state.active_job_ids = {job_id}
    state.share_difficulty = difficulty
    state.pending_share_difficulty = None
    state.vardiff_window_started_monotonic = time.monotonic() - 2
    state.vardiff_window_accepted = 0
    state.vardiff_window_submitted = 0
    state.vardiff_window_work = Decimal("0")
    server.clients.add(state)
    server.jobs[job_id] = state.active_job


def install_idle_job_cache(
    server: PrismCoordinator,
    *,
    tip: str = "00" * 32,
) -> CachedJobBundle:
    server._pool_ready_latched = True
    server.job_bundle_cache_seconds = 60.0
    server.job_counter = 0
    server.share_weights_by_username = {}
    server.default_share_weight = 1
    server._ensure_job_cache_state()
    template = gbt_template(tip)
    fingerprint = qbit_template_fingerprint(template)
    artifacts = CachedTemplateArtifacts(
        template=template,
        fingerprint=fingerprint,
        previousblockhash=tip,
        transaction_hexes=(),
        witness_merkle_leaves_hex=(),
        network_difficulty=1,
        fetched_monotonic=time.monotonic(),
        generation=1,
    )
    key = server._job_bundle_key(
        artifacts,
        mode="ready",
        payout_state_generation=server._payout_state_generation,
        payout_artifact_generation=0,
        worker=None,
    )
    payout_artifact_sha256 = "aa" * 32
    qbit_target = direct_stratum.difficulty_target(Decimal("1024"))
    base_job = direct_stratum.DirectQbitStratumJob(
        job_id="prism-template-base",
        previousblockhash_display=tip,
        prevhash=tip,
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
    bundle = CachedJobBundle(
        key=key,
        template=template,
        template_fingerprint=fingerprint,
        coinbase_manifest={},
        shares_json=[],
        prior_balances=[],
        found_block={"network_difficulty": 1},
        collection_only=False,
        issued_at_ms=12345,
        base_job=base_job,
        built_monotonic=time.monotonic(),
        template_generation=1,
        payout_state_generation=0,
        build_key=SimpleNamespace(
            payout_artifact_sha256=payout_artifact_sha256,
        ),
    )
    with server._job_cache_lock:
        server._template_artifacts = artifacts
        server._published_payout_state = dataclass_replace(
            server._published_payout_state,
            artifact=PayoutStateArtifact(
                generation=server._payout_state_generation,
                source_generation=0,
                prior_balances_json="[]",
                prior_balances_sha256=payout_artifact_sha256,
                prepared_monotonic=time.monotonic(),
            ),
        )
        server._job_bundle_cache.clear()
        server._job_bundle_cache[bundle.key] = bundle
    return bundle


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
    server.latest_coinbase_size_bytes = 250
    server.rpc = FakeRpc()
    server.qbit_chain = "regtest"
    server.blockpoll_seconds = 2.0
    # Failed-refresh spacing is opt-in per test: its holdoff waits on real
    # time, which deadlocks tests that freeze time.monotonic around failing
    # polls. Pacing behavior is covered by test_prism_refresh_retry_pacing.
    server.tip_refresh_failure_holdoff_seconds = 0.0
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


def build_vardiff_test_coordinator() -> PrismCoordinator:
    """Build a coordinator fixture without production init or worker startup."""
    return coordinator()


__all__ = [name for name in globals() if not name.startswith("__")]
