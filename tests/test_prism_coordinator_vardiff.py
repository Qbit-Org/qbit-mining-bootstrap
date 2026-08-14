#!/usr/bin/env python3

from __future__ import annotations

import errno
import hashlib
import inspect
import json
import os
import queue
import socket
import subprocess
import tempfile
import contextlib
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
from lab.prism.share_ledger import (
    LedgerOperationTimeout,
    PendingShare,
    PsqlShareLedger,
    SingleWriterShareLedger,
    WRITER_LEASE_HEARTBEAT_SESSION_PREFIX,
)
from lab.prism.prism_coordinator import (
    CachedJobBundle,
    CachedTemplateArtifacts,
    ClientState,
    DEFAULT_TESTNET_USERNAME_FALLBACK_ADDRESS,
    MAX_ACTIVE_PRISM_JOBS_PER_CLIENT,
    MAX_PENDING_SHARE_APPENDS,
    PRISM_CREDIT_POLICY_STALE_GRACE,
    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
    PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
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
    ShutdownInProgress,
    WorkerIdentity,
    WriterLeaseRenewalDeferred,
    _FanoutCancellation,
    _ObservedRLock,
    _PayoutStatePublicationBlocked,
    _JobBuildCancellation,
    _ReconcileFlight,
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


class RejectingSubmitTipRpc(TipRpc):
    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "submitblock":
            return "bad-prevblk"
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
            payout_append_invalidation_epoch=(
                server._payout_ledger_append_invalidation_epoch
            ),
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
    # This suite runs on a synthetic millisecond timeline (declared anchors
    # like 12000, share stamps of 1-2) while _expose_inflight_scan_anchor
    # prunes exposures older than a real-wall-clock ceiling. Every synthetic
    # anchor is decades past that ceiling, so the next exposure (a landing
    # publishing its declared anchor) silently pruned a test's standing
    # anchor and turned epoch-bump assertions into thread races against the
    # landing's own anchor retirement. Pin the ceiling out of reach so
    # exposed anchors live exactly as long as each test intends.
    server.payout_artifact_max_anchor_age_seconds = float("inf")
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
        or SimpleNamespace(
            share_id="miner-a:" + submission.block_hash_hex,
            job_issued_at_ms=0,
            accepted_at_ms=0,
        ),
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

    def test_vardiff_step_up_retarget_sends_new_difficulty_without_flush(self) -> None:
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

        # Difficulty is advertised by maybe_send_job alongside the job (gated on
        # a successful build). A step-up must not flush the miner's in-flight
        # work: old jobs validate at their own stamped share_target, so the
        # retarget requests a single non-clean job.
        self.assertEqual(state.pending_share_difficulty, Decimal("4"))
        self.assertEqual(sent["jobs"], 1)
        self.assertFalse(sent["clean"])

    def test_vardiff_step_down_retarget_flushes_with_clean_job(self) -> None:
        # Firmware that applies mining.set_difficulty retroactively would
        # submit sub-target shares against the old job after a step-down, so
        # the step-down keeps flushing in-flight work.
        server = coordinator()
        state = client()
        state.share_difficulty = Decimal("16")
        state.vardiff_window_started_monotonic = time.monotonic() - 65
        sent: dict[str, object] = {"jobs": 0}

        def fake_send_job(client: object, clean_jobs: bool) -> bool:
            sent.update({"jobs": sent["jobs"] + 1, "clean": clean_jobs})
            return True

        server.maybe_send_job = fake_send_job  # type: ignore[method-assign]

        server.note_vardiff_submitted_share(state)
        server.note_vardiff_accepted_share(state, FakeJob(Decimal("16")))  # type: ignore[arg-type]

        self.assertEqual(state.pending_share_difficulty, Decimal("4"))
        self.assertEqual(sent["jobs"], 1)
        self.assertTrue(sent["clean"])

    def test_step_up_retarget_keeps_old_job_submittable_at_old_difficulty(self) -> None:
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
        state.vardiff_window_started_monotonic = time.monotonic() - 2
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        server.clients = {state}
        server.rpc = TipTemplateRpc(tip=tip, template=gbt_template(tip))

        old_context = prism_context("old-job", tip, worker=worker, difficulty=Decimal("1"))
        state.active_job = old_context
        state.active_job_ids = {"old-job"}
        server.jobs["old-job"] = old_context

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            return prism_context(
                "fresh-job",
                tip,
                worker=worker,
                difficulty=client.pending_share_difficulty or client.share_difficulty,
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]

        server.note_vardiff_submitted_share(state)
        server.note_vardiff_accepted_share(state, FakeJob(Decimal("1")))  # type: ignore[arg-type]

        # The 1 -> 4 step-up pairs the new difficulty with a non-clean job and
        # keeps the old job registered and submittable.
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(sent[0]["params"], [4.0])
        self.assertEqual(sent[1]["params"][0], "fresh-job")
        self.assertFalse(sent[1]["params"][8])
        self.assertIn("old-job", server.jobs)
        self.assertIn("fresh-job", server.jobs)
        self.assertEqual(state.active_job_ids, {"old-job", "fresh-job"})

        # In-flight work against the old job still lands, validated at the old
        # job's own stamped difficulty: this share passes the diff-1 target it
        # was mined against, not the diff-4 target advertised afterward.
        def assemble_at_stamped_difficulty(job: object, **_kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(
                header_hex="aa" * 80,
                block_hash_hex="bb" * 32,
                share_pass=job.share_difficulty <= Decimal("1"),
                block_pass=False,
            )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            side_effect=assemble_at_stamped_difficulty,
        ):
            should_close = server.handle_submit(
                state,
                ["miner-a", "old-job", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertEqual(ledger.pending[0].job_id, "old-job")
        self.assertEqual(server.low_difficulty_share_count, 0)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_LOW_DIFFICULTY], 0)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_UNKNOWN_JOB], 0)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB], 0)

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

    def test_idle_vardiff_success_sends_paired_job_and_resets_window(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        sent: list[dict[str, object]] = []
        delivered = threading.Event()
        window_started = state.vardiff_window_started_monotonic

        def record(payload: dict[str, object]) -> None:
            sent.append(payload)
            if payload.get("method") == "mining.notify":
                delivered.set()

        state.send = record  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertTrue(delivered.wait(timeout=1))
        server.shutdown_vardiff_idle_executor()

        self.assertEqual(server.idle_retarget_count, 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(sent[0]["params"], [4.0])
        self.assertTrue(sent[1]["params"][8])
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertGreater(state.vardiff_window_started_monotonic, window_started)
        self.assertEqual(state.vardiff_window_accepted, 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(state.vardiff_window_work, Decimal("0"))

    def test_idle_vardiff_shutdown_after_delivery_keeps_committed_window(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        sent: list[dict[str, object]] = []
        delivered = threading.Event()
        window_started = state.vardiff_window_started_monotonic

        def stop_after_delivery(payload: dict[str, object]) -> None:
            sent.append(payload)
            if payload.get("method") == "mining.notify":
                server.stop_event.set()
                delivered.set()

        state.send = stop_after_delivery  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertTrue(delivered.wait(timeout=1))
        server.shutdown_vardiff_idle_executor()

        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(server.idle_retarget_count, 1)
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertGreater(state.vardiff_window_started_monotonic, window_started)
        self.assertEqual(state.vardiff_window_accepted, 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(state.vardiff_window_work, Decimal("0"))

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

    def test_idle_vardiff_share_after_snapshot_is_not_stepped_down(self) -> None:
        server = coordinator()
        blockers = [client(), client()]
        for connection_id, blocker in enumerate(blockers, start=1):
            prepare_idle_client(server, blocker, connection_id=connection_id)
        install_idle_job_cache(server)
        workers_started = threading.Barrier(3)
        release_workers = threading.Event()

        def block_delivery(payload: dict[str, object]) -> None:
            if payload.get("method") == "mining.set_difficulty":
                workers_started.wait(timeout=1)
                release_workers.wait(timeout=1)

        for blocker in blockers:
            blocker.send = block_delivery  # type: ignore[method-assign]

        target = client()
        target.connection_id = 3
        sent: list[dict[str, object]] = []
        target.send = sent.append  # type: ignore[method-assign]
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 2)
            workers_started.wait(timeout=1)
            prepare_idle_client(server, target, connection_id=3)
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)

            # The task is queued from an idle snapshot. A later submit changes
            # that exact window before any worker can commit the step-down.
            server.note_vardiff_submitted_share(target)
        finally:
            release_workers.set()
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(sent, [])
        self.assertIsNone(target.pending_share_difficulty)
        self.assertEqual(target.share_difficulty, Decimal("16"))
        self.assertEqual(target.vardiff_window_submitted, 1)
        self.assertGreaterEqual(server.vardiff_idle_skip_counts["not_idle"], 1)

    def test_idle_vardiff_failure_restores_pending_and_idle_window(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        window_state = (
            state.vardiff_window_started_monotonic,
            state.vardiff_window_accepted,
            state.vardiff_window_submitted,
            state.vardiff_window_work,
        )
        disconnected: list[ClientState] = []
        failure_finished = threading.Event()

        def failing_send(_payload: dict[str, object]) -> None:
            self.assertEqual(state.pending_share_difficulty, Decimal("4"))
            raise OSError("socket send failed")

        def fake_disconnect(client: ClientState) -> None:
            disconnected.append(client)
            server.clients.discard(client)
            failure_finished.set()

        state.send = failing_send  # type: ignore[method-assign]
        server.disconnect_client = fake_disconnect  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertTrue(failure_finished.wait(timeout=1))
        server.shutdown_vardiff_idle_executor()

        self.assertEqual(disconnected, [state])
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("16"))
        self.assertEqual(
            (
                state.vardiff_window_started_monotonic,
                state.vardiff_window_accepted,
                state.vardiff_window_submitted,
                state.vardiff_window_work,
            ),
            window_state,
        )
        self.assertEqual(server.vardiff_idle_task_failures, 1)

    def test_idle_vardiff_stamp_failure_restores_speculative_state(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        window_state = (
            state.vardiff_window_started_monotonic,
            state.vardiff_window_accepted,
            state.vardiff_window_submitted,
            state.vardiff_window_work,
        )

        def fail_stamp(*_args: object, **_kwargs: object) -> None:
            self.assertEqual(state.pending_share_difficulty, Decimal("4"))
            raise RuntimeError("cached job stamping failed")

        state.send = lambda payload: self.fail(  # type: ignore[method-assign]
            f"unexpected delivery: {payload}"
        )
        server.stamp_job_for_client = fail_stamp  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        server.shutdown_vardiff_idle_executor()

        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("16"))
        self.assertEqual(
            (
                state.vardiff_window_started_monotonic,
                state.vardiff_window_accepted,
                state.vardiff_window_submitted,
                state.vardiff_window_work,
            ),
            window_state,
        )
        self.assertEqual(server.job_build_failure_count, 1)
        self.assertEqual(server.vardiff_idle_task_failures, 1)

    def test_idle_cached_bundle_requires_live_reorg_trust(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        trust_checked = threading.Event()
        sent: list[dict[str, object]] = []
        window_started = state.vardiff_window_started_monotonic

        def reject_untrusted_tip() -> bool:
            trust_checked.set()
            return False

        state.send = sent.append  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            reject_untrusted_tip
        )

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        server.shutdown_vardiff_idle_executor()

        self.assertTrue(trust_checked.is_set())
        self.assertEqual(sent, [])
        self.assertEqual(server.idle_retarget_count, 0)
        self.assertEqual(state.share_difficulty, Decimal("16"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.vardiff_window_started_monotonic, window_started)
        self.assertGreaterEqual(server.vardiff_idle_skip_counts["superseded"], 1)

    def test_idle_retarget_defers_detected_payout_source_during_tip_divergence(
        self,
    ) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        state = client()
        prepare_idle_client(server, state, tip=old_tip)
        install_idle_job_cache(server, tip=old_tip)
        with server._job_cache_lock:
            published_artifacts = server._template_artifacts
        assert published_artifacts is not None
        detected_template = gbt_template(new_tip, height=11)
        detected_artifacts = CachedTemplateArtifacts(
            template=detected_template,
            fingerprint=qbit_template_fingerprint(detected_template),
            previousblockhash=new_tip,
            transaction_hexes=(),
            witness_merkle_leaves_hex=(),
            network_difficulty=1,
            fetched_monotonic=time.monotonic(),
            generation=2,
        )
        now = time.monotonic()
        server.current_tip_first_seen = (old_tip, now)
        server.current_tip_observed_monotonic = now
        server.latest_detected_tip = (new_tip, 2)
        server.tip_refresh_divergence_started_monotonic = now
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=published_artifacts.fingerprint,
            template_generation=published_artifacts.generation,
            template_artifacts=published_artifacts,
        )
        with server._job_cache_lock:
            server._template_artifacts = detected_artifacts
            server._published_payout_state = dataclass_replace(
                server._published_payout_state,
                source_tip_hash=new_tip,
            )
        window_state = (
            state.vardiff_window_started_monotonic,
            state.vardiff_window_accepted,
            state.vardiff_window_submitted,
            state.vardiff_window_work,
        )
        sent: list[dict[str, object]] = []

        server._build_idle_job_bundle = lambda _request: self.fail(  # type: ignore[method-assign]
            "idle divergence entered the shared build scheduler"
        )
        state.send = sent.append  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 0)
        server.shutdown_vardiff_idle_executor()

        self.assertEqual(sent, [])
        self.assertEqual(state.share_difficulty, Decimal("16"))
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(
            (
                state.vardiff_window_started_monotonic,
                state.vardiff_window_accepted,
                state.vardiff_window_submitted,
                state.vardiff_window_work,
            ),
            window_state,
        )
        self.assertEqual(server.vardiff_idle_skip_counts["superseded"], 1)

    def test_replacement_tip_build_survives_repeated_idle_sweeps(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        state = client()
        prepare_idle_client(server, state, tip=old_tip)
        install_idle_job_cache(server, tip=old_tip)
        with server._job_cache_lock:
            published_artifacts = server._template_artifacts
        assert published_artifacts is not None
        detected_template = gbt_template(new_tip, height=11)
        detected_artifacts = CachedTemplateArtifacts(
            template=detected_template,
            fingerprint=qbit_template_fingerprint(detected_template),
            previousblockhash=new_tip,
            transaction_hexes=(),
            witness_merkle_leaves_hex=(),
            network_difficulty=1,
            fetched_monotonic=time.monotonic(),
            generation=2,
        )
        now = time.monotonic()
        server.current_tip_first_seen = (old_tip, now)
        server.current_tip_observed_monotonic = now
        server.latest_detected_tip = (new_tip, 2)
        server.tip_refresh_divergence_started_monotonic = now
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=published_artifacts.fingerprint,
            template_generation=published_artifacts.generation,
            template_artifacts=published_artifacts,
        )
        with server._job_cache_lock:
            server._template_artifacts = detected_artifacts
        server._ensure_job_cache_state()
        replacement_cancellation = _JobBuildCancellation(timeout_seconds=60.0)
        replacement_request = SimpleNamespace(
            artifacts=detected_artifacts,
            cancellation=replacement_cancellation,
        )
        replacement_flight = SimpleNamespace(request=replacement_request)
        with server._job_build_scheduler_lock:
            server._job_build_active = replacement_flight
        build_called = threading.Event()
        window_started = state.vardiff_window_started_monotonic

        def unexpected_idle_build(_request: object) -> CachedJobBundle:
            build_called.set()
            raise AssertionError("idle sweep displaced the replacement build")

        server._build_idle_job_bundle = unexpected_idle_build  # type: ignore[method-assign]

        for _sweep in range(4):
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)

        # Close the race where a worker passed its first divergence check just
        # before detection. Scheduler admission must reject that idle request
        # without superseding the active replacement build.
        racing_idle_cancellation = _JobBuildCancellation(timeout_seconds=60.0)
        racing_idle_request = SimpleNamespace(
            idle_retarget=True,
            cancellation=racing_idle_cancellation,
            promise=Future(),
        )
        racing_idle_promise = server._request_job_build(racing_idle_request)  # type: ignore[arg-type]
        with self.assertRaises(JobBuildCancelled):
            racing_idle_promise.result()

        with server._job_build_scheduler_lock:
            self.assertIs(server._job_build_active, replacement_flight)
        self.assertFalse(replacement_cancellation.is_set())
        self.assertTrue(racing_idle_cancellation.is_set())
        self.assertFalse(build_called.is_set())
        self.assertEqual(state.vardiff_window_started_monotonic, window_started)
        self.assertEqual(server.vardiff_idle_skip_counts["superseded"], 4)
        with server._job_build_scheduler_lock:
            server._job_build_active = None
        server.shutdown_vardiff_idle_executor()

    def test_idle_shared_build_does_not_retry_scheduler_divergence_race(
        self,
    ) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        server = coordinator()
        state = client()
        prepare_idle_client(server, state, tip=old_tip)
        install_idle_job_cache(server, tip=old_tip)
        with server._job_cache_lock:
            old_artifacts = server._template_artifacts
            server._job_bundle_cache.clear()
        assert old_artifacts is not None
        now = time.monotonic()
        server.current_tip_first_seen = (old_tip, now)
        server.current_tip_observed_monotonic = now
        server.latest_detected_tip = (old_tip, 1)
        server.tip_template_snapshot = QbitTipTemplateSnapshot(
            bestblockhash=old_tip,
            previousblockhash=old_tip,
            template_fingerprint=old_artifacts.fingerprint,
            template_generation=old_artifacts.generation,
            template_artifacts=old_artifacts,
        )
        server._ensure_job_cache_state()
        replacement_cancellation = _JobBuildCancellation(timeout_seconds=60.0)
        replacement_flight = SimpleNamespace(
            request=SimpleNamespace(
                artifacts=SimpleNamespace(previousblockhash=new_tip),
                cancellation=replacement_cancellation,
            )
        )
        with server._job_build_scheduler_lock:
            server._job_build_active = replacement_flight
        request_builds = 0
        admission_attempts = 0
        idle_cancellation: _JobBuildCancellation | None = None
        original_request_job_build = server._request_job_build

        def make_idle_request(
            _artifacts: CachedTemplateArtifacts,
            _worker: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            nonlocal request_builds, idle_cancellation
            request_builds += 1
            self.assertTrue(kwargs["idle_retarget"])
            idle_cancellation = _JobBuildCancellation(timeout_seconds=60.0)
            return SimpleNamespace(
                idle_retarget=True,
                cancellation=idle_cancellation,
                promise=Future(),
            )

        def detect_before_admission(request: object) -> Future[CachedJobBundle]:
            nonlocal admission_attempts
            admission_attempts += 1
            with server.lock:
                server.latest_detected_tip = (new_tip, 2)
                server.tip_refresh_divergence_started_monotonic = time.monotonic()
            return original_request_job_build(request)  # type: ignore[arg-type]

        server._new_job_build_request = make_idle_request  # type: ignore[method-assign]
        server._request_job_build = detect_before_admission  # type: ignore[method-assign]
        server._schedule_tip_refresh_retry = lambda: None  # type: ignore[method-assign]
        assert state.worker is not None

        with self.assertRaises(JobBuildSuperseded):
            server._build_idle_job_bundle(SimpleNamespace(worker=state.worker))  # type: ignore[arg-type]

        self.assertEqual(request_builds, 1)
        self.assertEqual(admission_attempts, 1)
        self.assertIs(server._template_artifacts, old_artifacts)
        self.assertIsNotNone(idle_cancellation)
        assert idle_cancellation is not None
        self.assertTrue(idle_cancellation.is_set())
        self.assertFalse(replacement_cancellation.is_set())
        with server._job_build_scheduler_lock:
            self.assertIs(server._job_build_active, replacement_flight)
            server._job_build_active = None
        server.shutdown_vardiff_idle_executor()

    def test_idle_cached_collection_bundle_refreshes_readiness(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        ready_bundle = install_idle_job_cache(server)
        assert state.worker is not None
        with server._job_cache_lock:
            artifacts = server._template_artifacts
            assert artifacts is not None
            collection_key = server._job_bundle_key(
                artifacts,
                mode="collection",
                payout_state_generation=server._payout_state_generation,
                payout_artifact_generation=0,
                worker=state.worker,
            )
            collection_bundle = dataclass_replace(
                ready_bundle,
                key=collection_key,
                collection_only=True,
                collection_identity=(
                    state.worker.payout_address,
                    state.worker.p2mr_program_hex,
                ),
            )
            server._job_bundle_cache.clear()
            server._job_bundle_cache[collection_key] = collection_bundle
        server._pool_ready_latched = False
        server.min_ready_miners = 3
        server.accepted_share_stats = lambda: (3, 3)  # type: ignore[method-assign]
        rebuilt = threading.Event()
        sent: list[dict[str, object]] = []

        def build_ready(_request: object) -> CachedJobBundle:
            rebuilt.set()
            with server._job_cache_lock:
                server._job_bundle_cache[ready_bundle.key] = ready_bundle
            return ready_bundle

        state.send = sent.append  # type: ignore[method-assign]
        server._build_idle_job_bundle = build_ready  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        server.shutdown_vardiff_idle_executor()

        self.assertTrue(server._pool_ready_latched)
        self.assertTrue(rebuilt.is_set())
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertIsNotNone(state.active_job)
        self.assertFalse(state.active_job.collection_only)

    def test_idle_cached_ready_bundle_rebinds_same_tip_observation(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        cached = install_idle_job_cache(server)
        updated_template = dict(cached.template)
        updated_template["curtime"] = int(updated_template["curtime"]) + 30
        current = server.store_template_artifacts(updated_template, generation=2)
        assert current is not None
        delivered = threading.Event()
        rebound = threading.Event()
        sent: list[dict[str, object]] = []

        def bind_current_observation(
            bundle: CachedJobBundle,
            artifacts: CachedTemplateArtifacts,
        ) -> CachedJobBundle:
            rebound.set()
            return dataclass_replace(
                bundle,
                template=artifacts.template,
                base_job=dataclass_replace(
                    bundle.base_job,
                    ntime=f'{artifacts.template["curtime"]:08x}',
                ),
                template_generation=artifacts.generation,
            )

        def record(payload: dict[str, object]) -> None:
            sent.append(payload)
            if payload.get("method") == "mining.notify":
                delivered.set()

        state.send = record  # type: ignore[method-assign]
        server._bind_cached_bundle_to_artifacts = (  # type: ignore[method-assign]
            bind_current_observation
        )

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        self.assertTrue(delivered.wait(timeout=1))
        server.shutdown_vardiff_idle_executor()

        self.assertIsNotNone(state.active_job)
        self.assertTrue(rebound.is_set())
        self.assertIs(state.active_job.template, current.template)
        self.assertEqual(state.active_job.template_generation, current.generation)
        expected_ntime = f'{updated_template["curtime"]:08x}'
        self.assertEqual(state.active_job.job.ntime, expected_ntime)
        notify = next(
            payload
            for payload in sent
            if payload.get("method") == "mining.notify"
        )
        self.assertEqual(notify["params"][7], expected_ntime)

    def test_repeated_idle_sweeps_do_not_enqueue_duplicate_connection_work(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        delivery_started = threading.Event()
        release_delivery = threading.Event()
        sent: list[dict[str, object]] = []

        def blocking_send(payload: dict[str, object]) -> None:
            if payload.get("method") == "mining.set_difficulty":
                delivery_started.set()
                release_delivery.wait(timeout=1)
            sent.append(payload)

        state.send = blocking_send  # type: ignore[method-assign]
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)
            self.assertTrue(delivery_started.wait(timeout=1))
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)
            with server._vardiff_idle_lock:
                self.assertEqual(len(server._vardiff_idle_pending), 1)
        finally:
            release_delivery.set()
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertGreaterEqual(
            server.vardiff_idle_skip_counts["superseded"]
            + server.vardiff_idle_skip_counts["not_idle"],
            1,
        )

    def test_idle_sweep_skips_busy_client_lock_and_returns_promptly(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        install_idle_job_cache(server)
        lock_held = threading.Event()
        release_lock = threading.Event()

        def hold_client_lock() -> None:
            state.job_update_lock.acquire()
            try:
                lock_held.set()
                release_lock.wait(timeout=1)
            finally:
                state.job_update_lock.release()

        holder = threading.Thread(target=hold_client_lock)
        holder.start()
        self.assertTrue(lock_held.wait(timeout=1))
        started = time.monotonic()
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)
        finally:
            elapsed = time.monotonic() - started
            release_lock.set()
            holder.join(timeout=1)
            server.shutdown_vardiff_idle_executor()

        self.assertLess(elapsed, 0.25)
        self.assertEqual(server.vardiff_idle_skip_counts["busy"], 1)

    def test_stuck_bundle_builder_does_not_stale_idle_sweep_heartbeat(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        bundle = install_idle_job_cache(server)
        with server._job_cache_lock:
            server._job_bundle_cache.clear()
        state.send = lambda _payload: None  # type: ignore[method-assign]
        build_started = threading.Event()
        release_build = threading.Event()

        def blocked_build(
            *_args: object,
            **_kwargs: object,
        ) -> CachedJobBundle:
            build_started.set()
            if not release_build.wait(timeout=1):
                raise AssertionError("idle retarget bundle build was not released")
            return bundle

        server.build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
        server._record_heartbeat("vardiff_idle_sweep")
        heartbeat_before = server._heartbeats["vardiff_idle_sweep"]
        started = time.monotonic()
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)
            elapsed = time.monotonic() - started
            self.assertTrue(build_started.wait(timeout=0.25))
            self.assertLess(elapsed, 0.25)
            self.assertGreater(
                server._heartbeats["vardiff_idle_sweep"],
                heartbeat_before,
            )
        finally:
            release_build.set()
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(server.vardiff_idle_skip_counts["cache_miss"], 1)

    def test_idle_sweep_cache_miss_builds_only_on_bounded_worker(self) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        bundle = install_idle_job_cache(server)
        server.job_bundle_cache_seconds = 10.0
        expired_bundle = dataclass_replace(
            bundle,
            built_monotonic=time.monotonic() - 11.0,
        )
        with server._job_cache_lock:
            server._job_bundle_cache[bundle.key] = expired_bundle
        build_started = threading.Event()
        release_build = threading.Event()
        build_thread_ids: list[int] = []
        sent: list[dict[str, object]] = []

        def blocked_build(
            *_args: object,
            **_kwargs: object,
        ) -> CachedJobBundle:
            build_thread_ids.append(threading.get_ident())
            build_started.set()
            if not release_build.wait(timeout=1):
                raise AssertionError("idle retarget bundle build was not released")
            return bundle

        state.send = sent.append  # type: ignore[method-assign]
        server.build_shared_job_bundle = blocked_build  # type: ignore[method-assign]
        sweep_thread_id = threading.get_ident()
        started = time.monotonic()
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)
            elapsed = time.monotonic() - started
            self.assertTrue(build_started.wait(timeout=0.25))
            self.assertLess(elapsed, 0.25)
            self.assertEqual(server.vardiff_idle_sweep_once(), 0)
            self.assertEqual(len(build_thread_ids), 1)
            self.assertNotEqual(build_thread_ids[0], sweep_thread_id)
            with server._vardiff_idle_lock:
                self.assertEqual(len(server._vardiff_idle_pending), 1)
        finally:
            release_build.set()
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(server.vardiff_idle_skip_counts["cache_miss"], 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(server.idle_retarget_count, 1)
        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertEqual(state.vardiff_window_accepted, 0)
        self.assertEqual(state.vardiff_window_submitted, 0)
        self.assertEqual(state.vardiff_window_work, Decimal("0"))

    def test_idle_retarget_delivers_fresh_bundle_when_cache_is_disabled(
        self,
    ) -> None:
        server = coordinator()
        state = client()
        prepare_idle_client(server, state)
        bundle = install_idle_job_cache(server)
        server.job_bundle_cache_seconds = 0.0
        with server._job_cache_lock:
            server._job_bundle_cache.clear()
        built = threading.Event()
        sent: list[dict[str, object]] = []

        def build_uncached(_request: object) -> CachedJobBundle:
            built.set()
            return bundle

        server._build_idle_job_bundle = build_uncached  # type: ignore[method-assign]
        state.send = sent.append  # type: ignore[method-assign]

        self.assertEqual(server.vardiff_idle_sweep_once(), 1)
        server.shutdown_vardiff_idle_executor()

        self.assertTrue(built.is_set())
        self.assertEqual(server.vardiff_idle_skip_counts["cache_miss"], 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )
        self.assertEqual(server.idle_retarget_count, 1)
        self.assertEqual(state.share_difficulty, Decimal("4"))
        self.assertIsNone(state.pending_share_difficulty)
        with server._job_cache_lock:
            self.assertEqual(server._job_bundle_cache, {})

    def test_idle_preparation_oserror_keeps_client_connected(self) -> None:
        for failure_phase in ("bundle", "reorg"):
            with self.subTest(failure_phase=failure_phase):
                server = coordinator()
                state = client()
                prepare_idle_client(server, state)
                install_idle_job_cache(server)
                disconnected: list[ClientState] = []
                window_started = state.vardiff_window_started_monotonic

                def fail_bundle(_request: object) -> CachedJobBundle:
                    raise OSError("qbit RPC transport unavailable")

                def fail_reorg() -> bool:
                    raise OSError("qbit trust RPC transport unavailable")

                def unexpected_send(payload: dict[str, object]) -> None:
                    self.fail(f"unexpected idle delivery: {payload}")

                def record_disconnect(client_state: ClientState) -> None:
                    disconnected.append(client_state)

                if failure_phase == "bundle":
                    server._build_idle_job_bundle = (  # type: ignore[method-assign]
                        fail_bundle
                    )
                else:
                    server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
                        fail_reorg
                    )
                state.send = unexpected_send  # type: ignore[method-assign]
                server.disconnect_client = record_disconnect  # type: ignore[method-assign]

                self.assertEqual(server.vardiff_idle_sweep_once(), 1)
                server.shutdown_vardiff_idle_executor()

                self.assertEqual(disconnected, [])
                self.assertIn(state, server.clients)
                self.assertFalse(state.closing)
                self.assertEqual(server.vardiff_idle_task_failures, 1)
                self.assertIsNone(state.pending_share_difficulty)
                self.assertEqual(state.share_difficulty, Decimal("16"))
                self.assertEqual(
                    state.vardiff_window_started_monotonic,
                    window_started,
                )

    def test_disconnect_while_idle_retarget_pending_prevents_delivery(self) -> None:
        server = coordinator()
        blockers = [client(), client()]
        for connection_id, blocker in enumerate(blockers, start=1):
            prepare_idle_client(server, blocker, connection_id=connection_id)
        install_idle_job_cache(server)
        workers_started = threading.Barrier(3)
        release_workers = threading.Event()

        def block_delivery(payload: dict[str, object]) -> None:
            if payload.get("method") == "mining.set_difficulty":
                workers_started.wait(timeout=1)
                release_workers.wait(timeout=1)

        for blocker in blockers:
            blocker.send = block_delivery  # type: ignore[method-assign]

        target = client()
        target_sent: list[dict[str, object]] = []
        target.send = target_sent.append  # type: ignore[method-assign]
        target.close = lambda: None  # type: ignore[method-assign]
        disconnected_skip = threading.Event()
        record_skip = server._record_vardiff_idle_skip

        def record_and_signal(reason: str) -> None:
            record_skip(reason)
            if reason == "disconnected":
                disconnected_skip.set()

        server._record_vardiff_idle_skip = record_and_signal  # type: ignore[method-assign]
        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 2)
            workers_started.wait(timeout=1)
            prepare_idle_client(server, target, connection_id=3)
            self.assertEqual(server.vardiff_idle_sweep_once(), 1)
            server.disconnect_client(target)
        finally:
            release_workers.set()
        try:
            self.assertTrue(disconnected_skip.wait(timeout=1))
        finally:
            server.shutdown_vardiff_idle_executor()

        self.assertEqual(target_sent, [])
        self.assertTrue(target.closing)
        self.assertNotIn(target, server.clients)
        self.assertGreaterEqual(server.vardiff_idle_skip_counts["disconnected"], 1)

    def test_hundreds_of_busy_and_dead_clients_cannot_stall_idle_sweep(self) -> None:
        server = coordinator()

        class BusyLock:
            def acquire(self, blocking: bool = True) -> bool:
                self.assert_nonblocking = blocking
                return False

            def release(self) -> None:
                raise AssertionError("unacquired busy lock released")

        for connection_id in range(1, 201):
            state = client()
            prepare_idle_client(server, state, connection_id=connection_id)
            state.job_update_lock = BusyLock()  # type: ignore[assignment]
        for connection_id in range(201, 401):
            state = client()
            prepare_idle_client(server, state, connection_id=connection_id)
            state.closing = True

        server.vardiff_idle_sweep_seconds = 0.5
        started = time.monotonic()
        self.assertEqual(server.vardiff_idle_sweep_once(), 0)
        elapsed = time.monotonic() - started
        server.shutdown_vardiff_idle_executor()

        self.assertLess(elapsed, server.vardiff_idle_sweep_seconds)
        self.assertEqual(server.vardiff_idle_clients_inspected, 400)
        self.assertEqual(server.vardiff_idle_skip_counts["busy"], 200)
        self.assertEqual(server.vardiff_idle_skip_counts["disconnected"], 200)

    def test_idle_retarget_queue_is_globally_bounded(self) -> None:
        server = coordinator()
        states = [client() for _ in range(9)]
        for connection_id, state in enumerate(states, start=1):
            prepare_idle_client(server, state, connection_id=connection_id)
        install_idle_job_cache(server)
        release_workers = threading.Event()
        two_workers_started = threading.Event()
        started_lock = threading.Lock()
        started_count = 0

        def block_delivery(payload: dict[str, object]) -> None:
            nonlocal started_count
            if payload.get("method") != "mining.set_difficulty":
                return
            with started_lock:
                started_count += 1
                if started_count == 2:
                    two_workers_started.set()
            release_workers.wait(timeout=1)

        for state in states:
            state.send = block_delivery  # type: ignore[method-assign]

        try:
            self.assertEqual(server.vardiff_idle_sweep_once(), 8)
            self.assertTrue(two_workers_started.wait(timeout=1))
            with server._vardiff_idle_lock:
                self.assertEqual(len(server._vardiff_idle_pending), 8)
                self.assertEqual(server.vardiff_idle_inflight, 2)
                self.assertEqual(server.vardiff_idle_queue_depth, 6)
            self.assertEqual(server.vardiff_idle_skip_counts["queue_full"], 1)
        finally:
            release_workers.set()
            server.shutdown_vardiff_idle_executor()

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
        server._ensure_vardiff_idle_state()
        with server._vardiff_idle_lock:
            server.vardiff_idle_clients_inspected = 8
            server.vardiff_idle_skip_counts["busy"] = 2
            server.vardiff_idle_queue_depth = 1
            server.vardiff_idle_inflight = 2
            server.vardiff_idle_task_failures = 1
        server._observe_vardiff_idle_seconds("sweep", 0.005)
        server._observe_vardiff_idle_seconds("task", 0.01)

        metrics = server.metrics_payload()

        self.assertIn("qbit_prism_submitted_shares_total 10", metrics)
        self.assertIn("qbit_prism_stale_shares_total 2", metrics)
        self.assertIn("qbit_prism_duplicate_shares_total 1", metrics)
        self.assertIn("qbit_prism_low_difficulty_shares_total 3", metrics)
        self.assertIn("qbit_prism_grace_credited_shares_total 6", metrics)
        self.assertIn("qbit_prism_stratum_active_connections 0", metrics)
        self.assertIn("qbit_prism_stratum_connection_limit 384", metrics)
        self.assertIn("qbit_prism_stratum_peak_active_connections 0", metrics)
        self.assertIn("qbit_prism_stratum_pending_initial_jobs 0", metrics)
        self.assertIn("qbit_prism_stratum_pending_initial_job_limit 128", metrics)
        self.assertIn(
            "qbit_prism_stratum_oldest_genuinely_pending_initial_job_seconds 0.0",
            metrics,
        )
        self.assertIn(
            "qbit_prism_stratum_current_tip_coverage_gap_seconds 0.0",
            metrics,
        )
        self.assertIn("qbit_prism_stratum_current_tip_job_coverage 1.0", metrics)
        self.assertIn("qbit_prism_stratum_handler_threads 0", metrics)
        self.assertIn("qbit_prism_job_delivery_queue_depth 0", metrics)
        self.assertIn("qbit_prism_job_delivery_active_workers 0", metrics)
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
        self.assertIn("qbit_prism_vardiff_idle_clients_inspected_total 8", metrics)
        self.assertIn('qbit_prism_vardiff_idle_skips_total{reason="busy"} 2', metrics)
        self.assertIn("qbit_prism_vardiff_idle_queue_depth 1", metrics)
        self.assertIn("qbit_prism_vardiff_idle_inflight 2", metrics)
        self.assertIn("qbit_prism_vardiff_idle_task_failures_total 1", metrics)
        self.assertIn("qbit_prism_vardiff_idle_sweep_seconds_count 1", metrics)
        self.assertIn("qbit_prism_vardiff_idle_retarget_task_seconds_count 1", metrics)
        self.assertIn("qbit_prism_stale_share_percent 20", metrics)
        self.assertIn("qbit_prism_coinbase_weight_headroom_bytes 1999750", metrics)
        self.assertIn("qbit_prism_vardiff_enabled 1", metrics)
        self.assertIn("qbit_prism_qbitd_initial_block_download 0", metrics)
        self.assertIn("qbit_prism_qbitd_peers 4", metrics)

    def test_hot_lock_and_pending_candidate_metrics_are_bounded(self) -> None:
        server = coordinator()
        server.lock = _ObservedRLock()
        lock_wait_started = threading.Event()

        def contend() -> None:
            lock_wait_started.set()
            with server.lock:
                pass

        with server.lock:
            waiter = threading.Thread(target=contend)
            waiter.start()
            self.assertTrue(lock_wait_started.wait(1))
            time.sleep(0.02)
        waiter.join(1)
        self.assertFalse(waiter.is_alive())

        ledger = SingleWriterShareLedger()
        block_hash = "bc" * 32
        ledger.persist_block_candidate_intent(
            {
                "schema": "qbit.prism.block-candidate-intent.v1",
                "block_hash_hex": block_hash,
                "block_hex": "00",
            }
        )
        server.ledger = ledger
        before_attempt = "\n".join(
            server.coordinator_lock_metrics_lines()
            + server.block_submitter_metrics_lines()
        )
        self.assertIn("qbit_prism_coordinator_lock_contentions_total 1", before_attempt)
        self.assertIn("qbit_prism_block_candidates_pending 1", before_attempt)
        self.assertRegex(
            before_attempt,
            r"qbit_prism_block_candidate_oldest_unattempted_seconds 0\.\d+",
        )

        self.assertTrue(ledger.mark_block_candidate_attempted(block_hash=block_hash))
        after_attempt = "\n".join(server.block_submitter_metrics_lines())
        self.assertIn(
            "qbit_prism_block_candidate_oldest_unattempted_seconds 0.000000",
            after_attempt,
        )

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
        server.observe_ctv_fanout_broadcaster_chunk(
            SimpleNamespace(processed_count=1, elapsed_seconds=0.25)
        )
        server._record_ctv_fanout_broadcaster_yield()

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
        self.assertIn(
            "qbit_prism_ctv_fanout_broadcaster_tip_refresh_yields_total 1",
            metrics,
        )
        self.assertIn(
            'qbit_prism_ctv_fanout_broadcaster_chunk_seconds_bucket{le="0.25"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_ctv_fanout_broadcaster_chunk_rows_bucket{le="1"} 1',
            metrics,
        )

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

        with patch.dict(
            os.environ,
            {
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
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

    def test_prism_coinbase_output_policy_defaults_to_canonical_and_is_omitted(self) -> None:
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

        self.assertNotIn("coinbase_output_policy", policy)

    def test_prism_coinbase_output_policy_explicit_canonical_is_omitted(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="88")

        with patch.dict(
            os.environ,
            {
                "PRISM_COINBASE_OUTPUT_POLICY": "canonical",
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertNotIn("coinbase_output_policy", policy)

    def test_prism_coinbase_output_policy_pool_fee_first_is_included(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="88")

        with patch.dict(
            os.environ,
            {
                "PRISM_COINBASE_OUTPUT_POLICY": "pool-fee-first",
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ):
            policy = server.prism_payout_policy()

        self.assertEqual(policy["coinbase_output_policy"], "pool-fee-first")
        self.assertEqual(policy["pool_fee_policy"]["fee_bps"], 125)

    def test_prism_coinbase_output_policy_rejects_unknown_values(self) -> None:
        for invalid in ("fee-first", "POOL-FEE-FIRST", "canonical "):
            with self.subTest(invalid=invalid), patch.dict(
                os.environ,
                {
                    "PRISM_COINBASE_OUTPUT_POLICY": invalid,
                    "PRISM_POOL_FEE_ENABLED": "1",
                    "PRISM_POOL_FEE_BPS": "125",
                    "PRISM_POOL_FEE_ADDRESS": "tq1fee",
                },
                clear=True,
            ):
                server = coordinator()
                server.rpc = AddressRpc(valid_address="tq1fee")
                with self.assertRaisesRegex(
                    SystemExit,
                    "PRISM_COINBASE_OUTPUT_POLICY must be one of: canonical, pool-fee-first",
                ):
                    server.prism_payout_policy()

    def test_prism_coinbase_output_policy_pool_fee_first_requires_pool_fee(self) -> None:
        cases = [
            {"PRISM_COINBASE_OUTPUT_POLICY": "pool-fee-first"},
            {
                "PRISM_COINBASE_OUTPUT_POLICY": "pool-fee-first",
                "PRISM_POOL_FEE_ENABLED": "0",
            },
        ]
        for env_vars in cases:
            with self.subTest(env_vars=env_vars), patch.dict(os.environ, env_vars, clear=True):
                server = coordinator()
                with self.assertRaisesRegex(SystemExit, "requires PRISM_POOL_FEE_ENABLED=1"):
                    server.prism_payout_policy()

    def test_build_audit_bundle_passes_coinbase_output_policy_to_cli_payload(self) -> None:
        server = coordinator()
        server.rpc = AddressRpc(valid_address="tq1fee", script_byte="99")
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

        with patch.dict(
            os.environ,
            {
                "PRISM_COINBASE_OUTPUT_POLICY": "pool-fee-first",
                "PRISM_POOL_FEE_ENABLED": "1",
                "PRISM_POOL_FEE_BPS": "125",
                "PRISM_POOL_FEE_ADDRESS": "tq1fee",
            },
            clear=True,
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
            server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
            )

        payout_policy = captured["payload"]["payout_policy"]
        self.assertEqual(payout_policy["coinbase_output_policy"], "pool-fee-first")
        self.assertEqual(payout_policy["pool_fee_policy"]["fee_bps"], 125)

    def test_build_audit_bundle_passes_ctv_settlement_config_to_cli_payload(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        captured: dict[str, object] = {}

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
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(captured),
        ):
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

    def test_build_audit_bundle_preserves_exact_canonical_output_file(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        canonical_bytes = b'{"ok":true,"nested":[1,2]}'
        captured: dict[str, object] = {}

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(
                captured,
                output_text=canonical_bytes.decode("utf-8"),
            ),
        ):
            output_path = Path(tmp) / "candidate.audit.json"
            bundle = server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                canonical_output_path=output_path,
            )

            self.assertEqual(bundle, {"ok": True, "nested": [1, 2]})
            self.assertEqual(output_path.read_bytes(), canonical_bytes)
            self.assertIn("--canonical-output", captured["cmd"])
            self.assertEqual(captured["payload"]["shares"], [])

    def test_build_audit_bundle_summary_only_requests_and_parses_job_summary(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        summary = {
            "found_block": {
                "block_height": 10,
                "coinbase_value_sats": 50_00000000,
            },
            "signed_coinbase_manifest": {
                "manifest": {"coinbase_tx_hex": "c0ffee"},
                "signature": {"signature_hex": "11" * 64},
            },
        }
        captured: dict[str, object] = {}

        with patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            fake_audit_bundle_popen(
                captured,
                output_text=json.dumps(summary, separators=(",", ":")),
            ),
        ), patch.object(
            Path,
            "open",
            side_effect=AssertionError("summary-only build must not open an output path"),
        ):
            bundle = server.build_audit_bundle(
                shares=[],
                found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
            )

        self.assertEqual(bundle, summary)
        self.assertEqual(set(bundle), {"found_block", "signed_coinbase_manifest"})
        self.assertIn("--job-summary-output", captured["cmd"])
        self.assertNotIn("--canonical-output", captured["cmd"])

    def test_build_audit_bundle_removes_partial_output_after_builder_failure(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32

        captured: dict[str, object] = {}
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "candidate.audit.json"
            with patch(
                "lab.prism.prism_coordinator.subprocess.Popen",
                fake_audit_bundle_popen(
                    captured,
                    output_text='{"partial":',
                    returncode=9,
                    stderr_text="synthetic builder failure",
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "synthetic builder failure"):
                    server.build_audit_bundle(
                        shares=[],
                        found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                        prior_balances=[],
                        coinbase_script_sig_suffix_hex="00",
                        canonical_output_path=output_path,
                    )
            self.assertFalse(output_path.exists())
            self.assertEqual(server.job_build_worker_counts["crashes"], 1)

            with patch(
                "lab.prism.prism_coordinator.subprocess.Popen",
                fake_audit_bundle_popen(captured),
            ):
                recovered = server.build_audit_bundle(
                    shares=[],
                    found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                )

            self.assertEqual(recovered, {"ok": True})
            self.assertEqual(server.job_build_worker_counts["restarts"], 1)

    def test_build_audit_bundle_recovers_after_cancelled_worker_timeout(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        cancellation = _JobBuildCancellation(timeout_seconds=60)
        process_calls = 0

        class FakeStdin:
            def write(self, value: str) -> int:
                return len(value)

            def close(self) -> None:
                return None

        class HungThenHealthyPopen:
            def __init__(self, _cmd: list[str], **kwargs: object) -> None:
                nonlocal process_calls
                process_calls += 1
                self.healthy = process_calls == 2
                self.stdin = FakeStdin()
                self.stdout = kwargs["stdout"]
                self.stderr = kwargs["stderr"]
                self.returncode: int | None = None
                self.output_written = False

            def poll(self) -> int | None:
                if self.returncode is not None:
                    return self.returncode
                if self.healthy:
                    if not self.output_written:
                        self.stdout.write('{"ok":true}')  # type: ignore[union-attr]
                        self.output_written = True
                    self.returncode = 0
                    return 0
                cancellation.cancel("timeout")
                return None

            def terminate(self) -> None:
                self.returncode = -15

            def kill(self) -> None:
                self.returncode = -9

            def wait(self, timeout: float | None = None) -> int:
                del timeout
                assert self.returncode is not None
                return self.returncode

        build_kwargs = {
            "shares": [],
            "found_block": {
                "block_height": 10,
                "coinbase_value_sats": 50_00000000,
            },
            "prior_balances": [],
            "coinbase_script_sig_suffix_hex": "00",
        }
        with patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            HungThenHealthyPopen,
        ):
            with self.assertRaisesRegex(JobBuildCancelled, "timeout"):
                server.build_audit_bundle(
                    **build_kwargs,
                    cancellation=cancellation,
                )
            recovered = server.build_audit_bundle(
                **build_kwargs,
                cancellation=_JobBuildCancellation(timeout_seconds=60),
            )

        self.assertEqual(recovered, {"ok": True})
        self.assertEqual(process_calls, 2)
        self.assertEqual(server.job_build_worker_counts["terminations"], 1)
        self.assertEqual(server.job_build_worker_counts["restarts"], 1)

    def test_build_audit_bundle_does_not_unlink_preexisting_output_path(self) -> None:
        server = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32

        with tempfile.TemporaryDirectory() as tmp, patch(
            "lab.prism.prism_coordinator.subprocess.Popen"
        ) as popen:
            output_path = Path(tmp) / "preexisting-candidate.audit.json"
            output_path.write_bytes(b"do-not-clobber")
            with self.assertRaises(FileExistsError):
                server.build_audit_bundle(
                    shares=[],
                    found_block={"block_height": 10, "coinbase_value_sats": 50_00000000},
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                    canonical_output_path=output_path,
                )

            self.assertEqual(output_path.read_bytes(), b"do-not-clobber")
            popen.assert_not_called()

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

            def run_once(
                self,
                *,
                limit: int,
                progress_callback: object,
                chunk_size: int,
                tip_refresh_pending: object,
                chunk_callback: object,
            ) -> object:
                captured["limit"] = limit
                captured["chunk_size"] = chunk_size
                captured["tip_refresh_pending"] = tip_refresh_pending
                captured["chunk_callback"] = chunk_callback
                return SimpleNamespace(
                    scanned_count=1,
                    submitted_count=0,
                    updated_count=1,
                    failed_count=0,
                    yielded_to_tip_refresh=False,
                )

        with patch("lab.prism.prism_coordinator.CtvFanoutBroadcastDaemon", FakeDaemon):
            result = server.run_ctv_fanout_broadcaster_once()

        self.assertIs(captured["ledger"], server.ledger)
        self.assertEqual(captured["fee_sats"], 0)
        self.assertEqual(captured["limit"], 7)
        self.assertEqual(captured["chunk_size"], 5)
        self.assertTrue(callable(captured["tip_refresh_pending"]))
        self.assertTrue(callable(captured["chunk_callback"]))
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
        # Tip observation is recorded as a detection before expensive template
        # construction so obsolete builders can be cancelled immediately, but
        # submit authority is published only alongside a coherent snapshot.
        # The incoherent template never enters the artifact cache, client job
        # maps, or the published tip state.
        self.assertEqual(server.latest_detected_tip[0], old_tip)
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
        server.rpc = TipRpc(old_tip)
        self.assertTrue(server.observe_tip_first_seen(old_tip))

        def overtake_poll() -> QbitTipTemplateSnapshot:
            self.assertTrue(server.observe_tip_for_refresh(new_tip))
            return old_snapshot

        server.fetch_qbit_tip_template_snapshot = overtake_poll  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshSuperseded,
            "tip/template poll was superseded during template fetch",
        ):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.latest_detected_tip[0], new_tip)
        self.assertEqual(server.current_tip_first_seen[0], old_tip)
        self.assertIsNone(server.tip_template_snapshot)
        self.assertIsNone(
            getattr(server, "template_refresh_failure_started_monotonic", None)
        )
        self.assertTrue(server._consume_tip_refresh_retry())
        self.assertFalse(server._consume_tip_refresh_retry())

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
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._payout_state_source[0], 1)

    @staticmethod
    def _reconcile_coordinator_with_artifact_counter(
        ledger: ReorgLedger,
        rpc: ReorgRpc,
    ) -> tuple[PrismCoordinator, list[tuple[int, int, bool]]]:
        """Coordinator armed so candidate preparation would build an artifact."""
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.ledger = ledger
        server.rpc = rpc
        server._ensure_job_cache_state()
        # Keep the speculative background preparer out of the way so the
        # counter observes only builds requested by reconcile passes.
        server._payout_artifact_executor_shutdown = True
        server._pool_ready_latched = True
        server._template_artifacts = SimpleNamespace(network_difficulty=1)
        build_calls: list[tuple[int, int, bool]] = []

        def fake_build(
            expected_payout_state_generation: int,
            artifact_payout_state_generation: int,
            network_difficulty: int,
            force_full_rescan: bool = False,
            bypass_build_interval: bool = False,
            during_publication: bool = False,
        ) -> None:
            del network_difficulty, bypass_build_interval, during_publication
            build_calls.append(
                (
                    expected_payout_state_generation,
                    artifact_payout_state_generation,
                    force_full_rescan,
                )
            )
            return None

        server._build_payout_ledger_artifact = fake_build  # type: ignore[method-assign]
        return server, build_calls

    def test_ledger_artifact_not_built_when_publication_not_required(self) -> None:
        tip = "5b" * 32
        ledger = ReorgLedger([])
        server, build_calls = self._reconcile_coordinator_with_artifact_counter(
            ledger,
            ReorgRpc(
                tip=tip,
                template=gbt_template(tip, height=11),
                height=10,
                block_hashes={10: tip},
            ),
        )

        first = server.reconcile_prism_pool_blocks_once(tip_hash=tip)
        self.assertEqual(first["published_generation"], 1)
        self.assertEqual(len(build_calls), 1)
        self.assertFalse(build_calls[0][2])

        second = server.reconcile_prism_pool_blocks_once(tip_hash=tip)

        self.assertIsNone(second["published_generation"])
        self.assertEqual(
            len(build_calls),
            1,
            "a pass without publication need must not build the ledger artifact",
        )
        self.assertEqual(
            ledger.events,
            [("watch", 10), ("mature", 10), ("watch", 10), ("mature", 10)],
        )

    def test_payout_change_forces_publication_and_builds_artifact(self) -> None:
        tip = "5d" * 32
        pool_block_hash = "ae" * 32
        ledger = ReorgLedger([])
        server, build_calls = self._reconcile_coordinator_with_artifact_counter(
            ledger,
            ReorgRpc(
                tip=tip,
                template=gbt_template(tip, height=11),
                height=10,
                block_hashes={10: tip},
            ),
        )

        first = server.reconcile_prism_pool_blocks_once(tip_hash=tip)
        self.assertEqual(first["published_generation"], 1)
        self.assertEqual(len(build_calls), 1)

        # An orphaned confirmed block appears without any new payout source:
        # payout_changed alone must still force a publication.
        ledger.rows.append(
            {
                "block_hash": pool_block_hash,
                "block_height": 12,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
        )

        second = server.reconcile_prism_pool_blocks_once(tip_hash=tip)

        self.assertEqual(second["inactive_blocks"], 1)
        self.assertEqual(second["published_generation"], 2)
        self.assertEqual(len(build_calls), 2)
        self.assertTrue(build_calls[1][2])
        self.assertEqual(ledger.rows[0]["chain_state"], "inactive")
        self.assertIn(("inactive", pool_block_hash, 10), ledger.events)

    def test_lost_mutation_response_publishes_fresh_balances_with_forced_rescan(
        self,
    ) -> None:
        tip = "5e" * 32
        pool_block_hash = "af" * 32

        class LostResponseLedger(ReorgLedger):
            def __init__(self) -> None:
                super().__init__([])
                self.prior_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.prior_balance_reads += 1
                if any(row["chain_state"] == "confirmed" for row in self.rows):
                    return [
                        {
                            "recipient_id": "stale-carry",
                            "order_key": "01:stale-carry",
                            "p2mr_program_hex": "44" * 32,
                            "balance_sats": 546,
                        }
                    ]
                return []

            def mark_pool_block_inactive(
                self,
                *,
                block_hash: str,
                active_tip_height: int,
            ) -> dict[str, object]:
                result = super().mark_pool_block_inactive(
                    block_hash=block_hash,
                    active_tip_height=active_tip_height,
                )
                if int(result["inactive_count"]):
                    raise ConnectionError("mutation response lost")
                return result

        ledger = LostResponseLedger()
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )
        forced_rescans: list[bool] = []
        original_prepare = server._prepared_payout_state_candidate

        def record_prepare(
            captured: tuple[int, int, str | None, str, float],
            *,
            force_full_window_rescan: bool = False,
            bypass_build_interval: bool = False,
        ) -> object:
            forced_rescans.append(force_full_window_rescan)
            return original_prepare(
                captured,
                force_full_window_rescan=force_full_window_rescan,
                bypass_build_interval=bypass_build_interval,
            )

        server._prepared_payout_state_candidate = record_prepare  # type: ignore[method-assign]

        first = server.reconcile_prism_pool_blocks_once(tip_hash=tip)
        self.assertEqual(first["published_generation"], 1)
        with server._job_cache_lock:
            initially_published = server._published_payout_state.artifact
        assert initially_published is not None
        self.assertEqual(len(initially_published.prior_balances()), 0)

        ledger.rows.append(
            {
                "block_hash": pool_block_hash,
                "block_height": 12,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
        )
        # Rebuild the current generation's immutable state so the test starts
        # from balances that predate the lost-response mutation.
        with server._job_cache_lock:
            server._published_payout_state = dataclass_replace(
                server._published_payout_state,
                artifact=None,
            )
        stale_published = server._current_payout_state_artifact()
        self.assertEqual(len(stale_published.prior_balances()), 1)
        reads_before_error = ledger.prior_balance_reads
        forced_rescans.clear()

        with self.assertRaisesRegex(ConnectionError, "mutation response lost"):
            server.reconcile_prism_pool_blocks_once(tip_hash=tip)

        with server._job_cache_lock:
            healed_state = server._published_payout_state
        assert healed_state.artifact is not None
        self.assertEqual(healed_state.generation, 2)
        self.assertEqual(healed_state.artifact.prior_balances(), [])
        self.assertEqual(ledger.prior_balance_reads, reads_before_error + 1)
        self.assertEqual(forced_rescans, [True])
        self.assertEqual(ledger.rows[0]["chain_state"], "inactive")

    def test_read_phase_reconcile_failure_does_not_force_balance_rescan(
        self,
    ) -> None:
        tip = "5f" * 32

        class PrefetchFailureLedger(ReorgLedger):
            fail_prefetch = False

            def __init__(self) -> None:
                super().__init__([])
                self.prior_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.prior_balance_reads += 1
                return []

            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                if self.fail_prefetch:
                    raise TimeoutError("reconcile prefetch join exceeded 20s")
                return super().reorg_watch_blocks(
                    active_tip_height=active_tip_height
                )

        ledger = PrefetchFailureLedger()
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )
        first = server.reconcile_prism_pool_blocks_once(tip_hash=tip)
        self.assertEqual(first["published_generation"], 1)
        reads_before_failure = ledger.prior_balance_reads
        with server._job_cache_lock:
            generation_before_failure = server._published_payout_state.generation

        forced_rescans: list[bool] = []
        original_prepare = server._prepared_payout_state_candidate

        def record_prepare(
            captured: tuple[int, int, str | None, str, float],
            *,
            force_full_window_rescan: bool = False,
            bypass_build_interval: bool = False,
        ) -> object:
            forced_rescans.append(force_full_window_rescan)
            return original_prepare(
                captured,
                force_full_window_rescan=force_full_window_rescan,
                bypass_build_interval=bypass_build_interval,
            )

        server._prepared_payout_state_candidate = record_prepare  # type: ignore[method-assign]
        ledger.fail_prefetch = True

        with self.assertRaisesRegex(
            TimeoutError,
            "reconcile prefetch join exceeded 20s",
        ):
            server.reconcile_prism_pool_blocks_once(tip_hash=tip)

        with server._job_cache_lock:
            generation_after_failure = server._published_payout_state.generation
        self.assertEqual(forced_rescans, [])
        self.assertEqual(ledger.prior_balance_reads, reads_before_failure)
        self.assertEqual(generation_after_failure, generation_before_failure)

    def test_concurrent_same_tip_reconciles_share_one_pass(self) -> None:
        tip = "5c" * 32
        entered = threading.Event()
        release = threading.Event()

        class BlockingReorgLedger(ReorgLedger):
            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                entered.set()
                if not release.wait(timeout=5.0):
                    raise AssertionError("reconcile release was never signaled")
                return super().reorg_watch_blocks(
                    active_tip_height=active_tip_height
                )

        ledger = BlockingReorgLedger([])
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )
        server._ensure_job_cache_state()

        results: list[dict[str, object] | None] = [None, None]

        def reconcile(slot: int) -> None:
            results[slot] = server.reconcile_prism_pool_blocks_once(tip_hash=tip)

        leader = threading.Thread(target=reconcile, args=(0,))
        leader.start()
        self.assertTrue(entered.wait(timeout=5.0))
        with server._reconcile_flight_lock:
            flight = server._reconcile_flights[tip]

        follower_waiting = threading.Event()

        class SignalingEvent:
            def __init__(self, inner: threading.Event) -> None:
                self._inner = inner

            def wait(self, timeout: float | None = None) -> bool:
                follower_waiting.set()
                return self._inner.wait(timeout)

            def set(self) -> None:
                self._inner.set()

        flight.event = SignalingEvent(flight.event)  # type: ignore[assignment]
        follower = threading.Thread(target=reconcile, args=(1,))
        follower.start()
        self.assertTrue(follower_waiting.wait(timeout=5.0))

        release.set()
        leader.join(timeout=5.0)
        follower.join(timeout=5.0)
        self.assertFalse(leader.is_alive())
        self.assertFalse(follower.is_alive())

        self.assertEqual(
            ledger.events,
            [("watch", 10), ("mature", 10)],
            "the follower must reuse the leader's pass, not run its own",
        )
        self.assertEqual(results[0], results[1])
        self.assertIsNotNone(results[0])
        self.assertIsNot(results[0], results[1])
        with server._reconcile_flight_lock:
            self.assertEqual(server._reconcile_flights, {})

    def test_lock_owner_reconcile_bypasses_same_tip_flight(self) -> None:
        tip = "5f" * 32
        ledger = ReorgLedger([])
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )
        server._ensure_job_cache_state()

        leader_lock_attempted = threading.Event()
        flight_waited = threading.Event()
        flight_completed = threading.Event()
        underlying_lock = threading.RLock()
        leader: threading.Thread | None = None

        class ObservedBalanceLock:
            def __enter__(self) -> ObservedBalanceLock:
                if threading.current_thread() is leader:
                    if underlying_lock.acquire(blocking=False):
                        underlying_lock.release()
                        raise AssertionError(
                            "reconcile leader unexpectedly acquired the owned lock"
                        )
                    leader_lock_attempted.set()
                    underlying_lock.acquire()
                else:
                    underlying_lock.acquire()
                return self

            def __exit__(self, *_args: object) -> None:
                underlying_lock.release()

        class WaitForbiddenEvent:
            def __init__(self, inner: threading.Event) -> None:
                self._inner = inner

            def wait(self, timeout: float | None = None) -> bool:
                flight_waited.set()
                raise AssertionError(
                    "lock-owning reconciliation joined the same-tip flight"
                )

            def set(self) -> None:
                flight_completed.set()
                self._inner.set()

        server._payout_balance_mutation_lock = ObservedBalanceLock()  # type: ignore[assignment]
        leader_results: list[dict[str, object]] = []
        leader_errors: list[BaseException] = []

        def lead_reconcile() -> None:
            try:
                leader_results.append(
                    server.reconcile_prism_pool_blocks_once(tip_hash=tip)
                )
            except BaseException as exc:  # noqa: BLE001 - asserted below
                leader_errors.append(exc)

        leader = threading.Thread(target=lead_reconcile)
        try:
            with server._payout_balance_mutation_lock:
                leader.start()
                self.assertTrue(leader_lock_attempted.wait(timeout=5.0))
                with server._reconcile_flight_lock:
                    flight = server._reconcile_flights[tip]
                    flight.event = WaitForbiddenEvent(  # type: ignore[assignment]
                        flight.event
                    )

                bypassed = server.ensure_reorg_reconciled_for_tip(
                    tip,
                    _coalesce_same_tip=False,
                )

                self.assertTrue(bypassed)
                self.assertFalse(flight_waited.is_set())
                self.assertTrue(leader.is_alive())
                self.assertEqual(
                    ledger.events,
                    [("watch", 10), ("mature", 10)],
                )
        finally:
            leader.join(timeout=5.0)

        self.assertFalse(leader.is_alive())
        if leader_errors:
            raise leader_errors[0]
        self.assertEqual(len(leader_results), 1)
        self.assertTrue(flight_completed.is_set())
        self.assertFalse(flight_waited.is_set())
        self.assertEqual(
            ledger.events,
            [
                ("watch", 10),
                ("mature", 10),
                ("watch", 10),
                ("mature", 10),
            ],
        )
        with server._reconcile_flight_lock:
            self.assertEqual(server._reconcile_flights, {})

    def test_lock_owner_reconcile_leads_visible_flight_when_absent(self) -> None:
        tip = "60" * 32
        entered = threading.Event()
        release = threading.Event()

        class BlockingReorgLedger(ReorgLedger):
            def reorg_watch_blocks(
                self,
                *,
                active_tip_height: int,
            ) -> list[dict[str, object]]:
                entered.set()
                if not release.wait(timeout=5.0):
                    raise AssertionError("reconcile release was never signaled")
                return super().reorg_watch_blocks(
                    active_tip_height=active_tip_height
                )

        ledger = BlockingReorgLedger([])
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )
        server._ensure_job_cache_state()

        lock_owner_result: list[bool] = []
        follower_result: list[dict[str, object]] = []
        errors: list[BaseException] = []
        follower_waiting = threading.Event()

        class SignalingEvent:
            def __init__(self, inner: threading.Event) -> None:
                self._inner = inner

            def wait(self, timeout: float | None = None) -> bool:
                follower_waiting.set()
                return self._inner.wait(timeout)

            def set(self) -> None:
                self._inner.set()

        def reconcile_while_owning_lock() -> None:
            try:
                with server._payout_balance_mutation_lock:
                    lock_owner_result.append(
                        server.ensure_reorg_reconciled_for_tip(
                            tip,
                            _coalesce_same_tip=False,
                        )
                    )
            except BaseException as exc:  # noqa: BLE001 - asserted below
                errors.append(exc)

        def follow_reconcile() -> None:
            try:
                follower_result.append(
                    server.reconcile_prism_pool_blocks_once(tip_hash=tip)
                )
            except BaseException as exc:  # noqa: BLE001 - asserted below
                errors.append(exc)

        lock_owner = threading.Thread(target=reconcile_while_owning_lock)
        follower = threading.Thread(target=follow_reconcile)
        lock_owner.start()
        try:
            self.assertTrue(entered.wait(timeout=5.0))
            with server._reconcile_flight_lock:
                flight = server._reconcile_flights[tip]
                flight.event = SignalingEvent(flight.event)  # type: ignore[assignment]

            follower.start()
            self.assertTrue(follower_waiting.wait(timeout=5.0))
            self.assertTrue(lock_owner.is_alive())
            self.assertTrue(follower.is_alive())
            self.assertEqual(ledger.events, [])
        finally:
            release.set()
            lock_owner.join(timeout=5.0)
            if follower.ident is not None:
                follower.join(timeout=5.0)

        self.assertFalse(lock_owner.is_alive())
        self.assertFalse(follower.is_alive())
        if errors:
            raise errors[0]
        self.assertEqual(lock_owner_result, [True])
        self.assertEqual(len(follower_result), 1)
        self.assertEqual(
            ledger.events,
            [("watch", 10), ("mature", 10)],
            "the ordinary caller must reuse the lock owner's visible pass",
        )
        with server._reconcile_flight_lock:
            self.assertEqual(server._reconcile_flights, {})

    def test_forced_and_reserved_reconciles_bypass_flight_reuse(self) -> None:
        tip = "5e" * 32
        ledger = ReorgLedger([])
        server = coordinator()
        server.reorg_reconciler_enabled = True
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )
        server._ensure_job_cache_state()

        sentinel_summary: dict[str, object] = {"sentinel": True}
        flight = _ReconcileFlight()
        flight.summary = sentinel_summary
        flight.event.set()
        with server._reconcile_flight_lock:
            server._reconcile_flights[tip] = flight

        joined = server.reconcile_prism_pool_blocks_once(tip_hash=tip)
        self.assertEqual(joined, sentinel_summary)
        self.assertIsNot(joined, sentinel_summary)
        self.assertEqual(ledger.events, [])

        forced = server.reconcile_prism_pool_blocks_once(
            tip_hash=tip,
            _force_publish=True,
        )
        self.assertEqual(forced["published_generation"], 1)
        self.assertEqual(ledger.events, [("watch", 10), ("mature", 10)])

        reserved = server.reconcile_prism_pool_blocks_once(
            tip_hash=tip,
            _source_reserved=True,
        )
        self.assertIsNone(reserved["published_generation"])
        self.assertEqual(
            ledger.events,
            [("watch", 10), ("mature", 10), ("watch", 10), ("mature", 10)],
        )

        with server._reconcile_flight_lock:
            server._reconcile_flights.pop(tip, None)

    def test_pass_spanning_detection_cycle_does_not_arm_memo(self) -> None:
        # A pass whose reads straddle a flip away and back finishes with the
        # latest detected hash matching its tip again, but its proof belongs
        # to the closed epoch: arming must be refused so every memo consumer
        # (refresh joins, initial-job and vardiff-idle builds) re-proves.
        tip = "5c" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        ledger = ReorgLedger([])

        def epoch_bumping_watch(*, active_tip_height: int) -> list[dict[str, object]]:
            with server.lock:
                server.tip_detection_epoch = (
                    int(getattr(server, "tip_detection_epoch", 0)) + 2
                )
            return []

        ledger.reorg_watch_blocks = epoch_bumping_watch  # type: ignore[method-assign]
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip,
            template=gbt_template(tip, height=11),
            height=10,
            block_hashes={10: tip},
        )

        self.assertTrue(server.ensure_reorg_reconciled_for_tip(tip))

        with server.lock:
            self.assertNotIn(tip, server._reorg_reconcile_trusted_memo)

    def test_row_mutating_pass_evicts_other_tip_memo_entries(self) -> None:
        tip_a = "a1" * 32
        tip_b = "b2" * 32
        pool_block_hash = "cd" * 32
        server = coordinator()
        server.reorg_reconciler_enabled = True
        ledger = ReorgLedger([])
        server.ledger = ledger
        server.rpc = ReorgRpc(
            tip=tip_b,
            template=gbt_template(tip_b, height=11),
            height=10,
            block_hashes={10: tip_b},
        )
        server._ensure_job_cache_state()
        server._note_reorg_reconcile_outcome(tip_a, trusted=True)

        # A pass for another tip that mutates no rows leaves tip A's cached
        # proof valid: rows and tip-A chain state are both unchanged.
        first = server.reconcile_prism_pool_blocks_once(tip_hash=tip_b)
        self.assertEqual(first["published_generation"], 1)
        with server.lock:
            self.assertIn(tip_a, server._reorg_reconcile_trusted_memo)
            self.assertIn(tip_b, server._reorg_reconcile_trusted_memo)

        # A pass that applies orphan/maturity row mutations ends every other
        # tip's memo epoch, even though tip B was never observed by
        # observe_tip_for_refresh.
        ledger.rows.append(
            {
                "block_hash": pool_block_hash,
                "block_height": 12,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
        )
        second = server.reconcile_prism_pool_blocks_once(tip_hash=tip_b)
        self.assertEqual(second["inactive_blocks"], 1)
        with server.lock:
            self.assertNotIn(tip_a, server._reorg_reconcile_trusted_memo)
            self.assertIn(tip_b, server._reorg_reconcile_trusted_memo)

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

    def test_generated_coordinator_session_advertises_heartbeat_capability(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        env = {
            "PRISM_POSTGRES_PSQL_COMMAND": "psql postgresql://example.invalid/qbit",
        }

        with patch.dict(os.environ, env, clear=True), patch(
            "lab.prism.prism_coordinator.PsqlShareLedger"
        ) as fake_ledger:
            fake_ledger.return_value = SimpleNamespace(backend_name="postgres-psql")
            server.make_ledger()

        self.assertTrue(
            fake_ledger.call_args.kwargs["writer_session_token"].startswith(
                WRITER_LEASE_HEARTBEAT_SESSION_PREFIX
            )
        )

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
            {**base, "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS": "0"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                SystemExit,
                "PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS",
            ):
                validate_prism_production_gate()

        with patch.dict(
            os.environ,
            {**base, "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS": "0"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                SystemExit,
                "PRISM_STRATUM_MAX_PENDING_INITIAL_JOBS",
            ):
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

    def test_template_refresh_failure_budget_starts_at_first_failure(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.template_refresh_failure_exit_seconds = 120
        server.last_successful_template_refresh_monotonic = 100.0

        server._record_template_refresh_failure(500.0)
        self.assertFalse(server.template_refresh_failure_expired(500.0))
        self.assertEqual(server.template_refresh_failure_started_monotonic, 500.0)
        self.assertFalse(server.template_refresh_failure_expired(619.999))
        self.assertTrue(server.template_refresh_failure_expired(620.0))

    def test_disabled_template_refresh_failure_budget_does_not_start_or_expire(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.template_refresh_failure_exit_seconds = 0

        server._record_template_refresh_failure(500.0)
        self.assertFalse(server.template_refresh_failure_expired(500.0))
        self.assertFalse(hasattr(server, "template_refresh_failure_started_monotonic"))

    def test_coordination_blocked_refresh_does_not_start_failure_budget(self) -> None:
        for blocked_error in (
            TemplateRefreshSuperseded("qbit tip changed during sequential refresh"),
            _PayoutStatePublicationBlocked(
                "accepted block payout confirmation is still pending"
            ),
        ):
            with self.subTest(blocked=type(blocked_error).__name__):
                server = coordinator()
                server.template_refresh_failure_exit_seconds = 10.0
                server.coordination_blocked_exit_seconds = 30.0
                server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
                server.rpc = TipRpc("11" * 32)

                def raise_blocked(error: Exception = blocked_error) -> QbitTipTemplateSnapshot:
                    raise error

                server.fetch_qbit_tip_template_snapshot = raise_blocked  # type: ignore[method-assign]
                with (
                    patch(
                        "lab.prism.prism_coordinator.time.monotonic",
                        return_value=100.0,
                    ),
                    self.assertRaises(type(blocked_error)),
                ):
                    server.poll_qbit_tip_template_once()

                self.assertIsNone(
                    getattr(server, "template_refresh_failure_started_monotonic", None)
                )
                self.assertFalse(server.template_refresh_failure_expired(10_000.0))
                self.assertAlmostEqual(
                    server.coordination_blocked_streak_age_seconds(109.999),
                    9.999,
                )
                self.assertFalse(
                    server.coordination_blocked_streak_expired(129.999)
                )

    def test_coordination_blocked_default_budget_is_fifteen_minutes(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server._record_coordination_blocked_refresh(100.0)

        self.assertFalse(server.coordination_blocked_streak_expired(999.999))
        self.assertTrue(server.coordination_blocked_streak_expired(1000.0))

    def test_non_coordination_blocked_refresh_still_starts_failure_budget(self) -> None:
        # Plain TemplateRefreshBlocked also wraps genuine failures (malformed
        # template artifacts, job builds failing, untrusted chain views); only
        # the TemplateRefreshSuperseded/payout-fence subclasses are exempt.
        server = coordinator()
        server.template_refresh_failure_exit_seconds = 10.0
        server.coordination_blocked_exit_seconds = 10.0
        server._record_coordination_blocked_refresh(90.0)
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.rpc = TipRpc("11" * 32)

        def raise_blocked() -> QbitTipTemplateSnapshot:
            raise TemplateRefreshBlocked(
                "unable to derive exact artifacts for observed qbit template"
            )

        server.fetch_qbit_tip_template_snapshot = raise_blocked  # type: ignore[method-assign]
        with (
            patch("lab.prism.prism_coordinator.time.monotonic", return_value=100.0),
            self.assertRaises(TemplateRefreshBlocked),
        ):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.template_refresh_failure_started_monotonic, 100.0)
        self.assertEqual(server.coordination_blocked_streak_age_seconds(110.0), 0.0)
        self.assertTrue(server.template_refresh_failure_expired(110.0))

    def test_sustained_blocked_refresh_storm_exits_via_coordination_budget(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server.coordination_blocked_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.rpc = TipRpc("11" * 32)
        clock = {"now": 100.0}
        blocked_polls = 0

        def blocked_fetch() -> QbitTipTemplateSnapshot:
            nonlocal blocked_polls
            blocked_polls += 1
            clock["now"] += 6.0
            if blocked_polls >= 4:
                server.stop_event.set()
            if blocked_polls % 2:
                raise TemplateRefreshSuperseded(
                    "qbit tip changed during sequential refresh; immediate retry scheduled"
                )
            raise _PayoutStatePublicationBlocked(
                "payout state invalidation is pending publication"
            )

        server.fetch_qbit_tip_template_snapshot = blocked_fetch  # type: ignore[method-assign]
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
        ):
            server.blockpoll_loop()

        self.assertEqual(blocked_polls, 4)
        self.assertTrue(server.coordination_blocked_streak_expired(clock["now"]))
        self.assertIsNone(
            getattr(server, "template_refresh_failure_started_monotonic", None)
        )
        server.stop_event.clear()
        server.watchdog_enabled = False
        server.watchdog_interval_seconds = 0.001
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                return_value=clock["now"],
            ),
            patch(
                "lab.prism.prism_coordinator.os._exit",
                side_effect=SystemExit(1),
            ) as exit_process,
            patch("builtins.print"),
            self.assertRaises(SystemExit),
        ):
            server.watchdog_loop()

        exit_process.assert_called_once_with(1)

    def test_armed_budget_is_not_fired_by_coordination_blocked_refresh(self) -> None:
        # A transient budgeted failure armed the clock, qbitd recovered, and
        # only coordination churn follows. Blocked attempts must not trip the
        # armed budget in blockpoll_loop: the exit is reserved for the next
        # budgeted failure, and the clock clears on the next completed refresh.
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server.template_refresh_failure_started_monotonic = 100.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.rpc = TipRpc("11" * 32)
        clock = {"now": 100.0}
        blocked_polls = 0

        def blocked_fetch() -> QbitTipTemplateSnapshot:
            nonlocal blocked_polls
            blocked_polls += 1
            clock["now"] += 6.0
            if blocked_polls >= 4:
                server.stop_event.set()
            raise TemplateRefreshSuperseded(
                "qbit tip changed during sequential refresh; immediate retry scheduled"
            )

        server.fetch_qbit_tip_template_snapshot = blocked_fetch  # type: ignore[method-assign]
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
        ):
            server.blockpoll_loop()

        self.assertEqual(blocked_polls, 4)
        self.assertGreater(
            clock["now"],
            server.template_refresh_failure_started_monotonic
            + server.template_refresh_failure_exit_seconds,
        )
        exit_process.assert_not_called()
        self.assertEqual(server.template_refresh_failure_started_monotonic, 100.0)

    def test_rpc_outage_arms_and_exhausts_failure_budget(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        clock = {"now": 100.0}

        class OutageRpc:
            def call(self, method: str, params: list[object] | None = None, **_kwargs: object) -> object:
                clock["now"] += 6.0
                raise ConnectionError("qbitd unreachable")

        server.rpc = OutageRpc()
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
            self.assertRaises(SystemExit),
        ):
            server.blockpoll_loop()

        exit_process.assert_called_once_with(1)
        self.assertIsNotNone(server.template_refresh_failure_started_monotonic)

    def test_persistent_blocked_template_derivation_arms_and_exhausts_failure_budget(self) -> None:
        # Top-of-poll RPCs stay healthy, but every refresh fails with plain
        # TemplateRefreshBlocked (e.g. malformed template artifacts, all job
        # builds failing). A fresh process must still arm the budget from its
        # first such failure and take the budgeted restart path.
        server = coordinator()
        server.blockpoll_seconds = 0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        server.rpc = TipRpc("11" * 32)
        clock = {"now": 100.0}

        def blocked_fetch() -> QbitTipTemplateSnapshot:
            clock["now"] += 6.0
            raise TemplateRefreshBlocked(
                "unable to derive exact artifacts for observed qbit template"
            )

        server.fetch_qbit_tip_template_snapshot = blocked_fetch  # type: ignore[method-assign]
        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
            self.assertRaises(SystemExit),
        ):
            server.blockpoll_loop()

        exit_process.assert_called_once_with(1)
        self.assertIsNotNone(server.template_refresh_failure_started_monotonic)

    def test_healthy_noop_template_poll_resets_refresh_failure_clock(self) -> None:
        server = coordinator()
        server.blockpoll_seconds = 0
        server.last_successful_template_refresh_monotonic = 100.0
        server.template_refresh_failure_started_monotonic = 190.0
        server.template_refresh_failure_exit_seconds = 10.0
        server.coordination_blocked_exit_seconds = 10.0
        server._record_coordination_blocked_refresh(190.0)
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.tip_template_snapshot = snapshot
        server.rpc = TipRpc(snapshot.bestblockhash)
        server.fetch_qbit_tip_template_snapshot = lambda: snapshot  # type: ignore[method-assign]

        def trusted_chain_view(_tip: str) -> bool:
            server.stop_event.set()
            return True

        server.ensure_reorg_reconciled_for_tip = trusted_chain_view  # type: ignore[method-assign]
        with patch("lab.prism.prism_coordinator.time.monotonic", return_value=200.0):
            server.blockpoll_loop()

        self.assertEqual(server.last_successful_template_refresh_monotonic, 200.0)
        self.assertIsNone(server.template_refresh_failure_started_monotonic)
        self.assertEqual(server.coordination_blocked_streak_age_seconds(300.0), 0.0)
        self.assertFalse(server.coordination_blocked_streak_expired(300.0))
        self.assertFalse(server.template_refresh_failure_expired(300.0))
        self.assertIsNone(server.template_refresh_failure_started_monotonic)
        server._record_template_refresh_failure(300.0)
        self.assertEqual(server.template_refresh_failure_started_monotonic, 300.0)

    def test_shared_template_poll_records_success_for_blockwait_callers(self) -> None:
        server = coordinator()
        server.last_successful_template_refresh_monotonic = 100.0
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.rpc = TipRpc(snapshot.bestblockhash)
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
        server.template_refresh_failure_started_monotonic = 100.0
        server.template_refresh_failure_exit_seconds = 10.0
        server._record_heartbeat = lambda _name: None  # type: ignore[method-assign]
        snapshot = QbitTipTemplateSnapshot(
            bestblockhash="11" * 32,
            previousblockhash="11" * 32,
            template_fingerprint="22" * 32,
        )
        server.rpc = TipRpc(snapshot.bestblockhash)
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
        server.template_refresh_failure_started_monotonic = 100.0
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
        server.rpc = TipRpc(new_tip)
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

    def test_guarded_sequential_build_supersession_does_not_arm_failure_budget(self) -> None:
        # Non-ready/collection mode: the sequential loop's guarded client
        # build detects a superseded snapshot inside maybe_send_job. That is
        # coordination churn, not template unhealthiness -- the real raise
        # site must carry TemplateRefreshSuperseded so sustained pre-ready
        # churn neither arms nor fires the budget.
        old_tip = "00" * 32
        new_tip = "33" * 32
        server = coordinator()
        server.blockpoll_seconds = 0
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
        server.rpc = TipRpc(new_tip)
        clock = {"now": 100.0}
        fetches = 0

        def fetch_snapshot() -> QbitTipTemplateSnapshot:
            nonlocal fetches
            fetches += 1
            clock["now"] += 6.0
            if fetches >= 4:
                server.stop_event.set()
            return snapshot

        server.fetch_qbit_tip_template_snapshot = fetch_snapshot  # type: ignore[method-assign]
        # The guarded pre-build currency check loses the race on every pass.
        server._tip_refresh_snapshot_current_locked = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: False
        )

        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch("lab.prism.prism_coordinator.traceback.print_exc"),
            patch("lab.prism.prism_coordinator.os._exit", side_effect=SystemExit(1)) as exit_process,
        ):
            server.blockpoll_loop()

        self.assertEqual(fetches, 4)
        self.assertGreater(clock["now"] - 100.0, server.template_refresh_failure_exit_seconds)
        exit_process.assert_not_called()
        self.assertIsNone(
            getattr(server, "template_refresh_failure_started_monotonic", None)
        )

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

        with (
            patch("builtins.print") as emitted,
            patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ),
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
        self.assertEqual(server.block_solves_dropped_counts, {"stale_grace": 1})
        self.assertIn(
            'qbit_prism_block_solves_dropped_total{reason="stale_grace"} 1',
            server.metrics_payload(),
        )
        self.assertEqual(emitted.call_count, 1)
        log_line = " ".join(str(value) for value in emitted.call_args.args)
        self.assertIn(submission.block_hash_hex, log_line)
        self.assertIn(old_tip, log_line)

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

    def test_share_ack_latency_histogram_observes_accept_and_reject(self) -> None:
        # The read-to-ack instrument for share-ingest saturation, driven
        # through the real handler loop: both outcomes are observed after
        # their response write, so accepted-vs-rejected separates commit
        # pressure from transport/thread saturation.
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]
        submission = SimpleNamespace(
            header_hex="af" * 80,
            block_hash_hex="cf" * 32,
            share_pass=True,
            block_pass=False,
        )
        coordinator_sock, peer = socket.socketpair()
        state.sock = coordinator_sock
        accepted_line = json.dumps(
            {
                "id": 7,
                "method": "mining.submit",
                "params": ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            }
        )
        rejected_line = json.dumps(
            {
                "id": 8,
                "method": "mining.submit",
                "params": [
                    "miner-a",
                    "missing-job",
                    "00" * 8,
                    "00000001",
                    "00000002",
                ],
            }
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            handler = threading.Thread(target=server.handle_client, args=(state,))
            handler.start()
            peer.sendall((accepted_line + "\n" + rejected_line + "\n").encode())
            peer.close()
            handler.join(5)

        self.assertFalse(handler.is_alive())
        self.assertEqual(sent[0], {"id": 7, "result": True, "error": None})
        self.assertEqual(sent[1]["id"], 8)
        self.assertIsNotNone(sent[1]["error"])
        metrics = "\n".join(server.share_ack_metrics_lines())
        self.assertIn(
            'qbit_prism_share_ack_seconds_count{result="accepted"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_share_ack_seconds_count{result="rejected"} 1',
            metrics,
        )

    def test_reconnected_same_username_submits_against_disconnected_job(self) -> None:
        # A proxy flap leaves devices mining the dead connection's jobs;
        # their shares arrive on the replacement connection and must credit
        # against the retained context instead of rejecting unknown-job.
        tip = "00" * 32
        server, state, ledger = submit_coordinator(tip=tip)
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]
        server.disconnect_client(state)
        retained = server.evicted_job_graveyard.get("job-1")
        self.assertIsNotNone(retained)
        assert retained is not None
        self.assertIsNone(retained.client)

        reconnected = ClientState(
            sock=object(),
            address=("127.0.0.1", 2),
            connection_id=2,
            extranonce1_hex="00000002",
        )
        reconnected.subscribed = True
        reconnected.authorized = True
        reconnected.username = "miner-a"
        reconnected.worker = worker_identity("miner-a")
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
                reconnected,
                ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
            )

        self.assertFalse(should_close)
        self.assertEqual(len(ledger.pending), 1)
        self.assertIsNone(ledger.pending[0].credit_policy)
        # Credit follows the retained context's original worker identity.
        self.assertEqual(ledger.pending[0].share_id, "miner-a:" + "cf" * 32)
        self.assertEqual(
            server.evicted_job_submit_counts[
                "accepted_same_tip_cross_connection"
            ],
            1,
        )

    def test_cross_connection_submit_requires_same_username(self) -> None:
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]
        server.disconnect_client(state)
        self.assertIn("job-1", server.evicted_job_graveyard)

        intruder = ClientState(
            sock=object(),
            address=("127.0.0.1", 3),
            connection_id=3,
            extranonce1_hex="00000003",
        )
        intruder.subscribed = True
        intruder.authorized = True
        intruder.username = "miner-b"
        intruder.worker = worker_identity("miner-b")
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
            with self.assertRaises(StratumError) as raised:
                server.handle_submit(
                    intruder,
                    ["miner-b", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)

    def test_cross_connection_submit_uses_retained_version_mask(self) -> None:
        # In-flight work from the dead connection was version-rolled under
        # the mask negotiated there; the replacement's own mask (0 before
        # mining.configure) must not judge those version bits.
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.jobs["job-1"].version_mask = 0x1FFFE000
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]
        server.disconnect_client(state)

        reconnected = ClientState(
            sock=object(),
            address=("127.0.0.1", 2),
            connection_id=2,
            extranonce1_hex="00000002",
        )
        reconnected.subscribed = True
        reconnected.authorized = True
        reconnected.username = "miner-a"
        reconnected.worker = worker_identity("miner-a")
        self.assertEqual(reconnected.version_mask, 0)
        submission = SimpleNamespace(
            header_hex="af" * 80,
            block_hash_hex="cf" * 32,
            share_pass=True,
            block_pass=False,
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ) as assemble:
            self.assertFalse(
                server.handle_submit(
                    reconnected,
                    [
                        "miner-a",
                        "job-1",
                        "00" * 8,
                        "00000001",
                        "00000002",
                        "1fffe000",
                    ],
                )
            )

        self.assertEqual(
            assemble.call_args.kwargs["version_mask"],
            0x1FFFE000,
        )

    def test_cross_connection_block_candidate_keeps_original_extranonce(self) -> None:
        # The mined coinbase embeds the extranonce1 the retained job was
        # stamped with; a candidate recording the replacement connection's
        # extranonce would fail the audit fence after submitblock.
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.stop_after_block = False
        server.max_blocks = 10
        server.jobs["job-1"].job.extranonce1_hex = state.extranonce1_hex
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]
        server.disconnect_client(state)

        reconnected = ClientState(
            sock=object(),
            address=("127.0.0.1", 2),
            connection_id=2,
            extranonce1_hex="00000002",
        )
        reconnected.subscribed = True
        reconnected.authorized = True
        reconnected.username = "miner-a"
        reconnected.worker = worker_identity("miner-a")
        submission = SimpleNamespace(
            header_hex="af" * 80,
            block_hash_hex="cf" * 32,
            share_pass=True,
            block_pass=True,
            coinbase_tx_hex="c0ffee",
            block_hex="00",
        )

        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            self.assertFalse(
                server.handle_submit(
                    reconnected,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            )

        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        candidate = server.block_candidate_queue.get_nowait()
        self.assertEqual(candidate.extranonce1_hex, "00000001")
        self.assertIs(candidate.client, reconnected)

    def test_detached_entry_never_falls_back_to_stale_grace(self) -> None:
        # A tip moving between the graveyard lookup and submit
        # classification must not let a detached cross-connection entry
        # borrow the reconnect's stale-grace window: cross-connection
        # resumes are same-tip only.
        tip_old = "00" * 32
        tip_new = "11" * 32
        server, state, _ledger = submit_coordinator(tip=tip_old)
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]
        server.disconnect_client(state)
        detached = server.evicted_job_graveyard.get("job-1")
        self.assertIsNotNone(detached)
        assert detached is not None
        self.assertIsNone(detached.client)

        reconnected = ClientState(
            sock=object(),
            address=("127.0.0.1", 2),
            connection_id=2,
            extranonce1_hex="00000002",
        )
        reconnected.subscribed = True
        reconnected.authorized = True
        reconnected.username = "miner-a"
        reconnected.worker = worker_identity("miner-a")
        eligibility_calls: list[str] = []

        def recording_eligibility(
            _client: ClientState,
            _context: object,
            current_tip: str,
        ) -> bool:
            eligibility_calls.append(current_tip)
            return True

        server.context_eligible_for_stale_grace = (  # type: ignore[method-assign]
            recording_eligibility
        )

        self.assertIsNone(
            server.evicted_submit_context(reconnected, detached, tip_new)
        )
        self.assertEqual(eligibility_calls, [])

        # Same-connection entries keep today's stale-grace classification.
        live_state = client()
        server.jobs["job-2"] = prism_context(
            "job-2",
            tip_old,
            worker=live_state.worker,
        )
        server.bury_evicted_job(live_state, "job-2")
        live_entry = server.evicted_job_graveyard["job-2"]
        result = server.evicted_submit_context(live_state, live_entry, tip_new)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result[1], PRISM_CREDIT_POLICY_STALE_GRACE)
        self.assertEqual(eligibility_calls, [tip_new])

    def test_disconnected_retention_capacity_is_bounded(self) -> None:
        tip = "00" * 32
        server, state, _ledger = submit_coordinator(tip=tip)
        server.disconnected_job_retention = 1
        server.jobs["job-2"] = prism_context("job-2", tip, worker=state.worker)
        state.active_job_ids = {"job-1", "job-2"}
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]

        server.disconnect_client(state)

        self.assertEqual(len(server._disconnected_evicted_job_ids), 1)
        self.assertEqual(len(server.evicted_job_graveyard), 1)
        self.assertEqual(
            server.evicted_job_capacity_eviction_counts["disconnected"],
            1,
        )

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

    def test_tip_change_prunes_and_disconnect_detaches_retained_contexts(self) -> None:
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
        # Same-tip work survives the disconnect for a same-username
        # reconnect; the dead connection's entry is detached and ages out on
        # the normal same-tip TTL instead of vanishing with the socket.
        retained = server.evicted_job_graveyard.get("job-2")
        self.assertIsNotNone(retained)
        assert retained is not None
        self.assertIsNone(retained.client)
        server.prune_evicted_job_graveyard(
            now=time.monotonic()
            + float(server.same_tip_job_retention_seconds)
            + 1.0
        )
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
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        ledger.durable_payout_state = True  # type: ignore[attr-defined]
        server.ledger = ledger
        server.jobs["job-1"].prior_balances = [
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
        pending = self._pending_append("balance-stale").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        # The share was already accepted at submit time, so a lost block is a
        # block-abandonment, not a stale share rejection.
        self.assertEqual(server.stale_share_count, 0)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(
            server.stale_job_abandon_counts,
            {"tip_moved": 0, "balance_stale": 1, "append_epoch_stale": 0},
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "abandoned")
        self.assertEqual(
            outbox_row["last_error"],
            "prior balances changed since the job was issued",
        )
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="tip_moved"} 0',
            metrics,
        )
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="balance_stale"} 1',
            metrics,
        )

    def test_block_submit_lands_as_issued_after_late_append_epoch_invalidation(self) -> None:
        # A late-visible replay append advances the live epoch and schedules
        # the refresh wave asynchronously; until that wave retires the job,
        # membership admission still lets the pre-append job submit. The
        # fast lane offers the solve to qbitd without ledger
        # synchronization, so once the node accepts it the landing keeps
        # the as-issued payout snapshot for accounting (the replayed share
        # rides the next window) instead of terminally abandoning an
        # accepted block.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "ea" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ea" * 32,
            block_hex="00",
        )
        pending = self._pending_append("append-epoch-stale").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        # The share was already accepted at submit time, so a lost block is a
        # block-abandonment, not a stale share rejection.
        self.assertEqual(server.stale_share_count, 0)
        # The node accepted the pre-append solve: accounting keeps the
        # as-issued snapshot instead of terminally abandoning it.
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="append_epoch_stale"} 0',
            metrics,
        )

    def test_replayed_block_candidate_is_exempt_from_append_epoch_fence(self) -> None:
        # Epochs are process-local: a candidate reconstructed from durable
        # intent carries no meaningful stamp, so an epoch advanced by this
        # process's own share replay must not abandon the recovered block.
        # Cross-restart payout drift is governed by the durable share-window
        # replay and prior-balance fences instead.
        server, state, ledger = submit_coordinator()
        server.stop_after_block = False
        server.max_blocks = 10
        block_hash = "cd" * 32
        pending = self._pending_append("replayed-epoch").pending_share
        original = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            ),
            pending_share=pending,
        )
        candidate = server.block_candidate_from_intent(
            server.block_candidate_intent(original)
        )
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            accepted = server.submit_block_candidate(candidate)

        self.assertTrue(accepted)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)

    def test_offered_replay_keeps_as_issued_snapshot_despite_window_omission(
        self,
    ) -> None:
        # Revalidation guards an offer the node has not yet seen. The
        # dedicated submitter's fast lane offers the durable bytes before
        # the landing runs, so once the node accepts, the recorded coinbase
        # is settled reality: the landing must keep the as-issued snapshot
        # and skip the audit-window walk entirely instead of terminally
        # abandoning payout accounting for an accepted block (or defer-
        # looping behind the walk's deadline under saturation).
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        ledger.durable_payout_state = True  # type: ignore[attr-defined]
        window_calls: list[tuple[int, int]] = []

        def durable_window(
            *,
            anchor_job_issued_at_ms: int,
            network_difficulty: int,
        ) -> list[dict[str, object]]:
            window_calls.append((anchor_job_issued_at_ms, network_difficulty))
            return [
                {"share_id": "recorded-window-share"},
                {"share_id": "late-appended-share"},
            ]

        ledger.audit_share_window = durable_window  # type: ignore[method-assign]
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.shares_json = [{"share_id": "recorded-window-share"}]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "ec" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ec" * 32,
            block_hex="00",
        )
        pending = self._pending_append("replayed-window-omission").pending_share
        original = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        intent = server.block_candidate_intent(original)
        candidate = server.block_candidate_from_intent(intent)
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        ledger.append_batch([(pending, intent)])
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        # The fast lane offered first, so the walk never runs and the
        # as-issued snapshot is accounted.
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertEqual(window_calls, [])
        self.assertEqual(len(bundle_builds), 1)
        self.assertEqual(server.stale_share_count, 0)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_unoffered_replay_still_rejects_window_omitting_durable_append(
        self,
    ) -> None:
        # The pre-offer half of the same fence: a direct embedder resumes a
        # reconstructed candidate with no node offer made (attempted=False,
        # as the compatibility probe reports when block bytes are not
        # retained), and the durable window surfaces a share the recorded
        # coinbase omitted — the landing must still refuse to mint that
        # coinbase, and the revalidation walk must run before any offer.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        ledger.durable_payout_state = True  # type: ignore[attr-defined]
        window_calls: list[tuple[int, int]] = []

        def durable_window(
            *,
            anchor_job_issued_at_ms: int,
            network_difficulty: int,
        ) -> list[dict[str, object]]:
            window_calls.append((anchor_job_issued_at_ms, network_difficulty))
            return [
                {"share_id": "recorded-window-share"},
                {"share_id": "late-appended-share"},
            ]

        ledger.audit_share_window = durable_window  # type: ignore[method-assign]
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.shares_json = [{"share_id": "recorded-window-share"}]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError(
                "audit bundle must not be built from a payout window omitting "
                "a durably appended share"
            )
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ed" * 32,
            block_hex="00",
        )
        pending = self._pending_append("unoffered-window-omission").pending_share
        original = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        intent = server.block_candidate_intent(original)
        candidate = server.block_candidate_from_intent(intent)
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        ledger.append_batch([(pending, intent)])

        self.assertFalse(
            server.submit_block_candidate(
                candidate,
                node_submission=SimpleNamespace(
                    attempted=False,
                    error=None,
                    result=None,
                ),
            )
        )

        self.assertEqual(window_calls, [(12000, 1)])
        self.assertEqual(
            server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB],
            1,
        )
        self.assertEqual(
            server.stale_job_abandon_counts,
            {"tip_moved": 0, "balance_stale": 0, "append_epoch_stale": 1},
        )
        # Direct embedders do not use the outbox-finalization wrapper:
        # the terminal outcome is recorded and counted, while the durable
        # row stays pending for queue-driven replay to finalize.
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "pending")

    def test_replayed_block_candidate_lands_when_durable_window_replays_intact(
        self,
    ) -> None:
        # The durable revalidation is a fence against omitted appends, not a
        # new obstacle to recovery: when the audit window at the declared
        # anchor replays exactly the recorded shares, the reconstructed
        # candidate still lands -- even while this process's own live epoch
        # has advanced past the meaningless replayed stamp.
        server, state, ledger = submit_coordinator()
        server.stop_after_block = False
        server.max_blocks = 10
        ledger.durable_payout_state = True
        ledger.audit_share_window = (  # type: ignore[attr-defined]
            lambda *, anchor_job_issued_at_ms, network_difficulty: [
                {"share_id": "recorded-window-share"}
            ]
        )
        context = server.jobs["job-1"]
        context.shares_json = [{"share_id": "recorded-window-share"}]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        block_hash = "cf" * 32
        pending = self._pending_append("replayed-durable-window").pending_share
        original = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            ),
            pending_share=pending,
        )
        candidate = server.block_candidate_from_intent(
            server.block_candidate_intent(original)
        )
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            accepted = server.submit_block_candidate(candidate)

        self.assertTrue(accepted)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)

    def test_block_submit_keeps_as_issued_accounting_when_append_races_the_landing(
        self,
    ) -> None:
        # Node propagation is the fast lane: the block is offered to qbitd
        # before payout accounting notices anything, so an append-side
        # invalidation racing the landing can no longer block submitblock.
        # The landing still observes the bump at its epoch fence, but the
        # node already accepted the block, so accounting proceeds with the
        # as-issued snapshot instead of terminally abandoning a live
        # block's payouts.
        # The bump here is driven through the REAL invalidation path from
        # the drain hook -- with no armed artifact or in-flight walk in the
        # harness, it fires only because the landing exposed its own
        # declared anchor.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32
        late_append = self._pending_append("late-during-landing").pending_share

        race_fired: list[bool] = []
        original_drain = server._await_unfenced_appends_predating_anchor

        def racing_drain(anchor_ms: int) -> None:
            original_drain(anchor_ms)
            if not race_fired:
                race_fired.append(True)
                server._invalidate_incremental_payout_window_for_append(
                    late_append
                )

        server._await_unfenced_appends_predating_anchor = racing_drain  # type: ignore[method-assign]

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "da" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="da" * 32,
            block_hex="00",
        )
        pending = self._pending_append("append-races-landing").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(race_fired, [True])
        # The fast lane offered the block before the race could be observed.
        self.assertEqual(submitblock_calls, [["00"]])
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
        self.assertEqual(server.stale_share_count, 0)
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)
        # The landing retires its exposed anchor on the way out.
        with server._job_cache_lock:
            self.assertEqual(server._payout_window_inflight_scan_anchors, {})

    def test_offered_replay_lands_as_issued_without_revalidation_walk(
        self,
    ) -> None:
        # The fast lane offers a reconstructed candidate's durable bytes
        # before the landing runs, so the slow audit-window walk is skipped
        # outright — even while this process's live epoch advances mid-
        # flight — and accounting keeps the as-issued snapshot rather than
        # abandoning (or re-walking) an accepted block.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        ledger.durable_payout_state = True  # type: ignore[attr-defined]

        window_calls: list[tuple[int, int]] = []

        def recording_audit_share_window(
            *, anchor_job_issued_at_ms: int, network_difficulty: int
        ) -> list[dict[str, object]]:
            window_calls.append((anchor_job_issued_at_ms, network_difficulty))
            return [{"share_id": "recorded-window-share"}]

        ledger.audit_share_window = recording_audit_share_window  # type: ignore[method-assign]
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.shares_json = [{"share_id": "recorded-window-share"}]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "db" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="db" * 32,
            block_hex="00",
        )
        pending = self._pending_append("replayed-append-race").pending_share
        original = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        intent = server.block_candidate_intent(original)
        candidate = server.block_candidate_from_intent(intent)
        self.assertEqual(candidate.context.payout_append_invalidation_epoch, -1)
        ledger.append_batch([(pending, intent)])
        server.enqueue_block_candidate(candidate)
        # A live epoch bump mid-flight is irrelevant to a reconstructed
        # candidate whose offer is already out: no revalidation rebases it
        # and the epoch fences stand down (no effective epoch).
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._payout_ledger_append_invalidation_epoch += 1

        self.assertTrue(server.submit_next_block_candidate())
        # The fast lane offered the replayed block; the walk never ran.
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertEqual(window_calls, [])
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_landing_blocked_by_fenced_append_lands_as_issued_on_bumped_epoch(
        self,
    ) -> None:
        # A replay-shaped append holds the landing fence across the durable
        # commit itself, not only across its epoch bump. The node offer is
        # the fast lane and does not wait, but a landing whose offer is
        # already in must still wait at the fence before ACCOUNTING; with
        # the offer already accepted, the bumped epoch is recorded and
        # accounting proceeds with the as-issued coinbase (the durable
        # predating share rides the next window) instead of abandoning the
        # accepted block.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "dc" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dc" * 32,
            block_hex="00",
        )
        pending = self._pending_append("landing-vs-fenced-append").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        # Expose a live anchor the late row predates, as an armed window
        # would: the writer's pre-commit peek must fire before any landing
        # is in flight to declare its own anchor.
        anchor_token = server._expose_inflight_scan_anchor(12000)
        self.addCleanup(server._retire_inflight_scan_anchor, anchor_token)

        commit_entered = threading.Event()
        release_commit = threading.Event()
        self.addCleanup(release_commit.set)
        original_append_batch = ledger.append_batch

        def gated_append_batch(entries: list[object]) -> list[object]:
            commit_entered.set()
            release_commit.wait(timeout=10.0)
            return original_append_batch(entries)

        ledger.append_batch = gated_append_batch  # type: ignore[method-assign]
        late_entry = self._pending_append("late-fenced-append")
        writer = threading.Thread(
            target=server._append_share_batch,
            args=([late_entry],),
            daemon=True,
        )
        writer.start()
        self.assertTrue(commit_entered.wait(timeout=10.0))
        # The fence is held while the predating row's commit is in flight.
        self.assertTrue(server._payout_append_landing_fence_lock.locked())

        landing = threading.Thread(
            target=server.submit_next_block_candidate,
            daemon=True,
        )
        landing.start()
        # Give the landing time to run up against the fence: the fast-lane
        # node offer already went out, but with the commit still gated the
        # landing must not have reached a terminal accounting decision.
        time.sleep(0.05)
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertEqual(server.block_candidate_abandoned_counts, {})

        release_commit.set()
        writer.join(timeout=10.0)
        self.assertFalse(writer.is_alive())
        landing.join(timeout=10.0)
        self.assertFalse(landing.is_alive())

        # The bump landed together with the durable append; the landing
        # observed it, and with the offer already accepted it kept the
        # as-issued snapshot for accounting.
        self.assertEqual(submitblock_calls, [["00"]])
        self.assertIn(late_entry.pending_share.share_id, ledger._share_ids)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_predating_append_commits_while_fast_lane_offer_is_in_flight(
        self,
    ) -> None:
        # The other side of the fence boundary flipped with the node fast
        # lane: submitblock no longer holds the landing fence, so a
        # predating append's durable commit does NOT wait for the RPC to
        # return. The row may become durable mid-offer with its epoch bump
        # landing under the fence; the landing's post-offer accounting
        # fences (not the offer itself) are what observe the invalidation.
        # Here the node rejects the block outright, so the rejection -- not
        # the epoch race -- terminally abandons the candidate.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        # Keep an anchor the late row predates exposed for the whole test,
        # independent of the landing's own declared-anchor lifetime.
        anchor_token = server._expose_inflight_scan_anchor(12000)
        self.addCleanup(server._retire_inflight_scan_anchor, anchor_token)

        late_entry = self._pending_append("mid-rpc-append")
        durable_mid_rpc: list[bool] = []
        append_threads: list[threading.Thread] = []

        class MidRpcAppendRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    thread = threading.Thread(
                        target=server._append_share_entry,
                        args=(late_entry,),
                        daemon=True,
                    )
                    thread.start()
                    append_threads.append(thread)
                    deadline = time.monotonic() + 0.25
                    became_durable = False
                    while time.monotonic() < deadline:
                        if late_entry.pending_share.share_id in ledger._share_ids:
                            became_durable = True
                            break
                        time.sleep(0.005)
                    durable_mid_rpc.append(became_durable)
                    return "rejected-by-test"
                return super().call(method, params)

        server.rpc = MidRpcAppendRpc("00" * 32)
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dd" * 32,
            block_hex="00",
        )
        pending = self._pending_append("landing-holds-fence").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        # The fast-lane RPC holds no fence, so the predating row became
        # durable while the offer was still in flight.
        self.assertEqual(durable_mid_rpc, [True])
        self.assertTrue(append_threads)
        append_threads[0].join(timeout=10.0)
        self.assertFalse(append_threads[0].is_alive())
        self.assertIn(late_entry.pending_share.share_id, ledger._share_ids)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
        self.assertEqual(
            server.block_candidate_abandoned_counts[
                PRISM_REJECTION_SUBMITBLOCK_REJECTED
            ],
            1,
        )

    def test_lease_deferral_unwinds_landed_bar_before_submitblock(self) -> None:
        # The landed bar is armed before the lease fence to keep a transport
        # failure's uncertain submitblock outcome conservative. A
        # WriterLeaseRenewalDeferred refusal fires before the RPC, so the
        # outcome is not uncertain: qbitd provably never saw the block, and
        # leaving the bar armed in the (now surviving) process would bar
        # reconciliation and payout-state publication for as long as the
        # writer's own fenced write keeps the renewal deferred. The pending
        # preview must survive, though: child work keeps waiting for the
        # retry instead of snapshotting pre-accept balances.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.block_candidate_retry_initial_seconds = 0.0

        class NoSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    raise AssertionError(
                        "submitblock must not run after a deferral refusal"
                    )
                return super().call(method, params)

        server.rpc = NoSubmitRpc("00" * 32)
        # Force the fenced fallback lane: with no fast-lane node offer
        # (attempted=False), the disposition itself must clear the lease
        # fence before submitblock — the branch the deferral exits from.
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=False,
                result=None,
                error=None,
            )
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="de" * 32,
            block_hex="00",
        )
        pending = self._pending_append("deferral-unwind").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        with patch.object(
            server,
            "_require_fresh_ledger_lease_for_external_side_effect",
            side_effect=WriterLeaseRenewalDeferred("renewal is deferred"),
        ) as fence:
            self.assertTrue(server.submit_next_block_candidate())

        fence.assert_called_once_with("submitblock")
        block_hash = str(submission.block_hash_hex).lower()
        self.assertFalse(
            server._accepted_block_payout_transition_landed(block_hash)
        )
        with server._accepted_block_payout_preview_condition:
            self.assertIn(block_hash, server._accepted_block_payout_previews)

    def test_lease_deferral_keeps_prior_attempts_landed_bar(self) -> None:
        # A transition landed by an earlier attempt that DID reach
        # submitblock records a genuinely uncertain outcome (a lost RPC
        # ack); a later attempt's pre-RPC deferral must not withdraw that
        # bar, or payout state could publish without the possibly-active
        # coinbase's base preserved.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.block_candidate_retry_initial_seconds = 0.0

        class NoSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    raise AssertionError(
                        "submitblock must not run after a deferral refusal"
                    )
                return super().call(method, params)

        server.rpc = NoSubmitRpc("00" * 32)
        # Force the fenced fallback lane, as above.
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=False,
                result=None,
                error=None,
            )
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="df" * 32,
            block_hex="00",
        )
        pending = self._pending_append("deferral-keeps-bar").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        block_hash = str(submission.block_hash_hex).lower()
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server.enqueue_block_candidate(candidate)

        with patch.object(
            server,
            "_require_fresh_ledger_lease_for_external_side_effect",
            side_effect=WriterLeaseRenewalDeferred("renewal is deferred"),
        ) as fence:
            self.assertTrue(server.submit_next_block_candidate())

        fence.assert_called_once_with("submitblock")
        self.assertTrue(
            server._accepted_block_payout_transition_landed(block_hash)
        )

    def test_landing_drains_unfenced_inflight_append_before_epoch_fences(
        self,
    ) -> None:
        # The unfenced classification is a one-time predicate: an append
        # checked while no anchor is exposed commits outside the landing
        # fence, so a landing that exposes its declared anchor afterwards
        # could hold the fence across submitblock while the row becomes
        # durable mid-RPC and its epoch bump queues behind that same
        # fence -- arriving too late to reject the block. The landing must
        # instead drain such in-flight commits right after exposing its
        # anchor, so the bump lands before its epoch fences run.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dc" * 32,
            block_hex="00",
        )
        pending = self._pending_append("landing-drains").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        late_entry = self._pending_append("unfenced-inflight")
        commit_entered = threading.Event()
        commit_release = threading.Event()
        original_append_batch = ledger.append_batch

        def gated_append_batch(batch: list[object]) -> list[object]:
            commit_entered.set()
            if not commit_release.wait(timeout=10.0):
                raise AssertionError("gated ledger commit was never released")
            return original_append_batch(batch)

        ledger.append_batch = gated_append_batch  # type: ignore[method-assign]

        # Release the gated commit exactly when the landing starts
        # draining, so the test exercises the wait itself rather than a
        # lucky ordering.
        drained_anchors: list[int] = []
        original_drain = server._await_unfenced_appends_predating_anchor

        def recording_drain(anchor_ms: int) -> None:
            drained_anchors.append(int(anchor_ms))
            commit_release.set()
            original_drain(anchor_ms)

        server._await_unfenced_appends_predating_anchor = recording_drain  # type: ignore[method-assign]

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "dc" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)

        append_thread = threading.Thread(
            target=server._append_share_entry,
            args=(late_entry,),
            daemon=True,
        )
        append_thread.start()
        # The append was classified with no anchor exposed and is now
        # committing outside the fence.
        self.assertTrue(commit_entered.wait(timeout=10.0))

        self.assertTrue(server.submit_next_block_candidate())

        append_thread.join(timeout=10.0)
        self.assertFalse(append_thread.is_alive())
        self.assertEqual(drained_anchors, [12000])
        self.assertIn(late_entry.pending_share.share_id, ledger._share_ids)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
            self.assertEqual(
                server._payout_unfenced_append_inflight_stamps, {}
            )
        # The drained append's bump landed before the landing's epoch
        # fences; the landing observed it and, with the node offer already
        # accepted, accounted the as-issued window (the durable predating
        # share rides the next window).
        self.assertEqual(submitblock_calls, [["00"]])
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_completed_append_predating_seedless_published_window_fences(
        self,
    ) -> None:
        # A build that seeds no artifact (the documented
        # PRISM_PAYOUT_ARTIFACT_REUSE=0 rollback mode) retires its walk
        # exposure at the publication fence, yet jobs stamped from it keep
        # serving that window until they retire. A replay-shaped append
        # that started AND finished in that gap used to see no live
        # anchor: no epoch bump, and its registry entry was popped on
        # completion -- so a later landing drained nothing, its
        # nonnegative context epoch skipped durable revalidation, and
        # submitblock accepted a coinbase omitting the durable share.
        # Publication now hands the declared anchor to the
        # published-window watermark, so the append commits under the
        # landing fence with its epoch bump -- and the landing, whose
        # offer already landed, keeps the as-issued snapshot for
        # accounting.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        bundle_builds: list[dict[str, object]] = []

        def recording_build_audit_bundle(**kwargs: object) -> dict[str, object]:
            bundle_builds.append(dict(kwargs))
            return verified_block_bundle()

        server.build_audit_bundle = recording_build_audit_bundle  # type: ignore[method-assign]
        server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]
        tail_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tail_dir.cleanup)
        server.audit_dir = Path(tail_dir.name)
        server.evidence_path = Path(tail_dir.name) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32
        context = server.jobs["job-1"]
        context.found_block = {
            "network_difficulty": 1,
            "anchor_job_issued_at_ms": 12000,
        }
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="dd" * 32,
            block_hex="00",
        )
        pending = self._pending_append("seedless-window").pending_share
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        server.enqueue_block_candidate(candidate)

        # The publication fence of a seedless build hands the bundle's
        # declared anchor to the watermark when it retires the walk
        # exposure.
        server._ensure_job_cache_state()
        with server._job_cache_lock:
            server._publish_seedless_job_window_anchor_locked(12000)

        # The ordinary share hot path stays off the fence: a row stamped
        # above the watermark commits unfenced and bumps nothing.
        ordinary_entry = self._pending_append(
            "ordinary-above-watermark", accepted_at_ms=13000
        )
        server._append_share_entry(ordinary_entry)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 0)
            self.assertEqual(
                server._payout_unfenced_append_inflight_stamps, {}
            )

        # The replay-shaped append starts and finishes entirely before the
        # landing begins: nothing is in flight for the landing to drain.
        late_entry = self._pending_append("completed-before-landing")
        server._append_share_entry(late_entry)
        self.assertIn(late_entry.pending_share.share_id, ledger._share_ids)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
            self.assertEqual(
                server._payout_unfenced_append_inflight_stamps, {}
            )

        submitblock_calls: list[object] = []

        class RecordingSubmitRpc(TipRpc):
            def call(
                rpc_self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitblock_calls.append(params)
                    return None
                if method == "getblockhash":
                    return "dd" * 32
                return super().call(method, params)

        server.rpc = RecordingSubmitRpc("00" * 32)

        self.assertTrue(server.submit_next_block_candidate())

        # The bump already happened at commit time; the landing's epoch
        # fence observes it and, with the node offer already accepted,
        # accounts the as-issued coinbase (the durable predating share
        # rides the next window).
        self.assertEqual(submitblock_calls, [["00"]])
        # The landing observed the bump but the node already accepted
        # the block: accounting keeps the as-issued snapshot instead of
        # terminally abandoning a live block's payouts.
        self.assertEqual(len(bundle_builds), 1)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "stale_job_abandon_counts", {}).get(
                "append_epoch_stale", 0
            ),
            0,
        )
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertEqual(server.accepted_block_count, 1)

    def test_block_submit_defers_descendant_until_active_ancestor_is_durable(
        self,
    ) -> None:
        parent_hash = "ed" * 32
        ancestor_hash = "ee" * 32
        server, state, ledger = submit_coordinator(tip=parent_hash)
        server.max_blocks = 10
        server.stop_after_block = False
        ledger.durable_payout_state = True
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        context = server.jobs["job-1"]
        context.template["height"] = 12
        context.prior_balances = preview
        original_rpc_call = server.rpc.call
        submit_calls: list[str] = []

        def active_ancestor_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return ancestor_hash
            if method == "submitblock":
                submit_calls.append(method)
            return original_rpc_call(method, params)

        server.rpc.call = active_ancestor_call  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            ancestor_hash,
            block_height=10,
        )
        server._publish_accepted_block_payout_preview(ancestor_hash, preview)
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("descendant must wait for ancestor durability")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="ef" * 32,
            block_hex="00",
        )

        accepted = server.submit_block_candidate(
            block_candidate(server, state, submission)
        )

        self.assertFalse(accepted)
        # Node propagation is the fast lane: the durable descendant is offered
        # to qbitd before payout/accounting notices the ancestor transition.
        # Its accounting still defers until that ancestor is durable.
        self.assertEqual(submit_calls, ["submitblock"])
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        )
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertFalse(server.stop_event.is_set())

    def test_active_descendant_replay_stays_landed_until_ancestor_is_durable(
        self,
    ) -> None:
        parent_hash = "ea" * 32
        ancestor_hash = "eb" * 32
        descendant_hash = "ec" * 32
        server, state, ledger = submit_coordinator(tip=parent_hash)
        server.max_blocks = 10
        server.stop_after_block = False
        ledger.durable_payout_state = True
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        context = server.jobs["job-1"]
        context.template["height"] = 12
        context.prior_balances = preview
        original_rpc_call = server.rpc.call

        def active_descendant_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getbestblockhash":
                return descendant_hash
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return ancestor_hash
            if method == "submitblock":
                raise AssertionError("active descendant must not be resubmitted")
            return original_rpc_call(method, params)

        server.rpc.call = active_descendant_call  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            ancestor_hash,
            block_height=10,
        )
        server._publish_accepted_block_payout_preview(ancestor_hash, preview)
        server.build_audit_bundle = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError("active descendant must wait for ancestor durability")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=descendant_hash,
            block_hex="00",
        )

        accepted = server.submit_block_candidate(
            block_candidate(server, state, submission)
        )

        self.assertFalse(accepted)
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_LEDGER_CONFIRMATION_FAILED,
        )
        transition = server._accepted_block_payout_previews[descendant_hash]
        self.assertTrue(transition.landed)
        self.assertIsNone(transition.preview)
        self.assertFalse(server.stop_event.is_set())

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
        server.request_shutdown()
        writer.join(timeout=2)

    def test_job_snapshot_anchor_precedes_stamped_uncommitted_share(self) -> None:
        # A share is stamped accepted_at_ms at validation time, before its
        # group commit. Anchors chosen while that commit is pending must
        # predate the stamp, or the issued window would omit a share that a
        # later re-derivation at the same anchor includes.
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

        def submit() -> None:
            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        submitter = threading.Thread(target=submit)
        submitter.start()
        self.assertTrue(commit_started.wait(timeout=1))

        floor = dict(server._pending_share_commit_floor)
        self.assertEqual(len(floor), 1)
        (floor_entry,) = floor.values()
        stamped_ms = int(floor_entry[0].accepted_at_ms)
        self.assertEqual(
            server._job_snapshot_anchor_ms(stamped_ms + 60_000),
            stamped_ms - 1,
        )

        release_commit.set()
        submitter.join(timeout=2)
        self.assertFalse(submitter.is_alive())
        self.assertEqual(server._pending_share_commit_floor, {})
        # Drained floor: the anchor still sits strictly below the clamp
        # instant so a same-millisecond stamp can never tie it.
        self.assertEqual(
            server._job_snapshot_anchor_ms(stamped_ms + 60_000),
            stamped_ms + 60_000 - 1,
        )
        server.request_shutdown()
        writer.join(timeout=2)

    def test_share_batch_failure_still_restores_snapshot_anchor(self) -> None:
        server, _state, _ledger = submit_coordinator()
        entry = self._pending_append("aa", accepted_at_ms=2)
        server._ensure_pending_share_commit_state()
        with server._pending_share_commit_lock:
            server._pending_share_commit_floor[id(entry.pending_share)] = [
                entry.pending_share,
                time.monotonic(),
                False,
            ]
        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 1)

        class FailingLedger(FakeLedger):
            def append(self, pending: object) -> object:
                raise RuntimeError("ledger unavailable")

        server.ledger = FailingLedger()
        self.assertFalse(server._append_share_batch([entry]))
        self.assertTrue(entry.committed.is_set())
        self.assertEqual(server._pending_share_commit_floor, {})
        self.assertEqual(server._job_snapshot_anchor_ms(10_000), 9_999)

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
        server.request_shutdown()
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
            pending_share=SimpleNamespace(
                share_id="miner-a:" + "ee" * 32,
                job_issued_at_ms=0,
                accepted_at_ms=0,
            ),
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

    def test_replay_recovered_shares_commit_under_the_landing_fence(self) -> None:
        # Recovered rows reconstruct pre-crash timestamps, so they predate
        # live anchors. Each replay append must hold the landing fence across
        # the ledger commit and its epoch bump -- never bump after the row is
        # already durable, where a landing could verify the pre-bump epoch
        # and submit a coinbase omitting the replayed share.
        server, _state, ledger = submit_coordinator()
        anchor_token = server._expose_inflight_scan_anchor(12000)
        self.addCleanup(server._retire_inflight_scan_anchor, anchor_token)
        with tempfile.TemporaryDirectory() as tempdir:
            server.share_recovery_path = Path(tempdir) / "recovery.jsonl"
            server._recover_share_to_disk(self._pending_append("h1"), "test")

            fence_held_during_commit: list[bool] = []
            original_append = ledger.append

            def recording_append(pending: object) -> object:
                fence_held_during_commit.append(
                    server._payout_append_landing_fence_lock.locked()
                )
                return original_append(pending)

            ledger.append = recording_append  # type: ignore[method-assign]
            replayed = server.replay_recovered_shares()

        self.assertEqual(replayed, 1)
        self.assertEqual(fence_held_during_commit, [True])
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)

    def test_sync_append_commits_predating_row_under_the_landing_fence(
        self,
    ) -> None:
        # The synchronous (no-writer) append path takes the same fence
        # boundary as the group-commit writer for a row that predates a
        # live anchor.
        server, _state, ledger = submit_coordinator()
        anchor_token = server._expose_inflight_scan_anchor(12000)
        self.addCleanup(server._retire_inflight_scan_anchor, anchor_token)

        fence_held_during_commit: list[bool] = []
        original_append = ledger.append

        def recording_append(pending: object) -> object:
            fence_held_during_commit.append(
                server._payout_append_landing_fence_lock.locked()
            )
            return original_append(pending)

        ledger.append = recording_append  # type: ignore[method-assign]
        self.assertTrue(server._append_share_entry(self._pending_append("h2")))
        self.assertEqual(fence_held_during_commit, [True])
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)

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
            entries = []
            for tag in ("s1", "s2", "s3"):
                entry = self._pending_append(tag)
                entries.append(entry)
                server.enqueue_share_append(entry)

            server.request_shutdown()
            server.share_append_loop()

            # The compatibility ledger fails on the first row; the whole batch
            # is reported failed and no uncommitted share is called durable.
            self.assertEqual(append_calls, ["miner-a:s1"])
            self.assertEqual(ledger.pending, [])
            self.assertTrue(all(entry.committed.is_set() for entry in entries))
            self.assertTrue(all(entry.error is not None for entry in entries))
            self.assertFalse(server.share_recovery_path.exists())

    def test_landing_builds_audit_bundle_outside_balance_serializer(self) -> None:
        # The builder/verifier subprocess time must stay off the
        # payout-balance mutation lock: job delivery and reconciliation
        # queue behind that lock, and holding it across the bundle build was
        # the dominant term of the finalization delivery stall.
        server, state, ledger = submit_coordinator()
        server.stop_after_block = False
        server.max_blocks = 10
        lock_held_during_build: list[bool] = []
        build_calls: list[int] = []

        def probe_lock_from_other_thread() -> bool:
            # An RLock re-acquires freely on the owning thread, so the probe
            # must come from a thread that cannot be the owner.
            acquired: list[bool] = []

            def attempt() -> None:
                got = server._payout_balance_mutation_lock.acquire(
                    blocking=False
                )
                if got:
                    server._payout_balance_mutation_lock.release()
                acquired.append(not got)

            prober = threading.Thread(target=attempt)
            prober.start()
            prober.join(5)
            return bool(acquired and acquired[0])

        def fake_build_audit_bundle(**_kwargs: object) -> dict[str, object]:
            build_calls.append(1)
            lock_held_during_build.append(probe_lock_from_other_thread())
            return verified_block_bundle()

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "cc" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
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
        self.assertEqual(build_calls, [1])
        self.assertEqual(lock_held_during_build, [False])
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)

    def test_block_submitted_before_audit_bundle_build(self) -> None:
        # The audit build runs with the balance serializer released, but
        # never before submitblock: announcement of a freshly solved block
        # must not wait on audit construction (a lost race is a lost round).
        server, state, ledger = submit_coordinator()
        server.stop_after_block = False
        server.max_blocks = 10
        order: list[str] = []
        block_hash = "cc" * 32

        class OrderRecordingRpc(SubmitRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "submitblock":
                    order.append("submitblock")
                return super().call(method, params)

        def ordered_build(**_kwargs: object) -> dict[str, object]:
            order.append("build_audit_bundle")
            return verified_block_bundle()

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = OrderRecordingRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = ordered_build  # type: ignore[method-assign]
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
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
        self.assertEqual(order, ["submitblock", "build_audit_bundle"])
        self.assertEqual(len(ledger.persisted), 1)
        self.assertEqual(len(ledger.confirmed), 1)

    def test_successful_fast_lane_reconciles_observed_post_submit_tip(self) -> None:
        parent_hash = "00" * 32
        block_hash = "cd" * 32
        server, state, ledger = submit_coordinator(tip=parent_hash)
        server.stop_after_block = False
        server.max_blocks = 10
        server.rpc = SubmitAcceptingTemplateRpc(
            old_tip=parent_hash,
            block_hash=block_hash,
            ledger=ledger,
        )
        reconciled: list[tuple[str, bool]] = []
        landing_inputs: list[tuple[str, bool]] = []

        def reconcile(
            tip_hash: str,
            *,
            _coalesce_same_tip: bool = True,
        ) -> bool:
            reconciled.append((tip_hash, _coalesce_same_tip))
            return True

        original_land = server._land_and_confirm_block_candidate

        def observe_landing(
            candidate: PrismBlockCandidate,
            **kwargs: object,
        ) -> object:
            landing_inputs.append(
                (str(kwargs["current_tip"]), bool(kwargs["already_active"]))
            )
            return original_land(candidate, **kwargs)  # type: ignore[arg-type]

        server.ensure_reorg_reconciled_for_tip = reconcile  # type: ignore[method-assign]
        server._land_and_confirm_block_candidate = observe_landing  # type: ignore[method-assign]
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertTrue(accepted)
        self.assertEqual(landing_inputs, [(block_hash, False)])
        self.assertEqual(reconciled, [(block_hash, False)])

    def test_submitter_offers_block_before_writer_admission_with_rpc_deadline(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.block_submit_rpc_timeout_seconds = 0.75
        order: list[object] = []

        class TimeoutRecordingRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    order.append(("submitblock", timeout))
                    return None
                return super().call(method, params)

        class WriterAdmission:
            def __enter__(self) -> None:
                order.append("writer-admission")

            def __exit__(self, *_args: object) -> None:
                return None

        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="ce" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
        )
        server.rpc = TimeoutRecordingRpc("00" * 32)
        server._writer_operation = lambda _component: WriterAdmission()  # type: ignore[method-assign]

        def account(
            _candidate: PrismBlockCandidate,
            *,
            node_submission: object,
        ) -> bool:
            order.append("accounting")
            self.assertIsNone(getattr(node_submission, "result"))
            return True

        server._submit_next_block_candidate_writer = account  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(
            order,
            [("submitblock", 0.75), "writer-admission", "accounting"],
        )

    def test_block_submit_histogram_measures_landed_to_rpc_interval(self) -> None:
        # The race-critical span (candidate landed -> submitblock returned)
        # must be observed exactly once per attempted submit, independent of
        # the post-submit audit/persistence bookkeeping the outbox
        # created_at/completed_at interval also includes.
        old_tip = "00" * 32
        block_hash = "ab" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def build_fresh_job(client: ClientState, *, clean_jobs: bool) -> object:
            return prism_context(
                "fresh-job",
                block_hash,
                worker=state.worker,
                difficulty=Decimal("1"),
                clean_jobs=clean_jobs,
            )

        server.build_job_for_client = build_fresh_job  # type: ignore[method-assign]
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            share_pass=True,
            block_pass=True,
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitAcceptingTemplateRpc(
                old_tip=old_tip,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = lambda **_kwargs: verified_block_bundle()  # type: ignore[method-assign]
            server.verify_bundle = lambda *_args, **_kwargs: verified_audit_report()  # type: ignore[method-assign]

            with patch(
                "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                return_value=submission,
            ):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )
            self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(server.accepted_block_count, 1)
        metrics = "\n".join(server.block_submitter_metrics_lines())
        self.assertIn("qbit_prism_block_submit_seconds_count 1", metrics)
        self.assertIn(
            'qbit_prism_block_submit_seconds_bucket{le="+Inf"} 1',
            metrics,
        )
        # The candidate landed moments ago in this process, so the interval
        # must fall inside the finite buckets, not only the +Inf overflow.
        self.assertIn(
            'qbit_prism_block_submit_seconds_bucket{le="30"} 1',
            metrics,
        )

    def test_orphaned_block_candidate_keeps_share_credit(self) -> None:
        # Option-A semantics: a share that met its target stays credited even
        # when its block candidate loses the tip race in the submitter.
        old_tip = "00" * 32
        new_tip = "11" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            block_hash_hex="cc" * 32,
            block_hex="00",
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

        self.assertEqual(len(ledger.all_shares()), 1)
        # The tip moves before the submitter drains the candidate.
        server.rpc = RejectingSubmitTipRpc(new_tip)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(server.accepted_block_count, 0)
        # Counted as a block abandonment, not a share rejection.
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(
            server.stale_job_abandon_counts,
            {"tip_moved": 1, "balance_stale": 0, "append_epoch_stale": 0},
        )
        self.assertEqual(server.stale_share_count, 0)
        # The credited share survives the lost block race.
        self.assertEqual(len(ledger.all_shares()), 1)
        outbox_row = ledger._block_candidate_outbox[submission.block_hash_hex]
        self.assertEqual(outbox_row["state"], "abandoned")
        self.assertEqual(
            outbox_row["last_error"],
            f"tip moved before submit: {new_tip}",
        )
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="tip_moved"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="balance_stale"} 0',
            metrics,
        )

    def test_accepted_candidate_with_moved_tip_finalizes_as_submitted(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        block_hash = "cc" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("accepted-tip-race").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        submitted: list[str] = []
        abandoned: list[str] = []
        original_submitted = ledger.mark_block_candidate_submitted
        original_abandoned = ledger.mark_block_candidate_abandoned

        def mark_submitted(*, block_hash: str) -> bool:
            submitted.append(block_hash)
            return original_submitted(block_hash=block_hash)

        def mark_abandoned(*, block_hash: str, error: str) -> bool:
            abandoned.append(block_hash)
            return original_abandoned(block_hash=block_hash, error=error)

        ledger.mark_block_candidate_submitted = mark_submitted  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = mark_abandoned  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            block_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            block_hash,
            block_height=10,
        )
        with server.lock:
            # This is the process-local record written immediately before the
            # "qbit accepted direct PRISM block" log.
            server._accounted_accepted_block_hashes.add(block_hash)
        server.rpc = TipRpc(new_tip)
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("an accounted candidate must not be rebuilt")
            )
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(submitted, [block_hash])
        self.assertEqual(abandoned, [])
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )

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

        self.assertEqual(server.replay_pending_block_candidates(), 3)
        self.assertTrue(server.block_candidate_queue.empty())
        first = server._block_replay_candidate_queue.get_nowait()
        second = server._block_replay_candidate_queue.get_nowait()
        third = server._block_replay_candidate_queue.get_nowait()
        self.assertEqual(
            [
                first.submission.block_hash_hex,
                second.submission.block_hash_hex,
                third.submission.block_hash_hex,
            ],
            ["aa" * 32, "bb" * 32, "cc" * 32],
        )
        ledger.mark_block_candidate_submitted(block_hash="aa" * 32)
        ledger.mark_block_candidate_abandoned(block_hash="bb" * 32, error="stale")
        self.assertEqual(third.pending_share.share_id, "miner-a:" + "cc" * 32)

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

    def test_retryable_parent_stays_ahead_of_queued_child(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.block_candidate_retry_initial_seconds = 0
        server.block_candidate_retry_max_seconds = 0
        parent = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a1" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
        )
        child = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="b1" * 32,
                block_hex="01",
                share_pass=True,
                block_pass=True,
            ),
        )
        attempts: list[str] = []

        def submit(candidate: PrismBlockCandidate) -> bool:
            block_hash = str(candidate.submission.block_hash_hex)
            attempts.append(block_hash)
            if block_hash == "a1" * 32 and attempts.count(block_hash) == 1:
                server._abandon_block_candidate(
                    PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
                    "temporary parent finalization failure",
                    block_hash=block_hash,
                    worker="miner-a",
                )
                return False
            return True

        server.submit_block_candidate = submit  # type: ignore[method-assign]
        self.assertTrue(server.enqueue_block_candidate(parent))
        self.assertTrue(server.enqueue_block_candidate(child))

        self.assertTrue(server.submit_next_block_candidate())
        self.assertIs(server._retry_block_candidate, parent)
        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        self.assertTrue(server.submit_next_block_candidate())
        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
        self.assertEqual(server.block_candidate_queue.qsize(), 1)
        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(attempts, ["a1" * 32, "a1" * 32, "b1" * 32])
        self.assertTrue(server.block_candidate_queue.empty())

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

        self.assertEqual(len(waits), 6)
        for observed, expected in zip(
            waits,
            [0.1, 0.2, 0.25, 0.15, 0.25, 0.15],
            strict=True,
        ):
            self.assertAlmostEqual(observed, expected)
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
            block_hash = _candidate.submission.block_hash_hex
            block_height = int(_candidate.context.template["height"])
            server._begin_accepted_block_payout_preview(
                block_hash,
                block_height=block_height,
            )
            server._mark_accepted_block_payout_landed(
                block_hash,
                block_height=block_height,
            )
            server._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                "tip moved",
                block_hash=block_hash,
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = terminal  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertNotIn(candidate.submission.block_hash_hex, server.block_candidate_retry_delays)
        self.assertEqual(server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertNotIn(
            candidate.submission.block_hash_hex,
            server._accepted_block_payout_previews,
        )
        self.assertNotIn(
            candidate.submission.block_hash_hex,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_terminal_abandon_immediately_clears_payout_preview(self) -> None:
        server, _state, _ledger = submit_coordinator()
        block_hash = "a4" * 32
        server._begin_accepted_block_payout_preview(
            block_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            block_hash,
            block_height=10,
        )

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved",
            block_hash=block_hash,
            worker="miner-a",
        )

        self.assertFalse(accepted_race_won)
        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )
        # The tombstone protects pending durable replay, but unlike the leaked
        # landed transition it does not block payout reconciliation.
        with server._payout_balance_mutation():
            pass

    def test_tip_moved_abandon_yields_to_acceptance_during_cleanup(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        block_hash = "a5" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("accepted-during-clear").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )
        submitted: list[str] = []
        abandoned: list[str] = []
        original_submitted = ledger.mark_block_candidate_submitted
        original_abandoned = ledger.mark_block_candidate_abandoned
        original_clear = server._clear_accepted_block_payout_preview

        def mark_submitted(*, block_hash: str) -> bool:
            submitted.append(block_hash)
            return original_submitted(block_hash=block_hash)

        def mark_abandoned(*, block_hash: str, error: str) -> bool:
            abandoned.append(block_hash)
            return original_abandoned(block_hash=block_hash, error=error)

        def clear_while_accepting(
            candidate_hash: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            original_clear(
                candidate_hash,
                invalidate_published=invalidate_published,
            )
            if invalidate_published:
                with server.lock:
                    server._accounted_accepted_block_hashes.add(
                        candidate_hash.lower()
                    )

        ledger.mark_block_candidate_submitted = mark_submitted  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = mark_abandoned  # type: ignore[method-assign]
        server._clear_accepted_block_payout_preview = clear_while_accepting  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            block_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            block_hash,
            block_height=10,
        )
        server.rpc = RejectingSubmitTipRpc(new_tip)
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(submitted, [block_hash])
        self.assertEqual(abandoned, [])
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="tip_moved"} 0',
            metrics,
        )
        self.assertIn(
            'qbit_prism_stale_job_abandons_total{class="balance_stale"} 0',
            metrics,
        )
        self.assertEqual(ledger.pending_block_candidates(), [])
        outbox_row = ledger._block_candidate_outbox[block_hash]
        self.assertEqual(outbox_row["state"], "submitted")
        self.assertIsNone(outbox_row["last_error"])
        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_terminal_abandonment_keeps_tombstone_when_outbox_update_fails(
        self,
    ) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("terminal-outbox-failure").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a3" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        block_hash = candidate.submission.block_hash_hex
        block_height = int(candidate.context.template["height"])
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])

        def terminal(_candidate: PrismBlockCandidate) -> bool:
            server._begin_accepted_block_payout_preview(
                block_hash,
                block_height=block_height,
            )
            server._mark_accepted_block_payout_landed(
                block_hash,
                block_height=block_height,
            )
            server._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                "tip moved",
                block_hash=block_hash,
                worker="miner-a",
            )
            return False

        server.submit_block_candidate = terminal  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)
        with patch.object(
            ledger,
            "mark_block_candidate_abandoned",
            side_effect=RuntimeError("postgres unavailable"),
        ):
            self.assertTrue(server.submit_next_block_candidate())

        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertIn(block_hash, server._invalidated_accepted_block_payout_previews)
        self.assertEqual(
            [intent["block_hash_hex"] for intent in ledger.pending_block_candidates()],
            [block_hash],
        )

    def test_finalize_failure_replays_with_candidate_backoff(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("f1").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="f1" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        candidate_intent = server.block_candidate_intent(candidate)
        ledger.append_batch([(pending, candidate_intent)])
        server.block_candidate_retry_initial_seconds = 0.1
        server.block_candidate_retry_max_seconds = 0.4
        # The block itself lands once; only the terminal outbox update fails.
        # Replays must pace like any other candidate retry AND run finalize-
        # only, never re-entering submission.
        submit_calls = 0

        def accepting_submit(_candidate: PrismBlockCandidate) -> bool:
            nonlocal submit_calls
            submit_calls += 1
            return True

        server.submit_block_candidate = accepting_submit  # type: ignore[method-assign]
        original_finish = ledger.mark_block_candidate_submitted
        original_mark_attempted = ledger.mark_block_candidate_attempted
        finish_attempts = 0
        attempt_marks = 0

        def mark_attempted(*, block_hash: str) -> bool:
            nonlocal attempt_marks
            attempt_marks += 1
            return original_mark_attempted(block_hash=block_hash)

        def flaky_finish(*, block_hash: str) -> bool:
            nonlocal finish_attempts
            finish_attempts += 1
            if finish_attempts <= 4:
                raise RuntimeError("ledger unavailable")
            return original_finish(block_hash=block_hash)

        ledger.mark_block_candidate_attempted = mark_attempted  # type: ignore[method-assign]
        ledger.mark_block_candidate_submitted = flaky_finish  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            for _attempt in range(4):
                server.enqueue_block_candidate(candidate)
                self.assertTrue(server.submit_next_block_candidate())
                self.assertEqual(ledger.pending_block_candidates(), [candidate_intent])
            server.enqueue_block_candidate(candidate)
            self.assertTrue(server.submit_next_block_candidate())

        # The first accepted finalize failure returns unpaced so the caller
        # can refresh the fleet immediately; the ladder starts at the first
        # finalize-only replay.
        self.assertEqual(len(waits), 4)
        for observed, expected in zip(
            waits,
            [0.1, 0.2, 0.25, 0.15],
            strict=True,
        ):
            self.assertAlmostEqual(observed, expected)
        self.assertEqual(finish_attempts, 5)
        self.assertEqual(attempt_marks, 1)
        self.assertEqual(submit_calls, 1)
        self.assertNotIn(candidate.submission.block_hash_hex, server.block_candidate_retry_delays)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(server._block_candidate_finalize_retries, {})
        self.assertIn(
            "qbit_prism_block_candidate_retries_total 4",
            server.metrics_payload(),
        )

    def test_crash_after_submit_before_attempt_mark_replays_duplicate_once(self) -> None:
        parent_hash = "00" * 32
        block_hash = "cf" * 32
        ledger = SingleWriterShareLedger()
        first, state, _recording = submit_coordinator(tip=parent_hash)
        first.ledger = ledger
        first.stop_after_block = False
        first.max_blocks = 10
        pending = self._pending_append("submit-before-mark-crash").pending_share
        candidate = block_candidate(
            first,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        intent = first.block_candidate_intent(candidate)
        ledger.append_batch([(pending, intent)])

        class RestartAwareRpc(FakeRpc):
            def __init__(self) -> None:
                self.submit_results: list[object] = []

            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    result = None if not self.submit_results else "duplicate"
                    self.submit_results.append(result)
                    return result
                if method == "getbestblockhash":
                    return parent_hash
                if method == "getblockhash":
                    return block_hash
                if method == "getblockcount":
                    return 9
                return super().call(method, params)

        rpc = RestartAwareRpc()
        first.rpc = rpc
        original_mark = ledger.mark_block_candidate_attempted

        def crash_before_mark(*, block_hash: str) -> bool:
            raise SystemExit(f"simulated crash before marking {block_hash}")

        ledger.mark_block_candidate_attempted = crash_before_mark  # type: ignore[method-assign]
        first.enqueue_block_candidate(candidate)
        with self.assertRaisesRegex(SystemExit, "simulated crash"):
            first.submit_next_block_candidate()

        self.assertEqual(rpc.submit_results, [None])
        self.assertEqual(ledger.pending_block_candidates(), [intent])
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["attempt_count"],
            0,
        )

        ledger.mark_block_candidate_attempted = original_mark  # type: ignore[method-assign]
        persisted_calls = 0
        confirmed_calls = 0
        original_persist = ledger.persist_accepted_block
        original_confirm = ledger.confirm_accepted_block

        def persist_once(**kwargs: object) -> dict[str, object]:
            nonlocal persisted_calls
            persisted_calls += 1
            return original_persist(**kwargs)

        def confirm_once(**kwargs: object) -> dict[str, object]:
            nonlocal confirmed_calls
            confirmed_calls += 1
            return original_confirm(**kwargs)

        ledger.persist_accepted_block = persist_once  # type: ignore[method-assign]
        ledger.confirm_accepted_block = confirm_once  # type: ignore[method-assign]

        restarted, _restart_state, _recording = submit_coordinator(tip=parent_hash)
        restarted.ledger = ledger
        restarted.rpc = rpc
        restarted.stop_after_block = False
        restarted.max_blocks = 10
        with tempfile.TemporaryDirectory() as tempdir:
            restarted.audit_dir = Path(tempdir)
            restarted.evidence_path = Path(tempdir) / "evidence.json"
            restarted.ledger_writer_public_key_hex = "aa" * 32
            restarted.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            restarted.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )

            self.assertEqual(restarted.replay_pending_block_candidates(), 1)
            self.assertTrue(restarted.submit_next_block_candidate())
            self.assertEqual(restarted.replay_pending_block_candidates(), 0)

        self.assertEqual(rpc.submit_results, [None, "duplicate"])
        self.assertEqual(persisted_calls, 1)
        self.assertEqual(confirmed_calls, 1)
        self.assertEqual(restarted.accepted_block_count, 1)
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["state"],
            "submitted",
        )

    def test_abandon_finalize_failure_counts_one_abandonment(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        moved_tip = "11" * 32
        expected_error = f"tip moved before submit: {moved_tip}"
        pending = self._pending_append("f2").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="f2" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server.block_candidate_retry_initial_seconds = 0.1
        server.block_candidate_retry_max_seconds = 0.4
        submit_calls = 0

        def terminal_submit(_candidate: PrismBlockCandidate) -> bool:
            nonlocal submit_calls
            submit_calls += 1
            server._abandon_block_candidate(
                PRISM_REJECTION_STALE_JOB,
                expected_error,
                block_hash=_candidate.submission.block_hash_hex,
                worker="miner-a",
                stale_job_class="tip_moved",
            )
            return False

        server.submit_block_candidate = terminal_submit  # type: ignore[method-assign]
        original_finish = ledger.mark_block_candidate_abandoned
        finish_attempts = 0

        def flaky_finish(*, block_hash: str, error: str) -> bool:
            nonlocal finish_attempts
            finish_attempts += 1
            if finish_attempts <= 2:
                raise RuntimeError("ledger unavailable")
            return original_finish(block_hash=block_hash, error=error)

        ledger.mark_block_candidate_abandoned = flaky_finish  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            for _attempt in range(3):
                server.enqueue_block_candidate(candidate)
                self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(waits, [0.1, 0.2])
        self.assertEqual(submit_calls, 1)
        self.assertEqual(finish_attempts, 3)
        # Finalize-only replays must not recount the terminal abandonment.
        self.assertEqual(
            server.block_candidate_abandoned_counts,
            {PRISM_REJECTION_STALE_JOB: 1},
        )
        self.assertEqual(
            server.stale_job_abandon_counts,
            {"tip_moved": 1, "balance_stale": 0, "append_epoch_stale": 0},
        )
        self.assertEqual(ledger.pending_block_candidates(), [])
        outbox_row = ledger._block_candidate_outbox[
            candidate.submission.block_hash_hex
        ]
        self.assertEqual(outbox_row["state"], "abandoned")
        self.assertEqual(outbox_row["last_error"], expected_error)
        self.assertEqual(server._block_candidate_finalize_retries, {})

    def test_accepted_finalize_failure_still_triggers_post_accept_refresh(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        pending = self._pending_append("f3").pending_share
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="f3" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch([(pending, server.block_candidate_intent(candidate))])
        server.block_candidate_retry_initial_seconds = 0.1
        server.block_candidate_retry_max_seconds = 0.4
        server.submit_block_candidate = lambda _candidate: True  # type: ignore[method-assign]

        def failing_finish(*, block_hash: str) -> bool:
            raise RuntimeError("ledger unavailable")

        ledger.mark_block_candidate_submitted = failing_finish  # type: ignore[method-assign]
        refreshed_clients: list[object] = []
        server.refresh_jobs_after_pending_accepted_block = (  # type: ignore[method-assign]
            lambda client, **_kwargs: refreshed_clients.append(client) or 0
        )
        released_shares: list[object] = []
        original_release = server._finish_pending_share_commit

        def recording_release(pending_share: object) -> None:
            released_shares.append(pending_share)
            original_release(pending_share)

        server._finish_pending_share_commit = recording_release  # type: ignore[method-assign]
        waits: list[float] = []
        with patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda delay: waits.append(delay) or False,
        ):
            server.enqueue_block_candidate(candidate)
            self.assertTrue(server.submit_next_block_candidate())
            # The block is active on-chain; the fleet refresh fires on the
            # first finalize failure without waiting out a backoff, and the
            # snapshot anchor floor is released despite the pending outbox
            # mark.
            self.assertEqual(refreshed_clients, [state])
            self.assertEqual(waits, [])
            self.assertIn(pending, released_shares)
            self.assertTrue(server.submit_next_block_candidate())
            self.assertEqual(refreshed_clients, [state])
            self.assertEqual(waits, [0.1])

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
                # Malformed-row cleanup is lower-priority maintenance, so it
                # cannot form an N x database-timeout convoy ahead of valid
                # recovered blocks on the node-offer lane.
                self.assertTrue(
                    server._run_one_invalid_block_candidate_quarantine()
                )

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

    def test_block_submitter_honors_stop_after_block_above_one_block_capacity(
        self,
    ) -> None:
        server, state, ledger = submit_coordinator()
        server.accepted_block_count = 1
        server.max_blocks = 2
        server.stop_after_block = True
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex="de" * 32,
            block_hex="00",
        )

        accepted = server.submit_block_candidate(
            block_candidate(server, state, submission)
        )

        self.assertFalse(accepted)
        self.assertEqual(
            server.block_candidate_abandoned_counts[PRISM_REJECTION_POOL_CLOSED],
            1,
        )
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
        server._ensure_job_cache_state()
        server._ensure_tip_refresh_state()
        stale_bundle_key = ("stale-payout-bundle",)
        server._job_bundle_cache[stale_bundle_key] = object()  # type: ignore[assignment]
        active_fanout = _FanoutCancellation()
        server._active_tip_refresh = (  # type: ignore[assignment]
            SimpleNamespace(payout_state_generation=0),
            active_fanout,
        )
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
            # Deliberately use insertion order that differs from this fixture's
            # stand-in canonical serialization. The alternate builder creates
            # the requested output but writes pretty, noncanonical JSON, so
            # existence alone must not make the path eligible for persistence.
            alternate_bundle = {
                "signed_coinbase_manifest": {
                    "manifest": {
                        "coinbase_tx_hex": "c0ffee",
                        "payout_count": 1,
                    }
                },
                "payout_policy_manifest": {"accounts": []},
                "ledger_window_attestation": {"signature": {"public_key_hex": "aa" * 32}},
                "found_block": {"coinbase_value_sats": 50_00000000},
            }
            canonical_bytes = json.dumps(
                alternate_bundle,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            canonical_sha256 = hashlib.sha256(canonical_bytes).hexdigest()

            def fake_build_audit_bundle(**kwargs: object) -> dict[str, object]:
                build_kwargs.append(kwargs)
                output_path = kwargs["canonical_output_path"]
                assert isinstance(output_path, Path)
                output_path.write_text(
                    json.dumps(alternate_bundle, indent=2),
                    encoding="utf-8",
                )
                return alternate_bundle

            def fake_verify_bundle(bundle_path: Path, *_args: object, **_kwargs: object) -> dict[str, object]:
                candidate_bytes = bundle_path.read_bytes()
                self.assertEqual(json.loads(candidate_bytes), alternate_bundle)
                self.assertNotEqual(candidate_bytes, canonical_bytes)
                self.assertNotEqual(
                    hashlib.sha256(candidate_bytes).hexdigest(),
                    canonical_sha256,
                )
                return {
                    "coinbase_txid": "11" * 32,
                    "coinbase_manifest_sha256_hex": "22" * 32,
                    "audit_bundle_sha256_hex": canonical_sha256,
                    "coinbase_tx_hex": "c0ffee",
                }

            persist_accepted_block = ledger.persist_accepted_block

            def persist_with_canonicalization(**kwargs: object) -> dict[str, object]:
                self.assertIsNone(kwargs["canonical_bundle_path"])
                self.assertEqual(kwargs["final_bundle"], alternate_bundle)
                report = kwargs["audit_report"]
                assert isinstance(report, dict)
                self.assertEqual(report["audit_bundle_sha256_hex"], canonical_sha256)
                return persist_accepted_block(**kwargs)

            server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
            server.verify_bundle = fake_verify_bundle  # type: ignore[method-assign]
            ledger.persist_accepted_block = persist_with_canonicalization  # type: ignore[method-assign]
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
            self.assertTrue(
                server._ensure_shutdown_controller().writer_admission_closed()
            )

            live_files = sorted(Path(tempdir).glob("prism-live-audit-bundle-[0-9]*.json"))
            self.assertEqual(len(live_files), 1)
            self.assertEqual(list(Path(tempdir).glob("prism-live-audit-bundle-candidate-*.json")), [])
            self.assertEqual(list(Path(tempdir).glob(".prism-live-audit-bundle-candidate-*.tmp")), [])
            envelope = json.loads(live_files[0].read_text(encoding="utf-8"))
            self.assertEqual(envelope["schema"], "qbit.prism.live-audit-bundle-envelope.v1")
            self.assertEqual(envelope["block_hash"], block_hash)
            self.assertEqual(envelope["block_height"], 10)
            self.assertEqual(envelope["audit_bundle_sha256"], canonical_sha256)
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
        # This alternate test builder wrote noncanonical bytes to the requested
        # path. The verified content is valid, but the path is never claimed as
        # a byte-canonical artifact for ledger persistence.
        self.assertIsNone(ledger.persisted[0]["canonical_bundle_path"])
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._job_bundle_cache, {})
        self.assertTrue(active_fanout.is_set())
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_issued_preview_is_invalidated_when_final_coinbase_mismatches(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        block_hash = "cf" * 32
        server.rpc = SubmitRpc(tip="00" * 32, block_hash=block_hash, ledger=ledger)
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle("deadbeef")
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )
        candidate = block_candidate(server, state, submission)
        candidate.context.prospective_prior_balances = ()

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            self.assertFalse(server.submit_block_candidate(candidate))

        self.assertEqual(server._payout_state_generation, 2)
        self.assertEqual(server._accepted_block_payout_previews, {})
        self.assertEqual(ledger.persisted, [])
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_CANDIDATE_AUDIT_MISMATCH,
        )

    def test_accepted_block_persistence_allows_delivery_but_serializes_reconciliation(
        self,
    ) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        persist_started = threading.Event()
        persist_finished = threading.Event()
        release_persist = threading.Event()
        confirm_started = threading.Event()
        release_delivery = threading.Event()
        reconcile_lock_attempted = threading.Event()
        reconcile_mutated = threading.Event()
        event_lock = threading.Lock()
        events: list[str] = []
        original_persist = ledger.persist_accepted_block
        original_confirm = ledger.confirm_accepted_block

        def note(name: str) -> None:
            with event_lock:
                events.append(name)

        def blocking_persist(**kwargs: object) -> dict[str, object]:
            note("persist-start")
            persist_started.set()
            if not release_persist.wait(15):
                raise AssertionError("timed out waiting to release accepted-block persistence")
            result = original_persist(**kwargs)
            note("persist-end")
            persist_finished.set()
            return result

        def observed_confirm(**kwargs: object) -> dict[str, object]:
            note("confirm")
            confirm_started.set()
            return original_confirm(**kwargs)

        ledger.reorg_watch_blocks = lambda *, active_tip_height: [  # type: ignore[method-assign]
            {
                "block_height": active_tip_height - 2,
                "block_hash": "aa" * 32,
                "chain_state": "confirmed",
            }
        ]

        def mark_pool_block_inactive(**_kwargs: object) -> dict[str, object]:
            note("reconcile-mutation")
            reconcile_mutated.set()
            return {"backend": "fake", "inactive_count": 1}

        ledger.persist_accepted_block = blocking_persist  # type: ignore[method-assign]
        ledger.confirm_accepted_block = observed_confirm  # type: ignore[method-assign]
        ledger.mark_pool_block_inactive = mark_pool_block_inactive  # type: ignore[method-assign]
        accepted: list[bool] = []
        reconcile_results: list[dict[str, object]] = []
        errors: list[BaseException] = []
        delivery_admitted = threading.Event()
        delivery_thread: threading.Thread | None = None
        reconcile_thread: threading.Thread | None = None
        mutation_lock = server._payout_balance_mutation_lock

        class ObservedBalanceLock:
            def __enter__(self) -> ObservedBalanceLock:
                if threading.current_thread() is reconcile_thread:
                    reconcile_lock_attempted.set()
                mutation_lock.acquire()
                return self

            def __exit__(self, *_args: object) -> None:
                mutation_lock.release()

            # The landing's audit-build release window drives the serializer
            # through the plain lock interface as well.
            def acquire(self, *args: object, **kwargs: object) -> bool:
                return mutation_lock.acquire(*args, **kwargs)  # type: ignore[arg-type]

            def release(self) -> None:
                mutation_lock.release()

        server._payout_balance_mutation_lock = ObservedBalanceLock()  # type: ignore[assignment]

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "cd" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )
            candidate = block_candidate(server, state, submission)

            def submit() -> None:
                try:
                    accepted.append(server.submit_block_candidate(candidate))
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    errors.append(exc)

            def deliver() -> None:
                try:
                    with server._payout_state_delivery_gate.delivery():
                        note("replacement-delivery")
                        delivery_admitted.set()
                        if not release_delivery.wait(15):
                            raise AssertionError(
                                "timed out waiting to release payout delivery"
                            )
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    errors.append(exc)

            def reconcile() -> None:
                try:
                    reconcile_results.append(
                        server.reconcile_prism_pool_blocks_once(tip_hash=block_hash)
                    )
                except BaseException as exc:  # noqa: BLE001 - asserted below
                    errors.append(exc)

            submit_thread = threading.Thread(target=submit)
            submit_thread.start()
            confirmation_waited_for_delivery = False
            try:
                self.assertTrue(persist_started.wait(5))
                server.reorg_reconciler_enabled = True
                reconcile_thread = threading.Thread(target=reconcile)
                reconcile_thread.start()
                self.assertTrue(reconcile_lock_attempted.wait(5))
                self.assertFalse(reconcile_mutated.is_set())
                delivery_thread = threading.Thread(target=deliver)
                delivery_thread.start()
                admitted_while_persisting = delivery_admitted.wait(5)
                if admitted_while_persisting:
                    self.assertFalse(reconcile_mutated.is_set())
                    release_persist.set()
                    self.assertTrue(persist_finished.wait(5))
                    confirmation_waited_for_delivery = not confirm_started.wait(0.1)
            finally:
                release_persist.set()
                release_delivery.set()
                submit_thread.join(10)
                if delivery_thread is not None:
                    delivery_thread.join(10)
                if reconcile_thread is not None:
                    reconcile_thread.join(10)

            self.assertFalse(submit_thread.is_alive())
            self.assertIsNotNone(delivery_thread)
            self.assertFalse(delivery_thread.is_alive())
            self.assertIsNotNone(reconcile_thread)
            self.assertFalse(reconcile_thread.is_alive())
            if errors:
                raise errors[0]
            self.assertTrue(admitted_while_persisting)
            # Confirmation is durable catch-up to the already-published
            # prospective state and therefore does not wait on delivery.
            self.assertFalse(confirmation_waited_for_delivery)
            self.assertTrue(confirm_started.is_set())
            self.assertTrue(reconcile_mutated.is_set())
            self.assertEqual(accepted, [True])
            self.assertEqual(len(reconcile_results), 1)
            self.assertEqual(reconcile_results[0]["inactive_blocks"], 1)
            self.assertEqual(server._payout_state_generation, 2)
            self.assertLess(events.index("persist-start"), events.index("replacement-delivery"))
            self.assertLess(events.index("replacement-delivery"), events.index("persist-end"))
            self.assertLess(events.index("persist-end"), events.index("confirm"))
            self.assertLess(events.index("confirm"), events.index("reconcile-mutation"))

    def test_next_tip_preview_job_lands_after_parent_persistence(self) -> None:
        old_tip = "00" * 32
        parent_hash = "cd" * 32
        child_hash = "ce" * 32
        server, state, ledger = submit_coordinator(tip=old_tip)
        server._ensure_job_cache_state()
        ledger.durable_payout_state = True
        server.reorg_reconciler_enabled = False
        server.stop_after_block = False
        server.max_blocks = 10
        server.clients = {state}
        server.refresh_jobs_after_accepted_block = (  # type: ignore[method-assign]
            lambda **_kwargs: None
        )
        build_started = threading.Event()
        release_build = threading.Event()
        persist_started = threading.Event()
        release_persist = threading.Event()
        original_persist = ledger.persist_accepted_block
        original_confirm = ledger.confirm_accepted_block
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]

        def payout_bundle_payload() -> dict[str, object]:
            return {
                "found_block": {"coinbase_value_sats": 50_00000000},
                "ledger_window_attestation": {
                    "signature": {"public_key_hex": "aa" * 32}
                },
                "payout_policy_manifest": {
                    "accounts": [
                        {
                            "recipient_id": "miner-a",
                            "order_key": "miner-a",
                            "p2mr_program_hex": "11" * 32,
                            "gross_amount_sats": 25,
                            "prior_balance_sats": 0,
                            "candidate_balance_sats": 25,
                            "onchain_amount_sats": 0,
                            "carry_forward_balance_sats": 25,
                            "action": "accrued",
                        }
                    ]
                },
                "signed_coinbase_manifest": {
                    "manifest": {"coinbase_tx_hex": "c0ffee", "payout_count": 1}
                },
            }

        def blocking_payout_bundle(**_kwargs: object) -> dict[str, object]:
            if not build_started.is_set():
                build_started.set()
                if not release_build.wait(15):
                    raise AssertionError("timed out waiting to release parent bundle build")
            return payout_bundle_payload()

        def blocking_persist(**kwargs: object) -> dict[str, object]:
            if str(kwargs["block_hash"]).lower() == parent_hash:
                persist_started.set()
                if not release_persist.wait(15):
                    raise AssertionError("timed out waiting to release parent persistence")
            return original_persist(**kwargs)

        def expose_confirmed_preview(**kwargs: object) -> dict[str, object]:
            persisted = next(
                row
                for row in reversed(ledger.persisted)
                if str(row["block_hash"]).lower()
                == str(kwargs["block_hash"]).lower()
            )
            ledger.prior_balances = server._accepted_block_payout_preview_from_bundle(
                persisted["final_bundle"]  # type: ignore[arg-type]
            )
            return original_confirm(**kwargs)

        ledger.persist_accepted_block = blocking_persist  # type: ignore[method-assign]
        ledger.confirm_accepted_block = expose_confirmed_preview  # type: ignore[method-assign]

        class TwoBlockRpc:
            def __init__(self) -> None:
                self.tip = old_tip
                self.height = 9
                self.hashes = {9: old_tip}
                self.submitted: list[str] = []

            def call(self, method: str, params: object = None) -> object:
                if method == "getbestblockhash":
                    return self.tip
                if method == "getblockcount":
                    return self.height
                if method == "submitblock":
                    block_hex = str((params or [""])[0])  # type: ignore[index]
                    block_hash = parent_hash if block_hex == "00" else child_hash
                    self.height += 1
                    self.tip = block_hash
                    self.hashes[self.height] = block_hash
                    self.submitted.append(block_hash)
                    ledger.submit_seen = True
                    return None
                if method == "getblockhash":
                    return self.hashes[int((params or [0])[0])]  # type: ignore[index]
                raise RuntimeError(method)

        rpc = TwoBlockRpc()
        server.rpc = rpc
        server.build_audit_bundle = blocking_payout_bundle  # type: ignore[method-assign]
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        sent: list[dict[str, object]] = []
        state.send = lambda payload: sent.append(payload)  # type: ignore[method-assign]

        def build_child_job(client: ClientState, *, clean_jobs: bool) -> object:
            context = prism_context(
                "child-job",
                parent_hash,
                worker=client.worker,
                clean_jobs=clean_jobs,
            )
            context.template["height"] = 11
            context.prior_balances = server._prior_balances_for_job_parent(parent_hash)
            context.payout_state_generation = server._payout_state_generation
            return context

        server.build_job_for_client = build_child_job  # type: ignore[method-assign]
        parent_submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=parent_hash,
            block_hex="00",
        )
        parent_candidate = block_candidate(server, state, parent_submission)
        parent_candidate.context.prospective_prior_balances = (
            server._serialize_prior_balance_preview(preview)
        )
        parent_results: list[bool] = []
        parent_errors: list[BaseException] = []

        def submit_parent() -> None:
            try:
                parent_results.append(server.submit_block_candidate(parent_candidate))
            except BaseException as exc:  # noqa: BLE001 - asserted below
                parent_errors.append(exc)

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            parent_thread = threading.Thread(target=submit_parent)
            parent_thread.start()
            try:
                self.assertTrue(build_started.wait(5))
                self.assertEqual(ledger.current_prior_balances(), [])
                self.assertTrue(server.maybe_send_job(state, clean_jobs=True))
                child_context = state.active_job
                self.assertIsNotNone(child_context)
                assert child_context is not None
                self.assertTrue(parent_thread.is_alive())
                self.assertEqual(child_context.prior_balances, preview)
                self.assertEqual(
                    child_context.payout_state_generation,
                    server._payout_state_generation,
                )
                preview_generation = server._payout_state_generation
                self.assertEqual(sent[-1]["method"], "mining.notify")
                self.assertTrue(sent[-1]["params"][8])  # type: ignore[index]

                child_submission = SimpleNamespace(
                    header_hex="bb" * 80,
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=child_hash,
                    block_hex="01",
                    share_pass=True,
                    block_pass=True,
                )
                with patch(
                    "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
                    return_value=child_submission,
                ):
                    self.assertFalse(
                        server.handle_submit(
                            state,
                            ["miner-a", "child-job", "00" * 8, "00000001", "00000002"],
                        )
                    )
                self.assertEqual(server.block_candidate_queue.qsize(), 1)
                release_build.set()
                self.assertTrue(persist_started.wait(5))
                self.assertTrue(parent_thread.is_alive())
            finally:
                release_build.set()
                release_persist.set()
                parent_thread.join(10)

            self.assertFalse(parent_thread.is_alive())
            if parent_errors:
                raise parent_errors[0]
            self.assertEqual(parent_results, [True])
            self.assertEqual(ledger.current_prior_balances(), preview)
            self.assertEqual(server._payout_state_generation, preview_generation)
            self.assertTrue(server.submit_next_block_candidate())

        self.assertEqual(rpc.submitted, [parent_hash, child_hash])
        self.assertEqual(server.accepted_block_count, 2)
        self.assertEqual(len(ledger.persisted), 2)
        self.assertEqual(len(ledger.confirmed), 2)
        self.assertEqual(
            server.block_candidate_abandoned_counts.get(PRISM_REJECTION_STALE_JOB, 0),
            0,
        )
        self.assertEqual(server._accepted_block_payout_previews, {})

    def test_direct_block_preparation_does_not_hold_delivery_gate(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        entered = threading.Event()
        release = threading.Event()
        accepted: list[bool] = []
        errors: list[BaseException] = []
        block_hash = "cf" * 32
        original_persist = ledger.persist_accepted_block

        def blocking_persist(**kwargs: object) -> dict[str, object]:
            self.assertIsNone(
                server._payout_state_delivery_gate._mutation_owner
            )
            entered.set()
            if not release.wait(5):
                raise AssertionError("test did not release direct-block preparation")
            return original_persist(**kwargs)

        ledger.persist_accepted_block = blocking_persist  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            def submit() -> None:
                try:
                    accepted.append(
                        server.submit_block_candidate(
                            block_candidate(server, state, submission)
                        )
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    errors.append(exc)

            thread = threading.Thread(target=submit)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                with server._payout_state_delivery_gate.delivery_cancelable(
                    lambda: False,
                    generation=server._payout_state_generation,
                    priority=True,
                ) as admission:
                    self.assertTrue(admission)
                    release.set()
                    thread.join(5)
                    self.assertFalse(thread.is_alive())
                    self.assertTrue(
                        server._payout_state_prepare_lock.acquire(timeout=1)
                    )
                    server._payout_state_prepare_lock.release()
                    admission.mark_delivered()
            finally:
                release.set()
                thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(accepted, [True])
        self.assertEqual(server._payout_state_generation, 1)

    def test_failed_direct_block_audit_does_not_reserve_payout_source(self) -> None:
        for failure_phase in ("build", "verify"):
            with self.subTest(failure_phase=failure_phase):
                server, state, ledger = submit_coordinator()
                server._ensure_job_cache_state()
                initial_source = server._payout_state_source
                initial_published = server._published_payout_state
                block_hash = "ce" * 32
                server.rpc = SubmitRpc(
                    tip="00" * 32,
                    block_hash=block_hash,
                    ledger=ledger,
                )
                submission = SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                )

                with tempfile.TemporaryDirectory() as tempdir:
                    server.audit_dir = Path(tempdir)
                    server.ledger_writer_public_key_hex = "aa" * 32
                    if failure_phase == "build":
                        server.build_audit_bundle = (  # type: ignore[method-assign]
                            lambda **_kwargs: (_ for _ in ()).throw(
                                RuntimeError("audit reconstruction failed")
                            )
                        )
                    else:
                        server.build_audit_bundle = (  # type: ignore[method-assign]
                            lambda **_kwargs: verified_block_bundle()
                        )
                        server.verify_bundle = (  # type: ignore[method-assign]
                            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                                RuntimeError("audit verification failed")
                            )
                        )

                    with self.assertRaisesRegex(
                        RuntimeError,
                        "audit (reconstruction|verification) failed",
                    ):
                        server.submit_block_candidate(
                            block_candidate(server, state, submission)
                        )

                self.assertEqual(server._payout_state_source, initial_source)
                self.assertEqual(server._published_payout_state, initial_published)
                self.assertEqual(server._payout_state_generation, 0)
                self.assertEqual(ledger.persisted, [])

    def test_uncertain_direct_block_ledger_commit_fences_delivery(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        block_hash = "cd" * 32
        server.rpc = SubmitRpc(
            tip="00" * 32,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        ledger.confirm_accepted_block = (  # type: ignore[method-assign]
            lambda **_kwargs: (_ for _ in ()).throw(
                RuntimeError("ledger confirmation unavailable")
            )
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.ledger_writer_public_key_hex = "aa" * 32
            with self.assertRaisesRegex(
                RuntimeError,
                "ledger confirmation unavailable",
            ):
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )

        self.assertEqual(len(ledger.persisted), 1)
        # The prospective preview was source/generation 1; the uncertain
        # durable commit supersedes it with fenced source 2.
        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._payout_state_source[0], 2)
        self.assertEqual(server._published_payout_state.source_generation, 1)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)

    def test_uncertain_commit_supersedes_concurrently_published_source(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        block_hash = "cb" * 32
        newer_tip = "dc" * 32
        server.rpc = SubmitRpc(
            tip="00" * 32,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )

        def publish_newer_source_then_fail(**_kwargs: object) -> dict[str, object]:
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=newer_tip,
            )
            self.assertEqual(
                server._publish_payout_state_candidate(
                    server._current_payout_state_candidate()
                ),
                2,
            )
            raise RuntimeError("ledger confirmation unavailable")

        ledger.confirm_accepted_block = publish_newer_source_then_fail  # type: ignore[method-assign]
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.ledger_writer_public_key_hex = "aa" * 32
            with self.assertRaisesRegex(
                RuntimeError,
                "ledger confirmation unavailable",
            ):
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )

        self.assertEqual(server._payout_state_generation, 2)
        self.assertEqual(server._published_payout_state.source_generation, 2)
        self.assertEqual(server._payout_state_source[0], 3)
        self.assertEqual(server._payout_state_source[1], newer_tip)
        self.assertEqual(
            server._payout_state_source[2],
            "direct_block_uncertain",
        )
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)

    def test_post_confirm_publication_loss_completes_candidate_and_fences(self) -> None:
        # Once persist + confirm are durable, losing the forced payout
        # publication must not abort the candidate: the outbox row is marked
        # submitted and the success tail runs, while delivery stays fenced
        # until the scheduled refresh publishes the newest source.
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.max_blocks = 2
        server.stop_after_block = False
        block_hash = "e1" * 32
        server.rpc = SubmitRpc(
            tip="00" * 32,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        submitted: list[dict[str, object]] = []
        ledger.mark_block_candidate_submitted = (  # type: ignore[attr-defined]
            lambda **kwargs: submitted.append(kwargs) or True
        )
        real_confirm = ledger.confirm_accepted_block

        def confirm_then_reserve_newer_source(**kwargs: object) -> dict[str, object]:
            result = real_confirm(**kwargs)
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash="ee" * 32,
            )
            return result

        ledger.confirm_accepted_block = confirm_then_reserve_newer_source  # type: ignore[method-assign]
        server._publish_current_payout_state_with_retry_budget = (  # type: ignore[method-assign]
            lambda **_kwargs: None
        )
        submission = SimpleNamespace(
            coinbase_tx_hex="c0ffee",
            block_hash_hex=block_hash,
            block_hex="00",
        )

        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            self.assertTrue(
                server._submit_next_block_candidate_writer(
                    block_candidate(server, state, submission)
                )
            )

        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
        self.assertEqual(len(ledger.confirmed), 1)
        self.assertEqual(len(submitted), 1)
        self.assertEqual(submitted[0]["block_hash"], block_hash)
        self.assertEqual(server.accepted_block_count, 1)
        self.assertIn(block_hash, server._accounted_accepted_block_hashes)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)
        self.assertTrue(server.tip_refresh_is_pending())

        # The scheduled refresh publishes the pending source and reopens
        # delivery without any candidate replay.
        del server._publish_current_payout_state_with_retry_budget
        self.assertIsNotNone(server._publish_current_payout_state_with_retry_budget())
        self.assertFalse(server._payout_state_publication_blocked)
        self.assertFalse(server._payout_state_delivery_gate._delivery_blocked)

    def test_idempotent_direct_block_replay_skips_publication(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.max_blocks = 2
        server.stop_after_block = False
        block_hash = "d0" * 32
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.rpc = rpc
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )
            self.assertEqual(server._payout_state_generation, 1)
            self.assertEqual(
                server._published_payout_state.source_tip_hash,
                block_hash,
            )
            self.assertEqual(server._payout_state_source[0], 1)

            # Replay the durable candidate after its block landed, its
            # confirmation committed, and the network built on top of it.
            # qbit_confirm_pool_block reports confirmed_count=0 for an
            # already-confirmed block whose height no longer matches the
            # active tip — the sustained replay state of the post-block
            # livelock. The published payout state already covers the
            # candidate, so the replay must not reserve a source, bump the
            # generation, wipe the job-bundle cache, or schedule refresh
            # churn.
            child_tip = "d7" * 32

            class AncestorReplayRpc:
                def call(self, method: str, params: object = None) -> object:
                    if method == "getbestblockhash":
                        return child_tip
                    if method == "getblockheader":
                        if params != [block_hash]:
                            raise AssertionError(params)
                        return {"height": 10, "confirmations": 2}
                    if method == "getblockhash":
                        if params != [10]:
                            raise AssertionError(params)
                        return block_hash
                    if method == "getblockcount":
                        return 11
                    if method == "submitblock":
                        raise AssertionError(
                            "active ancestor must not be resubmitted"
                        )
                    raise RuntimeError(method)

            server.rpc = AncestorReplayRpc()
            ledger.pool_block_state = (  # type: ignore[attr-defined]
                lambda **_kwargs: {
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            )
            ledger.confirm_accepted_block = (  # type: ignore[method-assign]
                lambda **_kwargs: {"backend": "fake", "confirmed_count": 0}
            )
            retry_calls = 0

            def count_retry() -> None:
                nonlocal retry_calls
                retry_calls += 1

            server._schedule_tip_refresh_retry = count_retry  # type: ignore[method-assign]
            cache_key = ("sentinel",)
            server._job_bundle_cache[cache_key] = object()
            discarded_before = server.payout_state_candidates_discarded
            pending_marks_before = server._tip_refresh_pending_counter

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )

        self.assertEqual(server._payout_state_generation, 1)
        self.assertEqual(server._published_payout_state.source_generation, 1)
        self.assertEqual(server._published_payout_state.source_tip_hash, block_hash)
        self.assertEqual(server._payout_state_source[0], 1)
        self.assertIn(cache_key, server._job_bundle_cache)
        self.assertEqual(retry_calls, 0)
        self.assertEqual(
            server._tip_refresh_pending_counter,
            pending_marks_before,
        )
        self.assertEqual(
            server.payout_state_candidates_discarded,
            discarded_before,
        )
        self.assertFalse(server._payout_state_publication_blocked)
        self.assertFalse(server._payout_state_delivery_gate._delivery_blocked)

    def test_leaked_publication_fence_replay_republishes(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.max_blocks = 2
        server.stop_after_block = False
        block_hash = "d8" * 32
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )
            self.assertEqual(server._payout_state_generation, 1)

            # Simulate the exception tail a replay must heal: a prior attempt
            # force-blocked delivery while its source generation already
            # matched the published source, then failed before republishing.
            # The leaked fence blocks every job build until a publication
            # lands, so a confirmed_count=0 replay must not take the covered
            # skip here.
            server._block_payout_state_publication(force=True)
            self.assertTrue(server._payout_state_publication_blocked)
            self.assertEqual(
                server._payout_state_source[0],
                server._published_payout_state.source_generation,
            )
            child_tip = "d9" * 32

            class AncestorReplayRpc:
                def call(self, method: str, params: object = None) -> object:
                    if method == "getbestblockhash":
                        return child_tip
                    if method == "getblockheader":
                        if params != [block_hash]:
                            raise AssertionError(params)
                        return {"height": 10, "confirmations": 2}
                    if method == "getblockhash":
                        if params != [10]:
                            raise AssertionError(params)
                        return block_hash
                    if method == "getblockcount":
                        return 11
                    if method == "submitblock":
                        raise AssertionError(
                            "active ancestor must not be resubmitted"
                        )
                    raise RuntimeError(method)

            server.rpc = AncestorReplayRpc()
            ledger.pool_block_state = (  # type: ignore[attr-defined]
                lambda **_kwargs: {
                    "chain_state": "confirmed",
                    "maturity_state": "immature",
                }
            )
            ledger.confirm_accepted_block = (  # type: ignore[method-assign]
                lambda **_kwargs: {"backend": "fake", "confirmed_count": 0}
            )

            self.assertTrue(
                server.submit_block_candidate(
                    block_candidate(server, state, submission)
                )
            )

        self.assertEqual(server._payout_state_generation, 2)
        # Fence healing republishes identical covered state. It advances the
        # delivery generation without inventing a logical invalidation source.
        self.assertEqual(server._published_payout_state.source_generation, 1)
        self.assertEqual(server._payout_state_source[0], 1)
        self.assertFalse(server._payout_state_publication_blocked)
        self.assertFalse(server._payout_state_delivery_gate._delivery_blocked)

    def test_direct_block_disabled_reconciler_bounds_publish_supersession(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.payout_reconcile_supersession_retries = 2
        block_hash = "d3" * 32
        real_publish = server._publish_payout_state_candidate
        publish_attempts = 0

        def supersede_before_publish(candidate: object) -> int | None:
            nonlocal publish_attempts
            publish_attempts += 1
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=f"{publish_attempts + 20:064x}",
            )
            return real_publish(candidate)  # type: ignore[arg-type]

        server._publish_payout_state_candidate = supersede_before_publish  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertTrue(accepted)
        self.assertEqual(publish_attempts, 3)
        self.assertEqual(server._payout_state_generation, 0)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)
        self.assertTrue(server.tip_refresh_is_pending())

    def test_untrusted_direct_block_reconcile_publishes_newer_source_once(self) -> None:
        server, state, ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.reorg_reconciler_enabled = True
        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            lambda _tip, *, _coalesce_same_tip: not _coalesce_same_tip
        )
        server.qbit_chain_view_untrusted = lambda: True  # type: ignore[method-assign]
        block_hash = "d1" * 32
        newer_tip = "d2" * 32

        def superseding_noop_confirmation(**_kwargs: object) -> dict[str, object]:
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=newer_tip,
            )
            return {"backend": "fake", "confirmed_count": 0}

        ledger.confirm_accepted_block = superseding_noop_confirmation  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.rpc = SubmitRpc(
                tip="00" * 32,
                block_hash=block_hash,
                ledger=ledger,
            )
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            submission = SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
            )

            accepted = server.submit_block_candidate(
                block_candidate(server, state, submission)
            )

        self.assertTrue(accepted)
        self.assertEqual(server._payout_state_generation, 2)
        self.assertEqual(server._published_payout_state.source_tip_hash, newer_tip)
        self.assertEqual(server.payout_state_candidates_discarded, 0)
        self.assertEqual(server.reorg_reconcile_skip_count, 1)

    def test_verified_canonical_bundle_path_requires_exact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            candidate_path = Path(tempdir) / "candidate.json"
            candidate_bytes = b'{"canonical":true}'
            candidate_path.write_bytes(candidate_bytes)
            canonical_sha256 = hashlib.sha256(candidate_bytes).hexdigest()

            self.assertEqual(
                PrismCoordinator.verified_canonical_bundle_path(
                    candidate_path,
                    {"audit_bundle_sha256_hex": canonical_sha256.upper()},
                ),
                candidate_path,
            )
            self.assertIsNone(
                PrismCoordinator.verified_canonical_bundle_path(
                    candidate_path,
                    {"audit_bundle_sha256_hex": "00" * 32},
                )
            )

    def test_active_ancestor_candidate_resumes_full_finalization_without_resubmit(self) -> None:
        server, state, ledger = submit_coordinator()
        server.max_blocks = 10
        server.stop_after_block = False
        refreshes: list[str] = []
        server.refresh_jobs_after_accepted_block = (  # type: ignore[method-assign]
            lambda **kwargs: refreshes.append(str(kwargs["block_hash"]))
        )
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            block_hash = "ac" * 32

            class ActiveAncestorRpc:
                def __init__(self) -> None:
                    self.revalidated_heights: list[int] = []

                def call(self, method: str, params: object = None) -> object:
                    if method == "getbestblockhash":
                        return "ef" * 32
                    if method == "getblockheader":
                        self.assert_candidate(params)
                        return {"height": 10, "confirmations": 2}
                    if method == "getblockcount":
                        return 11
                    if method == "getblockhash":
                        if params != [10]:
                            raise AssertionError(params)
                        self.revalidated_heights.append(10)
                        return block_hash
                    if method == "submitblock":
                        raise AssertionError("active ancestor must not be resubmitted")
                    raise RuntimeError(method)

                @staticmethod
                def assert_candidate(params: object) -> None:
                    if params != [block_hash]:
                        raise AssertionError(params)

            active_rpc = ActiveAncestorRpc()
            server.rpc = active_rpc
            server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
                lambda _tip, *, _coalesce_same_tip: not _coalesce_same_tip
            )
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
            candidate = block_candidate(server, state, submission)

            self.assertTrue(server.submit_block_candidate(candidate))
            # Direct finalization records the refresh for its caller so slow
            # job fanout happens after writer admission has been released.
            self.assertEqual(
                state.post_accept_refresh_block,
                (10, block_hash),
            )
            server.refresh_jobs_after_pending_accepted_block(
                state,
                heartbeat_name="block_submitter",
            )
            # A failed outbox terminal update can replay A after later block B
            # changes global balances. Validate A against its height-bounded
            # active-chain view and suppress duplicate process-local accounting.
            ledger.durable_payout_state = True
            ledger.prior_balances = [
                {
                    "recipient_id": "later-miner",
                    "order_key": "later-miner",
                    "p2mr_program_hex": "22" * 32,
                    "balance_sats": 50,
                }
            ]
            ledger.pool_block_state = lambda **_kwargs: {  # type: ignore[attr-defined]
                "block_hash": block_hash,
                "block_height": 10,
                "parent_hash": "00" * 32,
                "chain_state": "confirmed",
                "maturity_state": "immature",
            }
            ledger.prior_balances_after_pool_block = (  # type: ignore[attr-defined]
                lambda **_kwargs: []
            )
            self.assertTrue(server.submit_block_candidate(candidate))

        self.assertEqual([row["block_hash"] for row in ledger.persisted], [block_hash] * 2)
        # This compatibility builder ignores canonical_output_path, so its
        # Python fallback remains verifier-only.
        self.assertIsNone(ledger.persisted[0]["canonical_bundle_path"])
        self.assertEqual(ledger.confirmed[0]["active_tip_height"], 10)
        self.assertEqual(ledger.confirmed[0]["block_hash"], block_hash)
        self.assertEqual(active_rpc.revalidated_heights, [10, 10])
        self.assertFalse(ledger.confirmed[0]["submit_seen_at_confirm"])
        # The share credit happens on the client thread at submit time now;
        # the block path itself appends nothing.
        self.assertEqual(len(ledger.pending), 0)
        self.assertFalse(server.stop_event.is_set())
        self.assertEqual(server.accepted_block_count, 1)
        self.assertEqual(refreshes, [block_hash])
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
            self.assertNotIn(
                "accepted_block_handling",
                server._ensure_shutdown_controller().snapshot()["active_writers"],
            )
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
                self.assertEqual(server.poll_qbit_tip_template_once(), 1)

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

    def test_post_accept_notification_does_not_run_failing_template_build(self) -> None:
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
        self.assertEqual(server.post_accept_refresh_failure_count, 0)
        self.assertEqual(state.active_job_ids, {"job-1"})
        self.assertIn("job-1", server.jobs)
        self.assertTrue(server.tip_refresh_is_pending())
        self.assertTrue(server._tip_refresh_retry.is_set())
        self.assertIn("qbit_prism_post_accept_refresh_failures_total 0", server.metrics_payload())

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
                self.assertEqual(server.poll_qbit_tip_template_once(), 1)

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
        server.lock = threading.RLock()
        server.stop_event = threading.Event()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 120.0
        server.watchdog_interval_seconds = 15.0
        return server

    def test_block_candidate_progress_stamps_only_on_submitter_thread(self) -> None:
        server = self._bare_coordinator()
        clock = {"now": 1000.0}
        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            server._record_heartbeat("block_submitter")
            clock["now"] += server.watchdog_timeout_seconds + 1.0
            self.assertEqual(
                server._overdue_heartbeats(clock["now"]), ["block_submitter"]
            )
            # Without a registered owner the helper never stamps.
            server._record_block_candidate_progress()
            self.assertEqual(
                server._overdue_heartbeats(clock["now"]), ["block_submitter"]
            )
            # The owner thread's stamps clear the overdue state.
            server._block_submitter_thread_ident = threading.get_ident()
            server._record_block_candidate_progress()
            self.assertEqual(server._overdue_heartbeats(clock["now"]), [])

    def test_client_thread_disposition_cannot_mask_frozen_submitter(self) -> None:
        server = self._bare_coordinator()
        clock = {"now": 1000.0}
        owner_ready = threading.Event()
        release_owner = threading.Event()

        def frozen_owner() -> None:
            server._block_submitter_thread_ident = threading.get_ident()
            server._record_heartbeat("block_submitter")
            owner_ready.set()
            release_owner.wait(5)

        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            owner = threading.Thread(target=frozen_owner)
            owner.start()
            try:
                self.assertTrue(owner_ready.wait(5))
                clock["now"] += server.watchdog_timeout_seconds + 1.0
                self.assertEqual(
                    server._overdue_heartbeats(clock["now"]), ["block_submitter"]
                )
                # A client connection thread reaching a disposition boundary
                # (synchronous below-target solve) must not refresh the
                # frozen dedicated thread's liveness budget.
                foreign = threading.Thread(
                    target=server._record_block_candidate_progress
                )
                foreign.start()
                foreign.join(5)
                self.assertEqual(
                    server._overdue_heartbeats(clock["now"]), ["block_submitter"]
                )
            finally:
                release_owner.set()
                owner.join(5)

    def test_wedged_evidence_read_stays_watchdog_eligible(self) -> None:
        server = self._bare_coordinator()
        clock = {"now": 1000.0}
        entered = threading.Event()
        release = threading.Event()

        def wedged_stats() -> tuple[int, int]:
            entered.set()
            if not release.wait(5):
                raise AssertionError("evidence read was not released")
            return (7, 3)

        server.accepted_share_stats = wedged_stats  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            server._record_heartbeat("block_submitter")
            reader = threading.Thread(target=server.accepted_share_stats)
            reader.start()
            try:
                self.assertTrue(entered.wait(5))
                clock["now"] += server.watchdog_timeout_seconds + 1.0
                # A blocked evidence read is deliberately NOT excused from
                # the liveness budget: exit-and-replay is the recovery path
                # for a genuinely wedged ledger read, and no disposition
                # code may pause the submitter's heartbeat to hide it.
                self.assertEqual(
                    server._overdue_heartbeats(clock["now"]), ["block_submitter"]
                )
                self.assertNotIn("block_submitter", server._watchdog_pauses)
            finally:
                release.set()
                reader.join(5)

    def test_metrics_include_accepted_stats_reconcile_status(self) -> None:
        server = self._bare_coordinator()
        server.ledger = SimpleNamespace(
            accepted_stats_reconcile_status=lambda: {
                "age_seconds": 12.5,
                "failures": 3,
            }
        )
        lines = server._accepted_stats_reconcile_metric_lines()
        self.assertIn(
            "qbit_prism_accepted_stats_reconcile_failures_total 3", lines
        )
        self.assertIn(
            "qbit_prism_accepted_stats_reconcile_age_seconds 12.500000", lines
        )
        # Ledgers without the accessor (file-backed, duck-typed) emit no
        # reconcile lines rather than failing the scrape.
        server.ledger = SimpleNamespace()
        self.assertEqual(server._accepted_stats_reconcile_metric_lines(), [])

    def test_positive_float_env_rejects_non_finite_values(self) -> None:
        for raw in ("nan", "inf", "-inf"):
            with self.subTest(raw=raw), patch.dict(
                os.environ, {"PRISM_WATCHDOG_TIMEOUT_SECONDS": raw}, clear=True
            ):
                with self.assertRaisesRegex(SystemExit, "PRISM_WATCHDOG_TIMEOUT_SECONDS must be finite"):
                    env_positive_float("PRISM_WATCHDOG_TIMEOUT_SECONDS", 120.0)

    def test_audit_verifier_subprocess_has_explicit_timeout(self) -> None:
        server = self._bare_coordinator()
        server.bundle_build_timeout_seconds = 0.25
        with patch(
            "lab.prism.prism_coordinator.subprocess.run",
            side_effect=subprocess.TimeoutExpired("audit-verify", 0.25),
        ) as run:
            with self.assertRaisesRegex(RuntimeError, "timed out after 0.25s"):
                server.verify_bundle(
                    Path("candidate.json"),
                    "00",
                    "11" * 32,
                    expected_coinbase_value_sats=1,
                )

        self.assertEqual(run.call_args.kwargs["timeout"], 0.25)

    def test_overdue_heartbeats_flags_only_stale_subsystems(self) -> None:
        server = self._bare_coordinator()
        server._record_heartbeat("stratum_accept")
        server._record_heartbeat("qbit_blockpoll")
        now = time.monotonic()

        self.assertEqual(server._overdue_heartbeats(now), [])

        with server._heartbeats_lock:
            server._heartbeats["qbit_blockpoll"] = now - 1_000.0

        self.assertEqual(server._overdue_heartbeats(now), ["qbit_blockpoll"])

    def test_overdue_submitter_heartbeat_names_the_stuck_phase(self) -> None:
        server = self._bare_coordinator()
        clock = {"now": 1_000.0}
        server._block_submitter_thread_ident = threading.get_ident()
        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            server._record_block_submitter_phase("replay-outbox-query")
            clock["now"] += server.watchdog_timeout_seconds + 1.0
            self.assertEqual(
                server._overdue_heartbeats(clock["now"]),
                ["block_submitter:replay-outbox-query"],
            )

    def test_sixty_second_attempt_mark_stall_does_not_delay_rpc_or_heartbeat(self) -> None:
        server, state, recording = submit_coordinator()
        entered_mark = threading.Event()
        release_mark = threading.Event()
        submitted = threading.Event()

        class StallingLedger(RecordingLedger):
            def __init__(self) -> None:
                super().__init__()
                self.mark_calls = 0

            def mark_block_candidate_attempted(self, *, block_hash: str) -> bool:
                self.mark_calls += 1
                entered_mark.set()
                release_mark.wait(60.0)
                return True

        class DeadlineRpc(SubmitRpc):
            def __init__(self, ledger: RecordingLedger) -> None:
                super().__init__(
                    tip="00" * 32,
                    block_hash="d2" * 32,
                    ledger=ledger,
                )
                self.timeouts: list[float | None] = []

            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    self.timeouts.append(timeout)
                    submitted.set()
                    if len(self.timeouts) > 1:
                        return "duplicate"
                return super().call(method, params)

        ledger = StallingLedger()
        server.ledger = ledger
        rpc = DeadlineRpc(ledger)
        server.rpc = rpc
        server.block_submit_rpc_timeout_seconds = 0.4
        server.block_submit_db_timeout_seconds = 0.05
        server.block_candidate_retry_initial_seconds = 0.01
        server.block_candidate_retry_max_seconds = 0.01
        server.watchdog_timeout_seconds = 0.2
        server._heartbeats = {}
        server._heartbeat_phases = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="d2" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
        )
        server.enqueue_block_candidate(candidate)
        started = time.monotonic()
        submitter = threading.Thread(target=server.block_submit_loop)
        with patch("builtins.print"):
            submitter.start()
            try:
                self.assertTrue(submitted.wait(2.0))
                self.assertLess(time.monotonic() - started, 2.0)
                self.assertTrue(entered_mark.wait(1.0))
                with server._heartbeats_lock:
                    first_heartbeat = server._heartbeats["block_submitter"]
                heartbeat_deadline = time.monotonic() + 1.0
                while time.monotonic() < heartbeat_deadline:
                    with server._heartbeats_lock:
                        latest_heartbeat = server._heartbeats["block_submitter"]
                    if latest_heartbeat > first_heartbeat:
                        break
                    time.sleep(0.01)
                self.assertGreater(latest_heartbeat, first_heartbeat)
                self.assertEqual(
                    server._overdue_heartbeats(time.monotonic()),
                    [],
                )
                self.assertEqual(ledger.mark_calls, 1)
                self.assertEqual(rpc.timeouts[0], 0.4)
            finally:
                server.stop_event.set()
                submitter.join(2.0)
                release_mark.set()
        self.assertFalse(submitter.is_alive())

    def test_accounting_retry_backoff_defers_instead_of_sleeping_under_admission(self) -> None:
        server, state, _recording = submit_coordinator()
        server.block_candidate_retry_initial_seconds = 30.0
        server.block_candidate_retry_max_seconds = 30.0

        class FailingMarkLedger(RecordingLedger):
            def mark_block_candidate_attempted(self, *, block_hash: str) -> bool:
                raise RuntimeError("attempt marker unavailable")

        server.ledger = FailingMarkLedger()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="ab" * 32,
                block_hex="ab",
                share_pass=True,
                block_pass=True,
            ),
        )
        block_hash = "ab" * 32
        server._block_accounting_thread_ident = threading.get_ident()
        server._block_accounting_holds_disposition = True
        started = time.monotonic()
        with patch("builtins.print"):
            handled = server._submit_next_block_candidate_writer(
                candidate,
                node_submission=SimpleNamespace(
                    attempted=False,
                    result=None,
                    error=None,
                ),
                disposition_held=True,
            )
        elapsed = time.monotonic() - started
        self.assertTrue(handled)
        # The 30s backoff must be recorded as a deadline, never slept while
        # the accounting thread holds admission and the disposition lease.
        self.assertLess(elapsed, 10.0)
        self.assertIs(
            getattr(server, "_block_accounting_deferred_retry_candidate", None),
            candidate,
        )
        with server.lock:
            deadline = server._block_candidate_retry_not_before.get(block_hash)
        self.assertIsNotNone(deadline)
        self.assertGreater(deadline, time.monotonic())

        # After the lease releases, the deferred candidate parks in the retry
        # slot and the dequeue honors the backoff deadline.
        server._block_accounting_holds_disposition = False
        with server.lock:
            server._block_accounting_deferred_retry_candidate = None
            server._merge_block_candidate_retry_locked(
                "_retry_block_candidate",
                candidate,
            )
        with patch("builtins.print"):
            self.assertFalse(
                server.submit_next_block_candidate(defer_accounting=True)
            )
        self.assertIs(getattr(server, "_retry_block_candidate", None), candidate)

        # Once the deadline passes the same candidate dequeues again.
        with server.lock:
            server._block_candidate_retry_not_before[block_hash] = (
                time.monotonic() - 1.0
            )
        with patch("builtins.print"):
            self.assertTrue(
                server.submit_next_block_candidate(defer_accounting=True)
            )

    def test_live_retry_reuses_definitive_node_acceptance_without_reoffer(self) -> None:
        parent_hash = "00" * 32
        block_hash = "ce" * 32
        moved_tip = "11" * 32
        ledger = SingleWriterShareLedger()
        server, state, _recording = submit_coordinator(tip=parent_hash)
        server.ledger = ledger
        server.stop_after_block = False
        server.max_blocks = 10
        server.block_candidate_retry_initial_seconds = 0.0
        server.block_candidate_retry_max_seconds = 0.0
        from lab.prism.share_ledger import PendingShare

        pending = PendingShare(
            share_id="miner-a:live-retry-duplicate",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=10,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=2,
            ntime=1_700_000_000,
        )
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        intent = server.block_candidate_intent(candidate)
        ledger.append_batch([(pending, intent)])

        expected_height = int(candidate.context.template["height"])
        header_state = {"confirmations": -1}

        class MovedTipRpc(FakeRpc):
            def __init__(self) -> None:
                self.submit_results: list[object] = []

            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    result = None if not self.submit_results else "duplicate"
                    self.submit_results.append(result)
                    return result
                if method == "getbestblockhash":
                    # The chain advances as soon as the first offer lands, so
                    # the in-process retry observes a moved tip.
                    return parent_hash if not self.submit_results else moved_tip
                if method == "getblockhash":
                    return block_hash
                if method == "getblockcount":
                    return 9
                if method == "getblockheader":
                    # Unprovable during the tip race (found, not yet active),
                    # then settled and active for the final pass.
                    return {
                        "height": expected_height,
                        "confirmations": header_state["confirmations"],
                    }
                return super().call(method, params)

        rpc = MovedTipRpc()
        server.rpc = rpc
        original_mark = ledger.mark_block_candidate_attempted
        mark_state = {"failures": 0}

        def flaky_mark(*, block_hash: str) -> bool:
            # Fail twice, starting at the first attempt marker after the
            # fresh node offer, mirroring statement timeouts under sustained
            # saturation. The retained acceptance must survive the failed
            # retry too, not just the first failure.
            if len(rpc.submit_results) == 1 and mark_state["failures"] < 2:
                mark_state["failures"] += 1
                raise RuntimeError("attempt marker timed out")
            return original_mark(block_hash=block_hash)

        ledger.mark_block_candidate_attempted = flaky_mark  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            server.ledger_writer_public_key_hex = "aa" * 32
            server.build_audit_bundle = (  # type: ignore[method-assign]
                lambda **_kwargs: verified_block_bundle()
            )
            server.verify_bundle = (  # type: ignore[method-assign]
                lambda *_args, **_kwargs: verified_audit_report()
            )
            with patch("builtins.print"):
                # First pass: fresh accept from the node, then the attempt
                # marker fails and the candidate is retained for an
                # in-process retry carrying the definitive node result.
                self.assertTrue(server.submit_next_block_candidate())
                self.assertEqual(rpc.submit_results, [None])
                self.assertEqual(
                    ledger._block_candidate_outbox[block_hash]["state"],
                    "pending",
                )
                # Retry 1 fails the marker again: the retained acceptance
                # must survive a consumed-and-failed retry, not just the
                # first failure.
                self.assertTrue(server.submit_next_block_candidate())
                self.assertEqual(rpc.submit_results, [None])
                self.assertEqual(
                    ledger._block_candidate_outbox[block_hash]["state"],
                    "pending",
                )
                # Retry 2: the stashed acceptance is still reused, so the
                # node is never asked again (no "duplicate" classification,
                # no chain probe reliance) and the landing tail finalizes
                # exactly as if the first pass had continued.
                self.assertTrue(server.submit_next_block_candidate())
        self.assertEqual(rpc.submit_results, [None])
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["state"],
            "submitted",
        )
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            getattr(server, "block_candidate_abandoned_counts", {}),
        )
        self.assertEqual(server.accepted_block_count, 1)

    def test_block_accounting_primary_queue_is_bounded_by_default(self) -> None:
        server, _state, _recording = submit_coordinator()
        server._ensure_block_accounting_state()
        # A bounded primary is what makes the documented result-preserving
        # spillover ordering reachable; the overflow queue stays unbounded.
        self.assertEqual(server._block_accounting_queue.maxsize, 8)
        self.assertEqual(server._block_accounting_overflow_queue.maxsize, 0)

    def test_accounting_saturation_does_not_convoy_node_offers(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 10
        server.stop_after_block = False
        server.block_candidate_retry_initial_seconds = 0.01
        server._block_accounting_queue = queue.PriorityQueue(maxsize=1)
        entered_accounting = threading.Event()
        release_accounting = threading.Event()
        submitted: list[str] = []

        class RecordingRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    submitted.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = RecordingRpc("00" * 32)

        def blocked_accounting(
            _candidate: PrismBlockCandidate,
            **_kwargs: object,
        ) -> bool:
            entered_accounting.set()
            release_accounting.wait(5)
            return True

        server._call_block_candidate_writer = blocked_accounting  # type: ignore[method-assign]
        for tag in ("a1", "b2", "c3", "d4"):
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=tag * 32,
                    block_hex=tag,
                    share_pass=True,
                    block_pass=True,
                ),
            )
            server.enqueue_block_candidate(candidate)

        submitter = threading.Thread(target=server.block_submit_loop)
        with patch("builtins.print"):
            submitter.start()
            try:
                self.assertTrue(entered_accounting.wait(1))
                deadline = time.monotonic() + 2
                while len(submitted) < 4 and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertEqual(submitted, ["a1", "b2", "c3", "d4"])
            finally:
                server.stop_event.set()
                release_accounting.set()
                submitter.join(2)
                accounting = getattr(server, "_block_accounting_thread", None)
                if accounting is not None:
                    accounting.join(2)
        self.assertFalse(submitter.is_alive())

    def test_replay_batch_reaches_node_while_oldest_accounting_stalls(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.max_blocks = 10
        server.stop_after_block = False
        submitted: list[str] = []
        release_accounting = threading.Event()
        entered_accounting = threading.Event()

        for index, tag in enumerate(("a5", "b6"), start=1):
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=tag * 32,
                    block_hex=tag,
                    share_pass=True,
                    block_pass=True,
                ),
            )
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
            candidate = dataclass_replace(candidate, pending_share=pending)
            ledger.append_batch(
                [(pending, server.block_candidate_intent(candidate))]
            )

        self.assertEqual(server.replay_pending_block_candidates(), 2)

        class RecordingRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    submitted.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = RecordingRpc("00" * 32)

        def blocked_accounting(
            _candidate: PrismBlockCandidate,
            **_kwargs: object,
        ) -> bool:
            entered_accounting.set()
            release_accounting.wait(5)
            return True

        server._call_block_candidate_writer = blocked_accounting  # type: ignore[method-assign]
        submitter = threading.Thread(target=server.block_submit_loop)
        with patch("builtins.print"):
            submitter.start()
            try:
                self.assertTrue(entered_accounting.wait(1))
                deadline = time.monotonic() + 2
                while len(submitted) < 2 and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertEqual(submitted, ["a5", "b6"])
            finally:
                server.stop_event.set()
                release_accounting.set()
                submitter.join(2)
                accounting = getattr(server, "_block_accounting_thread", None)
                if accounting is not None:
                    accounting.join(2)

    def test_startup_block_replay_timeout_starts_submitter_and_converges(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 10
        server.stop_after_block = False
        server.block_submit_db_timeout_seconds = 0.01
        # The startup enumeration is landing-class (issue #188 fix 4).
        server.block_landing_db_timeout_seconds = 0.01
        server.block_landing_db_timeout_max_seconds = 0.01
        server.block_candidate_retry_initial_seconds = 0.01
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        block_hash = "e5" * 32
        pending = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=1,
            accepted_at_ms=1,
            ntime=1,
        )
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="e5",
                share_pass=True,
                block_pass=True,
            ),
            pending_share=pending,
        )
        ledger.append_batch(
            [(pending, server.block_candidate_intent(candidate))]
        )

        query_started = threading.Event()
        release_query = threading.Event()
        query_calls = 0
        startup_call: list[object] = []
        original_pending_rows = ledger.pending_block_candidate_rows

        def slow_pending_rows(*, limit: int = 32) -> list[dict[str, object]]:
            nonlocal query_calls
            query_calls += 1
            if query_calls == 1:
                with server._block_submitter_ledger_calls_lock:
                    startup_call.append(
                        server._block_submitter_ledger_calls[
                            ("replay-outbox-query", 32)
                        ]
                    )
                query_started.set()
                if not release_query.wait(5):
                    raise AssertionError("timed out waiting to release outbox query")
            return original_pending_rows(limit=limit)

        ledger.pending_block_candidate_rows = slow_pending_rows  # type: ignore[method-assign]
        original_record_wait = server._record_block_submitter_wait
        loop_reuse_waiting = threading.Event()

        def observed_record_wait(phase: str) -> None:
            original_record_wait(phase)
            if (
                phase == "replay-outbox-query"
                and getattr(server, "_block_submitter_thread_ident", None)
                == threading.get_ident()
            ):
                loop_reuse_waiting.set()

        server._record_block_submitter_wait = observed_record_wait  # type: ignore[method-assign]

        submitted: list[str] = []

        class RecordingRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                if method == "submitblock":
                    submitted.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = RecordingRpc("00" * 32)
        accounted: list[str] = []

        def account_candidate(
            replayed: PrismBlockCandidate,
            **_kwargs: object,
        ) -> bool:
            replayed_hash = replayed.submission.block_hash_hex
            accounted.append(replayed_hash)
            ledger.mark_block_candidate_submitted(block_hash=replayed_hash)
            server.stop_event.set()
            return True

        server._call_block_candidate_writer = account_candidate  # type: ignore[method-assign]

        profile = SimpleNamespace(heartbeat_name="stratum_accept_default")
        listener_closed = threading.Event()
        listener = SimpleNamespace(close=listener_closed.set)
        server.listener_profiles = [profile]
        server.bind = "127.0.0.1"
        server.port = 0
        server.min_ready_miners = 1
        server.audit_bind = None
        server.audit_port = 0
        server.hot_path_log_enabled = False
        server.blockwait_enabled = False
        server.vardiff_idle_sweep_seconds = 0.0
        server.stratum_initial_job_timeout_seconds = 0.0
        server.watchdog_enabled = False
        server.watchdog_interval_seconds = 0.1
        server.template_refresh_failure_exit_seconds = 120.0
        server.coordination_blocked_exit_seconds = 120.0
        server.open_stratum_listeners = (  # type: ignore[method-assign]
            lambda _stack: [(listener, profile)]
        )
        server.validate_live_chain_identity = lambda: None  # type: ignore[method-assign]
        server.validate_live_template_and_fee_policy = lambda: None  # type: ignore[method-assign]
        server.prism_payout_policy = lambda: {}  # type: ignore[method-assign]
        server.prewarm_startup_jobs = lambda: None  # type: ignore[method-assign]
        server.watchdog_loop = lambda: None  # type: ignore[method-assign]
        server.blockpoll_loop = lambda: None  # type: ignore[method-assign]
        server.replay_recovered_shares = lambda: 0  # type: ignore[method-assign]
        server.share_append_loop = lambda: None  # type: ignore[method-assign]
        server.shutdown = lambda *, reason="graceful": True  # type: ignore[method-assign]

        def drain_threads(threads: list[tuple[threading.Thread, float]]) -> None:
            for thread, timeout in threads:
                thread.join(timeout)

        server.drain_non_writer_components = drain_threads  # type: ignore[method-assign]

        def accept_after_startup(_listener: object, _profile: object) -> None:
            self.assertTrue(loop_reuse_waiting.wait(2))
            self.assertIsNotNone(
                getattr(server, "_block_submitter_thread_ident", None)
            )
            self.assertTrue(query_started.wait(1))
            # The loop is waiting on the exact startup call while that worker
            # remains blocked; no replacement query was spawned.
            with server._block_submitter_ledger_calls_lock:
                self.assertIs(
                    server._block_submitter_ledger_calls.get(
                        ("replay-outbox-query", 32)
                    ),
                    startup_call[0],
                )
            self.assertEqual(query_calls, 1)
            release_query.set()
            deadline = time.monotonic() + 2
            while not accounted and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(accounted)

        server.accept_loop = accept_after_startup  # type: ignore[method-assign]

        try:
            with patch("builtins.print") as printed:
                server._serve_with_listener_stack(SimpleNamespace())  # type: ignore[arg-type]
        finally:
            server.stop_event.set()
            release_query.set()

        ledger.pending_block_candidate_rows = original_pending_rows  # type: ignore[method-assign]
        startup_logs = [
            " ".join(str(value) for value in call.args)
            for call in printed.call_args_list
        ]
        self.assertTrue(
            any(
                "prism coordinator: startup block candidate replay timed out "
                "phase=replay-outbox-query timeout=0.01s" in message
                for message in startup_logs
            )
        )
        self.assertTrue(query_started.is_set())
        self.assertTrue(listener_closed.is_set())
        self.assertEqual(submitted, ["e5"])
        self.assertEqual(accounted, [block_hash])
        self.assertEqual(ledger.pending_block_candidates(), [])

    def test_startup_block_replay_catches_psql_server_timeout(self) -> None:
        server, _state, _recording = submit_coordinator()
        ledger = PsqlShareLedger.__new__(PsqlShareLedger)
        ledger._command = ["psql"]
        ledger._native = None
        ledger._operation_timeout_local = threading.local()

        def timed_out_rows(*, limit: int = 32) -> list[dict[str, object]]:
            ledger._run_sql(f"SELECT {limit};")
            return []

        ledger.pending_block_candidate_rows = timed_out_rows  # type: ignore[method-assign]
        server.ledger = ledger
        completed = subprocess.CompletedProcess(
            args=["psql"],
            returncode=3,
            stdout="",
            stderr=(
                "ERROR:  57014: canceling statement due to statement timeout"
            ),
        )
        with patch(
            "lab.prism.share_ledger.subprocess.run",
            return_value=completed,
        ), patch("builtins.print") as printed:
            self.assertTrue(server._run_startup_block_candidate_replay())

        startup_logs = [
            " ".join(str(value) for value in call.args)
            for call in printed.call_args_list
        ]
        self.assertTrue(
            any(
                "startup block candidate replay timed out "
                "phase=replay-outbox-query" in message
                for message in startup_logs
            )
        )

    def test_startup_block_replay_keeps_hard_database_errors_fatal(self) -> None:
        server, _state, _recording = submit_coordinator()

        def failed_rows(*, limit: int = 32) -> list[dict[str, object]]:
            raise RuntimeError(f"postgres failed limit={limit}")

        server.ledger = SimpleNamespace(
            pending_block_candidate_rows=failed_rows,
        )
        with self.assertRaisesRegex(RuntimeError, "postgres failed"):
            server._run_startup_block_candidate_replay()

    def test_startup_block_replay_preserves_shutdown_stop(self) -> None:
        server = self._bare_coordinator()

        def shutdown_replay() -> int:
            raise ShutdownInProgress("shutdown won")

        server.replay_pending_block_candidates = shutdown_replay  # type: ignore[method-assign]

        self.assertFalse(server._run_startup_block_candidate_replay())

    def test_stuck_rpc_worker_pool_requests_nonzero_restart(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_stuck_call_exit_seconds = 0.04
        release = threading.Event()
        entered: list[str] = []

        class IgnoringRpc:
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                entered.append(str((params or [""])[0]))
                release.wait(5)
                return None

        server.rpc = IgnoringRpc()
        try:
            for tag in ("11", "22"):
                with self.assertRaises(TimeoutError):
                    server._run_submitblock_rpc_with_hard_deadline(
                        block_hash=tag * 32,
                        block_hex=tag,
                        timeout_seconds=0.01,
                    )
            time.sleep(0.05)
            with self.assertRaises(TimeoutError):
                server._run_submitblock_rpc_with_hard_deadline(
                    block_hash="33" * 32,
                    block_hex="33",
                    timeout_seconds=0.01,
                )
            self.assertEqual(entered, ["11", "22"])
            self.assertTrue(server.stop_event.is_set())
            self.assertTrue(server._fatal_exit_requested)
        finally:
            release.set()

    def test_block_landing_db_timeout_escalates_and_clears(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_landing_db_timeout_seconds = 30.0
        server.block_landing_db_timeout_max_seconds = 120.0
        block_hash = "ab" * 32
        self.assertEqual(server._block_landing_db_timeout(block_hash), 30.0)
        server._note_block_landing_timeout(block_hash)
        self.assertEqual(server._block_landing_db_timeout(block_hash), 60.0)
        server._note_block_landing_timeout(block_hash)
        self.assertEqual(server._block_landing_db_timeout(block_hash), 120.0)
        server._note_block_landing_timeout(block_hash)
        self.assertEqual(server._block_landing_db_timeout(block_hash), 120.0)
        self.assertEqual(server._block_landing_db_timeout("cd" * 32), 30.0)
        server._clear_block_candidate_retry_state(block_hash)
        self.assertEqual(server._block_landing_db_timeout(block_hash), 30.0)

    def test_landing_scope_uses_landing_budget_from_first_attempt(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        server.block_landing_db_timeout_seconds = 45.0
        server.block_landing_db_timeout_max_seconds = 120.0
        scopes: list[float] = []

        @contextlib.contextmanager
        def statement_timeout(seconds: float):
            scopes.append(seconds)
            yield

        server.ledger = SimpleNamespace(statement_timeout=statement_timeout)
        with server._block_landing_ledger_statement_timeout_scope("ab" * 32):
            pass
        self.assertEqual(scopes, [45.0])
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["calls_total"], 1)
        self.assertEqual(metrics["landing"]["timeouts_total"], 0)
        self.assertEqual(metrics["landing"]["last_budget_seconds"], 45.0)

    def test_landing_scope_timeout_escalates_next_attempt_budget(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_landing_db_timeout_seconds = 45.0
        server.block_landing_db_timeout_max_seconds = 120.0
        scopes: list[float] = []

        @contextlib.contextmanager
        def statement_timeout(seconds: float):
            scopes.append(seconds)
            yield

        server.ledger = SimpleNamespace(statement_timeout=statement_timeout)
        block_hash = "ab" * 32
        with self.assertRaises(TimeoutError):
            with server._block_landing_ledger_statement_timeout_scope(block_hash):
                raise LedgerOperationTimeout("postgres operation exceeded 45s")
        with server._block_landing_ledger_statement_timeout_scope(block_hash):
            pass
        self.assertEqual(scopes, [45.0, 90.0])
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["calls_total"], 2)
        self.assertEqual(metrics["landing"]["timeouts_total"], 1)
        self.assertEqual(metrics["landing"]["last_budget_seconds"], 90.0)

    def test_landing_scope_ignores_non_ledger_timeouts(self) -> None:
        """The scope guards a body that also runs node RPCs; an RPC timeout
        is not a database cancellation and must not escalate the next
        PostgreSQL budget or fire the landing-timeout alert."""
        server, _state, _recording = submit_coordinator()
        server.block_landing_db_timeout_seconds = 45.0
        server.block_landing_db_timeout_max_seconds = 120.0
        scopes: list[float] = []

        @contextlib.contextmanager
        def statement_timeout(seconds: float):
            scopes.append(seconds)
            yield

        server.ledger = SimpleNamespace(statement_timeout=statement_timeout)
        block_hash = "ab" * 32
        with self.assertRaises(TimeoutError):
            with server._block_landing_ledger_statement_timeout_scope(block_hash):
                raise TimeoutError("getblockhash exceeded 1s")
        with server._block_landing_ledger_statement_timeout_scope(block_hash):
            pass
        self.assertEqual(scopes, [45.0, 45.0])
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["calls_total"], 2)
        self.assertEqual(metrics["landing"]["timeouts_total"], 0)

    def test_fast_class_ledger_call_metrics_record_timeouts(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        release = threading.Event()

        def blocking_operation() -> None:
            release.wait(5)

        try:
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("metrics-key",),
                    "metrics-test",
                    blocking_operation,
                )
        finally:
            release.set()
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["fast"]["timeouts_total"], 1)
        self.assertEqual(metrics["fast"]["last_budget_seconds"], 0.01)

    def test_stuck_ledger_worker_pool_requests_nonzero_restart(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        server.block_submit_stuck_call_exit_seconds = 0.04
        release = threading.Event()
        entered: list[str] = []

        def blocking_operation(tag: str) -> str:
            entered.append(tag)
            release.wait(5)
            return tag

        try:
            for tag in ("one", "two"):
                with self.assertRaises(TimeoutError):
                    server._run_block_submitter_ledger_call(
                        (tag,),
                        f"test-{tag}",
                        lambda tag=tag: blocking_operation(tag),
                    )
            time.sleep(0.05)
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("three",),
                    "test-three",
                    lambda: blocking_operation("three"),
                )
            self.assertEqual(entered, ["one", "two"])
            self.assertTrue(server.stop_event.is_set())
            self.assertTrue(server._fatal_exit_requested)
        finally:
            release.set()

    def test_one_stuck_ledger_call_does_not_restart_with_spare_capacity(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        server.block_submit_stuck_call_exit_seconds = 0.04
        release = threading.Event()
        entered = threading.Event()

        def blocking_operation() -> None:
            entered.set()
            release.wait(5)

        try:
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("same-key",),
                    "same-key",
                    blocking_operation,
                )
            self.assertTrue(entered.wait(1))
            time.sleep(0.05)
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("same-key",),
                    "same-key",
                    blocking_operation,
                )
            self.assertFalse(server.stop_event.is_set())
            self.assertFalse(getattr(server, "_fatal_exit_requested", False))
        finally:
            release.set()

    def test_two_stuck_ledger_calls_restart_on_existing_key_retry(self) -> None:
        server, _state, _recording = submit_coordinator()
        server.block_submit_db_timeout_seconds = 0.01
        server.block_submit_stuck_call_exit_seconds = 0.04
        release = threading.Event()
        entered: list[str] = []

        def blocking_operation(tag: str) -> None:
            entered.append(tag)
            release.wait(5)

        try:
            for tag in ("one", "two"):
                with self.assertRaises(TimeoutError):
                    server._run_block_submitter_ledger_call(
                        (tag,),
                        tag,
                        lambda tag=tag: blocking_operation(tag),
                    )
            time.sleep(0.05)
            # No third key is required: a retry reusing either poisoned call
            # still observes that every bounded worker slot is exhausted.
            with self.assertRaises(TimeoutError):
                server._run_block_submitter_ledger_call(
                    ("one",),
                    "one",
                    lambda: blocking_operation("one"),
                )
            self.assertEqual(entered, ["one", "two"])
            self.assertTrue(server.stop_event.is_set())
            self.assertTrue(server._fatal_exit_requested)
        finally:
            release.set()

    def test_watchdog_starts_before_synchronous_startup_work(self) -> None:
        source = inspect.getsource(PrismCoordinator._serve_with_listener_stack)
        watchdog_start = source.index("target=self.watchdog_loop")

        self.assertEqual(source.count("target=self.watchdog_loop"), 1)
        self.assertLess(watchdog_start, source.index("self.prewarm_startup_jobs"))
        self.assertLess(watchdog_start, source.index("self.replay_recovered_shares"))

    def test_watchdog_hard_exits_when_fatal_stop_wins_during_startup(self) -> None:
        server = self._bare_coordinator()
        server._fatal_exit_requested = True
        server.stop_event.set()

        with patch("lab.prism.prism_coordinator.os._exit") as hard_exit:
            server.watchdog_loop()

        hard_exit.assert_called_once_with(1)

    def test_parent_retry_displacement_preserves_durable_descendant(self) -> None:
        server, state, _recording = submit_coordinator()

        def candidate_at(tag: str, height: int) -> PrismBlockCandidate:
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=tag * 32,
                    block_hex=tag,
                    share_pass=True,
                    block_pass=True,
                ),
            )
            context = SimpleNamespace(**vars(candidate.context))
            context.template = {**candidate.context.template, "height": height}
            return dataclass_replace(
                candidate,
                context=context,
                durable_replay=True,
            )

        descendant = candidate_at("b8", 11)
        parent = candidate_at("a8", 10)
        descendant_hash = descendant.submission.block_hash_hex
        server._ensure_block_candidate_disposition_state()
        server._ensure_block_replay_state()
        server._block_replay_inflight_hashes.add(descendant_hash)
        with server.lock:
            server._retry_block_candidate = descendant
            server._merge_block_candidate_retry_locked(
                "_retry_block_candidate",
                parent,
            )

        self.assertIs(server._retry_block_candidate, parent)
        self.assertIs(
            server._block_disposition_waiting_retries[descendant_hash],
            descendant,
        )

        # Once the parent reaches a terminal state, the descendant's preserved
        # wakeup is selected even though durable replay still deduplicates it.
        with server.lock:
            server._retry_block_candidate = None
        captured: list[object] = []
        server.max_blocks = 10
        server.stop_after_block = False
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=True,
                result="duplicate",
                error=None,
            )
        )
        server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
            lambda task: (captured.append(task), True)[1]
        )

        self.assertTrue(server.submit_next_block_candidate(defer_accounting=True))
        self.assertEqual(len(captured), 1)
        task = captured[0]
        self.assertIs(task.candidate, descendant)
        server._release_block_candidate_disposition(task.disposition_lease)

    def test_synchronous_waiter_joins_finalize_only_registry(self) -> None:
        server, state, _recording = submit_coordinator()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="c8" * 32,
                block_hex="c8",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        block_hash = candidate.submission.block_hash_hex
        server._block_candidate_finalize_retries = {}
        server._block_candidate_finalize_retries[block_hash] = (True, "")
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(
                AssertionError("finalize-only retry must not call qbitd")
            )
        )
        finalized: list[dict[str, object]] = []

        def finalize(
            _candidate: PrismBlockCandidate,
            **kwargs: object,
        ) -> bool:
            finalized.append(kwargs)
            return True

        server._finalize_block_candidate = finalize  # type: ignore[method-assign]

        self.assertTrue(server._submit_synchronous_block_candidate(candidate))
        self.assertEqual(len(finalized), 1)
        self.assertTrue(finalized[0]["accepted"])
        self.assertEqual(finalized[0]["block_hash"], block_hash)

    def test_synchronous_waiter_preserves_finalize_only_abandonment(self) -> None:
        server, state, _recording = submit_coordinator()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="c9" * 32,
                block_hex="c9",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        block_hash = candidate.submission.block_hash_hex
        server._block_candidate_finalize_retries = {
            block_hash: (False, "terminal rejection")
        }
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(
                AssertionError("finalize-only retry must not call qbitd")
            )
        )
        finalized: list[dict[str, object]] = []
        server._finalize_block_candidate = (  # type: ignore[method-assign]
            lambda _candidate, **kwargs: (finalized.append(kwargs), True)[1]
        )

        self.assertFalse(server._submit_synchronous_block_candidate(candidate))
        self.assertEqual(len(finalized), 1)
        self.assertFalse(finalized[0]["accepted"])
        self.assertEqual(finalized[0]["error"], "terminal rejection")

    def test_synchronous_abandon_rejects_prepared_state_before_outbox(self) -> None:
        server, state, ledger = submit_coordinator()
        block_hash = "ca" * 32
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="ca",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        events: list[str] = []
        prepared = {"value": True}

        def pool_block_state(*, block_hash: str) -> dict[str, str]:
            self.assertEqual(block_hash, candidate.submission.block_hash_hex)
            events.append("state")
            return {
                "chain_state": "prepared" if prepared["value"] else "rejected",
                "maturity_state": "immature",
            }

        def reject_prepared_block(**_kwargs: object) -> dict[str, object]:
            events.append("reject")
            prepared["value"] = False
            return {"backend": "fake", "rejected_count": 1}

        def mark_abandoned(**_kwargs: object) -> bool:
            events.append("abandon")
            return True

        ledger.pool_block_state = pool_block_state  # type: ignore[attr-defined]
        ledger.reject_prepared_block = reject_prepared_block  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = mark_abandoned  # type: ignore[attr-defined]

        class CleanupRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                return super().call(method, params)

        server.rpc = CleanupRpc("00" * 32)
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=False,
                result=None,
                error=None,
            )
        )
        server._mark_block_candidate_attempted = (  # type: ignore[method-assign]
            lambda _block_hash: True
        )

        def stage_terminal_rejection(
            _candidate: PrismBlockCandidate,
            *,
            node_submission: object,
        ) -> bool:
            self.assertIsNotNone(node_submission)
            outcome = server._block_candidate_outcome
            outcome.reason = PRISM_REJECTION_POOL_CLOSED
            outcome.error = "pool is no longer accepting blocks"
            outcome.stale_job_class = None
            return False

        server._submit_block_candidate_serialized = (  # type: ignore[method-assign]
            stage_terminal_rejection
        )

        self.assertFalse(server._submit_synchronous_block_candidate(candidate))
        self.assertEqual(events, ["state", "reject", "abandon"])
        self.assertEqual(
            server.block_candidate_abandoned_counts,
            {PRISM_REJECTION_POOL_CLOSED: 1},
        )
        self.assertFalse(server._block_candidate_terminal_outcome(block_hash))

    def test_synchronous_cleanup_failure_keeps_candidate_pending(self) -> None:
        server, state, ledger = submit_coordinator()
        block_hash = "cb" * 32
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex=block_hash,
                block_hex="cb",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        events: list[str] = []
        ledger.pool_block_state = (  # type: ignore[attr-defined]
            lambda *, block_hash: {
                "chain_state": "prepared",
                "maturity_state": "immature",
            }
        )

        def fail_reject(**_kwargs: object) -> dict[str, object]:
            events.append("reject")
            raise RuntimeError("postgres unavailable")

        def unexpected_abandon(**_kwargs: object) -> bool:
            events.append("abandon")
            return True

        ledger.reject_prepared_block = fail_reject  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = unexpected_abandon  # type: ignore[attr-defined]

        class CleanupRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount":
                    return 9
                return super().call(method, params)

        server.rpc = CleanupRpc("00" * 32)
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=False,
                result=None,
                error=None,
            )
        )
        server._mark_block_candidate_attempted = (  # type: ignore[method-assign]
            lambda _block_hash: True
        )

        def stage_terminal_rejection(
            _candidate: PrismBlockCandidate,
            *,
            node_submission: object,
        ) -> bool:
            self.assertIsNotNone(node_submission)
            outcome = server._block_candidate_outcome
            outcome.reason = PRISM_REJECTION_POOL_CLOSED
            outcome.error = "pool is no longer accepting blocks"
            outcome.stale_job_class = None
            return False

        server._submit_block_candidate_serialized = (  # type: ignore[method-assign]
            stage_terminal_rejection
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "could not reject prepared state for terminal candidate",
        ):
            server._submit_synchronous_block_candidate(candidate)

        self.assertEqual(events, ["reject"])
        self.assertIs(server._retry_block_candidate, candidate)
        self.assertEqual(server.block_candidate_abandoned_counts, {})
        self.assertIsNone(server._block_candidate_terminal_outcome(block_hash))
        self.assertEqual(
            server._block_candidate_outcome.reason,
            PRISM_REJECTION_BACKEND_RPC_UNAVAILABLE,
        )

    def test_restart_resubmit_honors_terminal_abandoned_outbox(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        block_hash = "e8" * 32
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex="e8",
            share_pass=False,
            block_pass=True,
        )
        first_pending = server.pending_share_from_submission(
            context=server.jobs["job-1"],
            submission=submission,
            ntime_hex="00000001",
        )
        first_candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=first_pending,
            credit_share_on_accept=True,
        )
        self.assertTrue(
            ledger.persist_block_candidate_intent(
                server.block_candidate_intent(first_candidate)
            )
        )
        self.assertTrue(
            ledger.mark_block_candidate_abandoned(
                block_hash=block_hash,
                error="terminal before restart",
            )
        )
        server._finish_pending_share_commit(first_pending)
        submitblock_calls: list[str] = []

        class NoResubmitRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    submitblock_calls.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = NoResubmitRpc("00" * 32)
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
        self.assertEqual(submitblock_calls, [])
        self.assertEqual(ledger._block_candidate_outbox[block_hash]["state"], "abandoned")
        self.assertEqual(len(ledger), 0)

    def test_restart_resubmit_coalesces_terminal_submitted_outbox(self) -> None:
        first_server, first_state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        first_server.ledger = ledger
        block_hash = "f8" * 32
        submission = SimpleNamespace(
            header_hex="ab" * 80,
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex="f8",
            share_pass=True,
            block_pass=True,
        )
        first_pending = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=7,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=12345,
            accepted_at_ms=1,
            ntime=1,
        )
        first_candidate = block_candidate(
            first_server,
            first_state,
            submission,
            pending_share=first_pending,
        )
        first_record = ledger.append_batch(
            [
                (
                    first_pending,
                    first_server.block_candidate_intent(first_candidate),
                )
            ]
        )[0]
        self.assertTrue(first_record.newly_inserted)
        self.assertTrue(
            ledger.mark_block_candidate_submitted(block_hash=block_hash)
        )

        # Model a clean restart: volatile duplicate/disposition state is gone,
        # while the ledger and terminal candidate outbox remain authoritative.
        server, state, _recording = submit_coordinator()
        server.ledger = ledger
        submitblock_calls: list[str] = []

        class NoResubmitRpc(TipRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
                *,
                timeout: float | None = None,
            ) -> object:
                if method == "submitblock":
                    submitblock_calls.append(str((params or [""])[0]))
                    return None
                return super().call(method, params)

        server.rpc = NoResubmitRpc("00" * 32)
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

        self.assertEqual(submitblock_calls, [])
        self.assertEqual(
            ledger._block_candidate_outbox[block_hash]["state"], "submitted"
        )
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger._shares[0].accepted_at_ms, 1)
        self.assertTrue(server.block_candidate_queue.empty())
        self.assertEqual(
            server.worker_share_counts["miner-a"],
            {"submitted": 1, "accepted": 0, "grace": 0},
        )

    def test_exact_share_replay_does_not_repeat_process_credit(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        block_hash = "d8" * 32
        submission = SimpleNamespace(
            coinbase_tx_hex="00",
            block_hash_hex=block_hash,
            block_hex="d8",
            share_pass=True,
            block_pass=True,
        )
        pending = PendingShare(
            share_id=f"miner-a:{block_hash}",
            miner_id="miner-a",
            order_key="miner-a",
            p2mr_program_hex="11" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id="job-1",
            job_issued_at_ms=12345,
            accepted_at_ms=12346,
            ntime=1,
        )
        candidate = block_candidate(
            server,
            state,
            submission,
            pending_share=pending,
        )
        intent = server.block_candidate_intent(candidate)
        worker_credits: list[str] = []
        vardiff_credits: list[str] = []
        server.note_worker_accepted_share = (  # type: ignore[method-assign]
            lambda worker, _policy: worker_credits.append(worker)
        )
        server.note_vardiff_accepted_share = (  # type: ignore[method-assign]
            lambda _client, job: vardiff_credits.append(job.job_id)
        )

        self.assertEqual(
            server.append_accepted_share(
                state,
                candidate.context,
                submission,
                pending,
                candidate_intent=intent,
            ),
            "pending",
        )
        self.assertEqual(
            server.append_accepted_share(
                state,
                candidate.context,
                submission,
                pending,
                candidate_intent=intent,
            ),
            "pending",
        )

        self.assertEqual(len(ledger), 1)
        self.assertEqual(worker_credits, ["miner-a"])
        self.assertEqual(vardiff_credits, ["job-1"])

    def test_same_hash_busy_lease_preserves_retry_behind_live_work(self) -> None:
        server, state, _recording = submit_coordinator()
        server.block_candidate_retry_initial_seconds = 0.0
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="a7" * 32,
                block_hex="a7",
                share_pass=True,
                block_pass=True,
            ),
        )
        block_hash = candidate.submission.block_hash_hex
        lease = server._claim_block_candidate_disposition(
            block_hash,
            blocking=True,
        )
        assert lease is not None
        server._retry_block_candidate = candidate
        server._ensure_block_replay_state()
        server._block_replay_inflight_hashes.add(block_hash)
        try:
            self.assertTrue(
                server.submit_next_block_candidate(defer_accounting=True)
            )
            self.assertIsNone(server._retry_block_candidate)
            self.assertIs(
                server._block_disposition_waiting_retries[block_hash],
                candidate,
            )
        finally:
            server._release_block_candidate_disposition(lease)

        captured: list[object] = []
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=True,
                result="duplicate",
                error=None,
            )
        )
        server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
            lambda task: (captured.append(task), True)[1]
        )
        self.assertTrue(server.submit_next_block_candidate(defer_accounting=True))
        self.assertEqual(len(captured), 1)
        task = captured[0]
        server._release_block_candidate_disposition(task.disposition_lease)

    def test_permanently_closed_pool_hands_outbox_to_accounting(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1
        server.accepted_block_count = 1
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="f2" * 32,
                block_hex="f2",
                share_pass=True,
                block_pass=True,
            ),
        )
        captured: list[object] = []
        server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
            lambda task: (captured.append(task), True)[1]
        )
        server.rpc = SimpleNamespace(
            call=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("closed pool must not call submitblock")
            )
        )
        server.enqueue_block_candidate(candidate)

        self.assertTrue(server.submit_next_block_candidate(defer_accounting=True))
        self.assertEqual(len(captured), 1)
        task = captured[0]
        self.assertFalse(task.node_submission.attempted)
        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
        server._release_block_candidate_disposition(task.disposition_lease)

    def test_accounted_block_replaces_its_capacity_reservation(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 2
        server.stop_after_block = False
        block_hash = "e2" * 32
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex=block_hash,
                block_hex="e2",
                share_pass=True,
                block_pass=True,
            ),
        )
        server._ensure_block_candidate_disposition_state()
        server._block_fast_lane_reservations.add(block_hash)
        server._land_and_confirm_block_candidate = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: (
                verified_block_bundle(),
                verified_audit_report(),
                {"persisted": True},
                {"confirmed_count": 1},
            )
        )
        server.accepted_share_stats = lambda: (0, 0)  # type: ignore[method-assign]
        with tempfile.TemporaryDirectory() as tempdir:
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            self.assertTrue(
                server._submit_block_candidate_serialized(
                    candidate,
                    node_submission=SimpleNamespace(
                        attempted=True,
                        result=None,
                        error=None,
                    ),
                )
            )

        self.assertEqual(server.accepted_block_count, 1)
        self.assertNotIn(block_hash, server._block_fast_lane_reservations)
        self.assertTrue(server._reserve_block_fast_lane_slot("e3" * 32))

    def test_synchronous_candidate_waits_for_fast_lane_capacity(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1
        server.stop_after_block = True
        reserved_hash = "e4" * 32
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex="e5" * 32,
                block_hex="e5",
                share_pass=False,
                block_pass=True,
            ),
            credit_share_on_accept=True,
        )
        server._ensure_block_candidate_disposition_state()
        self.assertTrue(server._reserve_block_fast_lane_slot(reserved_hash))
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(
                AssertionError("capacity-blocked candidate must not reach qbitd")
            )
        )

        with self.assertRaisesRegex(RuntimeError, "fast-lane capacity"):
            server._submit_synchronous_block_candidate(candidate)

        self.assertIs(server._retry_block_candidate, candidate)
        self.assertEqual(server._block_fast_lane_reservations, {reserved_hash})

    def test_node_offer_exception_retains_reserved_candidate(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1
        server.stop_after_block = True
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex="e6" * 32,
                block_hex="e6",
                share_pass=True,
                block_pass=True,
            ),
        )
        block_hash = candidate.submission.block_hash_hex
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: (_ for _ in ()).throw(
                RuntimeError("node offer bookkeeping failed")
            )
        )
        server.enqueue_block_candidate(candidate)

        with self.assertRaisesRegex(RuntimeError, "node offer bookkeeping failed"):
            server.submit_next_block_candidate(defer_accounting=True)

        self.assertIs(server._retry_block_candidate, candidate)
        self.assertIn(block_hash, server._block_fast_lane_reservations)
        self.assertFalse(server._reserve_block_fast_lane_slot("e7" * 32))
        lease = server._claim_block_candidate_disposition(
            block_hash,
            blocking=False,
        )
        self.assertIsNotNone(lease)
        assert lease is not None
        server._release_block_candidate_disposition(lease)

    def test_accounting_enqueue_exception_retains_reserved_candidate(self) -> None:
        server, state, _recording = submit_coordinator()
        server.max_blocks = 1
        server.stop_after_block = True
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="c0ffee",
                block_hash_hex="e8" * 32,
                block_hex="e8",
                share_pass=True,
                block_pass=True,
            ),
        )
        block_hash = candidate.submission.block_hash_hex
        server._node_submission_for_candidate = (  # type: ignore[method-assign]
            lambda _candidate: SimpleNamespace(
                attempted=True,
                result=None,
                error=None,
            )
        )
        server._enqueue_block_accounting_task = (  # type: ignore[method-assign]
            lambda _task: (_ for _ in ()).throw(
                RuntimeError("accounting enqueue failed")
            )
        )
        server.enqueue_block_candidate(candidate)

        with self.assertRaisesRegex(RuntimeError, "accounting enqueue failed"):
            server.submit_next_block_candidate(defer_accounting=True)

        self.assertIs(server._retry_block_candidate, candidate)
        self.assertIn(block_hash, server._block_fast_lane_reservations)
        self.assertFalse(server._reserve_block_fast_lane_slot("e9" * 32))
        lease = server._claim_block_candidate_disposition(
            block_hash,
            blocking=False,
        )
        self.assertIsNotNone(lease)
        assert lease is not None
        server._release_block_candidate_disposition(lease)

    def test_block_submitter_retry_wait_heartbeats_in_bounded_slices(self) -> None:
        server = self._bare_coordinator()
        server.watchdog_timeout_seconds = 0.3
        clock = {"now": 0.0}
        waits: list[float] = []
        overdue_samples: list[list[str]] = []

        class AdvancingStopEvent:
            def is_set(self) -> bool:
                return False

            def wait(self, timeout: float) -> bool:
                waits.append(timeout)
                clock["now"] += timeout
                overdue_samples.append(server._overdue_heartbeats(clock["now"]))
                return False

        server.stop_event = AdvancingStopEvent()  # type: ignore[assignment]
        with patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=lambda: clock["now"],
        ):
            self.assertFalse(server._wait_for_block_candidate_retry(1.0))

        self.assertEqual(waits, [0.25, 0.25, 0.25, 0.25])
        self.assertEqual(overdue_samples, [[], [], [], []])
        self.assertEqual(
            server._heartbeats["block_submitter"],
            1.0,
        )

    def test_blocked_candidate_phase_remains_watchdog_eligible(self) -> None:
        server, state, _recording = submit_coordinator()
        server._heartbeats = {}
        server._watchdog_pauses = {}
        server._heartbeats_lock = threading.Lock()
        server.watchdog_timeout_seconds = 0.05
        entered_submission = threading.Event()
        release_submission = threading.Event()
        candidate = block_candidate(
            server,
            state,
            SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="fa" * 32,
                block_hex="00",
                share_pass=True,
                block_pass=True,
            ),
        )

        def blocked_submission(_candidate: PrismBlockCandidate) -> bool:
            entered_submission.set()
            release_submission.wait(2)
            return True

        server.submit_block_candidate = blocked_submission  # type: ignore[method-assign]
        server.enqueue_block_candidate(candidate)
        server._record_heartbeat("block_submitter")
        submitter = threading.Thread(target=server.submit_next_block_candidate)
        submitter.start()
        try:
            self.assertTrue(entered_submission.wait(1))
            time.sleep(0.08)
            self.assertEqual(
                server._overdue_heartbeats(time.monotonic()),
                ["block_submitter"],
            )
        finally:
            release_submission.set()
            submitter.join(2)
        self.assertFalse(submitter.is_alive())

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
            def run_once(self, *, limit: int, progress_callback: object, **_kwargs: object) -> object:
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
                    yielded_to_tip_refresh=False,
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
            def run_once(self, *, limit: int, progress_callback: object, **_kwargs: object) -> object:
                clock["now"] += 102.0
                return SimpleNamespace(
                    scanned_count=200,
                    submitted_count=0,
                    updated_count=200,
                    failed_count=0,
                    yielded_to_tip_refresh=False,
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
            def run_once(self, *, limit: int, progress_callback: object, **_kwargs: object) -> object:
                entered_row.set()
                release_row.wait()
                return SimpleNamespace(
                    scanned_count=1,
                    submitted_count=0,
                    updated_count=1,
                    failed_count=0,
                    yielded_to_tip_refresh=False,
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

    def test_blockwait_advances_known_tip_before_notification_failure(self) -> None:
        server = self._bare_coordinator()
        tip_a = "aa" * 32
        tip_b = "bb" * 32
        server.rpc = SimpleNamespace(call=lambda method: tip_a)
        server.blockpoll_seconds = 1.0
        known_tips: list[str] = []
        detected_tips: list[str] = []

        def blockwait_once(known_tip: str) -> str:
            known_tips.append(known_tip)
            if len(known_tips) == 2:
                server.stop_event.set()
            return tip_b

        server.blockwait_once = blockwait_once  # type: ignore[method-assign]

        def observe_tip_for_refresh(tip_hash: str, **_kwargs: object) -> bool:
            detected_tips.append(tip_hash)
            if tip_hash == tip_b:
                raise TemplateRefreshBlocked("notification failed")
            return True

        def reject_premature_publication(*_args: object, **_kwargs: object) -> bool:
            raise AssertionError("blockwait must not publish submit authority")

        server.observe_tip_for_refresh = observe_tip_for_refresh  # type: ignore[method-assign]
        server.observe_tip_first_seen = reject_premature_publication  # type: ignore[method-assign]

        with patch("builtins.print"), patch(
            "lab.prism.prism_coordinator.traceback.print_exc"
        ), patch.object(
            server.stop_event,
            "wait",
            side_effect=lambda _timeout: server.stop_event.is_set(),
        ):
            server.blockwait_loop()

        self.assertEqual(known_tips, [tip_a, tip_b])
        self.assertEqual(detected_tips, [tip_a, tip_b])

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
        state.difficulty_generation = 7
        state.send = lambda payload: None  # type: ignore[method-assign]
        server.maybe_send_job = lambda client, *, clean_jobs: False  # type: ignore[method-assign]

        server.handle_suggest_difficulty(state, 9, [512])

        self.assertIsNone(state.pending_share_difficulty)
        self.assertEqual(state.share_difficulty, Decimal("1"))
        self.assertEqual(state.difficulty_generation, 7)

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

        def counting_send_job(current: ClientState, *, clean_jobs: bool) -> bool:
            send_job_calls.append(clean_jobs)
            current.active_job = SimpleNamespace(
                template={"previousblockhash": "aa" * 32},
                payout_state_generation=0,
            )
            server.current_tip_first_seen = ("aa" * 32, None)
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
            coinbase_manifest={},
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

    def test_below_target_accepted_tail_serializes_moved_tip_replay(self) -> None:
        old_tip = "00" * 32
        new_tip = "11" * 32
        block_hash = "b7" * 32
        server, state, _recording = submit_coordinator(tip=old_tip)
        server.max_blocks = 10
        server.stop_after_block = False
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        server.rpc = SubmitRpc(
            tip=old_tip,
            block_hash=block_hash,
            ledger=ledger,
        )
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        server.ledger_writer_public_key_hex = "aa" * 32
        server.refresh_jobs_after_pending_accepted_block = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: 0
        )
        submission = SimpleNamespace(
            header_hex="b7" * 80,
            coinbase_tx_hex="c0ffee",
            block_hex="00",
            block_hash_hex=block_hash,
            share_pass=False,
            block_pass=True,
        )

        durable_confirmation = threading.Event()
        accepted_tail_paused = threading.Event()
        release_accepted_tail = threading.Event()
        replay_guard_blocked = threading.Event()
        confirmation_calls: list[str] = []
        submitted: list[str] = []
        abandoned: list[str] = []
        synchronous_results: list[bool] = []
        replay_results: list[bool] = []
        errors: list[BaseException] = []
        replay_thread: threading.Thread | None = None
        original_confirm = ledger.confirm_accepted_block
        original_stats = server.accepted_share_stats
        original_submitted = ledger.mark_block_candidate_submitted
        original_abandoned = ledger.mark_block_candidate_abandoned

        class ObservedDispositionLock:
            def __init__(self) -> None:
                self.lock = threading.Lock()

            def acquire(
                self,
                blocking: bool = True,
                timeout: float = -1,
            ) -> bool:
                if threading.current_thread() is replay_thread:
                    if self.lock.acquire(blocking=False):
                        return True
                    # A failed non-blocking acquisition proves the accepted
                    # attempt still owns this exact same-hash guard.
                    replay_guard_blocked.set()
                if not blocking:
                    return self.lock.acquire(blocking=False)
                if timeout < 0:
                    return self.lock.acquire()
                return self.lock.acquire(timeout=timeout)

            def release(self) -> None:
                self.lock.release()

        server._ensure_block_candidate_disposition_state()
        with server._block_candidate_disposition_registry_lock:
            server._block_candidate_disposition_flights[block_hash] = (  # type: ignore[assignment]
                SimpleNamespace(lock=ObservedDispositionLock(), users=0)
            )

        def confirm_accepted_block(
            *,
            block_hash: str,
            active_tip_height: int,
        ) -> dict[str, int | str]:
            result = original_confirm(
                block_hash=block_hash,
                active_tip_height=active_tip_height,
            )
            confirmation_calls.append(block_hash)
            durable_confirmation.set()
            return result

        def pause_in_accepted_tail() -> tuple[int, int]:
            accepted_tail_paused.set()
            if not release_accepted_tail.wait(10):
                raise AssertionError("timed out waiting to release accepted success tail")
            return original_stats()

        def mark_submitted(*, block_hash: str) -> bool:
            submitted.append(block_hash)
            return original_submitted(block_hash=block_hash)

        def mark_abandoned(*, block_hash: str, error: str) -> bool:
            abandoned.append(block_hash)
            return original_abandoned(block_hash=block_hash, error=error)

        ledger.confirm_accepted_block = confirm_accepted_block  # type: ignore[method-assign]
        ledger.mark_block_candidate_submitted = mark_submitted  # type: ignore[method-assign]
        ledger.mark_block_candidate_abandoned = mark_abandoned  # type: ignore[method-assign]
        server.accepted_share_stats = pause_in_accepted_tail  # type: ignore[method-assign]

        def submit_synchronously() -> None:
            try:
                synchronous_results.append(
                    server.handle_submit(
                        state,
                        [
                            "miner-a",
                            "job-1",
                            "00" * 8,
                            "00000001",
                            "00000002",
                        ],
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - asserted below
                errors.append(exc)

        def replay_pending_candidate() -> None:
            try:
                replay_results.append(server.submit_next_block_candidate())
            except BaseException as exc:  # noqa: BLE001 - asserted below
                errors.append(exc)

        with tempfile.TemporaryDirectory() as tempdir, patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            server.audit_dir = Path(tempdir)
            server.evidence_path = Path(tempdir) / "evidence.json"
            synchronous_thread = threading.Thread(target=submit_synchronously)
            synchronous_thread.start()
            try:
                self.assertTrue(
                    accepted_tail_paused.wait(5),
                    msg=f"synchronous submit exited early: {errors!r}",
                )
                self.assertTrue(durable_confirmation.is_set())
                self.assertEqual(len(ledger), 1)
                self.assertEqual(len(ledger.pending_block_candidates()), 1)
                with server.lock:
                    self.assertNotIn(
                        block_hash,
                        server._accounted_accepted_block_hashes,
                    )

                self.assertEqual(server.replay_pending_block_candidates(), 1)
                server.rpc = TipRpc(new_tip)
                replay_thread = threading.Thread(target=replay_pending_candidate)
                replay_thread.start()
                self.assertTrue(replay_guard_blocked.wait(5))
                self.assertTrue(replay_thread.is_alive())
                self.assertEqual(submitted, [])
                self.assertEqual(abandoned, [])
            finally:
                release_accepted_tail.set()
                synchronous_thread.join(10)
                if replay_thread is not None:
                    replay_thread.join(10)

        self.assertFalse(synchronous_thread.is_alive())
        self.assertIsNotNone(replay_thread)
        assert replay_thread is not None
        self.assertFalse(replay_thread.is_alive())
        if errors:
            raise errors[0]
        self.assertEqual(synchronous_results, [False])
        self.assertEqual(replay_results, [True])
        self.assertEqual(confirmation_calls, [block_hash])
        self.assertGreaterEqual(len(submitted), 1)
        self.assertEqual(abandoned, [])
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger.pending_block_candidates(), [])
        self.assertEqual(server.accepted_block_count, 1)
        self.assertIn(block_hash, server._accounted_accepted_block_hashes)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertNotIn(block_hash, server._accepted_block_payout_previews)
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )
        self.assertEqual(server._block_candidate_disposition_flights, {})

    def test_below_target_intent_failure_does_not_create_unsafe_retry_slot(self) -> None:
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        submission = SimpleNamespace(
            header_hex="aa" * 80,
            coinbase_tx_hex="00",
            block_hex="00",
            block_hash_hex="be" * 32,
            share_pass=False,
            block_pass=True,
        )
        submit_calls = 0

        def fail_intent(_intent: dict[str, object]) -> bool:
            raise RuntimeError("outbox unavailable")

        def unsafe_submit(_candidate: PrismBlockCandidate) -> bool:
            nonlocal submit_calls
            submit_calls += 1
            return True

        ledger.persist_block_candidate_intent = fail_intent  # type: ignore[method-assign]
        server.submit_block_candidate = unsafe_submit  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
            return_value=submission,
        ):
            with self.assertRaisesRegex(RuntimeError, "outbox unavailable"):
                server.handle_submit(
                    state,
                    ["miner-a", "job-1", "00" * 8, "00000001", "00000002"],
                )

        self.assertEqual(submit_calls, 0)
        self.assertIsNone(getattr(server, "_retry_block_candidate", None))
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
                block_hash=_candidate.submission.block_hash_hex,
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
        self.assertIsNotNone(server._retry_block_candidate)
        assert server._retry_block_candidate is not None
        self.assertEqual(
            server._retry_block_candidate.submission.block_hash_hex,
            submission.block_hash_hex,
        )

    def test_post_block_notification_stamps_caller_heartbeat(self) -> None:
        server, _state, _ledger = submit_coordinator()
        seen: list[str] = []
        server._record_heartbeat = seen.append  # type: ignore[method-assign]
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

    def test_block_candidate_intent_round_trips_compact_payout_state(self) -> None:
        server, state, _ledger = submit_coordinator()
        server.jobs["job-1"].collection_only = True
        server.jobs["job-1"].prospective_prior_balances = (
            ("miner-a", "miner-a", "11" * 32, 25),
        )
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
        replayed = server.block_candidate_from_intent(intent)
        self.assertTrue(replayed.context.collection_only)
        self.assertEqual(
            replayed.context.prospective_prior_balances,
            (("miner-a", "miner-a", "11" * 32, 25),),
        )

        # Intents persisted before the flag existed replay as ready-window
        # candidates, which is all the outbox could ever have contained then.
        intent.pop("collection_only")
        intent.pop("prospective_prior_balances")
        replayed = server.block_candidate_from_intent(intent)
        self.assertFalse(replayed.context.collection_only)
        self.assertIsNone(replayed.context.prospective_prior_balances)

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


class LostAckSubmitRpc(FakeRpc):
    """qbitd accepts submitted blocks while every submitblock ack is lost.

    Models the mainnet 2026-07-25 interleaving: the block connects (the tip
    flips to the candidate hash, so blockwait reports it) but the RPC
    response never reaches the coordinator, and the retry disposition can
    then read a transient racing snapshot in which the tip has moved on and
    the header probe cannot prove the candidate active.
    """

    def __init__(self, *, start_tip: str, hash_by_hex: dict[str, str]) -> None:
        self.tip = start_tip
        self.height = 9
        self.hash_by_hex = dict(hash_by_hex)
        self.active: dict[str, int] = {}
        self.racing_tip: str | None = None
        self.getblockhash_override: str | None = None
        self.lose_acks = True
        self.submitblock_calls = 0

    def call(self, method: str, params: list[object] | None = None) -> object:
        if method == "getbestblockhash":
            return self.racing_tip or self.tip
        if method == "getblockcount":
            return self.height
        if method == "submitblock":
            self.submitblock_calls += 1
            block_hash = self.hash_by_hex[str((params or [""])[0])]
            if block_hash in self.active:
                return "duplicate"
            if self.racing_tip is not None:
                return "bad-prevblk"
            self.height += 1
            self.active[block_hash] = self.height
            self.tip = block_hash
            if self.lose_acks:
                raise OSError("connection reset by peer before submitblock ack")
            return None
        if method == "getblockheader":
            requested = str((params or [""])[0]).lower()
            if self.racing_tip is None and requested in self.active:
                block_height = self.active[requested]
                return {
                    "height": block_height,
                    "confirmations": self.height - block_height + 1,
                }
            raise RuntimeError("qbit RPC getblockheader failed: -5 Block not found")
        if method == "getblockhash":
            if self.getblockhash_override is not None:
                return self.getblockhash_override
            requested_height = int((params or [0])[0])
            for block_hash, block_height in self.active.items():
                if block_height == requested_height:
                    return block_hash
            raise RuntimeError(f"unknown height {requested_height}")
        return super().call(method, params)


class AcceptanceProbeRpc(FakeRpc):
    def __init__(
        self,
        *,
        tip: str,
        header: dict[str, object] | None = None,
        fail: bool = False,
        fail_tip: bool = False,
    ) -> None:
        self.tip = tip
        self.header = header
        self.fail = fail
        self.fail_tip = fail_tip

    def call(self, method: str, params: list[object] | None = None) -> object:
        if self.fail:
            raise RuntimeError("qbit RPC unavailable")
        if method == "getbestblockhash":
            if self.fail_tip:
                raise RuntimeError("qbit RPC getbestblockhash unavailable")
            return self.tip
        if method == "getblockheader":
            if self.header is None:
                raise RuntimeError("qbit RPC getblockheader failed: -5 Block not found")
            return self.header
        return super().call(method, params)


class PrismCoordinatorAcceptedBlockGapTests(unittest.TestCase):
    """Blockwait-first acceptance must survive every abandon-capable path.

    Regression coverage for the accepted-block blind spot left open by #89:
    the accepted registry was only written in the direct-accept submit tail,
    so a candidate whose submitblock ack was lost -- acceptance arriving via
    a blockwait tip observation instead -- could be terminally abandoned as
    stale-job by a retry that read one racing chain snapshot. The abandon
    withdrew the landed payout preview, fenced payout publication, and left
    template refreshes coordination-blocked until the watchdog restart.
    """

    def _accepted_tail_scaffolding(self, server: PrismCoordinator, tempdir: str) -> tuple[list[str], list[str]]:
        server._ensure_job_cache_state()
        server.reorg_reconciler_enabled = False
        server.block_candidate_retry_initial_seconds = 0.0
        server.audit_dir = Path(tempdir)
        server.evidence_path = Path(tempdir) / "evidence.json"
        server.ledger_writer_public_key_hex = "aa" * 32
        server.build_audit_bundle = (  # type: ignore[method-assign]
            lambda **_kwargs: verified_block_bundle()
        )
        server.verify_bundle = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: verified_audit_report()
        )
        submitted: list[str] = []
        abandoned: list[str] = []
        server.ledger.mark_block_candidate_submitted = (  # type: ignore[attr-defined]
            lambda **kwargs: submitted.append(str(kwargs["block_hash"])) or True
        )
        server.ledger.mark_block_candidate_abandoned = (  # type: ignore[attr-defined]
            lambda **kwargs: abandoned.append(str(kwargs["block_hash"])) or True
        )
        return submitted, abandoned

    def _age_retained_acceptance_past_window(
        self, server: PrismCoordinator
    ) -> None:
        """Backdate any stashed definitive ack past the acceptance window.

        These interleavings pin the terminal machinery that runs once
        first-party acceptance evidence has gone stale; a fresh stash
        correctly vetoes the terminal decision and defers instead (see
        test_definitive_ack_with_unprovable_chain_view_defers_not_abandons).
        """
        real_stash = server._stash_retained_block_candidate_node_submission

        def stash_then_age(block_hash: str, node_submission: object) -> None:
            real_stash(block_hash, node_submission)
            with server.lock:
                stamped = getattr(
                    server,
                    "_block_candidate_retained_submission_monotonic",
                    None,
                )
                if stamped and block_hash.lower() in stamped:
                    stamped[block_hash.lower()] -= 400.0

        server._stash_retained_block_candidate_node_submission = stash_then_age  # type: ignore[method-assign]

    def _retained_candidate(self, server: PrismCoordinator) -> PrismBlockCandidate:
        with server.lock:
            candidate = server._retry_block_candidate
            server._retry_block_candidate = None
        self.assertIsNotNone(candidate)
        return candidate

    def _chained_context(
        self,
        server: PrismCoordinator,
        *,
        job_id: str,
        parent_hash: str,
        height: int,
    ) -> None:
        base = server.jobs["job-1"]
        server.jobs[job_id] = SimpleNamespace(
            job=SimpleNamespace(
                job_id=job_id,
                share_target=base.job.share_target,
                share_difficulty=base.job.share_difficulty,
                transaction_hexes=(),
            ),
            template={
                "previousblockhash": parent_hash,
                "height": height,
                "coinbasevalue": 50_00000000,
            },
            found_block={"network_difficulty": 1},
            issued_at_ms=12345,
            collection_only=False,
            worker=base.worker,
            shares_json=[],
            prior_balances=[],
        )

    def test_blockwait_observed_acceptance_survives_stale_chain_probe(self) -> None:
        # The 2026-07-25 mainnet interleaving, end to end: lost submitblock
        # ack, blockwait reports the pool's own hash as the new tip, and the
        # retry disposition reads a racing snapshot in which the tip moved on
        # and the header probe cannot prove the candidate active. The
        # candidate must defer (never terminally abandon), keep its landed
        # payout preview, and finalize as submitted once the view settles --
        # with no "qbit accepted direct" tail having run before the defer.
        parent = "00" * 32
        block_hash = "b1" * 32
        racing_tip = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            server.rpc = rpc
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )

            # Attempt 1: submitblock lands the block but the ack is lost.
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(rpc.submitblock_calls, 1)
            self.assertIn(block_hash, server._accepted_block_payout_previews)
            self.assertTrue(
                server._accepted_block_payout_previews[block_hash].landed
            )
            candidate = self._retained_candidate(server)

            # Blockwait sees the pool's own hash as the new tip. This is the
            # only acceptance signal: no direct-submit ack ever arrived and
            # the accepted success tail has not run.
            self.assertTrue(server.observe_tip_for_refresh(block_hash))
            self.assertIn(block_hash, server._tip_observed_accepted_block_hashes)
            self.assertNotIn(block_hash, server._accounted_accepted_block_hashes)

            # Attempt 2: the disposition reads a racing snapshot -- tip moved
            # past the candidate and the header probe reports not-found. #89
            # abandoned here; the observation evidence must defer instead.
            rpc.racing_tip = racing_tip
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertNotIn(
                PRISM_REJECTION_STALE_JOB,
                getattr(server, "block_candidate_abandoned_counts", {}),
            )
            self.assertEqual(abandoned, [])
            self.assertEqual(server.block_candidate_accept_pending_defer_count, 1)
            # The landed preview is preserved -- not withdrawn, not
            # tombstoned -- so payout publication stays unfenced and the
            # reconcile/template path stays unblocked.
            self.assertIn(block_hash, server._accepted_block_payout_previews)
            self.assertTrue(
                server._accepted_block_payout_previews[block_hash].landed
            )
            self.assertNotIn(
                block_hash,
                server._invalidated_accepted_block_payout_previews,
            )
            self.assertFalse(server._payout_state_publication_blocked)
            candidate = self._retained_candidate(server)

            # Attempt 3: the chain view settles with the pool's own block as
            # the tip; the resume path runs the full accepted success tail
            # and finalizes the durable outbox as submitted.
            rpc.racing_tip = None
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(submitted, [block_hash])
            self.assertEqual(abandoned, [])
            self.assertEqual(rpc.submitblock_calls, 3)
            self.assertIn(block_hash, server._accounted_accepted_block_hashes)
            self.assertEqual(server.accepted_block_count, 1)
            self.assertEqual(len(ledger.persisted), 1)
            self.assertEqual(len(ledger.confirmed), 1)
            self.assertNotIn(block_hash, server._accepted_block_payout_previews)
            self.assertNotIn(
                block_hash,
                server._invalidated_accepted_block_payout_previews,
            )
            self.assertFalse(server._payout_state_publication_blocked)
            self.assertIsNone(
                server._accepted_block_payout_transition_for_parent(
                    block_hash,
                    parent_height=10,
                )
            )
            self.assertNotIn(
                block_hash, server._outstanding_block_candidate_hashes
            )
            self.assertNotIn(
                block_hash, server._tip_observed_accepted_block_hashes
            )
            self.assertIn(
                "qbit_prism_block_candidate_accept_pending_defers_total 1",
                server.metrics_payload(),
            )

    def test_three_blocks_quick_succession_lost_acks_finalize_all(self) -> None:
        # Three pool blocks land back to back (the 20:58 / 21:06:58 /
        # 21:07:20 pattern), every submitblock ack is lost, and one retry
        # even reads a transient foreign-tip snapshot. All three candidates
        # must finalize as submitted with payout publication unfenced.
        parent = "00" * 32
        hashes = ["b1" * 32, "b2" * 32, "b3" * 32]
        block_hexes = ["0a", "0b", "0c"]
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 5
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex=dict(zip(block_hexes, hashes)),
            )
            server.rpc = rpc
            parents = [parent, hashes[0], hashes[1]]
            for index, (block_hash, block_hex) in enumerate(
                zip(hashes, block_hexes)
            ):
                job_id = f"job-chain-{index}"
                self._chained_context(
                    server,
                    job_id=job_id,
                    parent_hash=parents[index],
                    height=10 + index,
                )
                candidate = block_candidate(
                    server,
                    state,
                    SimpleNamespace(
                        coinbase_tx_hex="c0ffee",
                        block_hash_hex=block_hash,
                        block_hex=block_hex,
                        share_pass=True,
                        block_pass=True,
                    ),
                    job_id=job_id,
                )

                # Lost-ack landing.
                self.assertTrue(
                    server._submit_next_block_candidate_writer(candidate)
                )
                candidate = self._retained_candidate(server)
                # Blockwait-first acceptance.
                self.assertTrue(server.observe_tip_for_refresh(block_hash))

                if index == 1:
                    # One retry reads a transient foreign-tip snapshot in
                    # which nothing can be proven active; it must defer.
                    rpc.racing_tip = "77" * 32
                    self.assertTrue(
                        server._submit_next_block_candidate_writer(candidate)
                    )
                    self.assertEqual(abandoned, [])
                    rpc.racing_tip = None
                    candidate = self._retained_candidate(server)

                # Settled view: the resume path finalizes as submitted.
                self.assertTrue(
                    server._submit_next_block_candidate_writer(candidate)
                )
                self.assertIn(
                    block_hash, server._accounted_accepted_block_hashes
                )

            self.assertEqual(submitted, hashes)
            self.assertEqual(abandoned, [])
            self.assertEqual(server.accepted_block_count, 3)
            self.assertEqual(
                getattr(server, "block_candidate_abandoned_counts", {}),
                {},
            )
            self.assertEqual(len(ledger.persisted), 3)
            self.assertEqual(len(ledger.confirmed), 3)
            self.assertFalse(server._payout_state_publication_blocked)
            self.assertEqual(server._accepted_block_payout_previews, {})
            self.assertEqual(
                server._invalidated_accepted_block_payout_previews, {}
            )
            self.assertEqual(server._outstanding_block_candidate_hashes, set())
            self.assertEqual(server._tip_observed_accepted_block_hashes, {})

    def test_observed_window_expiry_keeps_genuinely_stale_abandon_terminal(self) -> None:
        # A candidate that was once observed as the tip but has stayed off
        # the active chain past the observation window is genuinely stale
        # (permanently reorged out): the terminal abandon and its payout
        # preview withdrawal must still happen.
        parent = "00" * 32
        block_hash = "d4" * 32
        server, state, _ledger = submit_coordinator(tip=parent)
        server.observed_tip_accept_window_seconds = 60.0
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            server.rpc = rpc
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            candidate = self._retained_candidate(server)
            self.assertTrue(server.observe_tip_for_refresh(block_hash))

            # The chain moved on without the candidate and the observation
            # aged out: the block was reorged away for good.
            rpc.racing_tip = "77" * 32
            rpc.active.pop(block_hash, None)
            with server.lock:
                server._tip_observed_accepted_block_hashes[block_hash] = (
                    time.monotonic() - 61.0
                )

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(abandoned, [block_hash])
            self.assertEqual(submitted, [])
            self.assertEqual(
                server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB],
                1,
            )
            self.assertNotIn(block_hash, server._accepted_block_payout_previews)
            self.assertNotIn(block_hash, server._accounted_accepted_block_hashes)

    def test_block_candidate_acceptance_pending_probe_semantics(self) -> None:
        block_hash = "e5" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server.observed_tip_accept_window_seconds = 60.0
        server._register_outstanding_block_candidate(block_hash)

        def observed(age_seconds: float | None) -> None:
            with server.lock:
                server._tip_observed_accepted_block_hashes.pop(block_hash, None)
                if age_seconds is not None:
                    server._tip_observed_accepted_block_hashes[block_hash] = (
                        time.monotonic() - age_seconds
                    )

        # Own hash is the fresh best tip: accepted, and the probe itself
        # registers the observation for later checks.
        observed(None)
        server.rpc = AcceptanceProbeRpc(tip=block_hash)
        self.assertTrue(server._block_candidate_acceptance_pending(block_hash))
        self.assertIn(block_hash, server._tip_observed_accepted_block_hashes)

        # Active header at the expected height: accepted.
        observed(None)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 10, "confirmations": 2},
        )
        self.assertTrue(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Provably active at the wrong height: abandonable even when the
        # hash was recently observed.
        observed(1.0)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 9, "confirmations": 2},
        )
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Unprovable probe + fresh observation: accepted (defer).
        observed(1.0)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)
        self.assertTrue(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Unprovable probe + expired observation: genuinely stale.
        observed(61.0)
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Unprovable probe + no observation: genuinely stale.
        observed(None)
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Probe failure + fresh observation: acceptance evidence wins.
        observed(1.0)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, fail=True)
        self.assertTrue(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # Probe failure + no observation: unchanged legacy behavior.
        observed(None)
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # A best-tip lookup failure must not suppress the active-header
        # probe: the header alone proves acceptance, with no observation.
        observed(None)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 10, "confirmations": 2},
            fail_tip=True,
        )
        self.assertTrue(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

        # ... and the header's wrong-height verdict also stands alone,
        # overriding even a fresh observation.
        observed(1.0)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 9, "confirmations": 2},
            fail_tip=True,
        )
        self.assertFalse(
            server._block_candidate_acceptance_pending(
                block_hash, expected_height=10
            )
        )

    def test_post_persist_stale_view_defers_before_rejecting_prepared_rows(self) -> None:
        # Codex P1 / Bugbot: the post-persistence active-hash check could
        # reject the prepared payout rows and only then reach the abandon,
        # which now defers on acceptance evidence -- leaving rows
        # rejected/reversed that a later confirmation can never promote.
        # The acceptance check must run BEFORE reject_prepared_block.
        parent = "00" * 32
        block_hash = "c7" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            server.rpc = rpc
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            candidate = self._retained_candidate(server)
            self.assertTrue(server.observe_tip_for_refresh(block_hash))

            # Retry resumes on the own-hash tip, but the post-persist
            # getblockhash read races to a foreign winner for the height.
            rpc.getblockhash_override = racing_winner
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            # Deferred without touching the prepared payout rows.
            self.assertEqual(ledger.rejected, [])
            self.assertEqual(abandoned, [])
            self.assertNotIn(
                "block-stale",
                getattr(server, "block_candidate_abandoned_counts", {}),
            )
            candidate = self._retained_candidate(server)

            # The view settles with the pool's block active again: the
            # replayed tail confirms the still-prepared rows.
            rpc.getblockhash_override = None
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(submitted, [block_hash])
            self.assertEqual(ledger.rejected, [])
            self.assertEqual(len(ledger.confirmed), 1)
            self.assertIn(block_hash, server._accounted_accepted_block_hashes)

    def test_post_persist_stale_view_still_rejects_without_acceptance_evidence(self) -> None:
        # The terminal half of the same site: submitblock succeeds but a
        # sibling steals the height right after persistence and the pool's
        # block was never observed as the tip. With no FRESH acceptance
        # evidence (the retained ack has aged past the acceptance window),
        # the terminal decision seals first and only then are the prepared
        # rows rejected, exactly once.
        parent = "00" * 32
        block_hash = "c8" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            rpc.lose_acks = False
            server.rpc = rpc
            # The definitive ack retained at the offer would veto the
            # terminal decision while fresh; these tests pin the sealed
            # terminal machinery, so age it past the acceptance window.
            self._age_retained_acceptance_past_window(server)
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            real_persist = ledger.persist_accepted_block

            def persist_then_lose_race(**kwargs: object) -> dict[str, object]:
                result = real_persist(**kwargs)
                # A sibling wins the height after persistence: the tip and
                # the height's active hash both belong to the foreign winner
                # and the pool's block never became the chain tip.
                rpc.tip = racing_winner
                rpc.active.pop(block_hash, None)
                rpc.getblockhash_override = racing_winner
                return result

            ledger.persist_accepted_block = persist_then_lose_race  # type: ignore[method-assign]

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(len(ledger.rejected), 1)
            self.assertEqual(abandoned, [block_hash])
            self.assertEqual(submitted, [])
            self.assertEqual(
                server.block_candidate_abandoned_counts["block-stale"],
                1,
            )
            self.assertNotIn(block_hash, server._accounted_accepted_block_hashes)

    def test_acceptance_evidence_arriving_during_withdrawal_defers(self) -> None:
        # Bugbot: the payout-preview withdrawal inside the abandon can block
        # long enough for a blockwait observation to arrive; the terminal
        # commitment must consult that late evidence, not only the
        # completed-tail registry.
        block_hash = "a9" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)
        real_clear = server._clear_accepted_block_payout_preview

        def observing_clear(
            hash_arg: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            if invalidate_published:
                # A blockwait observation lands while the withdrawal blocks.
                with server.lock:
                    server._tip_observed_accepted_block_hashes[block_hash] = (
                        time.monotonic()
                    )
            return real_clear(
                hash_arg,
                invalidate_published=invalidate_published,
            )

        server._clear_accepted_block_payout_preview = observing_clear  # type: ignore[method-assign]

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved before submit: test",
            block_hash=block_hash,
            worker=None,
            preserve_if_accepted=True,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertEqual(
            getattr(outcome, "reason", None),
            PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
        )
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            getattr(server, "block_candidate_abandoned_counts", {}),
        )
        self.assertEqual(server.block_candidate_accept_pending_defer_count, 1)
        # The landed barrier is restored (and its fail-closed tombstone
        # popped) so descendant builders wait on the preview instead of
        # failing closed while the deferred retry heals it.
        self.assertIn(block_hash, server._accepted_block_payout_previews)
        self.assertTrue(
            server._accepted_block_payout_previews[block_hash].landed
        )
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_replay_restores_acceptance_evidence_from_durable_block_state(self) -> None:
        # Codex round 5: the observation registry dies with the process. A
        # durable prepared/confirmed pool-block row proves a prior process's
        # submitblock succeeded, so startup replay must restore acceptance
        # evidence before a replay disposition can race a transient fork
        # view and terminally abandon the accepted block.
        server, state, _recording = submit_coordinator()
        ledger = SingleWriterShareLedger()
        server.ledger = ledger
        accepted_hash = "e7" * 32
        unproven_hash = "e8" * 32
        unreadable_hash = "e9" * 32
        for index, block_hash in enumerate(
            (accepted_hash, unproven_hash, unreadable_hash), start=1
        ):
            pending = PendingShare(
                share_id=f"miner-a:{block_hash}",
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
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="00",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
                pending_share=pending,
            )
            ledger.append_batch(
                [(pending, server.block_candidate_intent(candidate))]
            )
        def durable_block_state(*, block_hash: str) -> dict[str, object] | None:
            if block_hash == accepted_hash:
                return {"chain_state": "prepared", "maturity_state": "immature"}
            if block_hash == unreadable_hash:
                raise RuntimeError("postgres unavailable")
            return None

        ledger.pool_block_state = durable_block_state  # type: ignore[attr-defined]

        self.assertEqual(server.replay_pending_block_candidates(), 3)
        replayed = [
            server._block_replay_candidate_queue.get_nowait()
            for _ in range(3)
        ]
        for candidate in replayed:
            server._restore_replayed_candidate_acceptance_evidence(candidate)

        self.assertIn(accepted_hash, server._tip_observed_accepted_block_hashes)
        self.assertIn(accepted_hash, server._outstanding_block_candidate_hashes)
        # No durable acceptance proof: replays without synthetic evidence.
        self.assertNotIn(
            unproven_hash, server._tip_observed_accepted_block_hashes
        )
        # An unreadable durable state fails safe: the candidate replays
        # protected, bounded by the observation window, instead of exposed
        # to a transient fork view racing its first disposition.
        self.assertIn(
            unreadable_hash, server._tip_observed_accepted_block_hashes
        )

    def test_late_defer_republishes_withdrawn_preview_and_unfences(self) -> None:
        # Bugbot round 5: when the withdrawal's own publication loses its
        # race, delivery is fenced before the late-acceptance defer runs.
        # The defer must republish the withdrawn preview so admission
        # reopens immediately instead of staying coordination-blocked
        # across the deferral cycles.
        block_hash = "ba" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server._publish_accepted_block_payout_preview(block_hash, [])
        self.assertFalse(server._payout_state_publication_blocked)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)

        real_publish = server._publish_payout_state_candidate

        def withdrawal_publication_lost(candidate: object) -> object:
            if getattr(candidate, "accepted_block_withdrawal", False):
                return None
            return real_publish(candidate)

        server._publish_payout_state_candidate = withdrawal_publication_lost  # type: ignore[method-assign]
        real_clear = server._clear_accepted_block_payout_preview

        def observing_clear(
            hash_arg: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            if invalidate_published:
                with server.lock:
                    server._tip_observed_accepted_block_hashes[block_hash] = (
                        time.monotonic()
                    )
            return real_clear(
                hash_arg,
                invalidate_published=invalidate_published,
            )

        server._clear_accepted_block_payout_preview = observing_clear  # type: ignore[method-assign]

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved before submit: test",
            block_hash=block_hash,
            worker=None,
            preserve_if_accepted=True,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertEqual(
            getattr(outcome, "reason", None),
            PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
        )
        # The barrier is restored AND publication admission reopened: the
        # withdrawn preview was republished despite the withdrawal's own
        # publication loss having fenced delivery.
        self.assertIn(block_hash, server._accepted_block_payout_previews)
        self.assertTrue(
            server._accepted_block_payout_previews[block_hash].landed
        )
        self.assertIsNotNone(
            server._accepted_block_payout_previews[block_hash].preview
        )
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )
        self.assertFalse(server._payout_state_publication_blocked)

    def test_late_gate_reprobes_after_withdrawal_heals_buried_block(self) -> None:
        # Bugbot round 7: an unknown pre-withdrawal probe verdict must
        # re-probe after the (blocking) withdrawal. A buried accepted block
        # is not always re-observed as the tip -- blockwait reports only the
        # newest of rapid connects -- so the recovered probe alone, with no
        # observation evidence at all, must defer the terminal abandon.
        block_hash = "bc" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)
        server.rpc = rpc
        real_clear = server._clear_accepted_block_payout_preview

        def healing_clear(
            hash_arg: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            if invalidate_published:
                # The chain view heals while the withdrawal blocks: the
                # candidate is provably active again, two blocks deep.
                rpc.header = {"height": 10, "confirmations": 2}
            return real_clear(
                hash_arg,
                invalidate_published=invalidate_published,
            )

        server._clear_accepted_block_payout_preview = healing_clear  # type: ignore[method-assign]

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved before submit: test",
            block_hash=block_hash,
            worker=None,
            preserve_if_accepted=True,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertEqual(
            getattr(outcome, "reason", None),
            PRISM_REJECTION_BLOCK_ACCEPT_PENDING,
        )
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            getattr(server, "block_candidate_abandoned_counts", {}),
        )
        self.assertIn(block_hash, server._accepted_block_payout_previews)
        self.assertTrue(
            server._accepted_block_payout_previews[block_hash].landed
        )
        self.assertNotIn(
            block_hash,
            server._invalidated_accepted_block_payout_previews,
        )

    def test_wrong_height_probe_verdict_overrules_late_observation(self) -> None:
        # Bugbot round 6: the probe must win both directions in the late
        # check too. An observation arriving during the withdrawal cannot
        # revive a candidate the pre-check probe proved active at the wrong
        # height -- header heights are immutable, so that verdict stands and
        # the abandon commits terminally instead of deferring to window
        # expiry.
        block_hash = "bb" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server.rpc = AcceptanceProbeRpc(
            tip="11" * 32,
            header={"height": 9, "confirmations": 2},
        )
        real_clear = server._clear_accepted_block_payout_preview

        def observing_clear(
            hash_arg: str,
            *,
            invalidate_published: bool = False,
        ) -> None:
            if invalidate_published:
                with server.lock:
                    server._tip_observed_accepted_block_hashes[block_hash] = (
                        time.monotonic()
                    )
            return real_clear(
                hash_arg,
                invalidate_published=invalidate_published,
            )

        server._clear_accepted_block_payout_preview = observing_clear  # type: ignore[method-assign]

        accepted_race_won = server._abandon_block_candidate(
            "block-stale",
            "candidate active at unexpected height 9",
            block_hash=block_hash,
            worker=None,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertEqual(getattr(outcome, "reason", None), "block-stale")
        self.assertNotIn(
            "block-stale",
            server.block_candidate_abandoned_counts,
        )
        self.assertEqual(
            getattr(server, "block_candidate_accept_pending_defer_count", 0),
            0,
        )

    def test_terminal_seal_excludes_observations_after_the_commit(self) -> None:
        # Codex round 3: acceptance evidence arriving after the terminal
        # commit but before the follow-up durable rejection must be excluded
        # deterministically. The terminal commit seals the hash (drops it
        # from outstanding) in the same critical section, so a later
        # observation is a no-op instead of contradicting the sealed
        # disposition mid-rejection.
        block_hash = "b8" * 32
        server, _state, _ledger = submit_coordinator()
        server._ensure_job_cache_state()
        server._register_outstanding_block_candidate(block_hash)
        server.rpc = AcceptanceProbeRpc(tip="11" * 32, header=None)

        accepted_race_won = server._abandon_block_candidate(
            PRISM_REJECTION_STALE_JOB,
            "tip moved before submit: test",
            block_hash=block_hash,
            worker=None,
            preserve_if_accepted=True,
            expected_height=10,
        )

        self.assertFalse(accepted_race_won)
        self.assertNotIn(
            PRISM_REJECTION_STALE_JOB,
            server.block_candidate_abandoned_counts,
        )
        self.assertNotIn(block_hash, server._outstanding_block_candidate_hashes)

        # A blockwait observation lands right after the sealed commit: it
        # must not register acceptance evidence for the sealed hash.
        self.assertTrue(server.observe_tip_for_refresh(block_hash))
        self.assertNotIn(block_hash, server._tip_observed_accepted_block_hashes)
        outcome = getattr(server, "_block_candidate_outcome", None)
        self.assertIsNotNone(outcome)
        server._record_committed_block_candidate_abandonment(block_hash, outcome)
        self.assertEqual(
            server.block_candidate_abandoned_counts[PRISM_REJECTION_STALE_JOB],
            1,
        )

    def test_observation_during_prepared_row_rejection_cannot_split_state(self) -> None:
        # The exact round-3 interleaving at the post-persist site: a
        # blockwait observation fires inside the gap between the sealed
        # terminal decision and reject_prepared_block (during the
        # getblockcount call). The seal excludes it, so the rows are
        # rejected exactly once with no contradictory evidence registered
        # and no defer emitted after the seal.
        parent = "00" * 32
        block_hash = "c9" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            rpc.lose_acks = False
            server.rpc = rpc
            # The definitive ack retained at the offer would veto the
            # terminal decision while fresh; these tests pin the sealed
            # terminal machinery, so age it past the acceptance window.
            self._age_retained_acceptance_past_window(server)
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            race_state: dict[str, bool] = {"post_persist": False}
            real_persist = ledger.persist_accepted_block

            def persist_then_lose_race(**kwargs: object) -> dict[str, object]:
                result = real_persist(**kwargs)
                rpc.tip = racing_winner
                rpc.active.pop(block_hash, None)
                rpc.getblockhash_override = racing_winner
                race_state["post_persist"] = True
                return result

            ledger.persist_accepted_block = persist_then_lose_race  # type: ignore[method-assign]
            real_call = rpc.call

            def observing_call(
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblockcount" and race_state["post_persist"]:
                    # Blockwait reports the candidate as tip inside the
                    # seal -> reject gap.
                    server.observe_tip_for_refresh(block_hash)
                return real_call(method, params)

            rpc.call = observing_call  # type: ignore[method-assign]

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(len(ledger.rejected), 1)
            self.assertEqual(abandoned, [block_hash])
            self.assertEqual(submitted, [])
            self.assertEqual(
                server.block_candidate_abandoned_counts["block-stale"],
                1,
            )
            self.assertNotIn(
                block_hash, server._tip_observed_accepted_block_hashes
            )
            self.assertNotIn(
                block_hash, server._outstanding_block_candidate_hashes
            )
            self.assertEqual(
                getattr(server, "block_candidate_accept_pending_defer_count", 0),
                0,
            )

    def test_retained_candidate_reregisters_for_tip_observations(self) -> None:
        # Codex round 8: the terminal seal stops observation matching, but
        # when the terminal cleanup itself fails (reject_prepared_block
        # raising) the candidate is retained for retry -- and evidence
        # arriving during that backoff gap must register again, not vanish.
        parent = "00" * 32
        block_hash = "cd" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            rpc.lose_acks = False
            server.rpc = rpc
            # The definitive ack retained at the offer would veto the
            # terminal decision while fresh; these tests pin the sealed
            # terminal machinery, so age it past the acceptance window.
            self._age_retained_acceptance_past_window(server)
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            real_persist = ledger.persist_accepted_block

            def persist_then_lose_race(**kwargs: object) -> dict[str, object]:
                result = real_persist(**kwargs)
                rpc.tip = racing_winner
                rpc.active.pop(block_hash, None)
                rpc.getblockhash_override = racing_winner
                return result

            ledger.persist_accepted_block = persist_then_lose_race  # type: ignore[method-assign]
            reject_attempts: list[dict[str, object]] = []

            def failing_reject(**kwargs: object) -> dict[str, object]:
                reject_attempts.append(kwargs)
                raise RuntimeError("psql briefly unavailable")

            ledger.reject_prepared_block = failing_reject  # type: ignore[method-assign]
            ledger.pool_block_state = (  # type: ignore[attr-defined]
                lambda *, block_hash: {
                    "chain_state": "prepared",
                    "maturity_state": "immature",
                }
            )

            # The sealed terminal pass aborts inside the rejection (site
            # attempt, then the writer's terminal-cleanup attempt): the
            # candidate is retained and must match observations again.
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(len(reject_attempts), 2)
            self.assertIsNotNone(getattr(server, "_retry_block_candidate", None))
            self.assertIn(
                block_hash, server._outstanding_block_candidate_hashes
            )
            self.assertNotIn(
                "block-stale",
                server.block_candidate_abandoned_counts,
            )

            # Blockwait reports the pool's own hash during the backoff gap:
            # the evidence registers instead of vanishing behind the seal.
            self.assertTrue(server.observe_tip_for_refresh(block_hash))
            self.assertIn(
                block_hash, server._tip_observed_accepted_block_hashes
            )
            self.assertEqual(abandoned, [])

            # The retry can subsequently prove the candidate active and
            # complete as submitted. The earlier reversible seals must never
            # leave a contradictory abandonment count behind.
            ledger.persist_accepted_block = real_persist  # type: ignore[method-assign]
            rpc.tip = block_hash
            rpc.height = 10
            rpc.active[block_hash] = 10
            rpc.getblockhash_override = None
            recovered_candidate = self._retained_candidate(server)
            self.assertTrue(
                server._submit_next_block_candidate_writer(recovered_candidate)
            )
            self.assertEqual(submitted, [block_hash])
            self.assertEqual(abandoned, [])
            self.assertEqual(server.accepted_block_count, 1)
            self.assertNotIn(
                "block-stale",
                server.block_candidate_abandoned_counts,
            )

    def test_definitive_ack_with_unprovable_chain_view_defers_not_abandons(
        self,
    ) -> None:
        # Release-review finding: a definitive submitblock success followed
        # by a chain view that cannot prove the block landed (foreign tip,
        # header unknown, height taken by a sibling in that instant's view)
        # must defer as accept-pending on the strength of the retained ack
        # -- never terminally abandon accounting for a block the node
        # accepted. The stash ages on the observation window, so a real
        # orphan still terminalizes once the ack is stale (the sealed
        # rejection tests above).
        parent = "00" * 32
        block_hash = "ce" * 32
        racing_winner = "77" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 2
        server.stop_after_block = False
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            rpc.lose_acks = False
            server.rpc = rpc
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )
            real_persist = ledger.persist_accepted_block

            def persist_then_lose_race(**kwargs: object) -> dict[str, object]:
                result = real_persist(**kwargs)
                rpc.tip = racing_winner
                rpc.active.pop(block_hash, None)
                rpc.getblockhash_override = racing_winner
                return result

            ledger.persist_accepted_block = persist_then_lose_race  # type: ignore[method-assign]

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            # Deferred, not terminal: prepared rows untouched, candidate
            # retained for retry, observation matching still open.
            self.assertEqual(ledger.rejected, [])
            self.assertEqual(abandoned, [])
            self.assertEqual(submitted, [])
            self.assertGreaterEqual(
                getattr(server, "block_candidate_accept_pending_defer_count", 0),
                1,
            )
            self.assertIsNotNone(getattr(server, "_retry_block_candidate", None))
            self.assertNotIn(
                "block-stale",
                server.block_candidate_abandoned_counts,
            )
            self.assertIn(
                block_hash, server._outstanding_block_candidate_hashes
            )

    def test_retained_acceptance_evidence_ages_on_observation_window(
        self,
    ) -> None:
        # The stash is acceptance evidence only while fresh: past the
        # observation window it stands down so a genuinely orphaned block
        # (whose probes never prove anything either way) regains
        # abandonability instead of deferring forever.
        server, _state, _ledger = submit_coordinator()
        block_hash = "cf" * 32
        server._stash_retained_block_candidate_node_submission(
            block_hash,
            SimpleNamespace(attempted=True, error=None, result=None),
        )
        self.assertTrue(server._block_candidate_acceptance_retained(block_hash))
        with server.lock:
            server._block_candidate_retained_submission_monotonic[
                block_hash
            ] -= 400.0
        self.assertFalse(server._block_candidate_acceptance_retained(block_hash))
        # A non-positive window means evidence never expires, mirroring the
        # tip-observation semantics.
        server.observed_tip_accept_window_seconds = 0.0
        self.assertTrue(server._block_candidate_acceptance_retained(block_hash))

    def test_pool_closed_gate_requires_probe_proven_acceptance(self) -> None:
        # Bugbot: observation evidence alone must not open the pool-closed
        # gate -- an off-chain candidate would fall through toward
        # submitblock after the pool stopped accepting blocks. Unprovable
        # views defer via the abandon path; expired evidence goes terminal.
        parent = "00" * 32
        block_hash = "d9" * 32
        server, state, _ledger = submit_coordinator(tip=parent)
        server.max_blocks = 1
        server.stop_after_block = False
        server.accepted_block_count = 1
        server.observed_tip_accept_window_seconds = 60.0
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            server.rpc = rpc
            server._register_outstanding_block_candidate(block_hash)
            with server.lock:
                server._tip_observed_accepted_block_hashes[block_hash] = (
                    time.monotonic()
                )
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )

            # Fresh observation, unprovable chain view: defer, and above all
            # never reach submitblock through the closed pool.
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(rpc.submitblock_calls, 0)
            self.assertEqual(abandoned, [])
            candidate = self._retained_candidate(server)

            # Evidence expires with the candidate still absent: terminal.
            with server.lock:
                server._tip_observed_accepted_block_hashes[block_hash] = (
                    time.monotonic() - 61.0
                )
            self.assertTrue(server._submit_next_block_candidate_writer(candidate))
            self.assertEqual(rpc.submitblock_calls, 0)
            self.assertEqual(abandoned, [block_hash])
            self.assertEqual(submitted, [])
            self.assertEqual(
                server.block_candidate_abandoned_counts[
                    PRISM_REJECTION_POOL_CLOSED
                ],
                1,
            )

    def test_pool_closed_gate_yields_to_on_chain_candidate(self) -> None:
        # A candidate already on the active chain must complete its payout
        # accounting even after the pool stops accepting new blocks; the
        # pool-closed gate previously abandoned it terminally.
        parent = "00" * 32
        block_hash = "f6" * 32
        server, state, ledger = submit_coordinator(tip=parent)
        server.max_blocks = 1
        server.stop_after_block = False
        server.accepted_block_count = 1
        with tempfile.TemporaryDirectory() as tempdir:
            submitted, abandoned = self._accepted_tail_scaffolding(server, tempdir)
            rpc = LostAckSubmitRpc(
                start_tip=parent,
                hash_by_hex={"00": block_hash},
            )
            # The block is already active with the pool's own hash as tip
            # (for example after a watchdog restart replayed the outbox).
            rpc.tip = block_hash
            rpc.height = 10
            rpc.active[block_hash] = 10
            server.rpc = rpc
            server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
                lambda _tip, *, _coalesce_same_tip: not _coalesce_same_tip
            )
            candidate = block_candidate(
                server,
                state,
                SimpleNamespace(
                    coinbase_tx_hex="c0ffee",
                    block_hash_hex=block_hash,
                    block_hex="00",
                    share_pass=True,
                    block_pass=True,
                ),
            )

            self.assertTrue(server._submit_next_block_candidate_writer(candidate))

            self.assertEqual(submitted, [block_hash])
            self.assertEqual(abandoned, [])
            self.assertNotIn(
                PRISM_REJECTION_POOL_CLOSED,
                getattr(server, "block_candidate_abandoned_counts", {}),
            )
            self.assertIn(block_hash, server._accounted_accepted_block_hashes)
            self.assertEqual(server.accepted_block_count, 2)


if __name__ == "__main__":
    unittest.main()
