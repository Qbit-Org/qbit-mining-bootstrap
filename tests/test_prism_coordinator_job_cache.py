#!/usr/bin/env python3
"""Per-template job-build cache, cached health snapshot, and latency metrics."""

from __future__ import annotations

import queue
import socket
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass, replace as dataclass_replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.prism_coordinator import (
    ClientState,
    JobBuildSuperseded,
    MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES,
    PendingShareAppend,
    PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
    PRISM_REJECTION_REASON_IDS,
    PrismCoordinator,
    ShutdownInProgress,
    TemplateRefreshBlocked,
    WorkerIdentity,
    canonical_json_sha256,
    canonical_json_text,
    default_prism_coinbase_tag_hex,
    now_ms,
    qbit_template_fingerprint,
)
from lab.prism.share_ledger import PendingShare, SingleWriterShareLedger

PAYOUT_ADDRESS = "tq1z70ukpvs96kye6jmgvl3nttevtkrq8uu89snkpm6m8gwqukw8u5dsz32kwa"
EXTRANONCE2_SIZE = 8


@dataclass(frozen=True)
class FakeShare:
    miner_id: str
    share_seq: int

    def to_prism_json(self) -> dict[str, object]:
        return {"share_seq": self.share_seq, "miner_id": self.miner_id}


class FakeLedger:
    backend_name = "fake"

    def __init__(self, miners: list[str] | None = None) -> None:
        self.miners = miners if miners is not None else ["miner-a", "miner-b", "miner-c"]
        self.snapshot_calls = 0
        self.stats_calls = 0

    def accepted_share_stats(self) -> dict[str, int]:
        self.stats_calls += 1
        return {
            "accepted_share_count": len(self.miners),
            "distinct_miner_count": len(set(self.miners)),
        }

    def all_shares(self) -> list[FakeShare]:
        raise AssertionError("all_shares must not be called when accepted_share_stats exists")

    def snapshot_at_job_issue(self, anchor_job_issued_at_ms: int, *, window_weight: int | None = None) -> list[FakeShare]:
        self.snapshot_calls += 1
        return [FakeShare(miner_id=miner, share_seq=seq + 1) for seq, miner in enumerate(self.miners)]

    def current_prior_balances(self) -> list[dict[str, object]]:
        return []

    def metrics(self) -> dict[str, int]:
        return {"blocks": 0, "owed_accounts": 0}


class MutableFakeLedger(FakeLedger):
    """Small durable-commit stand-in for payout snapshot concurrency tests."""

    def append_batch(
        self,
        entries: list[tuple[PendingShare, dict[str, object] | None]],
    ) -> list[FakeShare]:
        records: list[FakeShare] = []
        for pending, _intent in entries:
            self.miners.append(pending.miner_id)
            records.append(
                FakeShare(
                    miner_id=pending.miner_id,
                    share_seq=len(self.miners),
                )
            )
        return records


class ReadyLedgerWithEmptyFirstSnapshot(FakeLedger):
    def __init__(self) -> None:
        super().__init__(miners=["miner-a", "miner-b", "miner-c"])

    def snapshot_at_job_issue(self, anchor_job_issued_at_ms: int, *, window_weight: int | None = None) -> list[FakeShare]:
        self.snapshot_calls += 1
        if self.snapshot_calls == 1:
            return []
        return [FakeShare(miner_id=miner, share_seq=seq + 1) for seq, miner in enumerate(self.miners)]


class FakeRpc:
    def __init__(self, template: dict[str, object], tip: str) -> None:
        self.template = template
        self.tip = tip
        self.blockchain_info: dict[str, object] = {
            "initialblockdownload": False,
            "blocks": 100,
            "headers": 100,
        }
        self.calls: list[str] = []

    def call(self, method: str, params: list[object] | None = None) -> object:
        self.calls.append(method)
        if method == "getblocktemplate":
            return dict(self.template)
        if method == "getbestblockhash":
            return self.tip
        if method == "getblockchaininfo":
            return dict(self.blockchain_info)
        if method == "getblockcount":
            return int(self.blockchain_info["blocks"])
        raise AssertionError(f"unexpected RPC {method}")

    def count(self, method: str) -> int:
        return sum(1 for name in self.calls if name == method)


def synthetic_manifest_coinbase_hex(suffix_hex: str) -> str:
    """A structurally valid non-witness coinbase whose scriptSig ends with the
    extranonce placeholder suffix, as the audit bundle builder produces."""
    height_push = "03aabbcc"
    script_sig = height_push + suffix_hex
    script_sig_bytes = bytes.fromhex(script_sig)
    output = (50_00000000).to_bytes(8, "little").hex() + "0151"
    return (
        "01000000"
        + "01"
        + "00" * 32
        + "ffffffff"
        + direct_stratum.compact_size(len(script_sig_bytes)).hex()
        + script_sig
        + "ffffffff"
        + "01"
        + output
        + "00000000"
    )


def base_template(height: int = 10, prevhash: str = "11" * 32) -> dict[str, object]:
    # Realistic (non-regtest) bits: the network target must be harder than the
    # vardiff range for per-client share targets to differ, as on testnet4.
    return {
        "height": height,
        "previousblockhash": prevhash,
        "bits": "1b00ffff",
        "version": 0x20000000,
        "curtime": 1_700_000_000,
        "coinbasevalue": 50_00000000,
        "transactions": [],
    }


def worker(payout: str = PAYOUT_ADDRESS, username: str | None = None) -> WorkerIdentity:
    return WorkerIdentity(
        username=username or payout,
        payout_address=payout,
        worker_name=None,
        script_pubkey_hex="5220" + "22" * 32,
        p2mr_program_hex="22" * 32,
    )


def client(connection_id: int, identity: WorkerIdentity | None = None) -> ClientState:
    state = ClientState.__new__(ClientState)
    state.sock = None
    state.address = ("127.0.0.1", 40_000 + connection_id)
    state.connection_id = connection_id
    state.extranonce1_hex = f"{connection_id:08x}"
    state.subscribed = True
    state.authorized = True
    identity = identity or worker()
    state.username = identity.username
    state.worker = identity
    state.version_mask = 0
    state.active_job = None
    state.share_difficulty = Decimal("1")
    state.pending_share_difficulty = None
    state.active_job_ids = set()
    state.post_accept_refresh_block = None
    state.tip_work_delivered = None
    state.closing = False
    state.job_update_lock = threading.RLock()
    state.send_lock = threading.Lock()
    return state


class ObservedRLock:
    """RLock test double that exposes a contending acquire without sleeps."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.observe_acquires = False
        self.acquire_attempted = threading.Event()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if self.observe_acquires:
            self.acquire_attempted.set()
        return self._lock.acquire(blocking, timeout)

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> ObservedRLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


def coordinator(*, ledger: object | None = None, template: dict[str, object] | None = None) -> tuple[PrismCoordinator, FakeRpc]:
    server = PrismCoordinator.__new__(PrismCoordinator)
    template = template or base_template()
    rpc = FakeRpc(template, tip=str(template["previousblockhash"]))
    server.rpc = rpc
    server.qbit_chain = "regtest"
    server.lock = threading.RLock()
    server.stop_event = threading.Event()
    server.clients = set()
    server.jobs = {}
    server.job_counter = 0
    server.connection_counter = 0
    server.accepted_block_count = 0
    server.max_blocks = 1_000
    server.started_monotonic = time.monotonic()
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
    server.last_reorg_reconciled_monotonic = None
    server.latest_evidence = None
    server.latest_coinbase_size_bytes = None
    server.tip_template_snapshot = None
    server.extranonce2_size = EXTRANONCE2_SIZE
    server.coinbase_tag_hex = default_prism_coinbase_tag_hex()
    server.share_difficulty = Decimal("1")
    server.vardiff_config = vardiff.VardiffConfig(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("0.000000001"),
        max_difficulty=Decimal("1024"),
        retarget_interval_seconds=Decimal("90"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("1"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("0.4"),
        retarget_tolerance=Decimal("0.25"),
    )
    server.default_share_weight = 1
    server.share_weights_by_username = {}
    server.min_ready_miners = 3
    server.ledger = ledger if ledger is not None else FakeLedger()
    server.blockpoll_seconds = 2.0
    server.job_bundle_cache_seconds = 10.0
    server.template_cache_seconds = 2.0
    server.reorg_reconcile_cache_seconds = 5.0
    server.health_refresh_seconds = 5.0
    server.stratum_send_timeout_seconds = 20.0
    server._ensure_job_cache_state()
    return server, rpc


def install_fake_bundle_builder(server: PrismCoordinator) -> dict[str, object]:
    """Replace the audit bundle subprocess with a counting fake whose manifest
    coinbase embeds exactly the suffix the coordinator asked for."""
    recorded: dict[str, object] = {"calls": 0, "suffixes": []}

    def fake_build_audit_bundle(**kwargs: object) -> dict[str, object]:
        recorded["calls"] = int(recorded["calls"]) + 1
        suffix_hex = str(kwargs["coinbase_script_sig_suffix_hex"])
        recorded["suffixes"].append(suffix_hex)
        recorded["last_kwargs"] = kwargs
        return {
            "found_block": dict(kwargs["found_block"]),
            "payout_policy_manifest": {"accounts": []},
            "signed_coinbase_manifest": {
                "manifest": {
                    "coinbase_tx_hex": synthetic_manifest_coinbase_hex(suffix_hex),
                }
            },
        }

    server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
    return recorded


def stamped_pending_share(accepted_at_ms: int) -> PendingShare:
    return PendingShare(
        share_id=f"miner-a:{accepted_at_ms}",
        miner_id="miner-a",
        order_key="miner-a",
        p2mr_program_hex="22" * 32,
        share_difficulty=1,
        network_difficulty=1,
        template_height=9,
        job_id="job-1",
        job_issued_at_ms=accepted_at_ms - 1,
        accepted_at_ms=accepted_at_ms,
        ntime=1_700_000_000,
    )


def pending_share_append(sequence: int) -> PendingShareAppend:
    accepted_at_ms = 1_800_000_000_000 + sequence
    return PendingShareAppend(
        pending_share=PendingShare(
            share_id=f"miner-{sequence}:{sequence:064x}",
            miner_id=f"miner-{sequence}",
            order_key=f"miner-{sequence}",
            p2mr_program_hex=f"{sequence % 255:02x}" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id=f"job-{sequence}",
            job_issued_at_ms=accepted_at_ms - 1,
            accepted_at_ms=accepted_at_ms,
            ntime=1_700_000_000,
        ),
        username=f"miner-{sequence}",
        job_id=f"job-{sequence}",
        block_hash_hex=f"{sequence:064x}",
        collection_only=False,
        credit_policy=None,
    )


class AnchorRecordingLedger(FakeLedger):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[int] = []

    def snapshot_at_job_issue(
        self, anchor_job_issued_at_ms: int, *, window_weight: int | None = None
    ) -> list[FakeShare]:
        self.anchors.append(int(anchor_job_issued_at_ms))
        return super().snapshot_at_job_issue(
            anchor_job_issued_at_ms, window_weight=window_weight
        )


class SnapshotAnchorFloorTests(unittest.TestCase):
    def _hold_floor(self, server: PrismCoordinator, share: PendingShare) -> None:
        server._ensure_pending_share_commit_state()
        with server._pending_share_commit_lock:
            server._pending_share_commit_floor[id(share)] = [
                share,
                time.monotonic(),
                False,
            ]

    def test_job_bundle_anchor_clamps_below_pending_share_commit(self) -> None:
        # The issued snapshot must be reproducible from the durable ledger:
        # while a stamped share's commit is pending, the job anchor (which the
        # bundle declares as anchor_job_issued_at_ms) has to predate it, or
        # qbit_audit_share_window at the declared anchor would include a share
        # the published window omitted.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        stamped_ms = now_ms() - 5
        share = stamped_pending_share(stamped_ms)
        self._hold_floor(server, share)

        bundle = server.build_shared_job_bundle(
            server.current_template_artifacts(),
            worker(),
        )
        self.assertEqual(ledger.anchors[-1], stamped_ms - 1)
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"], stamped_ms - 1
        )
        self.assertEqual(bundle.issued_at_ms, stamped_ms - 1)

        server._finish_pending_share_commit(share)
        # The issued time is frozen per template generation; drop the frozen
        # entry so the rebuild stamps a fresh anchor now that no commit is
        # pending.
        with server._job_cache_lock:
            server._job_build_issued_at_ms.clear()
        rebuilt = server.build_shared_job_bundle(
            server.current_template_artifacts(),
            worker(),
        )
        self.assertGreaterEqual(ledger.anchors[-1], stamped_ms)
        self.assertGreaterEqual(
            int(rebuilt.found_block["anchor_job_issued_at_ms"]), stamped_ms
        )

    def test_payout_artifact_declares_its_own_snapshot_anchor(self) -> None:
        # An artifact snapshot is taken at its own (possibly clamped) anchor.
        # A bundle reusing the artifact must declare that anchor rather than
        # the fresher job-issue time: a share that was already durable at
        # artifact build time but stamped above the artifact's clamped anchor
        # is excluded from the artifact by construction, yet a re-derivation
        # at the job-issue anchor would include it.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.current_template_artifacts()
        stamped_ms = now_ms() - 5
        share = stamped_pending_share(stamped_ms)
        self._hold_floor(server, share)

        artifact = server._build_payout_ledger_artifact(
            0, 0, artifacts.network_difficulty
        )
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, stamped_ms - 1)
        self.assertEqual(ledger.anchors[-1], stamped_ms - 1)

        server._finish_pending_share_commit(share)
        # Construction re-validates that the passed artifact is the installed
        # current one.
        with server._job_cache_lock:
            server._payout_ledger_artifact = artifact
        bundle = server.build_shared_job_bundle(
            artifacts,
            worker(),
            payout_artifact=artifact,
        )
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"],
            artifact.snapshot_anchor_ms,
        )
        self.assertGreater(bundle.issued_at_ms, int(artifact.snapshot_anchor_ms))

def mark_progress_healthy(server: PrismCoordinator) -> None:
    snapshot = server.fetch_qbit_tip_template_snapshot()
    server._record_progress_tip_poll(snapshot)
    server._record_progress_publication(
        snapshot,
        int(getattr(server, "_payout_state_generation", 0)),
    )


class PayoutSnapshotEpochTests(unittest.TestCase):
    @staticmethod
    def _run_poll(
        server: PrismCoordinator,
        results: list[int],
        errors: list[BaseException],
    ) -> None:
        try:
            results.append(server.poll_qbit_tip_template_once())
        except BaseException as exc:  # noqa: BLE001 - test thread handoff
            errors.append(exc)

    def test_continuous_share_commits_publish_then_coalesce_one_followup(
        self,
    ) -> None:
        server, _rpc = coordinator(ledger=MutableFakeLedger())
        self.addCleanup(server.shutdown_tip_refresh_executor)
        recorded = install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        build_entered = threading.Event()
        release_build = threading.Event()
        original_builder = server.build_audit_bundle

        def blocking_builder(**kwargs: object) -> dict[str, object]:
            build_entered.set()
            self.assertTrue(release_build.wait(5))
            return original_builder(**kwargs)

        server.build_audit_bundle = blocking_builder  # type: ignore[method-assign]
        results: list[int] = []
        errors: list[BaseException] = []
        thread = threading.Thread(
            target=self._run_poll,
            args=(server, results, errors),
        )
        thread.start()
        try:
            self.assertTrue(build_entered.wait(5))
            for sequence in range(1, 33):
                self.assertTrue(
                    server._append_share_batch([pending_share_append(sequence)])
                )
            # A real settlement/carry generation can also advance while the
            # immutable ledger epoch is building. It belongs to the same one
            # follow-up, not to cancellation of the selected publication.
            self.assertEqual(server._advance_payout_state_generation(), 1)
            self.assertEqual(recorded["calls"], 0)
            with server._payout_snapshot_lock:
                self.assertIsNotNone(server._active_payout_snapshot_attempt)
        finally:
            release_build.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [1])
        self.assertEqual(recorded["calls"], 1)
        first_context = state.active_job
        self.assertIsNotNone(first_context)
        assert first_context is not None
        self.assertLess(
            first_context.payout_snapshot_id,
            server._latest_available_payout_snapshot_id,
        )
        self.assertEqual(server.payout_snapshot_advances_coalesced, 33)
        self.assertEqual(
            server.job_build_supersession_reason_counts["payout"],
            0,
        )
        self.assertEqual(server.payout_snapshot_followup_counts["scheduled"], 1)
        self.assertIsNone(server._active_payout_snapshot_attempt)

        self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(
            state.active_job.payout_snapshot_id,  # type: ignore[union-attr]
            server._latest_available_payout_snapshot_id,
        )
        self.assertEqual(server.payout_snapshot_followup_counts["scheduled"], 1)
        self.assertEqual(server.payout_snapshot_followup_counts["completed"], 1)
        self.assertIsNone(server._active_payout_snapshot_attempt)

    def test_chain_tip_change_supersedes_snapshot_build_and_releases_owner(
        self,
    ) -> None:
        server, rpc = coordinator(ledger=MutableFakeLedger())
        self.addCleanup(server.shutdown_tip_refresh_executor)
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        build_entered = threading.Event()
        release_build = threading.Event()
        original_builder = server.build_audit_bundle

        def blocking_builder(**kwargs: object) -> dict[str, object]:
            build_entered.set()
            self.assertTrue(release_build.wait(5))
            return original_builder(**kwargs)

        server.build_audit_bundle = blocking_builder  # type: ignore[method-assign]
        results: list[int] = []
        errors: list[BaseException] = []
        thread = threading.Thread(
            target=self._run_poll,
            args=(server, results, errors),
        )
        thread.start()
        new_tip = "44" * 32
        try:
            self.assertTrue(build_entered.wait(5))
            rpc.tip = new_tip
            rpc.template = base_template(height=11, prevhash=new_tip)
            self.assertTrue(server.observe_tip_for_refresh(new_tip))
        finally:
            release_build.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(results, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIsNone(server._active_payout_snapshot_attempt)
        self.assertGreaterEqual(
            server.job_build_supersession_reason_counts["chain_tip"],
            1,
        )
        self.assertIsNone(state.active_job)

        server.build_audit_bundle = original_builder  # type: ignore[method-assign]
        self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        self.assertEqual(
            state.active_job.template["previousblockhash"],  # type: ignore[union-attr]
            new_tip,
        )

    def test_job_coinbase_ctv_and_settlement_share_one_snapshot(self) -> None:
        server, _rpc = coordinator(ledger=MutableFakeLedger())
        self.addCleanup(server.shutdown_tip_refresh_executor)
        recorded = install_fake_bundle_builder(server)
        ctv_settlement = {
            "schema": "qbit.test.snapshot-settlement.v1",
            "outputs": [{"recipient_id": "miner-a", "amount_sats": 7}],
        }
        server.prism_ctv_settlement_config = (  # type: ignore[method-assign]
            lambda **_kwargs: ctv_settlement
        )
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}

        self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        context = state.active_job
        bundle = server._prepared_ready_bundle
        self.assertIsNotNone(context)
        self.assertIsNotNone(bundle)
        assert context is not None and bundle is not None
        self.assertIsNotNone(bundle.build_key)
        assert bundle.build_key is not None
        snapshot_identity = (
            bundle.payout_snapshot_id,
            bundle.payout_snapshot_sha256,
        )
        self.assertGreater(snapshot_identity[0], 0)
        self.assertEqual(
            snapshot_identity,
            (context.payout_snapshot_id, context.payout_snapshot_sha256),
        )
        self.assertEqual(
            snapshot_identity,
            (
                bundle.build_key.payout_snapshot_id,
                bundle.build_key.payout_snapshot_sha256,
            ),
        )
        self.assertEqual(
            bundle.found_block["payout_snapshot_id"],
            snapshot_identity[0],
        )
        self.assertEqual(
            bundle.found_block["payout_snapshot_sha256"],
            snapshot_identity[1],
        )
        self.assertEqual(recorded["last_kwargs"]["shares"], bundle.shares_json)
        self.assertEqual(
            recorded["last_kwargs"]["prior_balances"],
            bundle.prior_balances,
        )
        self.assertEqual(
            recorded["last_kwargs"]["ctv_settlement"],
            ctv_settlement,
        )
        self.assertEqual(
            bundle.build_key.ctv_settlement_sha256,
            canonical_json_sha256(ctv_settlement),
        )
        self.assertEqual(
            context.payout_policy_sha256,
            bundle.build_key.payout_policy_sha256,
        )
        self.assertEqual(
            context.ctv_settlement_sha256,
            bundle.build_key.ctv_settlement_sha256,
        )
        self.assertEqual(
            context.ctv_settlement_json,
            canonical_json_text(ctv_settlement),
        )
        self.assertEqual(
            bundle.coinbase_manifest["coinbase_tx_hex"],
            synthetic_manifest_coinbase_hex(recorded["suffixes"][0]),
        )
        metrics = server.metrics_payload()
        self.assertIn(
            f'qbit_prism_payout_snapshot_id{{state="selected"}} {snapshot_identity[0]}',
            metrics,
        )
        self.assertIn(
            f'qbit_prism_payout_snapshot_id{{state="published"}} {snapshot_identity[0]}',
            metrics,
        )
        self.assertIn("qbit_prism_payout_snapshot_retention_count 1", metrics)

    def test_older_issued_snapshot_remains_attributable_until_job_expiry(
        self,
    ) -> None:
        server, _rpc = coordinator(ledger=MutableFakeLedger())
        self.addCleanup(server.shutdown_tip_refresh_executor)
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        older = state.active_job
        self.assertIsNotNone(older)
        assert older is not None

        self.assertTrue(server._append_share_batch([pending_share_append(100)]))
        self.assertEqual(server.poll_qbit_tip_template_once(), 1)
        newer = state.active_job
        self.assertIsNotNone(newer)
        assert newer is not None
        self.assertGreater(newer.payout_snapshot_id, older.payout_snapshot_id)
        self.assertTrue(server._resolve_payout_snapshot_for_submission(older))

        candidate = SimpleNamespace(
            context=older,
            submission=SimpleNamespace(
                coinbase_tx_hex="00",
                block_hash_hex="55" * 32,
                block_hex="00",
            ),
            extranonce1_hex=state.extranonce1_hex,
            extranonce2_hex="00" * EXTRANONCE2_SIZE,
            pending_share=stamped_pending_share(1_800_000_000_500),
            credit_share_on_accept=False,
        )
        intent = server.block_candidate_intent(candidate)
        self.assertEqual(intent["payout_snapshot_id"], older.payout_snapshot_id)
        self.assertEqual(
            intent["payout_snapshot_sha256"],
            older.payout_snapshot_sha256,
        )
        replayed = server.block_candidate_from_intent(intent)
        self.assertEqual(
            replayed.context.payout_policy_json,
            older.payout_policy_json,
        )
        self.assertEqual(
            replayed.context.ctv_settlement_json,
            older.ctv_settlement_json,
        )
        self.assertTrue(
            server._resolve_payout_snapshot_for_submission(replayed.context)
        )
        with self.assertRaisesRegex(ValueError, "payout policy snapshot mismatch"):
            server.block_candidate_from_intent(
                {**intent, "payout_policy_json": "{}"}
            )
        self.assertFalse(
            server._resolve_payout_snapshot_for_submission(
                dataclass_replace(
                    older,
                    payout_snapshot_sha256="00" * 32,
                )
            )
        )

        older_key = (
            older.payout_snapshot_id,
            older.payout_snapshot_sha256,
        )
        server._prune_retained_payout_snapshots()
        self.assertIn(older_key, server._retained_payout_snapshots)
        with server.lock:
            server.evicted_job_graveyard = {
                key: entry
                for key, entry in server.evicted_job_graveyard.items()
                if entry.context is not older
            }
        server._prune_retained_payout_snapshots()
        self.assertNotIn(older_key, server._retained_payout_snapshots)
        self.assertGreaterEqual(
            server.payout_snapshot_eviction_counts["jobs_expired"],
            1,
        )

    def test_failure_shutdown_and_retention_bound_release_snapshot_state(
        self,
    ) -> None:
        failed_server, _rpc = coordinator(ledger=MutableFakeLedger())
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        failed_server.clients = {state}

        def fail_builder(**_kwargs: object) -> dict[str, object]:
            raise RuntimeError("forced snapshot build failure")

        failed_server.build_audit_bundle = fail_builder  # type: ignore[method-assign]
        with self.assertRaises(TemplateRefreshBlocked):
            failed_server.poll_qbit_tip_template_once()
        self.assertIsNone(failed_server._active_payout_snapshot_attempt)
        failed_server._prune_retained_payout_snapshots()
        self.assertEqual(failed_server._retained_payout_snapshots, {})
        failed_server.shutdown_tip_refresh_executor()

        retained_server, retained_rpc = coordinator(ledger=MutableFakeLedger())
        artifacts = retained_server.store_template_artifacts(
            dict(retained_rpc.template)
        )
        self.assertIsNotNone(artifacts)
        assert artifacts is not None
        with patch(
            "lab.prism.prism_coordinator.MAX_PRISM_RETAINED_PAYOUT_SNAPSHOTS",
            3,
        ):
            for sequence in range(12):
                with retained_server._payout_snapshot_lock:
                    retained_server._note_payout_snapshot_available_locked(
                        len(retained_server.ledger.miners) + sequence + 1
                    )
                retained_server._select_payout_publication_snapshot(
                    artifacts,
                    claim_owner=False,
                )
                self.assertLessEqual(
                    len(retained_server._retained_payout_snapshots),
                    3,
                )
        snapshot, attempt = retained_server._select_payout_publication_snapshot(
            artifacts,
            claim_owner=True,
        )
        self.assertIsNotNone(attempt)
        self.assertIn(
            (snapshot.snapshot_id, snapshot.snapshot_sha256),
            retained_server._retained_payout_snapshots,
        )
        retained_server.shutdown_tip_refresh_executor()
        self.assertIsNone(retained_server._active_payout_snapshot_attempt)
        self.assertEqual(retained_server._retained_payout_snapshots, {})
        self.assertGreaterEqual(
            retained_server.payout_snapshot_eviction_counts["shutdown"],
            1,
        )


class JobBundleCacheTests(unittest.TestCase):
    def test_tip_template_snapshot_stays_coherent_across_tip_transition(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        server, rpc = coordinator(template=base_template(prevhash=old_tip))
        new_template = base_template(height=11, prevhash=new_tip)
        original_call = rpc.call

        def transition_during_template_fetch(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblocktemplate":
                rpc.tip = new_tip
                rpc.template = new_template
            return original_call(method, params)

        rpc.call = transition_during_template_fetch  # type: ignore[method-assign]

        snapshot = server.fetch_qbit_tip_template_snapshot()

        self.assertEqual(snapshot.bestblockhash, new_tip)
        self.assertEqual(snapshot.previousblockhash, new_tip)
        self.assertEqual(
            snapshot.template_fingerprint,
            qbit_template_fingerprint(new_template),
        )
        self.assertEqual(rpc.calls[:2], ["getblocktemplate", "getbestblockhash"])

    def test_one_heavy_build_shared_across_clients_with_per_client_stamping(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        clients = [client(1), client(2), client(3)]

        contexts = [server.build_job_for_client(c, clean_jobs=True) for c in clients]

        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(rpc.count("getblocktemplate"), 1)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        # The heavy build uses the placeholder extranonce1, never a client's.
        self.assertEqual(
            recorded["suffixes"],
            [
                server.coinbase_tag_hex
                + PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX
                + "00" * EXTRANONCE2_SIZE
            ],
        )
        job_ids = {context.job.job_id for context in contexts}
        self.assertEqual(len(job_ids), 3)
        self.assertEqual(
            [context.job.extranonce1_hex for context in contexts],
            [c.extranonce1_hex for c in clients],
        )
        # coinb1/coinb2 exclude the extranonce window entirely, so the shared
        # split is byte-identical for every client.
        self.assertEqual(len({context.job.coinb1 for context in contexts}), 1)
        self.assertEqual(len({context.job.coinb2 for context in contexts}), 1)
        self.assertTrue(all(not hasattr(context, "bundle") for context in contexts))
        self.assertTrue(
            all(context.prospective_prior_balances == () for context in contexts)
        )
        cached = next(iter(server._job_bundle_cache.values()))
        self.assertFalse(hasattr(cached, "bundle"))
        self.assertEqual(cached.prospective_prior_balances, ())
        self.assertEqual(
            cached.coinbase_manifest["coinbase_tx_hex"],
            synthetic_manifest_coinbase_hex(recorded["suffixes"][0]),
        )
        self.assertIs(contexts[0].shares_json, contexts[1].shares_json)

    def test_latest_wins_scheduler_preserves_synchronous_builder_output(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identity = worker()
        cache_key = server._job_bundle_key(
            artifacts,
            mode="ready",
            payout_state_generation=0,
            worker=identity,
        )

        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_700_000_001_000):
            direct_request = server._new_job_build_request(
                artifacts,
                identity,
                mode="ready",
                payout_state_generation=0,
                cache_key=cache_key,
            )
            direct = server.build_shared_job_bundle(
                artifacts,
                identity,
                mode="ready",
                payout_state_generation=0,
                key=cache_key,
                build_request=direct_request,
            )
            scheduled_request = server._new_job_build_request(
                artifacts,
                identity,
                mode="ready",
                payout_state_generation=0,
                cache_key=cache_key,
            )
            scheduled = server._request_job_build(scheduled_request).result(5)

        server.shutdown_job_build_executor()
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(scheduled.key, direct.key)
        self.assertEqual(scheduled.template, direct.template)
        self.assertEqual(scheduled.template_fingerprint, direct.template_fingerprint)
        self.assertEqual(scheduled.coinbase_manifest, direct.coinbase_manifest)
        self.assertEqual(scheduled.shares_json, direct.shares_json)
        self.assertEqual(scheduled.prior_balances, direct.prior_balances)
        self.assertEqual(scheduled.found_block, direct.found_block)
        self.assertEqual(scheduled.collection_only, direct.collection_only)
        self.assertEqual(scheduled.issued_at_ms, direct.issued_at_ms)
        self.assertEqual(scheduled.base_job, direct.base_job)
        self.assertEqual(scheduled.build_key, direct.build_key)

    def test_stamped_job_reassembles_coinbase_with_client_extranonce(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(0x2A)

        context = server.build_job_for_client(state, clean_jobs=True)
        extranonce2_hex = "11" * EXTRANONCE2_SIZE
        submission = direct_stratum.assemble_submission(
            context.job,
            extranonce2_hex=extranonce2_hex,
            ntime_hex="65000000",
            nonce_hex="00000001",
        )

        expected_suffix = server.coinbase_tag_hex + state.extranonce1_hex + extranonce2_hex
        coinbase = bytes.fromhex(submission.coinbase_tx_hex)
        script_start, script_len = direct_stratum.coinbase_scriptsig_span(
            coinbase, field_name="stamped coinbase"
        )
        script_sig_hex = coinbase[script_start : script_start + script_len].hex()
        self.assertTrue(script_sig_hex.endswith(expected_suffix))
        self.assertNotIn(PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX, script_sig_hex[len("03aabbcc") :])

    def test_template_fingerprint_change_invalidates_bundle_cache(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.build_job_for_client(client(1), clean_jobs=True)

        new_template = base_template(height=11, prevhash="22" * 32)
        rpc.template = new_template
        rpc.tip = str(new_template["previousblockhash"])
        server.store_template_artifacts(dict(new_template))

        context = server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(context.template_fingerprint, qbit_template_fingerprint(new_template))
        # Bundles for the old fingerprint are evicted.
        self.assertEqual(
            {entry.template_fingerprint for entry in server._job_bundle_cache.values()},
            {qbit_template_fingerprint(new_template)},
        )

    def test_bundle_cache_ttl_expiry_rebuilds(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.job_bundle_cache_seconds = 0.05

        server.build_job_for_client(client(1), clean_jobs=True)
        time.sleep(0.06)
        server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(recorded["calls"], 2)

    def test_bundle_cache_lookup_prunes_every_expired_snapshot(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(base_template())
        assert artifacts is not None
        current = server.shared_job_bundle(artifacts, worker())
        expired_key = ("expired-template", "ready")
        expired = dataclass_replace(
            current,
            key=expired_key,
            template_fingerprint="expired-template",
            built_monotonic=time.monotonic() - 60,
        )
        server._job_bundle_cache[expired_key] = expired

        looked_up = server._lookup_job_bundle(current.key)

        self.assertIs(looked_up, current)
        self.assertNotIn(expired_key, server._job_bundle_cache)
        self.assertEqual(list(server._job_bundle_cache.values()), [current])

    def test_zero_ttl_disables_bundle_cache(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.job_bundle_cache_seconds = 0.0

        server.build_job_for_client(client(1), clean_jobs=True)
        server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(recorded["calls"], 2)

    def test_payout_state_change_during_build_retries_before_cache_or_return(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        assert artifacts is not None
        identity = worker()
        original_build = server.build_shared_job_bundle
        built_generations: list[int] = []

        def mutate_after_first_build(
            build_artifacts: object,
            build_worker: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            bundle = original_build(  # type: ignore[arg-type]
                build_artifacts,
                build_worker,
                **kwargs,
            )
            built_generations.append(bundle.payout_state_generation)
            if len(built_generations) == 1:
                server._advance_payout_state_generation()
            return bundle

        server.build_shared_job_bundle = mutate_after_first_build  # type: ignore[method-assign]

        bundle = server.shared_job_bundle(artifacts, identity)
        cached = server.shared_job_bundle(artifacts, identity)

        self.assertEqual(built_generations, [0, 1])
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(bundle.payout_state_generation, 1)
        self.assertIs(cached, bundle)

    def test_child_bundle_waits_for_pending_parent_preview_without_confirmed_read(
        self,
    ) -> None:
        class PreviewLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.current_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.current_balance_reads += 1
                return []

        class ObservedCondition(threading.Condition):
            def __init__(self) -> None:
                super().__init__()
                self.wait_entered = threading.Event()

            def wait(self, timeout: float | None = None) -> bool:
                self.wait_entered.set()
                return super().wait(timeout)

        ledger = PreviewLedger()
        server, rpc = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        assert artifacts is not None
        parent_hash = str(rpc.template["previousblockhash"])
        preview_condition = ObservedCondition()
        server._accepted_block_payout_preview_condition = preview_condition
        server.accepted_block_payout_preview_wait_seconds = 10
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        bundles: list[object] = []
        errors: list[BaseException] = []

        def build_child_bundle() -> None:
            try:
                bundles.append(server.shared_job_bundle(artifacts, worker()))
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        server._begin_accepted_block_payout_preview(parent_hash)
        thread = threading.Thread(target=build_child_bundle, daemon=True)
        thread.start()
        try:
            preview_wait_reached = preview_condition.wait_entered.wait(2)
            waiting_for_preview = thread.is_alive()
            confirmed_reads_while_pending = ledger.current_balance_reads
            builds_while_pending = int(recorded["calls"])
        finally:
            server._publish_accepted_block_payout_preview(parent_hash, preview)
            thread.join(5)
            server._clear_accepted_block_payout_preview(parent_hash)

        self.assertTrue(preview_wait_reached)
        self.assertTrue(waiting_for_preview)
        self.assertEqual(confirmed_reads_while_pending, 0)
        self.assertEqual(builds_while_pending, 0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(bundles), 1)
        bundle = bundles[0]
        self.assertEqual(bundle.prior_balances, preview)  # type: ignore[union-attr]
        self.assertEqual(recorded["last_kwargs"]["prior_balances"], preview)  # type: ignore[index]
        # Preview publication prepares an artifact from one confirmed snapshot;
        # the zero count captured above proves the waiting child did not read it.
        self.assertEqual(ledger.current_balance_reads, 1)
        self.assertEqual(  # type: ignore[union-attr]
            bundle.payout_state_generation,
            server._payout_state_generation,
        )

        self.assertEqual(server._prior_balances_for_job_parent(parent_hash), [])
        self.assertEqual(ledger.current_balance_reads, 2)

    def test_parent_preview_publication_is_idempotent_and_withdrawal_invalidates(
        self,
    ) -> None:
        server, _rpc = coordinator()
        parent_hash = "ab" * 32
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]

        server._begin_accepted_block_payout_preview(parent_hash)
        self.assertEqual(
            server._publish_accepted_block_payout_preview(parent_hash, preview),
            preview,
        )
        self.assertEqual(server._payout_state_generation, 1)

        self.assertEqual(
            server._publish_accepted_block_payout_preview(parent_hash, preview),
            preview,
        )
        self.assertEqual(server._payout_state_generation, 1)
        with self.assertRaisesRegex(RuntimeError, "changed during retry"):
            server._publish_accepted_block_payout_preview(
                parent_hash,
                [{**preview[0], "balance_sats": 26}],
            )

        server._clear_accepted_block_payout_preview(
            parent_hash,
            invalidate_published=True,
        )
        self.assertEqual(server._payout_state_generation, 2)
        self.assertEqual(server._accepted_block_payout_previews, {})
        self.assertEqual(
            server._invalidated_accepted_block_payout_previews,
            {parent_hash: None},
        )
        with self.assertRaisesRegex(TemplateRefreshBlocked, "was withdrawn"):
            server._prior_balances_for_job_parent(parent_hash)

        server._begin_accepted_block_payout_preview(parent_hash)
        self.assertEqual(server._invalidated_accepted_block_payout_previews, {})
        server._clear_accepted_block_payout_preview(parent_hash)

    def test_unpublished_parent_preview_retries_and_reopens_delivery(self) -> None:
        server, _rpc = coordinator()
        server.payout_reconcile_supersession_retries = 2
        parent_hash = "ac" * 32
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]

        server._begin_accepted_block_payout_preview(parent_hash)
        with patch.object(
            server,
            "_publish_payout_state_candidate",
            return_value=None,
        ) as publish_candidate:
            self.assertEqual(
                server._publish_accepted_block_payout_preview(parent_hash, preview),
                preview,
            )

        transition = server._accepted_block_payout_previews[parent_hash]
        self.assertEqual(publish_candidate.call_count, 3)
        self.assertIsNotNone(transition.preview)
        self.assertIsNone(transition.published_generation)
        self.assertEqual(server._payout_state_generation, 0)
        self.assertTrue(server._payout_state_publication_fenced())
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)

        self.assertEqual(
            server._publish_accepted_block_payout_preview(parent_hash, preview),
            preview,
        )

        transition = server._accepted_block_payout_previews[parent_hash]
        self.assertEqual(transition.published_generation, 1)
        self.assertEqual(server._payout_state_generation, 1)
        self.assertFalse(server._payout_state_publication_fenced())
        self.assertFalse(server._payout_state_delivery_gate._delivery_blocked)

    def test_withdrawn_landed_transition_blocks_active_descendant_fallback(
        self,
    ) -> None:
        class CountingLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.current_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.current_balance_reads += 1
                return []

        ledger = CountingLedger()
        server, rpc = coordinator(ledger=ledger)
        accepted_hash = "bc" * 32
        descendant_hash = "bd" * 32
        original_rpc_call = rpc.call

        def active_chain_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return accepted_hash
            return original_rpc_call(method, params)

        rpc.call = active_chain_call  # type: ignore[method-assign]
        server._begin_accepted_block_payout_preview(
            accepted_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            accepted_hash,
            block_height=10,
        )
        server._clear_accepted_block_payout_preview(
            accepted_hash,
            invalidate_published=True,
        )

        with self.assertRaisesRegex(TemplateRefreshBlocked, "was withdrawn"):
            server._prior_balances_for_job_parent(
                descendant_hash,
                parent_height=11,
            )
        self.assertEqual(ledger.current_balance_reads, 0)
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_inactive_landed_ancestor_rejects_preview_patched_artifact(
        self,
    ) -> None:
        accepted_hash = "c0" * 32
        alternate_tip = "c1" * 32
        server, rpc = coordinator(
            template=base_template(height=12, prevhash=alternate_tip)
        )
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        self.assertTrue(server.pool_readiness_latched())
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        server._begin_accepted_block_payout_preview(
            accepted_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            accepted_hash,
            block_height=10,
        )
        server._publish_accepted_block_payout_preview(accepted_hash, preview)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(list(artifact.prior_balances), preview)

        original_rpc_call = rpc.call

        def alternate_chain_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return "c2" * 32
            return original_rpc_call(method, params)

        rpc.call = alternate_chain_call  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "no longer active",
        ):
            server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(recorded["calls"], 0)
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_waiting_child_does_not_fall_back_after_transition_withdrawal(
        self,
    ) -> None:
        class CountingLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.current_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.current_balance_reads += 1
                return []

        class ObservedCondition(threading.Condition):
            def __init__(self) -> None:
                super().__init__()
                self.wait_entered = threading.Event()

            def wait(self, timeout: float | None = None) -> bool:
                self.wait_entered.set()
                return super().wait(timeout)

        ledger = CountingLedger()
        server, _rpc = coordinator(ledger=ledger)
        parent_hash = "be" * 32
        preview_condition = ObservedCondition()
        server._accepted_block_payout_preview_condition = preview_condition
        server.accepted_block_payout_preview_wait_seconds = 10
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=10,
        )
        server._mark_accepted_block_payout_landed(
            parent_hash,
            block_height=10,
        )
        errors: list[BaseException] = []

        def read_parent_balances() -> None:
            try:
                server._prior_balances_for_job_parent(
                    parent_hash,
                    parent_height=10,
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        thread = threading.Thread(target=read_parent_balances, daemon=True)
        thread.start()
        try:
            self.assertTrue(preview_condition.wait_entered.wait(2))
            self.assertTrue(thread.is_alive())
        finally:
            server._clear_accepted_block_payout_preview(
                parent_hash,
                invalidate_published=True,
            )
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIn("was withdrawn", str(errors[0]))
        self.assertEqual(ledger.current_balance_reads, 0)

    def test_pending_parent_preview_wait_is_bounded_and_retryable(self) -> None:
        server, _rpc = coordinator()
        parent_hash = "ac" * 32
        server.accepted_block_payout_preview_wait_seconds = 0.01
        server._begin_accepted_block_payout_preview(parent_hash)

        with self.assertRaisesRegex(TemplateRefreshBlocked, "not ready"):
            server._prior_balances_for_job_parent(parent_hash)

        self.assertTrue(server._tip_refresh_retry.is_set())
        server._clear_accepted_block_payout_preview(parent_hash)

    def test_replayed_active_ancestor_blocks_descendant_until_preview(self) -> None:
        class PreviewLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.current_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.current_balance_reads += 1
                return []

        class ObservedCondition(threading.Condition):
            def __init__(self) -> None:
                super().__init__()
                self.wait_entered = threading.Event()

            def wait(self, timeout: float | None = None) -> bool:
                self.wait_entered.set()
                return super().wait(timeout)

        ledger = PreviewLedger()
        server, rpc = coordinator(ledger=ledger)
        accepted_hash = "ad" * 32
        descendant_hash = "ae" * 32
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]
        original_rpc_call = rpc.call

        def active_chain_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblockhash":
                self.assertEqual(params, [10])
                return accepted_hash
            return original_rpc_call(method, params)

        rpc.call = active_chain_call  # type: ignore[method-assign]
        preview_condition = ObservedCondition()
        server._accepted_block_payout_preview_condition = preview_condition
        server.accepted_block_payout_preview_wait_seconds = 10
        server._begin_accepted_block_payout_preview(accepted_hash, block_height=10)
        balances: list[list[dict[str, object]]] = []
        errors: list[BaseException] = []

        def read_descendant_balances() -> None:
            try:
                balances.append(
                    server._prior_balances_for_job_parent(
                        descendant_hash,
                        parent_height=11,
                    )
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        thread = threading.Thread(target=read_descendant_balances, daemon=True)
        thread.start()
        try:
            self.assertTrue(preview_condition.wait_entered.wait(2))
            self.assertTrue(thread.is_alive())
            self.assertEqual(ledger.current_balance_reads, 0)
        finally:
            server._publish_accepted_block_payout_preview(accepted_hash, preview)
            thread.join(5)
            server._clear_accepted_block_payout_preview(accepted_hash)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(balances, [preview])
        self.assertEqual(ledger.current_balance_reads, 0)

    def test_landed_transition_bars_reconciliation_before_preview(self) -> None:
        server, _rpc = coordinator()
        block_hash = "af" * 32
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)

        with self.assertRaisesRegex(TemplateRefreshBlocked, "confirmation is still pending"):
            with server._payout_balance_mutation():
                self.fail("landed transition must bar payout reconciliation")

        server._clear_accepted_block_payout_preview(block_hash)
        with server._payout_balance_mutation():
            pass

    def test_readiness_latch_during_preparation_admission_reselects_ready_mode(self) -> None:
        ledger = FakeLedger(miners=["solo"])
        server, rpc = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        assert artifacts is not None
        first_lookup_entered = threading.Event()
        release_first_lookup = threading.Event()
        original_lookup = server._lookup_job_bundle
        lookup_calls = 0

        def block_first_lookup(key: tuple[object, ...]) -> object:
            nonlocal lookup_calls
            lookup_calls += 1
            if lookup_calls == 1:
                first_lookup_entered.set()
                self.assertTrue(release_first_lookup.wait(2.0))
            return original_lookup(key)

        server._lookup_job_bundle = block_first_lookup  # type: ignore[method-assign]
        bundles: list[object] = []
        errors: list[BaseException] = []

        def build_bundle() -> None:
            try:
                bundles.append(server.shared_job_bundle(artifacts, worker()))
            except BaseException as exc:  # pragma: no cover - assertion reports it
                errors.append(exc)

        build_thread = threading.Thread(target=build_bundle)
        build_thread.start()
        self.assertTrue(first_lookup_entered.wait(2.0))
        ledger.miners = ["miner-a", "miner-b", "miner-c"]
        release_first_lookup.set()

        build_thread.join(2.0)

        self.assertFalse(build_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(bundles), 1)
        bundle = bundles[0]
        self.assertFalse(bundle.collection_only)  # type: ignore[union-attr]
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(ledger.snapshot_calls, 1)

    def test_payout_generation_advance_does_not_cancel_new_generation_fanout(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        cancellations: list[str] = []

        class CurrentToken:
            payout_state_generation = 1

        class Cancellation:
            def cancel(self) -> None:
                cancellations.append("cancelled")

        class InjectCurrentFanoutLock:
            def __enter__(self) -> object:
                server._active_tip_refresh = (CurrentToken(), Cancellation())
                return self

            def __exit__(self, *_args: object) -> None:
                return None

        # Model a new-generation refresh registering after the cache-state
        # increment but before the invalidator reaches the coordinator lock.
        server.lock = InjectCurrentFanoutLock()  # type: ignore[assignment]

        self.assertEqual(server._advance_payout_state_generation(), 1)
        self.assertEqual(cancellations, [])
        self.assertFalse(server.tip_refresh_is_pending())
        self.assertFalse(server._tip_refresh_retry.is_set())

    def test_payout_generation_retry_marks_tip_refresh_pending(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()

        self.assertFalse(server.tip_refresh_is_pending())
        self.assertEqual(server._advance_payout_state_generation(), 1)

        self.assertTrue(server.tip_refresh_is_pending())
        self.assertTrue(server._tip_refresh_retry.is_set())

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertFalse(server.tip_refresh_is_pending())

    def test_payout_only_advance_bounds_publish_supersession(self) -> None:
        server, _rpc = coordinator()
        server.payout_reconcile_supersession_retries = 2
        real_publish = server._publish_payout_state_candidate
        publish_attempts = 0

        def supersede_before_publish(candidate: object) -> int | None:
            nonlocal publish_attempts
            publish_attempts += 1
            server._reserve_payout_state_source(
                "external_tip",
                tip_hash=f"{publish_attempts + 30:064x}",
            )
            return real_publish(candidate)  # type: ignore[arg-type]

        server._publish_payout_state_candidate = supersede_before_publish  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "payout-only invalidation was superseded",
        ):
            server._advance_payout_state_generation()

        self.assertEqual(publish_attempts, 3)
        self.assertEqual(server._payout_state_generation, 0)
        self.assertTrue(server._payout_state_publication_blocked)
        self.assertTrue(server._payout_state_delivery_gate._delivery_blocked)
        self.assertTrue(server.tip_refresh_is_pending())

    def test_payout_publication_fence_is_not_a_job_build_failure(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server._pool_ready_latched = True
        server._reserve_payout_state_source("payout_only")
        server._block_payout_state_publication()

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        with self.assertRaisesRegex(TemplateRefreshBlocked, "pending publication"):
            server.poll_qbit_tip_template_once()

        self.assertEqual(server.job_build_failure_count, 0)
        self.assertEqual(server.tip_refresh_client_counts["failed"], 0)
        self.assertEqual(server.tip_refresh_client_counts["skipped"], 1)

    def test_successful_poll_clears_payout_pending_created_during_reconcile(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        reconcile_entered = threading.Event()
        allow_reconcile = threading.Event()
        results: list[int] = []
        errors: list[BaseException] = []

        def reconcile(_tip_hash: str) -> bool:
            reconcile_entered.set()
            if not allow_reconcile.wait(5):
                raise AssertionError("test did not release reconciliation")
            return True

        def poll() -> None:
            try:
                results.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        server.ensure_reorg_reconciled_for_tip = reconcile  # type: ignore[method-assign]
        server._mark_tip_refresh_pending("seed")
        poll_thread = threading.Thread(target=poll)
        poll_thread.start()
        try:
            self.assertTrue(reconcile_entered.wait(5))
            self.assertEqual(server._advance_payout_state_generation(), 1)
        finally:
            allow_reconcile.set()
            poll_thread.join(5)

        self.assertFalse(poll_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [0])
        self.assertEqual(server._payout_state_generation, 1)
        self.assertIsNotNone(server.last_successful_template_refresh_monotonic)
        self.assertFalse(server.tip_refresh_is_pending())

    def test_failed_poll_preserves_pending_signal_until_successful_retry(self) -> None:
        server, _rpc = coordinator()
        server._ensure_tip_refresh_state()
        pending_token = server._mark_tip_refresh_pending("seed")

        def fail_reconciliation(_tip_hash: str) -> bool:
            server._schedule_tip_refresh_retry()
            raise RuntimeError("ledger unavailable")

        server.ensure_reorg_reconciled_for_tip = fail_reconciliation  # type: ignore[method-assign]

        with self.assertRaisesRegex(
            TemplateRefreshBlocked,
            "qbit reorg reconciliation failed",
        ):
            server.poll_qbit_tip_template_once()

        self.assertTrue(server._tip_refresh_retry.is_set())
        self.assertTrue(server.tip_refresh_is_pending())
        self.assertEqual(server._tip_refresh_pending_token, pending_token)

        # Model blockpoll claiming the immediate wake and completing the retry.
        server._tip_refresh_retry.clear()
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertFalse(server.tip_refresh_is_pending())

    def test_shutdown_during_reconciliation_stops_poll_without_refresh_failure(self) -> None:
        server, _rpc = coordinator()

        def rejected_reconciliation(_tip_hash: str) -> bool:
            server.stop_event.set()
            raise ShutdownInProgress("PRISM coordinator is shutting down")

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            rejected_reconciliation
        )

        self.assertEqual(server.poll_qbit_tip_template_once(), 0)
        self.assertIsNone(
            getattr(server, "last_successful_template_refresh_monotonic", None)
        )

    def test_completed_refresh_cannot_clear_newer_payout_pending(self) -> None:
        server, _rpc = coordinator()
        snapshot = server.fetch_qbit_tip_template_snapshot()
        sequence = server._reserve_tip_observation_sequence()
        self.assertTrue(
            server.observe_tip_first_seen(
                snapshot.bestblockhash,
                observation_sequence=sequence,
                publish_refresh_observation=True,
            )
        )
        with server.lock:
            server.tip_template_snapshot = snapshot
        completed_generation = server._payout_state_generation
        self.assertEqual(server._advance_payout_state_generation(), 1)
        newer_token = server._tip_refresh_pending_token

        self.assertFalse(
            server._clear_tip_refresh_pending_for_completed_refresh(
                snapshot,
                sequence,
                completed_generation,
            )
        )
        self.assertEqual(server._tip_refresh_pending_token, newer_token)
        self.assertTrue(server.tip_refresh_is_pending())

    def test_escaped_stale_bundle_is_rejected_before_direct_delivery(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]
        original_shared_job_bundle = server.shared_job_bundle
        advanced = False

        def advance_after_bundle(*args: object, **kwargs: object) -> object:
            nonlocal advanced
            bundle = original_shared_job_bundle(*args, **kwargs)  # type: ignore[arg-type]
            if not advanced:
                advanced = True
                server._advance_payout_state_generation()
            return bundle

        server.shared_job_bundle = advance_after_bundle  # type: ignore[method-assign]

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(sent, [])
        self.assertIsNone(state.active_job)
        self.assertEqual(server._payout_state_generation, 1)
        self.assertTrue(server._tip_refresh_retry.is_set())

        self.assertTrue(server.maybe_send_job(state, clean_jobs=True))
        self.assertIsNotNone(state.active_job)
        self.assertEqual(state.active_job.payout_state_generation, 1)
        self.assertEqual(
            [payload["method"] for payload in sent],
            ["mining.set_difficulty", "mining.notify"],
        )

    def test_priority_decision_uses_one_publication_snapshot(self) -> None:
        server, _rpc = coordinator()
        state = client(1)
        context = SimpleNamespace(
            payout_state_generation=0,
            template={"previousblockhash": "11" * 32},
        )
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: True
        )
        server.build_job_for_client = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: context
        )
        original_lock = server._job_cache_lock

        class PublishAfterPrioritySnapshot:
            advanced = False

            def __enter__(self) -> object:
                original_lock.acquire()
                return self

            def __exit__(
                self,
                _exc_type: object,
                _exc: object,
                _traceback: object,
            ) -> None:
                original_lock.release()
                if not self.advanced:
                    self.advanced = True
                    server._payout_state_generation = 1

        priorities: list[bool] = []

        class RecordingGate:
            @contextmanager
            def delivery_cancelable(
                self,
                _cancelled: object,
                *,
                priority: bool,
                **_kwargs: object,
            ) -> object:
                priorities.append(priority)
                yield False

        server._job_cache_lock = PublishAfterPrioritySnapshot()  # type: ignore[assignment]
        server._payout_state_delivery_gate = RecordingGate()  # type: ignore[assignment]

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(priorities, [True])
        self.assertEqual(server._payout_state_generation, 1)

    def test_zero_template_ttl_fetches_template_per_build(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.template_cache_seconds = 0.0

        server.build_job_for_client(client(1), clean_jobs=True)
        server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(rpc.count("getblocktemplate"), 2)

    def test_late_stale_template_fetch_cannot_replace_newer_artifacts(self) -> None:
        server, rpc = coordinator()
        server.template_cache_seconds = 0.0
        stale_template = dict(rpc.template)
        current_template = base_template(height=11, prevhash="22" * 32)
        fetch_started = threading.Event()
        release_fetch = threading.Event()
        results: list[object] = []
        errors: list[BaseException] = []
        original_call = rpc.call
        thread: threading.Thread

        def blocking_call(
            method: str,
            params: list[object] | None = None,
        ) -> object:
            if method == "getblocktemplate" and threading.current_thread() is thread:
                fetch_started.set()
                if not release_fetch.wait(5):
                    raise AssertionError("stale template fetch was not released")
                return dict(stale_template)
            return original_call(method, params)

        def fetch_stale_artifacts() -> None:
            try:
                results.append(server.current_template_artifacts())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        rpc.call = blocking_call  # type: ignore[method-assign]
        thread = threading.Thread(target=fetch_stale_artifacts)
        thread.start()
        try:
            self.assertTrue(fetch_started.wait(5))
            current_artifacts = server.store_template_artifacts(current_template)
            self.assertIsNotNone(current_artifacts)
            assert current_artifacts is not None
            self.assertGreater(current_artifacts.generation, 1)
        finally:
            release_fetch.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [current_artifacts])
        self.assertIs(server._template_artifacts, current_artifacts)
        self.assertEqual(
            current_artifacts.fingerprint,
            qbit_template_fingerprint(current_template),
        )

    def test_collection_mode_bundles_are_keyed_per_worker(self) -> None:
        server, _ = coordinator(ledger=FakeLedger(miners=["solo"]))
        recorded = install_fake_bundle_builder(server)
        server.min_ready_miners = 3

        worker_a = worker(payout="tq1worker-a")
        worker_b = worker(payout="tq1worker-b")
        context_a1 = server.build_job_for_client(client(1, worker_a), clean_jobs=True)
        context_a2 = server.build_job_for_client(client(2, worker_a), clean_jobs=True)
        context_b = server.build_job_for_client(client(3, worker_b), clean_jobs=True)

        self.assertTrue(context_a1.collection_only)
        self.assertTrue(context_b.collection_only)
        self.assertEqual(recorded["calls"], 2)
        self.assertTrue(
            all(not hasattr(context, "bundle") for context in (context_a1, context_a2, context_b))
        )
        self.assertIs(context_a1.shares_json, context_a2.shares_json)
        self.assertIsNot(context_a1.shares_json, context_b.shares_json)

    def test_collection_bundle_cache_rebuilds_when_pool_becomes_ready(self) -> None:
        ledger = FakeLedger(miners=["solo"])
        server, _ = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        state = client(1)

        collection_context = server.build_job_for_client(state, clean_jobs=True)
        ledger.miners = ["miner-a", "miner-b", "miner-c"]
        ready_context = server.build_job_for_client(state, clean_jobs=True)

        self.assertTrue(collection_context.collection_only)
        self.assertFalse(ready_context.collection_only)
        self.assertEqual(recorded["calls"], 2)
        self.assertEqual(ledger.snapshot_calls, 1)

    def test_ready_empty_snapshot_does_not_fall_back_to_worker_collection(self) -> None:
        ledger = ReadyLedgerWithEmptyFirstSnapshot()
        server, _ = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        state = client(1)

        with self.assertRaisesRegex(
            RuntimeError,
            "ready-pool ledger snapshot contained no payout shares",
        ):
            server.build_job_for_client(state, clean_jobs=True)
        ready_context = server.build_job_for_client(state, clean_jobs=True)

        self.assertFalse(ready_context.collection_only)
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(ledger.snapshot_calls, 2)

    def test_vardiff_difficulty_is_stamped_per_client(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        easy = client(1)
        hard = client(2)
        hard.pending_share_difficulty = Decimal("512")

        easy_context = server.build_job_for_client(easy, clean_jobs=True)
        hard_context = server.build_job_for_client(hard, clean_jobs=True)

        self.assertEqual(easy_context.job.coinb1, hard_context.job.coinb1)
        self.assertGreater(easy_context.job.share_target, hard_context.job.share_target)
        self.assertEqual(hard_context.job.share_difficulty, Decimal("512"))

    def test_template_artifacts_reuse_derivations_when_fingerprint_unchanged(self) -> None:
        server, _ = coordinator()
        template = base_template()
        first = server.store_template_artifacts(dict(template))
        refreshed_template = dict(template)
        refreshed_template["curtime"] = int(template["curtime"]) + 30
        second = server.store_template_artifacts(refreshed_template)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertIs(first.transaction_hexes, second.transaction_hexes)
        self.assertIs(first.witness_merkle_leaves_hex, second.witness_merkle_leaves_hex)

    def test_poll_seeds_template_cache_for_client_builds(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)

        refreshed = server.poll_qbit_tip_template_once()
        self.assertEqual(refreshed, 0)
        self.assertEqual(rpc.count("getblocktemplate"), 1)

        server.build_job_for_client(client(1), clean_jobs=True)
        self.assertEqual(rpc.count("getblocktemplate"), 1)

    def test_reorg_reconciliation_cached_per_tip(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            with server.lock:
                server.last_reorg_reconciled_tip_hash = tip_hash
                server.last_reorg_reconciled_trusted = True
                server.last_reorg_reconciled_monotonic = time.monotonic()
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(len(reconcile_calls), 1)

        rpc.tip = "33" * 32
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(len(reconcile_calls), 2)
        self.assertEqual(reconcile_calls[-1], "33" * 32)

    def test_reorg_cache_rechecks_chain_view_before_reuse(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        rpc.blockchain_info["headers"] = 101
        with server.lock:
            server.last_reorg_reconciled_tip_hash = rpc.tip
            server.last_reorg_reconciled_trusted = True
            server.last_reorg_reconciled_monotonic = time.monotonic()
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            with server.lock:
                server.last_reorg_reconciled_tip_hash = tip_hash
                server.last_reorg_reconciled_trusted = False
                server.last_reorg_reconciled_monotonic = time.monotonic()
            return {"untrusted": True}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertFalse(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(rpc.count("getblockchaininfo"), 1)
        self.assertEqual(reconcile_calls, [rpc.tip])
        self.assertFalse(server.last_reorg_reconciled_trusted)

    def test_single_flight_builds_once_under_concurrency(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        original_builder = server.build_audit_bundle
        build_started = threading.Event()

        def slow_builder(**kwargs: object) -> dict[str, object]:
            build_started.set()
            time.sleep(0.05)
            return original_builder(**kwargs)

        server.build_audit_bundle = slow_builder  # type: ignore[method-assign]
        errors: list[BaseException] = []

        def build(connection_id: int) -> None:
            try:
                server.build_job_for_client(client(connection_id), clean_jobs=True)
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        threads = [threading.Thread(target=build, args=(index + 1,)) for index in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(recorded["calls"], 1)

    def test_observed_tip_change_rejects_stale_cached_bundle_delivery(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        server, rpc = coordinator(template=base_template(prevhash=old_tip))
        install_fake_bundle_builder(server)
        state = client(1)
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]
        server.clients = {state}
        server.observe_tip_first_seen(old_tip, observation_sequence=1)

        self.assertTrue(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(len(sent), 2)
        sent.clear()

        rpc.tip = new_tip
        server.observe_tip_first_seen(new_tip, observation_sequence=2)

        self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(sent, [])

    def test_ready_ledger_snapshot_holds_payout_mutation_lock(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        entered_snapshot = threading.Event()
        release_snapshot = threading.Event()
        original_snapshot = server.ledger.snapshot_at_job_issue

        def blocked_snapshot(*args: object, **kwargs: object) -> object:
            entered_snapshot.set()
            self.assertTrue(release_snapshot.wait(2))
            return original_snapshot(*args, **kwargs)

        server.ledger.snapshot_at_job_issue = blocked_snapshot  # type: ignore[method-assign]
        errors: list[BaseException] = []
        build_thread = threading.Thread(
            target=lambda: self._capture_error(
                errors,
                lambda: server.shared_job_bundle(artifacts, mode="ready"),
            )
        )
        build_thread.start()
        try:
            self.assertTrue(entered_snapshot.wait(2))
            mutation_acquired = server._payout_state_prepare_lock.acquire(
                blocking=False
            )
            if mutation_acquired:
                server._payout_state_prepare_lock.release()
            self.assertFalse(mutation_acquired)
        finally:
            release_snapshot.set()
        build_thread.join(2)

        self.assertFalse(build_thread.is_alive())
        self.assertEqual(errors, [])

    def test_ready_build_identity_separates_clock_only_generations(self) -> None:
        server, rpc = coordinator()
        first = server.store_template_artifacts(dict(rpc.template))
        second_template = dict(rpc.template)
        second_template["curtime"] = int(second_template["curtime"]) + 1
        second = server.store_template_artifacts(second_template)
        assert first is not None and second is not None
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertNotEqual(first.generation, second.generation)
        payout_generation = server._payout_state_generation

        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=[1_700_000_000_000, 1_700_000_001_000],
        ):
            first_request = server._new_job_build_request(
                first,
                None,
                mode="ready",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    first,
                    mode="ready",
                    payout_state_generation=payout_generation,
                    worker=None,
                ),
            )
            second_request = server._new_job_build_request(
                second,
                None,
                mode="ready",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    second,
                    mode="ready",
                    payout_state_generation=payout_generation,
                    worker=None,
                ),
            )

        self.assertNotEqual(
            first_request.equivalence_key,
            second_request.equivalence_key,
        )
        self.assertNotEqual(
            first_request.key.issued_at_ms,
            second_request.key.issued_at_ms,
        )

    def test_precomputed_payout_artifact_matches_inline_output_exactly(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_700_000_123_000):
            inline = server.shared_job_bundle(artifacts, mode="ready")
            with server._job_cache_lock:
                server._job_bundle_cache.clear()
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            prepared = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(prepared.base_job, inline.base_job)
        self.assertEqual(prepared.coinbase_manifest, inline.coinbase_manifest)
        self.assertEqual(prepared.shares_json, inline.shares_json)
        self.assertEqual(prepared.prior_balances, inline.prior_balances)
        self.assertEqual(prepared.found_block, inline.found_block)
        self.assertGreater(prepared.payout_artifact_generation, 0)

    def test_accepted_preview_patches_artifact_across_normal_clear(self) -> None:
        class CountingBalanceLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__()
                self.prior_balance_reads = 0
                self.database_balances = [
                    {
                        "recipient_id": "stale-miner",
                        "order_key": "stale-miner",
                        "p2mr_program_hex": "22" * 32,
                        "balance_sats": 1,
                    }
                ]

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.prior_balance_reads += 1
                return [dict(balance) for balance in self.database_balances]

        ledger = CountingBalanceLedger()
        server, rpc = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        parent_hash = str(rpc.template["previousblockhash"])
        parent_height = int(rpc.template["height"]) - 1
        preview = [
            {
                "recipient_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "11" * 32,
                "balance_sats": 25,
            }
        ]

        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=parent_height,
        )
        server._publish_accepted_block_payout_preview(parent_hash, preview)

        artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.prior_balances, tuple(preview))
        self.assertEqual(
            artifact.payout_state_generation,
            server._payout_state_generation,
        )
        self.assertGreater(artifact.generation, 0)

        ledger.prior_balance_reads = 0
        preview_bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(preview_bundle.prior_balances, preview)
        self.assertEqual(recorded["last_kwargs"]["prior_balances"], preview)  # type: ignore[index]
        self.assertEqual(
            preview_bundle.payout_artifact_generation,
            artifact.generation,
        )
        self.assertEqual(ledger.prior_balance_reads, 0)

        payout_generation = server._payout_state_generation
        ledger.database_balances = [dict(balance) for balance in preview]
        server._clear_accepted_block_payout_preview(parent_hash)
        self.assertEqual(server._payout_state_generation, payout_generation)
        self.assertIs(server._payout_ledger_artifact, artifact)
        self.assertNotIn(parent_hash, server._accepted_block_payout_previews)
        self.assertNotIn(
            parent_hash,
            server._invalidated_accepted_block_payout_previews,
        )
        with server._job_cache_lock:
            server._job_bundle_cache.clear()

        post_clear_bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(post_clear_bundle.prior_balances, preview)
        self.assertEqual(recorded["last_kwargs"]["prior_balances"], preview)  # type: ignore[index]
        self.assertEqual(
            post_clear_bundle.payout_artifact_generation,
            artifact.generation,
        )
        self.assertEqual(ledger.prior_balance_reads, 0)

    def test_valid_precomputed_artifact_skips_tip_path_ledger_snapshot(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        assert snapshot.template_artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            snapshot.template_artifacts.network_difficulty,
        )
        server.ledger.snapshot_calls = 0

        bundle = server.prepare_tip_refresh_bundle(snapshot)

        self.assertFalse(bundle.collection_only)
        self.assertGreater(bundle.payout_artifact_generation, 0)
        self.assertEqual(server.ledger.snapshot_calls, 0)

    def test_mismatched_precomputed_artifact_falls_back_to_inline_snapshot(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._current_payout_state_artifact()
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            published = server._published_payout_state
            assert published.artifact is not None
            changed_balances = [{"miner_id": "miner-z", "balance_sats": 1}]
            server._published_payout_state = dataclass_replace(
                published,
                artifact=dataclass_replace(
                    published.artifact,
                    prior_balances_json=canonical_json_text(changed_balances),
                    prior_balances_sha256=canonical_json_sha256(changed_balances),
                ),
            )

        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        self.assertIsNone(server._payout_ledger_artifact)
        server.ledger.snapshot_calls = 0

        bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(bundle.payout_artifact_generation, 0)
        self.assertEqual(server.ledger.snapshot_calls, 1)

    def test_new_tip_cancels_blocked_old_bundle_without_publication(self) -> None:
        old_tip = "11" * 32
        new_tip = "22" * 32
        server, rpc = coordinator(template=base_template(prevhash=old_tip))
        recorded = install_fake_bundle_builder(server)
        original_builder = server.build_audit_bundle
        build_started = threading.Event()

        def cancelable_builder(**kwargs: object) -> dict[str, object]:
            control = server._job_build_phase_local.bundle_build_control
            build_started.set()
            self.assertTrue(control.cancel_event.wait(2))
            return original_builder(**kwargs)

        server.build_audit_bundle = cancelable_builder  # type: ignore[method-assign]
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.observe_tip_first_seen(old_tip, observation_sequence=1)
        errors: list[BaseException] = []
        thread = threading.Thread(
            target=lambda: self._capture_error(
                errors,
                lambda: server.shared_job_bundle(artifacts, mode="ready"),
            )
        )
        thread.start()
        self.assertTrue(build_started.wait(2))

        server.observe_tip_first_seen(new_tip, observation_sequence=2)
        thread.join(2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(server._active_job_bundle_builds, {})
        self.assertEqual(server.tip_refresh_build_inflight, 0)
        self.assertFalse(any(
            entry.template_fingerprint == artifacts.fingerprint
            for entry in server._job_bundle_cache.values()
        ))
        self.assertEqual(server.tip_refresh_superseded_results, 1)

    def test_builder_crash_and_timeout_fail_closed_then_recover(self) -> None:
        server, _rpc = coordinator()
        server.prism_ctv_settlement_config = lambda **_kwargs: None  # type: ignore[method-assign]
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        build_kwargs = {
            "shares": [],
            "found_block": {
                "block_height": 10,
                "coinbase_value_sats": 50_00000000,
                "network_difficulty": 1,
                "anchor_job_issued_at_ms": 1_700_000_000_000,
            },
            "prior_balances": [],
            "coinbase_script_sig_suffix_hex": "00",
        }

        with patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "raise SystemExit(7)"],
        ):
            with self.assertRaisesRegex(RuntimeError, "failed"):
                server.build_audit_bundle(**build_kwargs)

        server.bundle_build_timeout_seconds = 0.01
        with patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[
                sys.executable,
                "-c",
                "import time; time.sleep(5)",
            ],
        ):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                server.build_audit_bundle(**build_kwargs)

        server.bundle_build_timeout_seconds = 1.0
        recovery_script = (
            "import json,sys; json.load(sys.stdin); "
            "json.dump({'recovered': True}, sys.stdout)"
        )
        with patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", recovery_script],
        ):
            recovered = server.build_audit_bundle(**build_kwargs)
        self.assertEqual(recovered, {"recovered": True})

    def test_audit_builder_child_does_not_inherit_open_socket(self) -> None:
        server, _rpc = coordinator()
        server.prism_ctv_settlement_config = lambda **_kwargs: None  # type: ignore[method-assign]
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        probe_script = (
            "import json,os,sys; json.load(sys.stdin); fd=int(sys.argv[1]); "
            "inherited=True; "
            "\ntry: os.fstat(fd)"
            "\nexcept OSError: inherited=False"
            "\njson.dump({'inherited_socket': inherited}, sys.stdout)"
        )
        with socket.socket() as parent_socket:
            parent_socket.set_inheritable(True)
            with patch(
                "lab.prism.prism_coordinator.prism_tool_command",
                return_value=[
                    sys.executable,
                    "-c",
                    probe_script,
                    str(parent_socket.fileno()),
                ],
            ):
                result = server.build_audit_bundle(
                    shares=[],
                    found_block={
                        "block_height": 10,
                        "coinbase_value_sats": 50_00000000,
                        "network_difficulty": 1,
                        "anchor_job_issued_at_ms": 1_700_000_000_000,
                    },
                    prior_balances=[],
                    coinbase_script_sig_suffix_hex="00",
                )
        self.assertEqual(result, {"inherited_socket": False})

    def test_repeated_superseded_builds_leave_state_bounded(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        original_builder = server.build_audit_bundle
        starts: queue.Queue[None] = queue.Queue()

        def cancelable_builder(**kwargs: object) -> dict[str, object]:
            control = server._job_build_phase_local.bundle_build_control
            starts.put(None)
            self.assertTrue(control.cancel_event.wait(2))
            return original_builder(**kwargs)

        server.build_audit_bundle = cancelable_builder  # type: ignore[method-assign]
        current_tip = str(rpc.tip)
        server.observe_tip_first_seen(current_tip, observation_sequence=1)
        errors: list[BaseException] = []
        for index in range(8):
            rpc.template = base_template(height=10 + index, prevhash=current_tip)
            artifacts = server.store_template_artifacts(dict(rpc.template))
            assert artifacts is not None
            thread = threading.Thread(
                target=lambda current=artifacts: self._capture_error(
                    errors,
                    lambda: server.shared_job_bundle(current, mode="ready"),
                )
            )
            thread.start()
            starts.get(timeout=2)
            current_tip = f"{index + 2:064x}"
            rpc.tip = current_tip
            server.observe_tip_first_seen(
                current_tip,
                observation_sequence=index + 2,
            )
            thread.join(2)
            self.assertFalse(thread.is_alive())

        self.assertEqual(len(errors), 8)
        self.assertTrue(all(isinstance(exc, TemplateRefreshBlocked) for exc in errors))
        self.assertEqual(server._active_job_bundle_builds, {})
        self.assertEqual(server.tip_refresh_build_inflight, 0)
        self.assertEqual(server.tip_refresh_build_queue_depth, 0)
        self.assertLessEqual(
            len(server._job_bundle_cache),
            MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES,
        )
        self.assertEqual(server.tip_refresh_superseded_results, 8)

    @staticmethod
    def _capture_error(
        errors: list[BaseException],
        operation: object,
    ) -> None:
        try:
            operation()  # type: ignore[operator]
        except BaseException as exc:  # noqa: BLE001 - test thread handoff
            errors.append(exc)

    def test_ready_tip_refresh_builds_once_and_stamps_every_client(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        clients = [client(1), client(2), client(3)]
        clients[1].pending_share_difficulty = Decimal("8")
        sent: dict[int, list[dict[str, object]]] = {state.connection_id: [] for state in clients}
        for state in clients:
            state.send = (  # type: ignore[method-assign]
                lambda payload, connection_id=state.connection_id: sent[connection_id].append(payload)
            )
        server.clients = set(clients)

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 3)
        self.assertEqual(recorded["calls"], 1)
        contexts = [state.active_job for state in clients]
        self.assertEqual(len({context.job.job_id for context in contexts}), 3)
        self.assertEqual(
            [context.job.extranonce1_hex for context in contexts],
            [state.extranonce1_hex for state in clients],
        )
        self.assertEqual(contexts[1].job.share_difficulty, Decimal("8"))
        self.assertEqual(
            [payload["method"] for payload in sent[2]],
            ["mining.set_difficulty", "mining.notify"],
        )
        metrics = server.metrics_payload()
        self.assertIn('qbit_prism_tip_refresh_clients_total{result="sent"} 3', metrics)
        self.assertIn("qbit_prism_tip_refresh_first_delivery_seconds_count 1", metrics)
        self.assertIn("qbit_prism_tip_refresh_last_delivery_seconds_count 1", metrics)

    def test_ready_tip_refresh_shares_one_bundle_across_250_clients(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        reconciled: list[str] = []
        trust_checks = 0

        def reconcile_once(tip_hash: str) -> bool:
            reconciled.append(tip_hash)
            return True

        def chain_view_untrusted() -> bool:
            nonlocal trust_checks
            trust_checks += 1
            return False

        server.ensure_reorg_reconciled_for_tip = reconcile_once  # type: ignore[method-assign]
        server.qbit_chain_view_untrusted = chain_view_untrusted  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = (  # type: ignore[method-assign]
            lambda **_kwargs: self.fail("fanout repeated current-tip validation")
        )
        clients = [client(index + 1) for index in range(250)]
        sent: dict[int, list[dict[str, object]]] = {
            state.connection_id: [] for state in clients
        }
        for state in clients:
            state.send = (  # type: ignore[method-assign]
                lambda payload, connection_id=state.connection_id: sent[
                    connection_id
                ].append(payload)
            )
        server.clients = set(clients)

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 250)
        self.assertEqual(recorded["calls"], 1)
        cached = next(iter(server._job_bundle_cache.values()))
        self.assertEqual(
            len({id(state.active_job.shares_json) for state in clients}),
            1,
        )
        self.assertTrue(
            all(state.active_job.shares_json is cached.shares_json for state in clients)
        )
        self.assertEqual(reconciled, [rpc.tip])
        self.assertEqual(trust_checks, 2)
        # The early priority probe, snapshot coherence, pre-fanout validation,
        # and post-fanout detection are each constant-cost regardless of
        # client count.
        self.assertEqual(rpc.count("getbestblockhash"), 4)
        self.assertTrue(all(len(payloads) == 2 for payloads in sent.values()))
        fingerprints = {state.active_job.template_fingerprint for state in clients}
        self.assertEqual(len(fingerprints), 1)

    def test_same_fingerprint_bundle_rebinds_exact_template_observation(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        identity = worker()
        first = server.store_template_artifacts(base_template())
        assert first is not None
        original = server.shared_job_bundle(first, identity)
        updated_template = dict(first.template)
        updated_template["curtime"] = int(updated_template["curtime"]) + 30
        second = server.store_template_artifacts(updated_template)
        assert second is not None

        rebound = server.shared_job_bundle(second, identity)

        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertIs(rebound.template, second.template)
        self.assertIsNot(rebound.template, original.template)
        self.assertEqual(rebound.template_generation, second.generation)
        self.assertEqual(rebound.base_job.ntime, f'{updated_template["curtime"]:08x}')

    def test_clock_only_refresh_does_not_discard_inflight_ready_build(self) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        build_entered = threading.Event()
        release_build = threading.Event()
        original_build = server.build_shared_job_bundle
        build_calls = 0
        build_calls_lock = threading.Lock()

        def blocking_build(*args: object, **kwargs: object) -> object:
            nonlocal build_calls
            with build_calls_lock:
                build_calls += 1
            build_entered.set()
            self.assertTrue(release_build.wait(5))
            return original_build(*args, **kwargs)  # type: ignore[arg-type]

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        results: list[list[object]] = [[], []]
        errors: list[list[BaseException]] = [[], []]

        def build(index: int, build_artifacts: object) -> None:
            try:
                results[index].append(
                    server.shared_job_bundle(  # type: ignore[arg-type]
                        build_artifacts,
                        mode="ready",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors[index].append(exc)

        first_thread = threading.Thread(target=build, args=(0, first))
        second_thread: threading.Thread | None = None
        first_thread.start()
        try:
            self.assertTrue(build_entered.wait(5))
            updated_template = dict(first.template)
            updated_template["curtime"] = int(updated_template["curtime"]) + 30
            second = server.store_template_artifacts(updated_template)
            assert second is not None
            second_thread = threading.Thread(target=build, args=(1, second))
            second_thread.start()
            time.sleep(0.05)
            with build_calls_lock:
                self.assertEqual(build_calls, 1)
        finally:
            release_build.set()
            first_thread.join(5)
            if second_thread is not None:
                second_thread.join(5)

        self.assertFalse(first_thread.is_alive())
        assert second_thread is not None
        self.assertFalse(second_thread.is_alive())
        self.assertEqual(errors, [[], []])
        self.assertEqual([len(items) for items in results], [1, 1])
        built = results[0][0]
        self.assertIs(built.template, first.template)  # type: ignore[union-attr]
        rebound = results[1][0]
        self.assertIs(rebound.template, second.template)
        self.assertEqual(rebound.template_generation, second.generation)
        self.assertEqual(recorded["calls"], 1)

    def test_same_fingerprint_collection_bundle_rebuilds_exact_observation(self) -> None:
        server, _ = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        recorded = install_fake_bundle_builder(server)
        identity = worker()
        first = server.store_template_artifacts(base_template())
        assert first is not None
        original = server.shared_job_bundle(first, identity)
        updated_template = dict(first.template)
        updated_template["curtime"] = int(updated_template["curtime"]) + 30
        second = server.store_template_artifacts(updated_template)
        assert second is not None

        rebuilt = server.shared_job_bundle(second, identity)

        self.assertTrue(original.collection_only)
        self.assertTrue(rebuilt.collection_only)
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertEqual(recorded["calls"], 2)
        self.assertIsNot(rebuilt.coinbase_manifest, original.coinbase_manifest)
        self.assertIs(rebuilt.template, second.template)
        self.assertEqual(rebuilt.template_generation, second.generation)

    def test_job_bundle_cache_is_bounded(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(base_template())
        assert artifacts is not None
        bundle = server.shared_job_bundle(artifacts, worker())

        for index in range(MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES + 5):
            candidate = dataclass_replace(
                bundle,
                key=(artifacts.fingerprint, "test", index),
            )
            server._cache_job_bundle_if_current(candidate, artifacts)

        self.assertEqual(
            len(server._job_bundle_cache),
            MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES,
        )
        self.assertNotIn(
            (artifacts.fingerprint, "test", 0),
            server._job_bundle_cache,
        )

    def test_job_bundle_cache_preserves_coordinator_lock_order(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(base_template())
        assert artifacts is not None
        bundle = server.shared_job_bundle(artifacts, mode="ready")
        observed_cache_lock = ObservedRLock()
        server._job_cache_lock = observed_cache_lock  # type: ignore[assignment]
        errors: list[BaseException] = []

        def cache_bundle() -> None:
            try:
                server._cache_job_bundle_if_current(bundle, artifacts)
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        observed_cache_lock.acquire()
        observed_cache_lock.observe_acquires = True
        cache_thread = threading.Thread(target=cache_bundle)
        coordinator_lock_acquired = False
        try:
            cache_thread.start()
            self.assertTrue(observed_cache_lock.acquire_attempted.wait(5))
            coordinator_lock_acquired = server.lock.acquire(timeout=0.25)
            if coordinator_lock_acquired:
                server.lock.release()
        finally:
            observed_cache_lock.release()
            cache_thread.join(5)

        self.assertTrue(coordinator_lock_acquired)
        self.assertFalse(cache_thread.is_alive())
        self.assertEqual(errors, [])

    def test_active_gap_replaces_older_pending_job_build(self) -> None:
        server, rpc = coordinator()
        first = server.store_template_artifacts(dict(rpc.template))
        second_template = dict(rpc.template)
        second_template["coinbasevalue"] = int(second_template["coinbasevalue"]) + 1
        second = server.store_template_artifacts(second_template)
        assert first is not None and second is not None
        payout_generation = server._payout_state_generation

        def request_for(artifacts: object) -> object:
            return server._new_job_build_request(
                artifacts,  # type: ignore[arg-type]
                None,
                mode="ready",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    artifacts,  # type: ignore[arg-type]
                    mode="ready",
                    payout_state_generation=payout_generation,
                    worker=None,
                ),
            )

        pending = request_for(first)
        newest = request_for(second)
        server._job_build_active = None
        server._job_build_retiring = SimpleNamespace(request=pending)
        server._job_build_pending = pending
        server._start_job_build_locked = (  # type: ignore[method-assign]
            lambda request: SimpleNamespace(request=request, future=None)
        )
        server._arm_job_build_locked = lambda _flight: None  # type: ignore[method-assign]

        promise = server._request_job_build(newest)  # type: ignore[arg-type]

        self.assertIs(promise, newest.promise)  # type: ignore[union-attr]
        self.assertIsNone(server._job_build_pending)
        assert server._job_build_active is not None
        self.assertIs(server._job_build_active.request, newest)
        self.assertTrue(pending.promise.done())  # type: ignore[union-attr]
        self.assertIsInstance(
            pending.promise.exception(),  # type: ignore[union-attr]
            JobBuildSuperseded,
        )

    def test_cancelled_ready_does_not_block_collection_promotion(self) -> None:
        for placement in ("active", "retiring"):
            with self.subTest(placement=placement):
                server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
                artifacts = server.store_template_artifacts(dict(rpc.template))
                assert artifacts is not None
                payout_generation = server._payout_state_generation

                def request_for(
                    mode: str,
                    identity: WorkerIdentity | None,
                ) -> object:
                    return server._new_job_build_request(
                        artifacts,
                        identity,
                        mode=mode,
                        payout_state_generation=payout_generation,
                        cache_key=server._job_bundle_key(
                            artifacts,
                            mode=mode,
                            payout_state_generation=payout_generation,
                            worker=identity,
                        ),
                    )

                ready = request_for("ready", None)
                collection = request_for(
                    "collection",
                    worker("tq1collection", "tq1collection.rig"),
                )
                self.assertTrue(  # type: ignore[union-attr]
                    ready.cancellation.cancel("superseded")
                )
                ready_flight = SimpleNamespace(request=ready)
                if placement == "active":
                    server._job_build_active = ready_flight
                    server._job_build_retiring = None
                else:
                    server._job_build_active = None
                    server._job_build_retiring = ready_flight
                server._job_build_pending = collection
                armed: list[object] = []
                server._start_job_build_locked = (  # type: ignore[method-assign]
                    lambda request: SimpleNamespace(request=request, future=None)
                )
                server._arm_job_build_locked = armed.append  # type: ignore[method-assign]

                server._promote_pending_job_build_locked()

                self.assertIsNone(server._job_build_pending)
                assert server._job_build_active is not None
                self.assertIs(server._job_build_active.request, collection)
                self.assertIs(server._job_build_retiring, ready_flight)
                self.assertEqual(armed, [server._job_build_active])

    def test_immediate_collection_completion_does_not_reoccupy_slot(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        payout_generation = server._payout_state_generation
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(2)
        ]

        def request_for(identity: WorkerIdentity) -> object:
            return server._new_job_build_request(
                artifacts,
                identity,
                mode="collection",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    artifacts,
                    mode="collection",
                    payout_state_generation=payout_generation,
                    worker=identity,
                ),
            )

        pending = request_for(identities[0])
        incoming = request_for(identities[1])
        results: dict[int, object] = {}

        def completed_flight(request: object) -> object:
            result = SimpleNamespace(request=request)
            results[id(request)] = result
            future: Future[object] = Future()
            future.set_result(result)
            return SimpleNamespace(request=request, future=future)

        server._job_build_pending = pending
        server._start_job_build_locked = completed_flight  # type: ignore[method-assign]

        promise = server._request_job_build(incoming)  # type: ignore[arg-type]

        self.assertIs(promise.result(), results[id(incoming)])
        self.assertIs(  # type: ignore[union-attr]
            pending.promise.result(),
            results[id(pending)],
        )
        self.assertIsNone(server._job_build_active)
        self.assertIsNone(server._job_build_retiring)
        self.assertIsNone(server._job_build_pending)

    def test_independent_collection_workers_do_not_supersede_each_other(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(4)
        ]
        entered = [threading.Event() for _identity in identities]
        releases = [threading.Event() for _identity in identities]
        original_build = server.build_shared_job_bundle
        active_builds = 0
        max_active_builds = 0
        active_lock = threading.Lock()

        def blocking_build(
            build_artifacts: object,
            identity: WorkerIdentity,
            **kwargs: object,
        ) -> object:
            nonlocal active_builds, max_active_builds
            index = identities.index(identity)
            with active_lock:
                active_builds += 1
                max_active_builds = max(max_active_builds, active_builds)
            entered[index].set()
            try:
                self.assertTrue(releases[index].wait(5))
                return original_build(
                    build_artifacts,  # type: ignore[arg-type]
                    identity,
                    **kwargs,
                )
            finally:
                with active_lock:
                    active_builds -= 1

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        results: list[list[object]] = [[] for _identity in identities]
        errors: list[list[BaseException]] = [[] for _identity in identities]

        def build(index: int) -> None:
            try:
                results[index].append(
                    server.shared_job_bundle(
                        artifacts,
                        identities[index],
                        mode="collection",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors[index].append(exc)

        threads = [
            threading.Thread(target=build, args=(index,))
            for index in range(len(identities))
        ]
        threads[0].start()
        try:
            self.assertTrue(entered[0].wait(5))
            threads[1].start()
            self.assertTrue(entered[1].wait(5))
            threads[2].start()
            pending_deadline = time.monotonic() + 5
            while time.monotonic() < pending_deadline:
                with server._job_build_scheduler_lock:
                    pending = server._job_build_pending
                    if pending is not None and pending.worker == identities[2]:
                        break
                time.sleep(0.01)
            else:
                self.fail("third collection build was not queued")
            threads[3].start()
            self.assertEqual(server.job_build_scheduler_counts["starts"], 2)
            releases[0].set()
            self.assertTrue(entered[2].wait(5))
            releases[1].set()
            self.assertTrue(entered[3].wait(5))
            self.assertEqual(server.job_build_scheduler_counts["starts"], 4)
        finally:
            for release in releases:
                release.set()
            for thread in threads:
                thread.join(5)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [[], [], [], []])
        self.assertEqual([len(items) for items in results], [1, 1, 1, 1])
        self.assertEqual(recorded["calls"], 4)
        self.assertLessEqual(max_active_builds, 2)
        self.assertEqual(server.job_build_scheduler_counts["supersessions"], 0)

    def test_collection_independence_requires_one_immutable_cohort(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        second_template = dict(rpc.template)
        second_template["curtime"] = int(second_template["curtime"]) + 1
        second = server.store_template_artifacts(second_template)
        assert second is not None
        payout_generation = server._payout_state_generation
        first_worker = worker("tq1worker-1", "tq1worker-1.rig")
        second_worker = worker("tq1worker-2", "tq1worker-2.rig")

        def request_for(
            build_artifacts: object,
            identity: WorkerIdentity,
        ) -> object:
            return server._new_job_build_request(
                build_artifacts,  # type: ignore[arg-type]
                identity,
                mode="collection",
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    build_artifacts,  # type: ignore[arg-type]
                    mode="collection",
                    payout_state_generation=payout_generation,
                    worker=identity,
                ),
            )

        first_request = request_for(first, first_worker)
        peer_request = request_for(first, second_worker)
        newer_request = request_for(second, first_worker)

        self.assertTrue(
            server._collection_job_builds_are_independent(
                first_request,  # type: ignore[arg-type]
                peer_request,  # type: ignore[arg-type]
            )
        )
        self.assertFalse(
            server._collection_job_builds_are_independent(
                first_request,  # type: ignore[arg-type]
                newer_request,  # type: ignore[arg-type]
            )
        )

    def test_ready_build_cancels_both_live_collection_flights(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        server._ensure_tip_refresh_state()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(2)
        ]
        entered = [threading.Event(), threading.Event()]
        cancellation_observed = [threading.Event(), threading.Event()]
        release_cancelled = threading.Event()
        original_build = server.build_shared_job_bundle

        def blocking_build(
            build_artifacts: object,
            identity: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            request = kwargs["build_request"]
            if identity in identities:
                index = identities.index(identity)
                entered[index].set()
                while not request.cancellation.is_set():  # type: ignore[union-attr]
                    if release_cancelled.wait(0.01):
                        raise AssertionError("collection build was not cancelled")
                cancellation_observed[index].set()
                release_cancelled.wait(5)
                request.cancellation.raise_if_cancelled(  # type: ignore[union-attr]
                    "test collection hold"
                )
            return original_build(
                build_artifacts,  # type: ignore[arg-type]
                identity,
                **kwargs,
            )

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        payout_generation = server._payout_state_generation

        def request_for(
            mode: str,
            identity: WorkerIdentity | None,
        ) -> object:
            return server._new_job_build_request(
                artifacts,
                identity,
                mode=mode,
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    artifacts,
                    mode=mode,
                    payout_state_generation=payout_generation,
                    worker=identity,
                ),
            )

        collection_requests = [
            request_for("collection", identity) for identity in identities
        ]
        collection_promises = []
        ready_promise = None
        try:
            collection_promises.append(
                server._request_job_build(collection_requests[0])  # type: ignore[arg-type]
            )
            self.assertTrue(entered[0].wait(5))
            collection_promises.append(
                server._request_job_build(collection_requests[1])  # type: ignore[arg-type]
            )
            self.assertTrue(entered[1].wait(5))

            ready_request = request_for("ready", None)
            ready_promise = server._request_job_build(  # type: ignore[arg-type]
                ready_request
            )
            self.assertTrue(cancellation_observed[0].wait(5))
            self.assertTrue(cancellation_observed[1].wait(5))
            self.assertFalse(collection_promises[0].done())
            self.assertFalse(collection_promises[1].done())
            self.assertFalse(ready_promise.done())
            self.assertEqual(server.job_build_scheduler_counts["starts"], 2)
        finally:
            release_cancelled.set()

        assert ready_promise is not None
        ready_bundle = ready_promise.result(timeout=5)
        self.assertFalse(ready_bundle.collection_only)
        for request, promise in zip(collection_requests, collection_promises):
            self.assertTrue(request.cancellation.is_set())  # type: ignore[union-attr]
            self.assertIsInstance(
                promise.exception(timeout=5),
                JobBuildSuperseded,
            )
        self.assertEqual(server.job_build_scheduler_counts["starts"], 3)

    def test_ready_build_cancels_retiring_only_collection_flight(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        server._ensure_tip_refresh_state()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(2)
        ]
        entered = [threading.Event(), threading.Event()]
        release_active = threading.Event()
        retiring_cancelled = threading.Event()
        release_retiring = threading.Event()
        original_build = server.build_shared_job_bundle

        def blocking_build(
            build_artifacts: object,
            identity: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            request = kwargs["build_request"]
            if identity == identities[0]:
                entered[0].set()
                while not request.cancellation.is_set():  # type: ignore[union-attr]
                    if release_retiring.wait(0.01):
                        raise AssertionError("retiring build was not cancelled")
                retiring_cancelled.set()
                release_retiring.wait(5)
                request.cancellation.raise_if_cancelled(  # type: ignore[union-attr]
                    "test retiring-only hold"
                )
            elif identity == identities[1]:
                entered[1].set()
                self.assertTrue(release_active.wait(5))
            return original_build(
                build_artifacts,  # type: ignore[arg-type]
                identity,
                **kwargs,
            )

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        payout_generation = server._payout_state_generation

        def request_for(
            mode: str,
            identity: WorkerIdentity | None,
        ) -> object:
            return server._new_job_build_request(
                artifacts,
                identity,
                mode=mode,
                payout_state_generation=payout_generation,
                cache_key=server._job_bundle_key(
                    artifacts,
                    mode=mode,
                    payout_state_generation=payout_generation,
                    worker=identity,
                ),
            )

        first_request = request_for("collection", identities[0])
        second_request = request_for("collection", identities[1])
        first_promise = server._request_job_build(  # type: ignore[arg-type]
            first_request
        )
        self.assertTrue(entered[0].wait(5))
        second_promise = server._request_job_build(  # type: ignore[arg-type]
            second_request
        )
        self.assertTrue(entered[1].wait(5))
        try:
            release_active.set()
            second_bundle = second_promise.result(timeout=5)
            self.assertTrue(second_bundle.collection_only)
            with server._job_build_scheduler_lock:
                self.assertIsNone(server._job_build_active)
                assert server._job_build_retiring is not None
                self.assertIs(
                    server._job_build_retiring.request,
                    first_request,
                )

            ready_request = request_for("ready", None)
            ready_promise = server._request_job_build(  # type: ignore[arg-type]
                ready_request
            )
            self.assertTrue(retiring_cancelled.wait(5))
            ready_bundle = ready_promise.result(timeout=5)
            self.assertFalse(ready_bundle.collection_only)
            self.assertTrue(first_request.cancellation.is_set())  # type: ignore[union-attr]
        finally:
            release_active.set()
            release_retiring.set()

        self.assertIsInstance(
            first_promise.exception(timeout=5),
            JobBuildSuperseded,
        )

    def test_collection_retries_do_not_supersede_ready_build(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        server._ensure_tip_refresh_state()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        identities = [
            worker(f"tq1worker-{index}", f"tq1worker-{index}.rig")
            for index in range(2)
        ]
        collection_entered = [threading.Event(), threading.Event()]
        collection_cancelled = [threading.Event(), threading.Event()]
        release_cancelled = threading.Event()
        ready_entered = threading.Event()
        release_ready = threading.Event()
        stop_collections = threading.Event()
        ready_requests: list[object] = []
        collection_requests: list[object | None] = [None, None]
        original_build = server.build_shared_job_bundle

        def blocking_build(
            build_artifacts: object,
            identity: WorkerIdentity | None,
            **kwargs: object,
        ) -> object:
            request = kwargs["build_request"]
            if request.mode == "ready":  # type: ignore[union-attr]
                ready_requests.append(request)
                ready_entered.set()
                self.assertTrue(release_ready.wait(5))
            elif identity in identities:
                index = identities.index(identity)
                collection_requests[index] = request
                collection_entered[index].set()
                while not request.cancellation.is_set():  # type: ignore[union-attr]
                    if release_cancelled.wait(0.01):
                        raise AssertionError("collection build was not cancelled")
                collection_cancelled[index].set()
                release_cancelled.wait(5)
                request.cancellation.raise_if_cancelled(  # type: ignore[union-attr]
                    "test collection retry hold"
                )
            return original_build(
                build_artifacts,  # type: ignore[arg-type]
                identity,
                **kwargs,
            )

        server.build_shared_job_bundle = blocking_build  # type: ignore[method-assign]
        collection_results: list[list[object]] = [[], []]
        collection_errors: list[list[BaseException]] = [[], []]
        ready_results: list[object] = []
        ready_errors: list[BaseException] = []

        def build_collection(index: int) -> None:
            try:
                collection_results[index].append(
                    server.shared_job_bundle(
                        artifacts,
                        identities[index],
                        mode="collection",
                        cancelled=stop_collections.is_set,
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                collection_errors[index].append(exc)

        def build_ready() -> None:
            try:
                ready_results.append(
                    server.shared_job_bundle(
                        artifacts,
                        mode="ready",
                        retry_superseded=False,
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                ready_errors.append(exc)

        collection_threads = [
            threading.Thread(target=build_collection, args=(index,))
            for index in range(2)
        ]
        ready_thread = threading.Thread(target=build_ready)
        try:
            collection_threads[0].start()
            self.assertTrue(collection_entered[0].wait(5))
            collection_threads[1].start()
            self.assertTrue(collection_entered[1].wait(5))
            ready_thread.start()
            self.assertTrue(collection_cancelled[0].wait(5))
            self.assertTrue(collection_cancelled[1].wait(5))
            self.assertEqual(server.job_build_scheduler_counts["starts"], 2)

            release_cancelled.set()
            self.assertTrue(ready_entered.wait(5))
            retry_deadline = time.monotonic() + 5
            while (
                server.job_build_scheduler_counts["requests"] < 5
                and time.monotonic() < retry_deadline
            ):
                time.sleep(0.01)
            self.assertGreaterEqual(
                server.job_build_scheduler_counts["requests"],
                5,
            )
            self.assertEqual(server.job_build_scheduler_counts["starts"], 3)
            self.assertEqual(len(ready_requests), 1)
            self.assertFalse(
                ready_requests[0].cancellation.is_set()  # type: ignore[union-attr]
            )

            stop_collections.set()
            for thread in collection_threads:
                thread.join(2)
            release_ready.set()
            ready_thread.join(5)
        finally:
            stop_collections.set()
            release_cancelled.set()
            release_ready.set()
            for thread in collection_threads:
                if thread.ident is not None:
                    thread.join(5)
            if ready_thread.ident is not None:
                ready_thread.join(5)

        self.assertTrue(all(not thread.is_alive() for thread in collection_threads))
        self.assertFalse(ready_thread.is_alive())
        self.assertEqual(collection_results, [[], []])
        self.assertEqual([len(errors) for errors in collection_errors], [1, 1])
        self.assertEqual(ready_errors, [])
        self.assertEqual(len(ready_results), 1)
        self.assertFalse(ready_results[0].collection_only)  # type: ignore[union-attr]
        for request in collection_requests:
            assert request is not None
            self.assertTrue(request.cancellation.is_set())  # type: ignore[union-attr]

    def test_shutdown_cancels_builder_with_full_helper_input_pipe(self) -> None:
        server, rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        payout_generation = server._payout_state_generation
        request = server._new_job_build_request(
            artifacts,
            None,
            mode="ready",
            payout_state_generation=payout_generation,
            cache_key=server._job_bundle_key(
                artifacts,
                mode="ready",
                payout_state_generation=payout_generation,
                worker=None,
            ),
        )
        helper_started = threading.Event()
        helper_processes: list[subprocess.Popen[str]] = []
        real_popen = subprocess.Popen

        def capture_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
            process = real_popen(*args, **kwargs)  # type: ignore[arg-type]
            helper_processes.append(process)
            helper_started.set()
            return process

        def fill_helper_pipe(*_args: object, **kwargs: object) -> object:
            build_request = kwargs["build_request"]
            return server.build_audit_bundle(
                shares=[],
                found_block={
                    "block_height": 10,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[
                    {
                        "miner_id": "pipe-filler",
                        "balance_sats": 1,
                        "padding": "x" * (4 * 1024 * 1024),
                    }
                ],
                coinbase_script_sig_suffix_hex="00",
                cancellation=build_request.cancellation,  # type: ignore[union-attr]
            )

        server.build_shared_job_bundle = fill_helper_pipe  # type: ignore[method-assign]
        shutdown_finished = threading.Event()
        shutdown_errors: list[BaseException] = []

        def shutdown() -> None:
            try:
                server.shutdown_job_build_executor()
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                shutdown_errors.append(exc)
            finally:
                shutdown_finished.set()

        with patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            side_effect=capture_popen,
        ):
            promise = server._request_job_build(request)
            self.assertTrue(
                helper_started.wait(5),
                repr(promise.exception(timeout=1)) if promise.done() else None,
            )
            time.sleep(0.1)
            self.assertFalse(promise.done())
            shutdown_thread = threading.Thread(target=shutdown)
            shutdown_thread.start()
            shutdown_returned = shutdown_finished.wait(2)
            if not shutdown_returned:
                for process in helper_processes:
                    if process.poll() is None:
                        process.kill()
            shutdown_thread.join(5)

        self.assertTrue(shutdown_returned)
        self.assertFalse(shutdown_thread.is_alive())
        self.assertEqual(shutdown_errors, [])
        self.assertIsInstance(promise.exception(timeout=1), JobBuildSuperseded)

    def test_control_cancel_during_serialization_is_supersession(self) -> None:
        server, rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        helper_started = threading.Event()
        helper_processes: list[subprocess.Popen[str]] = []
        real_popen = subprocess.Popen

        def capture_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
            process = real_popen(*args, **kwargs)  # type: ignore[arg-type]
            helper_processes.append(process)
            helper_started.set()
            return process

        def fill_helper_pipe(*_args: object, **kwargs: object) -> object:
            build_request = kwargs["build_request"]
            return server.build_audit_bundle(
                shares=[],
                found_block={
                    "block_height": 10,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[
                    {
                        "miner_id": "pipe-filler",
                        "balance_sats": 1,
                        "padding": "x" * (4 * 1024 * 1024),
                    }
                ],
                coinbase_script_sig_suffix_hex="00",
                cancellation=build_request.cancellation,  # type: ignore[union-attr]
            )

        server.build_shared_job_bundle = fill_helper_pipe  # type: ignore[method-assign]
        errors: list[BaseException] = []

        def build() -> None:
            try:
                server.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        with patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            side_effect=capture_popen,
        ):
            build_thread = threading.Thread(target=build)
            build_thread.start()
            self.assertTrue(helper_started.wait(5))
            with server._job_cache_lock:
                controls = list(server._active_job_bundle_builds.values())
            self.assertEqual(len(controls), 1)
            controls[0].cancel_event.set()
            build_thread.join(2)
            if build_thread.is_alive():
                for process in helper_processes:
                    if process.poll() is None:
                        process.kill()
                build_thread.join(5)

        self.assertFalse(build_thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        self.assertEqual(server.shared_bundle_build_counts["superseded"], 1)
        self.assertEqual(server.shared_bundle_build_counts["failed"], 0)
        self.assertEqual(server.tip_refresh_superseded_results, 1)

    def test_full_helper_input_pipe_obeys_builder_timeout(self) -> None:
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        server.bundle_build_timeout_seconds = 0.05
        helper_started = threading.Event()
        helper_processes: list[subprocess.Popen[str]] = []
        real_popen = subprocess.Popen

        def capture_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
            process = real_popen(*args, **kwargs)  # type: ignore[arg-type]
            helper_processes.append(process)
            helper_started.set()
            return process

        errors: list[BaseException] = []

        def build() -> None:
            try:
                server.build_audit_bundle(
                    shares=[],
                    found_block={
                        "block_height": 10,
                        "coinbase_value_sats": 50_00000000,
                        "network_difficulty": 1,
                        "anchor_job_issued_at_ms": 1_700_000_000_000,
                    },
                    prior_balances=[
                        {
                            "miner_id": "pipe-filler",
                            "balance_sats": 1,
                            "padding": "x" * (4 * 1024 * 1024),
                        }
                    ],
                    coinbase_script_sig_suffix_hex="00",
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        with patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=[sys.executable, "-c", "import time; time.sleep(30)"],
        ), patch(
            "lab.prism.prism_coordinator.subprocess.Popen",
            side_effect=capture_popen,
        ):
            build_thread = threading.Thread(target=build)
            build_thread.start()
            self.assertTrue(helper_started.wait(5))
            build_thread.join(2)
            if build_thread.is_alive():
                for process in helper_processes:
                    if process.poll() is None:
                        process.kill()
                build_thread.join(5)

        self.assertFalse(build_thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertIn("timed out", str(errors[0]))

    def test_supersession_retry_wakes_blockpoll_without_full_interval(self) -> None:
        server, _ = coordinator()
        server.blockpoll_seconds = 60.0
        server._ensure_tip_refresh_state()
        poll_called = threading.Event()

        def poll_once() -> int:
            poll_called.set()
            server.stop_event.set()
            return 0

        server.poll_qbit_tip_template_once = poll_once  # type: ignore[method-assign]
        thread = threading.Thread(target=server.blockpoll_loop)
        thread.start()
        try:
            server._schedule_tip_refresh_retry()
            self.assertTrue(poll_called.wait(1))
        finally:
            server.stop_event.set()
            server._schedule_tip_refresh_retry()
            thread.join(1)

        self.assertFalse(thread.is_alive())

    def test_ready_tip_refresh_respects_executor_bound(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 2
        clients = [client(index + 1) for index in range(6)]
        server.clients = set(clients)
        release = threading.Event()
        two_started = threading.Event()
        counter_lock = threading.Lock()
        active = 0
        maximum = 0

        def send(payload: dict[str, object]) -> None:
            nonlocal active, maximum
            if payload["method"] != "mining.notify":
                return
            with counter_lock:
                active += 1
                maximum = max(maximum, active)
                if active == 2:
                    two_started.set()
            try:
                self.assertTrue(release.wait(5))
            finally:
                with counter_lock:
                    active -= 1

        for state in clients:
            state.send = send  # type: ignore[method-assign]
        result: list[int] = []
        thread = threading.Thread(target=lambda: result.append(server.poll_qbit_tip_template_once()))
        thread.start()
        try:
            self.assertTrue(two_started.wait(5))
            self.assertLessEqual(maximum, 2)
        finally:
            release.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [6])
        self.assertEqual(maximum, 2)

    def test_blocked_socket_does_not_delay_another_client(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 2
        blocked = client(1)
        healthy = client(2)
        server.clients = {blocked, healthy}
        blocked_started = threading.Event()
        healthy_delivered = threading.Event()
        release = threading.Event()

        def blocked_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                blocked_started.set()
                self.assertTrue(release.wait(5))

        def healthy_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                healthy_delivered.set()

        blocked.send = blocked_send  # type: ignore[method-assign]
        healthy.send = healthy_send  # type: ignore[method-assign]
        result: list[int] = []
        thread = threading.Thread(target=lambda: result.append(server.poll_qbit_tip_template_once()))
        thread.start()
        try:
            self.assertTrue(blocked_started.wait(5))
            self.assertTrue(healthy_delivered.wait(5))
        finally:
            release.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertEqual(result, [2])

    def test_shutdown_drains_inflight_tip_refresh_worker(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        state = client(1)
        server.clients = {state}
        worker_started = threading.Event()
        worker_send_finished = threading.Event()
        release_worker = threading.Event()
        shutdown_complete = threading.Event()
        poll_errors: list[BaseException] = []

        def blocked_send(payload: dict[str, object]) -> None:
            if payload["method"] != "mining.notify":
                return
            worker_started.set()
            try:
                self.assertTrue(release_worker.wait(5))
            finally:
                worker_send_finished.set()

        def poll() -> None:
            try:
                server.poll_qbit_tip_template_once()
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                poll_errors.append(exc)

        def shutdown() -> None:
            server.shutdown_tip_refresh_executor()
            shutdown_complete.set()

        state.send = blocked_send  # type: ignore[method-assign]
        poll_thread = threading.Thread(target=poll)
        shutdown_thread = threading.Thread(target=shutdown)
        poll_thread.start()
        try:
            self.assertTrue(worker_started.wait(5))
            server.stop_event.set()
            shutdown_thread.start()
            self.assertFalse(shutdown_complete.wait(0.05))
            self.assertFalse(worker_send_finished.is_set())
        finally:
            release_worker.set()
            shutdown_thread.join(5)
            poll_thread.join(5)

        self.assertFalse(shutdown_thread.is_alive())
        self.assertFalse(poll_thread.is_alive())
        self.assertTrue(shutdown_complete.is_set())
        self.assertTrue(worker_send_finished.is_set())
        self.assertEqual(poll_errors, [])
        self.assertEqual(server.tip_refresh_inflight, 0)
        with self.assertRaisesRegex(RuntimeError, "executor is shut down"):
            server.tip_refresh_executor()

    def test_queued_fanout_stops_when_chain_view_becomes_untrusted(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        trust_checks = 0

        def chain_view_untrusted() -> bool:
            nonlocal trust_checks
            trust_checks += 1
            return True

        server.qbit_chain_view_untrusted = chain_view_untrusted  # type: ignore[method-assign]
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        sent: list[dict[str, object]] = []
        first.send = sent.append  # type: ignore[method-assign]
        second.send = sent.append  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(TemplateRefreshBlocked, "became untrusted"):
                server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(trust_checks, 1)
        self.assertEqual(sent, [])
        self.assertIsNone(first.active_job)
        self.assertIsNone(second.active_job)

    def test_queued_fanout_stops_when_live_tip_changes(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        first_blocked = threading.Event()
        release_first = threading.Event()
        second_sent: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_blocked.set()
                self.assertTrue(release_first.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(first_blocked.wait(5))
            rpc.tip = "33" * 32
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(refreshed, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIn("immediate retry scheduled", str(errors[0]))
        self.assertTrue(server._tip_refresh_retry.is_set())
        self.assertIsNotNone(first.active_job)
        self.assertIsNotNone(second.active_job)
        self.assertEqual(
            [payload["method"] for payload in second_sent],
            ["mining.set_difficulty", "mining.notify"],
        )

    def test_multiworker_cancel_releases_client_lock_while_draining_peer(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        admitted = client(1)
        queued = client(2)
        server.clients = [admitted, queued]  # type: ignore[assignment]
        admitted_send_started = threading.Event()
        release_admitted_send = threading.Event()
        queued_sent: list[dict[str, object]] = []

        def admitted_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                admitted_send_started.set()
                self.assertTrue(release_admitted_send.wait(5))

        admitted.send = admitted_send  # type: ignore[method-assign]
        queued.send = queued_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(admitted_send_started.wait(5))
            server.observe_tip_for_refresh("33" * 32)
            lock_acquired = queued.job_update_lock.acquire(timeout=0.1)
            self.assertTrue(lock_acquired)
            if lock_acquired:
                queued.job_update_lock.release()
            # The coordinator still waits for the admitted peer delivery, but
            # queued workers observe cancellation without taking client state.
            self.assertTrue(thread.is_alive())
            self.assertIsNone(queued.active_job)
        finally:
            release_admitted_send.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(refreshed, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIsNotNone(admitted.active_job)
        self.assertIsNone(queued.active_job)
        self.assertEqual(queued_sent, [])

    def test_same_tip_cache_refresh_during_fanout_does_not_abort(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        second = client(2)
        # Preserve task order so the cache replacement happens after one
        # delivery and before the next worker task starts.
        server.clients = [first, second]  # type: ignore[assignment]
        first_blocked = threading.Event()
        release_first = threading.Event()
        second_sent: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_blocked.set()
                self.assertTrue(release_first.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(first_blocked.wait(5))
            replacement = dict(rpc.template)
            replacement["coinbasevalue"] = int(replacement["coinbasevalue"]) + 1
            replacement_artifacts = server.store_template_artifacts(replacement)
            self.assertIsNotNone(replacement_artifacts)
            self.assertNotEqual(
                replacement_artifacts.fingerprint,
                first.active_job.template_fingerprint,
            )
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(refreshed, [2])
        self.assertIsNotNone(server.last_successful_template_refresh_monotonic)
        self.assertEqual(
            [payload["method"] for payload in second_sent],
            ["mining.set_difficulty", "mining.notify"],
        )

    def test_queued_fanout_does_not_overwrite_intervening_job(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        first_blocked = threading.Event()
        release_first = threading.Event()
        second_sent: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_blocked.set()
                self.assertTrue(release_first.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(first_blocked.wait(5))
            replacement = dict(rpc.template)
            replacement["coinbasevalue"] = int(replacement["coinbasevalue"]) + 1
            replacement_artifacts = server.store_template_artifacts(replacement)
            self.assertIsNotNone(replacement_artifacts)
            self.assertTrue(server.maybe_send_job(second, clean_jobs=False))
            intervening_job = second.active_job
            self.assertEqual(
                intervening_job.template_fingerprint,
                replacement_artifacts.fingerprint,
            )
            self.assertGreater(
                intervening_job.template_generation,
                first.active_job.template_generation,
            )
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(refreshed, [1])
        self.assertIs(second.active_job, intervening_job)
        self.assertEqual(
            [payload["method"] for payload in second_sent],
            ["mining.set_difficulty", "mining.notify"],
        )

    def test_queued_fanout_replaces_stale_intervening_job(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(old_artifacts)
        assert old_artifacts is not None
        assert second.worker is not None
        old_bundle = server.shared_job_bundle(old_artifacts, second.worker)
        refreshed_template = dict(rpc.template)
        refreshed_template["coinbasevalue"] = int(
            refreshed_template["coinbasevalue"]
        ) + 1
        rpc.template = refreshed_template
        first_blocked = threading.Event()
        release_first = threading.Event()
        second_sent: list[dict[str, object]] = []

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_blocked.set()
                self.assertTrue(release_first.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        second.send = second_sent.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surface to the test
                errors.append(exc)

        thread = threading.Thread(target=poll)
        thread.start()
        try:
            self.assertTrue(first_blocked.wait(5))
            with second.job_update_lock, server.lock:
                stale_intervening_job = server.stamp_job_for_client(
                    second,
                    old_bundle,
                    clean_jobs=False,
                )
                second.active_job = stale_intervening_job
                second.active_job_ids.add(stale_intervening_job.job.job_id)
                server.jobs[stale_intervening_job.job.job_id] = stale_intervening_job
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(refreshed, [2])
        self.assertIsNot(second.active_job, stale_intervening_job)
        self.assertEqual(
            second.active_job.template_fingerprint,
            qbit_template_fingerprint(refreshed_template),
        )
        self.assertGreater(
            second.active_job.template_generation,
            stale_intervening_job.template_generation,
        )
        self.assertEqual(
            [payload["method"] for payload in second_sent],
            ["mining.set_difficulty", "mining.notify"],
        )

    def test_newer_template_does_not_supersede_current_payout_refresh(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(artifacts)
        assert artifacts is not None and state.worker is not None
        stale_bundle = server.shared_job_bundle(artifacts, state.worker)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        stale_intervening_job = dataclass_replace(
            server.stamp_job_for_client(
                state,
                stale_bundle,
                clean_jobs=False,
            ),
            template_generation=snapshot.template_generation + 1,
        )
        server._advance_payout_state_generation()

        self.assertFalse(
            server.intervening_job_supersedes_snapshot(
                stale_intervening_job,
                None,
                snapshot,
            )
        )
        current_intervening_job = dataclass_replace(
            stale_intervening_job,
            payout_state_generation=server._payout_state_generation,
        )
        self.assertTrue(
            server.intervening_job_supersedes_snapshot(
                current_intervening_job,
                None,
                snapshot,
            )
        )

    def test_higher_generation_old_tip_does_not_supersede_new_tip_snapshot(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        self.assertIsNotNone(old_artifacts)
        assert old_artifacts is not None and state.worker is not None
        old_bundle = server.shared_job_bundle(old_artifacts, state.worker)

        new_tip = "22" * 32
        rpc.tip = new_tip
        rpc.template = base_template(height=11, prevhash=new_tip)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        old_tip_job = dataclass_replace(
            server.stamp_job_for_client(
                state,
                old_bundle,
                clean_jobs=False,
            ),
            template_generation=snapshot.template_generation + 1,
        )

        self.assertNotEqual(
            old_tip_job.template_fingerprint,
            snapshot.template_fingerprint,
        )
        self.assertFalse(
            server.intervening_job_supersedes_snapshot(
                old_tip_job,
                None,
                snapshot,
            )
        )

    def test_broken_socket_disconnects_only_that_client(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        broken = client(1)
        healthy = client(2)
        server.clients = {broken, healthy}
        healthy_sent: list[dict[str, object]] = []
        disconnected: list[ClientState] = []
        broken.send = lambda _payload: (_ for _ in ()).throw(OSError("closed"))  # type: ignore[method-assign]
        healthy.send = healthy_sent.append  # type: ignore[method-assign]
        server.disconnect_client = disconnected.append  # type: ignore[method-assign]

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(disconnected, [broken])
        self.assertEqual(
            [payload["method"] for payload in healthy_sent],
            ["mining.set_difficulty", "mining.notify"],
        )

    def test_client_removed_before_pending_task_runs_is_skipped(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        first = client(1)
        removed = client(2)
        clients = [first, removed]
        server.clients = set(clients)
        snapshot = server.fetch_qbit_tip_template_snapshot()
        server.observe_tip_first_seen(snapshot.bestblockhash)
        server.pool_readiness_latched()
        server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        blocked = threading.Event()
        release = threading.Event()

        def first_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                blocked.set()
                self.assertTrue(release.wait(5))

        first.send = first_send  # type: ignore[method-assign]
        removed.send = lambda _payload: self.fail("removed client received a job")  # type: ignore[method-assign]
        result: list[tuple[int, float | None, float | None, int]] = []
        thread = threading.Thread(
            target=lambda: result.append(
                server._fanout_prepared_tip_refresh(
                    clients,
                    bundle,
                    snapshot,
                    heartbeat_name="qbit_blockpoll",
                )
            )
        )
        thread.start()
        try:
            self.assertTrue(blocked.wait(5))
            with server.lock:
                server.clients.remove(removed)
        finally:
            release.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertEqual(result[0][0], 1)
        self.assertIsNone(removed.active_job)
        self.assertEqual(removed.active_job_ids, set())

    def test_template_fingerprint_race_uses_snapshot_owned_artifacts(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        states = [client(1), client(2)]
        sent: list[dict[str, object]] = []
        for state in states:
            state.send = sent.append  # type: ignore[method-assign]
        server.clients = set(states)
        original_shared_job_bundle = server.shared_job_bundle
        race_calls = 0

        def race_artifacts(
            artifacts: object,
            identity: WorkerIdentity | None = None,
            **kwargs: object,
        ) -> object:
            nonlocal race_calls
            race_calls += 1
            bundle = original_shared_job_bundle(artifacts, identity, **kwargs)
            with server._job_cache_lock:
                server._template_artifacts = dataclass_replace(
                    server._template_artifacts,
                    fingerprint="ff" * 32,
                )
            return bundle

        server.shared_job_bundle = race_artifacts  # type: ignore[method-assign]

        refreshed = server.poll_qbit_tip_template_once()

        self.assertEqual(race_calls, 1)
        self.assertEqual(refreshed, 2)
        self.assertEqual(len(sent), 4)
        self.assertIsNotNone(server.tip_template_snapshot)
        snapshot = server.tip_template_snapshot
        assert snapshot is not None and snapshot.template_artifacts is not None
        for state in states:
            self.assertIs(state.active_job.template, snapshot.template_artifacts.template)
            self.assertEqual(
                state.active_job.template_fingerprint,
                snapshot.template_fingerprint,
            )


class ClientCleanupTests(unittest.TestCase):
    def test_disconnect_retires_and_closes_before_job_lock_cleanup(self) -> None:
        server, _ = coordinator()
        state = client(1)
        server.clients = {state}
        socket_closed = threading.Event()
        state.close = socket_closed.set  # type: ignore[method-assign]
        state.job_update_lock.acquire()
        disconnect = threading.Thread(target=server.disconnect_client, args=(state,))
        try:
            disconnect.start()
            self.assertTrue(socket_closed.wait(5))
            with server.lock:
                self.assertNotIn(state, server.clients)
                self.assertTrue(state.closing)
            self.assertTrue(disconnect.is_alive())
        finally:
            state.job_update_lock.release()
            disconnect.join(5)

        self.assertFalse(disconnect.is_alive())

    def test_disconnect_during_prepared_refresh_skips_without_job_state(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        observed_lock = ObservedRLock()
        state.job_update_lock = observed_lock  # type: ignore[assignment]
        server.clients = {state}
        snapshot = server.fetch_qbit_tip_template_snapshot()
        server.observe_tip_first_seen(snapshot.bestblockhash)
        server.pool_readiness_latched()
        server.tip_template_snapshot = snapshot
        bundle = server.prepare_tip_refresh_bundle(snapshot)
        state.send = lambda _payload: self.fail(  # type: ignore[method-assign]
            "disconnected client received prepared work"
        )
        socket_closed = threading.Event()
        state.close = socket_closed.set  # type: ignore[method-assign]
        results: list[tuple[int, float | None, float | None, int]] = []
        errors: list[BaseException] = []

        def refresh() -> None:
            try:
                results.append(
                    server._fanout_prepared_tip_refresh(
                        [state],
                        bundle,
                        snapshot,
                        heartbeat_name="qbit_blockpoll",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surface thread failures
                errors.append(exc)

        observed_lock.acquire()
        observed_lock.observe_acquires = True
        refresh_thread = threading.Thread(target=refresh)
        disconnect_thread = threading.Thread(
            target=server.disconnect_client,
            args=(state,),
        )
        try:
            refresh_thread.start()
            self.assertTrue(observed_lock.acquire_attempted.wait(5))
            disconnect_thread.start()
            self.assertTrue(socket_closed.wait(5))
            refresh_thread.join(5)
            self.assertFalse(refresh_thread.is_alive())
            self.assertTrue(disconnect_thread.is_alive())
        finally:
            observed_lock.release()
            refresh_thread.join(5)
            disconnect_thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(disconnect_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results[0][0], 0)
        self.assertIsNone(state.active_job)
        self.assertEqual(state.active_job_ids, set())
        self.assertEqual(server.jobs, {})

    def test_mass_disconnect_releases_active_connection_accounting(self) -> None:
        server, _ = coordinator()
        states = [client(index) for index in range(1, 129)]
        for state in states:
            state.close = lambda: None  # type: ignore[method-assign]
        server.clients = set(states)

        for state in states:
            server.disconnect_client(state)

        with server.lock:
            self.assertEqual(len(server.clients), 0)
        self.assertTrue(all(state.closing for state in states))

    def test_concurrent_disconnect_is_idempotent_and_deadlock_free(self) -> None:
        server, _ = coordinator()
        state = client(1)
        server.clients = {state}
        close_count = 0
        close_count_lock = threading.Lock()
        caller_count = 16
        start = threading.Barrier(caller_count + 1)
        errors: list[BaseException] = []

        def close() -> None:
            nonlocal close_count
            with close_count_lock:
                close_count += 1

        def disconnect() -> None:
            try:
                start.wait()
                server.disconnect_client(state)
            except BaseException as exc:  # noqa: BLE001 - surface thread failures
                errors.append(exc)

        state.close = close  # type: ignore[method-assign]
        callers = [threading.Thread(target=disconnect) for _ in range(caller_count)]
        for caller in callers:
            caller.start()
        start.wait()
        for caller in callers:
            caller.join(5)

        self.assertTrue(all(not caller.is_alive() for caller in callers))
        self.assertEqual(errors, [])
        self.assertEqual(close_count, 1)
        self.assertNotIn(state, server.clients)

    def test_disconnect_removes_active_and_evicted_job_contexts(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        active = server.build_job_for_client(state, clean_jobs=True)
        evicted = server.build_job_for_client(state, clean_jobs=False)
        active_id = active.job.job_id
        evicted_id = evicted.job.job_id
        state.active_job = active
        state.active_job_ids = {active_id}
        server.jobs = {active_id: active, evicted_id: evicted}
        server.bury_evicted_job(state, evicted_id)
        server.jobs.pop(evicted_id)
        server.clients = {state}
        state.close = lambda: None  # type: ignore[method-assign]

        server.disconnect_client(state)

        self.assertIsNone(state.active_job)
        self.assertEqual(state.active_job_ids, set())
        self.assertNotIn(active_id, server.jobs)
        self.assertNotIn(evicted_id, server.evicted_job_graveyard)
        self.assertNotIn(state.connection_id, server.evicted_jobs_by_connection)

    def test_reconnect_storm_leaves_no_handler_threads_or_ghost_clients(self) -> None:
        server, _ = coordinator()
        connection_count = 32
        start = threading.Barrier(connection_count + 1)
        peers: list[socket.socket] = []
        handlers: list[threading.Thread] = []

        def handle(state: ClientState) -> None:
            start.wait()
            server.handle_client(state)

        for connection_id in range(1, connection_count + 1):
            coordinator_socket, peer_socket = socket.socketpair()
            state = client(connection_id)
            state.sock = coordinator_socket
            server.clients.add(state)
            peers.append(peer_socket)
            handler = threading.Thread(
                target=handle,
                args=(state,),
                name=f"prism-test-handler-{connection_id}",
            )
            handlers.append(handler)
            handler.start()

        start.wait()
        for peer in peers:
            peer.close()
        for handler in handlers:
            handler.join(5)

        self.assertTrue(all(not handler.is_alive() for handler in handlers))
        with server.lock:
            self.assertEqual(server.clients, set())


class HealthSnapshotTests(unittest.TestCase):
    def test_health_payload_uses_aggregate_stats_not_all_shares(self) -> None:
        ledger = FakeLedger()
        server, _ = coordinator(ledger=ledger)
        mark_progress_healthy(server)
        payload = server.health_payload()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["accepted_share_count"], 3)
        self.assertEqual(payload["ready_miner_count"], 3)
        self.assertGreaterEqual(ledger.stats_calls, 1)

    def test_cached_health_payload_computes_inline_without_refresher(self) -> None:
        server, _ = coordinator()
        mark_progress_healthy(server)
        status, payload = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])

    def test_cached_health_payload_serves_snapshot_and_flags_staleness(self) -> None:
        server, _ = coordinator()
        mark_progress_healthy(server)
        server.refresh_health_snapshot()
        server._health_refresh_loop_running = True

        status, payload = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertIn("snapshot_age_seconds", payload)

        # Even if the ledger becomes unusable, the snapshot keeps serving.
        server.ledger = None  # type: ignore[assignment]
        status, payload = server.cached_health_payload()
        self.assertEqual(status, 200)

        server._health_snapshot_monotonic = time.monotonic() - 1_000
        status, payload = server.cached_health_payload()
        self.assertEqual(status, 503)
        self.assertFalse(payload["ok"])

    def test_accepted_share_stats_falls_back_to_all_shares(self) -> None:
        server, _ = coordinator(ledger=SingleWriterShareLedger())
        self.assertEqual(server.accepted_share_stats(), (0, 0))

    def test_single_writer_ledger_stats(self) -> None:
        ledger = SingleWriterShareLedger()
        self.assertEqual(
            ledger.accepted_share_stats(),
            {"accepted_share_count": 0, "distinct_miner_count": 0},
        )


class JobBuildMetricsTests(unittest.TestCase):
    def test_metrics_include_job_build_histogram_and_cache_counters(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.build_job_for_client(client(1), clean_jobs=True)
        server.build_job_for_client(client(2), clean_jobs=True)
        server.observe_job_build_elapsed(0.3, {"bundle": 0.2, "stamp": 0.01})
        server._observe_tip_refresh_build_phase("payout_state_derivation", 0.25)
        server._record_tip_refresh_ipc_bytes("input", 123)

        metrics = server.metrics_payload()

        self.assertIn('qbit_prism_job_build_seconds_bucket{le="0.5"} 1', metrics)
        self.assertIn('qbit_prism_job_build_seconds_bucket{le="+Inf"} 1', metrics)
        self.assertIn("qbit_prism_job_build_seconds_count 1", metrics)
        self.assertIn('qbit_prism_job_cache_hits_total{cache="bundle"} 1', metrics)
        self.assertIn('qbit_prism_job_cache_misses_total{cache="bundle"} 1', metrics)
        self.assertIn('qbit_prism_job_build_phase_seconds_total{phase="bundle"} 0.2', metrics)
        self.assertIn(
            'qbit_prism_tip_refresh_bundle_phase_seconds_count{phase="payout_state_derivation"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_tip_refresh_builder_ipc_bytes_total{direction="input"} 123',
            metrics,
        )
        self.assertIn("qbit_prism_tip_refresh_bundle_queue_depth 0", metrics)
        self.assertIn("qbit_prism_tip_refresh_bundle_inflight 0", metrics)
        self.assertIn("qbit_prism_connected_clients 0", metrics)

    def test_metrics_split_payout_preparation_publication_and_delivery(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]

        self.assertEqual(server._advance_payout_state_generation(), 1)
        self.assertTrue(server.maybe_send_job(state, clean_jobs=True))

        metrics = server.metrics_payload()

        self.assertIn("qbit_prism_payout_preparation_seconds_count 1", metrics)
        self.assertIn("qbit_prism_payout_publish_seconds_count 1", metrics)
        self.assertIn(
            "qbit_prism_payout_invalidation_first_delivery_seconds_count 1",
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_gate_wait_seconds_count{generation="current"} 1',
            metrics,
        )
        self.assertIn("qbit_prism_payout_candidates_discarded_total 0", metrics)


if __name__ == "__main__":
    unittest.main()
