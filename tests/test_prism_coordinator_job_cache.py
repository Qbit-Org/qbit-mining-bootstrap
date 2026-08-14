#!/usr/bin/env python3
"""Per-template job-build cache, cached health snapshot, and latency metrics."""

from __future__ import annotations

import contextlib
import io
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass, replace as dataclass_replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism import prism_coordinator as prism_coordinator_module
from lab.prism.prism_coordinator import (
    ClientState,
    JobBuildAdmissionDeadlineExceeded,
    JobBuildSuperseded,
    MAX_PRISM_JOB_BUNDLE_CACHE_ENTRIES,
    PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
    PRISM_REJECTION_REASON_IDS,
    PendingShareAppend,
    PayoutLedgerArtifact,
    PrismCoordinator,
    ShutdownInProgress,
    TemplateRefreshBlocked,
    WorkerIdentity,
    _PayoutStatePublicationBlocked,
    _compact_share_payload,
    canonical_json_sha256,
    canonical_json_text,
    default_prism_coinbase_tag_hex,
    now_ms,
    qbit_template_fingerprint,
)
from lab.prism.share_ledger import (
    LedgerOperationTimeout,
    PendingShare,
    SingleWriterShareLedger,
)

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


def durable_candidate_row(index: int) -> dict[str, object]:
    """A minimal valid outbox row whose intent survives block_candidate_from_intent."""
    block_hash = f"{index + 1:064x}"
    return {
        "block_hash": block_hash,
        "candidate": {
            "schema": "qbit.prism.block-candidate-intent.v1",
            "block_hash_hex": block_hash,
            "block_hex": "00",
            "coinbase_tx_hex": "00",
            "parent_hash": "11" * 32,
            "expected_height": 10,
            "template": {
                "previousblockhash": "11" * 32,
                "height": 10,
                "coinbasevalue": 50_00000000,
            },
            "shares_json": [],
            "prior_balances": [],
            "found_block": {},
            "prospective_prior_balances": None,
            "witness_merkle_leaves_hex": [],
            "extranonce1_hex": "00000001",
            "extranonce2_hex": "00",
            "username": "miner-a",
            "pending_share": {
                "share_id": f"miner-a:{block_hash}",
                "miner_id": "miner-a",
                "order_key": "miner-a",
                "p2mr_program_hex": "22" * 32,
                "share_difficulty": 1,
                "network_difficulty": 1,
                "template_height": 10,
                "job_id": "job-1",
                "job_issued_at_ms": 1,
                "accepted_at_ms": 1,
                "ntime": 1,
            },
            "credit_share_on_accept": False,
            "collection_only": False,
        },
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
    # Failed-refresh spacing is opt-in per test: its holdoff waits on real
    # time, which deadlocks tests that freeze time.monotonic around failing
    # polls. Pacing behavior is covered by test_prism_refresh_retry_pacing.
    server.tip_refresh_failure_holdoff_seconds = 0.0
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


class IncrementalRecordingLedger(SingleWriterShareLedger):
    def __init__(self) -> None:
        super().__init__()
        self.full_snapshot_calls = 0
        self.delta_snapshot_calls = 0

    def snapshot_at_job_issue(
        self,
        anchor_job_issued_at_ms: int,
        *,
        window_weight: int | None = None,
    ) -> list[object]:
        self.full_snapshot_calls += 1
        return super().snapshot_at_job_issue(
            anchor_job_issued_at_ms,
            window_weight=window_weight,
        )

    def snapshot_between_job_issues(
        self,
        previous_anchor_job_issued_at_ms: int,
        anchor_job_issued_at_ms: int,
    ) -> list[object]:
        self.delta_snapshot_calls += 1
        return super().snapshot_between_job_issues(
            previous_anchor_job_issued_at_ms,
            anchor_job_issued_at_ms,
        )


def append_incremental_share(
    ledger: SingleWriterShareLedger,
    *,
    share_seq: int,
    accepted_at_ms: int,
) -> None:
    ledger.append(
        PendingShare(
            share_id=f"incremental-{share_seq}",
            miner_id=f"miner-{share_seq % 3}",
            order_key=f"miner-{share_seq % 3}",
            p2mr_program_hex=f"{share_seq % 256:02x}" * 32,
            share_difficulty=1,
            network_difficulty=1,
            template_height=9,
            job_id=f"job-{share_seq}",
            job_issued_at_ms=accepted_at_ms - 1,
            accepted_at_ms=accepted_at_ms,
            ntime=1_700_000_000 + share_seq,
        )
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
        # An artifact snapshot is taken at its own earlier anchor. A bundle
        # reusing the artifact must declare that anchor rather than the
        # fresher job-issue time: a share stamped between the two anchors is
        # excluded from the artifact by construction, yet a re-derivation at
        # the job-issue anchor would include it.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.current_template_artifacts()
        clamp_now_ms = now_ms() - 6
        # Anchor selection sits strictly below the clamp instant so a share
        # stamped in the same millisecond can never tie the anchor.
        artifact_anchor_ms = clamp_now_ms - 1
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            return_value=clamp_now_ms,
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, artifact_anchor_ms)
        self.assertEqual(ledger.anchors[-1], artifact_anchor_ms)

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

    def test_artifact_paths_proceed_at_the_clamped_anchor_floor(
        self,
    ) -> None:
        # The pending-commit clamp is anchor selection, not a fence: every
        # share stamped at or below the clamped anchor is already durable,
        # so the window read at that anchor is exact and reproducible, and
        # both artifact producers publish it. Shares stamped above the
        # anchor deterministically belong to the next window.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.current_template_artifacts()
        stamped_ms = now_ms() - 5
        share = stamped_pending_share(stamped_ms)
        self._hold_floor(server, share)
        try:
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            self.assertIsNotNone(artifact)
            assert artifact is not None
            self.assertEqual(artifact.snapshot_anchor_ms, stamped_ms - 1)
            self.assertEqual(ledger.anchors[-1], stamped_ms - 1)

            # The synchronous build proceeds at the clamped anchor and seeds
            # its window for reuse under that same anchor; cache publication
            # installs the seed.
            bundle = server.build_shared_job_bundle(artifacts, worker())
            self.assertEqual(bundle.issued_at_ms, stamped_ms - 1)
            seeded = bundle.prepared_ledger_artifact
            self.assertIsNotNone(seeded)
            assert seeded is not None
            self.assertEqual(seeded.snapshot_anchor_ms, stamped_ms - 1)
            server._install_payout_ledger_artifact(seeded)
            with server._job_cache_lock:
                published = server._payout_ledger_artifact
            self.assertIsNotNone(published)
            assert published is not None
            self.assertEqual(published.snapshot_anchor_ms, stamped_ms - 1)
        finally:
            server._finish_pending_share_commit(share)

    def test_background_build_refuses_a_pathologically_old_floor(self) -> None:
        # A floor held further below now than the audit anchor ceiling (a
        # wedged writer or leaked release) would arm an artifact that is
        # born expired; refuse before paying the window walk so the
        # re-arm backoff paces retries.
        ledger = AnchorRecordingLedger()
        server, _rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.current_template_artifacts()
        share = stamped_pending_share(now_ms() - 400_000)
        self._hold_floor(server, share)
        try:
            self.assertIsNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            self.assertEqual(ledger.snapshot_calls, 0)
        finally:
            server._finish_pending_share_commit(share)

    def test_anchor_selection_excludes_same_millisecond_stamps(self) -> None:
        # Millisecond granularity can hand a share stamped right after
        # anchor selection the same accepted_at_ms as the clamp instant,
        # and such a share is never protected by the pending floor. The
        # window predicate is anchor-inclusive, so the anchor must sit
        # strictly below the clamp-time millisecond or that share would
        # join audit replays of a window that never contained it.
        server, _rpc = coordinator(ledger=AnchorRecordingLedger())
        clamp_now = now_ms()
        self.assertEqual(
            server._job_snapshot_anchor_ms(clamp_now),
            clamp_now - 1,
        )

        share = stamped_pending_share(clamp_now - 7)
        self._hold_floor(server, share)
        try:
            self.assertEqual(
                server._job_snapshot_anchor_ms(clamp_now),
                clamp_now - 8,
            )
        finally:
            server._finish_pending_share_commit(share)


class IncrementalPayoutArtifactTests(unittest.TestCase):
    def configured_server(
        self,
    ) -> tuple[PrismCoordinator, IncrementalRecordingLedger, object]:
        ledger = IncrementalRecordingLedger()
        append_incremental_share(ledger, share_seq=1, accepted_at_ms=999_900)
        append_incremental_share(ledger, share_seq=2, accepted_at_ms=999_910)
        append_incremental_share(ledger, share_seq=3, accepted_at_ms=999_920)
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 60.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        return server, ledger, artifacts

    @staticmethod
    def replay_shaped_append_entry(
        *,
        accepted_at_ms: int,
        block_hash_hex: str,
    ) -> PendingShareAppend:
        persisted = stamped_pending_share(accepted_at_ms)
        pending = PendingShare(**dict(vars(persisted)))
        return PendingShareAppend(
            pending_share=pending,
            username="miner-a",
            job_id=pending.job_id,
            block_hash_hex=block_hash_hex,
            collection_only=False,
            credit_policy=None,
            candidate_intent={
                "block_hash_hex": block_hash_hex,
                "pending_share": dict(vars(pending)),
                "credit_share_on_accept": True,
            },
        )

    def test_late_visible_replay_append_forces_next_build_to_full_oracle(
        self,
    ) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")

            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertTrue(server._install_payout_ledger_artifact(advanced))
            anchor_ms = int(advanced.snapshot_anchor_ms)

            entry = self.replay_shaped_append_entry(
                accepted_at_ms=anchor_ms - 10,
                block_hash_hex="44" * 32,
            )
            self.assertTrue(server._append_share_batch([entry]))
            self.assertIsNone(server._incremental_payout_artifact_window)
            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)
            self.assertFalse(server._install_payout_ledger_artifact(advanced))
            self.assertIsNone(
                server._cached_found_block_payout_artifact(
                    base_generation=0,
                    artifact_payout_state_generation=1,
                    network_difficulty=artifacts.network_difficulty,
                    fallback_reason="prepare_lock_busy",
                )
            )

            clock_ms[0] = 1_000_040
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert rebuilt is not None
        self.assertEqual(rebuilt.window_build_mode, "full_rescan")
        self.assertEqual(
            rebuilt.window_full_rescan_reason,
            "late_visible_append",
        )
        self.assertEqual(ledger.full_snapshot_calls, 2)
        self.assertIn(
            entry.pending_share.share_id,
            {str(share["share_id"]) for share in rebuilt.shares_json},
        )

    def test_replay_shaped_batch_commits_under_the_landing_fence(self) -> None:
        # The durable append and its epoch bump must share the landing fence
        # boundary: a bump that only starts after append_batch() returned
        # leaves a gap where a landing verifies the pre-bump epoch and enters
        # submitblock with the row already durable but unpaid. A batch holding
        # a replay-shaped row therefore holds the fence across the ledger
        # commit itself; an ordinary batch stays off the lock entirely.
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertTrue(server._install_payout_ledger_artifact(advanced))
            anchor_ms = int(advanced.snapshot_anchor_ms)

            fence_held_during_commit: list[bool] = []
            original_append_batch = ledger.append_batch

            def recording_append_batch(entries: list[object]) -> list[object]:
                fence_held_during_commit.append(
                    server._payout_append_landing_fence_lock.locked()
                )
                return original_append_batch(entries)

            ledger.append_batch = recording_append_batch  # type: ignore[method-assign]

            fresh = self.replay_shaped_append_entry(
                accepted_at_ms=anchor_ms + 50,
                block_hash_hex="88" * 32,
            )
            self.assertTrue(server._append_share_batch([fresh]))
            self.assertEqual(fence_held_during_commit, [False])
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 0
                )

            predating = self.replay_shaped_append_entry(
                accepted_at_ms=anchor_ms - 10,
                block_hash_hex="99" * 32,
            )
            self.assertTrue(server._append_share_batch([predating]))
            self.assertEqual(fence_held_during_commit, [False, True])
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 1
                )
                self.assertIsNone(server._payout_ledger_artifact)
            self.assertIsNone(server._incremental_payout_artifact_window)

    def test_replay_append_during_cold_scan_invalidates_inflight_walk(
        self,
    ) -> None:
        # A cold-start walk exposes no cached window or armed artifact while
        # its ledger read runs. A replayed row committing mid-walk with
        # timestamps below the walk's anchor must still record an
        # invalidation, or the walk installs a window that can never
        # rediscover the row through delta reads.
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        entry = self.replay_shaped_append_entry(
            accepted_at_ms=999_950,
            block_hash_hex="55" * 32,
        )
        original_snapshot = ledger.snapshot_at_job_issue
        raced = threading.Event()
        append_threads: list[threading.Thread] = []

        def racing_snapshot(
            anchor_job_issued_at_ms: int,
            *,
            window_weight: int | None = None,
        ) -> list[object]:
            # Capture the read result first: the replayed row committing
            # afterwards models a database snapshot taken before the commit.
            records = original_snapshot(
                anchor_job_issued_at_ms,
                window_weight=window_weight,
            )
            if not raced.is_set():
                raced.set()
                thread = threading.Thread(
                    target=server._append_share_batch,
                    args=([entry],),
                )
                thread.start()
                append_threads.append(thread)
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    with server._job_cache_lock:
                        if server._payout_ledger_append_invalidation_epoch:
                            break
                    time.sleep(0.001)
            return records

        ledger.snapshot_at_job_issue = racing_snapshot  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")
            self.assertNotIn(
                entry.pending_share.share_id,
                {str(share["share_id"]) for share in initial.shares_json},
            )
            self.assertTrue(append_threads)
            append_threads[0].join(timeout=2.0)
            self.assertFalse(append_threads[0].is_alive())
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 1
                )
            self.assertFalse(server._install_payout_ledger_artifact(initial))
            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)

            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_040
            rebuilt = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert rebuilt is not None
        self.assertEqual(rebuilt.window_build_mode, "full_rescan")
        self.assertEqual(
            rebuilt.window_full_rescan_reason,
            "late_visible_append",
        )
        self.assertIn(
            entry.pending_share.share_id,
            {str(share["share_id"]) for share in rebuilt.shares_json},
        )

    def test_inflight_scan_anchor_records_invalidation_without_windows(
        self,
    ) -> None:
        # Directly exercise the published in-flight anchor: with no cached
        # window and no armed artifact, only a predating append may advance
        # the epoch, and fresh shares above the anchor must leave the walk
        # undisturbed.
        server, _ledger, _artifacts = self.configured_server()
        server._ensure_job_cache_state()
        predating = stamped_pending_share(999_950)
        fresh = stamped_pending_share(1_000_050)

        server._invalidate_incremental_payout_window_for_append(predating)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 0)

        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            token = server._expose_inflight_scan_anchor(999_999)
        try:
            server._invalidate_incremental_payout_window_for_append(fresh)
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 0
                )
            server._invalidate_incremental_payout_window_for_append(predating)
        finally:
            server._retire_inflight_scan_anchor(token)
        with server._job_cache_lock:
            self.assertEqual(server._payout_ledger_append_invalidation_epoch, 1)
            self.assertEqual(server._payout_window_inflight_scan_anchors, {})
        self.assertEqual(
            server._incremental_payout_artifact_window_invalidation_reason,
            "late_visible_append",
        )

    def test_replay_append_during_bundle_construction_invalidates_build(
        self,
    ) -> None:
        # A cold synchronous ready build exposes no cached window or armed
        # artifact once its ledger read returns, while manifest construction
        # and cache publication still lie ahead. A replayed row committing in
        # that gap with timestamps below the read's anchor must still advance
        # the epoch -- the exposure outlives the read -- so the publication
        # fence supersedes the bundle instead of seeding a window that delta
        # reads can never rediscover the row from.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        clock_ms = [1_000_000]
        entry = self.replay_shaped_append_entry(
            accepted_at_ms=999_950,
            block_hash_hex="66" * 32,
        )
        raced = threading.Event()
        original_build = server.build_audit_bundle

        def racing_build(**kwargs: object) -> dict[str, object]:
            # The ledger read has already returned; the replayed row commits
            # while the coinbase manifest is being constructed.
            if not raced.is_set():
                raced.set()
                server._append_share_batch([entry])
            return original_build(**kwargs)

        server.build_audit_bundle = racing_build  # type: ignore[method-assign]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            with self.assertRaises(JobBuildSuperseded):
                server.build_shared_job_bundle(
                    server.current_template_artifacts(),
                    worker(),
                )
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 1
                )
                self.assertIsNone(server._payout_ledger_artifact)

            # A fresh build keyed to the advanced epoch re-reads the ledger
            # and rediscovers the replayed row.
            with server._job_cache_lock:
                server._job_build_issued_at_ms.clear()
            clock_ms[0] = 1_000_040
            rebuilt = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
        self.assertIn(
            entry.pending_share.share_id,
            {str(share["share_id"]) for share in rebuilt.shares_json},
        )

    def test_seeded_artifact_exposure_survives_until_install_fence(
        self,
    ) -> None:
        # The synchronous build's exposed scan anchor rides its seeded
        # artifact: a replayed row committing after the bundle returned but
        # before cache publication installs the seed must advance the epoch,
        # and the doomed seed's install fence must discard it (retiring the
        # exposure) instead of arming the pre-append window.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            bundle = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
            seed = bundle.prepared_ledger_artifact
            assert seed is not None
            self.assertIsNotNone(seed.inflight_scan_anchor_token)

            entry = self.replay_shaped_append_entry(
                accepted_at_ms=999_950,
                block_hash_hex="77" * 32,
            )
            self.assertTrue(server._append_share_batch([entry]))
            with server._job_cache_lock:
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch, 1
                )

            self.assertFalse(server._install_payout_ledger_artifact(seed))
        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)
            self.assertEqual(server._payout_window_inflight_scan_anchors, {})

    def test_late_append_disarms_before_prepare_lock_is_available(self) -> None:
        server, _ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            self.assertTrue(server._install_payout_ledger_artifact(artifact))
            anchor_ms = int(artifact.snapshot_anchor_ms)
            pending = stamped_pending_share(anchor_ms - 10)

            server._payout_state_prepare_lock.acquire()
            # Exercise the artifact anchor independently: an already-cold
            # incremental cache must not hide an unsafe armed artifact.
            server._incremental_payout_artifact_window = None
            invalidation_finished = threading.Event()

            def invalidate() -> None:
                server._invalidate_incremental_payout_window_for_append(
                    pending
                )
                invalidation_finished.set()

            thread = threading.Thread(target=invalidate)
            thread.start()
            try:
                deadline = time.monotonic() + 1.0
                while time.monotonic() < deadline:
                    with server._job_cache_lock:
                        if server._payout_ledger_artifact is None:
                            break
                    time.sleep(0.001)
                with server._job_cache_lock:
                    self.assertIsNone(server._payout_ledger_artifact)
                    self.assertGreater(
                        server._payout_ledger_append_invalidation_epoch,
                        artifact.append_invalidation_epoch,
                    )
                self.assertFalse(invalidation_finished.is_set())
                self.assertFalse(
                    server._install_payout_ledger_artifact(artifact)
                )
            finally:
                server._payout_state_prepare_lock.release()
                thread.join(timeout=1.0)
            self.assertFalse(thread.is_alive())
            self.assertTrue(invalidation_finished.is_set())

    def test_atomic_publication_rejects_pre_append_artifact(self) -> None:
        server, _ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            candidate = server._current_payout_state_candidate()
            stale = candidate.ledger_artifact
            assert stale is not None
            anchor_ms = int(stale.snapshot_anchor_ms)
            server._invalidate_incremental_payout_window_for_append(
                stamped_pending_share(anchor_ms - 10)
            )
            with server._payout_artifact_executor_lock:
                server._payout_artifact_executor_shutdown = True
            published = server._publish_payout_state_candidate(candidate)

        self.assertIsNotNone(published)
        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)
            self.assertGreater(
                server._payout_ledger_append_invalidation_epoch,
                stale.append_invalidation_epoch,
            )

    def test_late_append_supersedes_in_flight_sync_seed_bundle(self) -> None:
        server, _ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            artifact = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert artifact is not None
            self.assertTrue(server._install_payout_ledger_artifact(artifact))
            pending = stamped_pending_share(
                int(artifact.snapshot_anchor_ms) - 10
            )

            def invalidating_builder(**kwargs: object) -> dict[str, object]:
                server._invalidate_incremental_payout_window_for_append(
                    pending
                )
                suffix_hex = str(kwargs["coinbase_script_sig_suffix_hex"])
                return {
                    "found_block": dict(kwargs["found_block"]),
                    "payout_policy_manifest": {"accounts": []},
                    "signed_coinbase_manifest": {
                        "manifest": {
                            "coinbase_tx_hex": synthetic_manifest_coinbase_hex(
                                suffix_hex
                            ),
                        }
                    },
                }

            server.build_audit_bundle = invalidating_builder  # type: ignore[method-assign]
            with self.assertRaises(JobBuildSuperseded):
                server.build_shared_job_bundle(
                    artifacts,
                    worker(),
                    payout_artifact=None,
                )

        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)

    def test_normal_append_preserves_incremental_window(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            cached = server._incremental_payout_artifact_window
            self.assertIsNotNone(cached)
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            with server._job_cache_lock:
                armed = server._payout_ledger_artifact
                append_epoch = (
                    server._payout_ledger_append_invalidation_epoch
                )
            self.assertIsNotNone(armed)

            entry = self.replay_shaped_append_entry(
                accepted_at_ms=1_000_010,
                block_hash_hex="55" * 32,
            )
            self.assertTrue(server._append_share_entry(entry))
            self.assertIs(server._incremental_payout_artifact_window, cached)
            with server._job_cache_lock:
                self.assertIs(server._payout_ledger_artifact, armed)
                self.assertEqual(
                    server._payout_ledger_append_invalidation_epoch,
                    append_epoch,
                )
            self.assertIs(
                server._usable_payout_ledger_artifact(
                    0,
                    artifacts.network_difficulty,
                ),
                armed,
            )

            server.payout_artifact_min_build_interval_seconds = 0.0
            clock_ms[0] = 1_000_020
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert advanced is not None
        self.assertEqual(advanced.window_build_mode, "incremental")
        self.assertIsNone(advanced.window_full_rescan_reason)
        self.assertEqual(advanced.window_touched_pages, 1)
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 1)
        self.assertIn(
            entry.pending_share.share_id,
            {str(share["share_id"]) for share in advanced.shares_json},
        )

    def test_debounce_then_delta_and_forced_full_rescan(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")
            self.assertEqual(ledger.full_snapshot_calls, 1)

            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            debounced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert debounced is not None
            self.assertEqual(debounced.window_build_mode, "debounced")
            self.assertEqual(ledger.full_snapshot_calls, 1)
            self.assertEqual(ledger.delta_snapshot_calls, 0)
            self.assertEqual(len(debounced.shares_json), 3)

            server.payout_artifact_min_build_interval_seconds = 0.0
            advanced = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert advanced is not None
            self.assertEqual(advanced.window_build_mode, "incremental")
            self.assertEqual(advanced.window_delta_rows, 1)
            self.assertEqual(ledger.delta_snapshot_calls, 1)
            self.assertEqual(len(advanced.shares_json), 4)

            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )
            assert forced is not None
            self.assertEqual(forced.window_build_mode, "full_rescan")
            self.assertEqual(
                forced.window_full_rescan_reason,
                "reconcile_invalidation",
            )
            self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_periodic_self_check_compares_delta_to_full_oracle(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            server.payout_artifact_min_build_interval_seconds = 0.0
            server.payout_artifact_full_rescan_seconds = 0.0

            checked = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert checked is not None
        self.assertEqual(checked.window_build_mode, "self_check_match")
        self.assertEqual(ledger.delta_snapshot_calls, 1)
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_routine_build_runs_overdue_self_check_despite_preview_refresh(
        self,
    ) -> None:
        # Accepted-block previews bypass the build interval and refresh
        # refreshed_monotonic on every block, yet never run the periodic
        # oracle themselves. Under a sustained sub-interval block cadence
        # every routine build used to return from the debounce branch
        # before reaching the self-check condition, so the configured
        # full-rescan window and balance oracle could be postponed
        # indefinitely. An overdue self-check now disarms the debounce
        # for routine builds while the urgent preview path stays
        # debounce-free and oracle-free.
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            preview = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty, False, True
            )
            assert preview is not None
            self.assertEqual(preview.window_build_mode, "incremental")
            # The next preview arrives with the self-check long overdue;
            # the urgent path must still not pay the oracle walk.
            server.payout_artifact_full_rescan_seconds = 0.0
            append_incremental_share(
                ledger,
                share_seq=5,
                accepted_at_ms=1_000_030,
            )
            clock_ms[0] = 1_000_040
            urgent = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty, False, True
            )
            assert urgent is not None
            self.assertEqual(urgent.window_build_mode, "incremental")
            self.assertEqual(ledger.full_snapshot_calls, 1)
            # A routine build inside the min build interval used to
            # debounce here -- forever, while blocks kept refreshing the
            # window above -- and the self-check never ran.
            checked = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert checked is not None
        self.assertEqual(checked.window_build_mode, "self_check_match")
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_periodic_self_check_replaces_divergent_delta_with_full_bytes(
        self,
    ) -> None:
        class OmittingDeltaLedger(IncrementalRecordingLedger):
            def snapshot_between_job_issues(
                self,
                previous_anchor_job_issued_at_ms: int,
                anchor_job_issued_at_ms: int,
            ) -> list[object]:
                del previous_anchor_job_issued_at_ms, anchor_job_issued_at_ms
                self.delta_snapshot_calls += 1
                return []

        ledger = OmittingDeltaLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            server.payout_artifact_min_build_interval_seconds = 0.0
            server.payout_artifact_full_rescan_seconds = 0.0
            repaired = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )

        assert repaired is not None and forced is not None
        self.assertEqual(repaired.window_build_mode, "self_check_mismatch")
        self.assertEqual(list(repaired.shares_json), list(forced.shares_json))
        self.assertEqual(
            repaired.share_snapshot_sha256,
            forced.share_snapshot_sha256,
        )

    def test_periodic_self_check_invalidates_drifted_published_balances(
        self,
    ) -> None:
        class DriftingCarryLedger(IncrementalRecordingLedger):
            def __init__(self) -> None:
                super().__init__()
                self.balance_sats = 546
                self.prior_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.prior_balance_reads += 1
                return [
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": self.balance_sats,
                    }
                ]

        ledger = DriftingCarryLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        build_logs: list[dict[str, object]] = []
        server._payout_artifact_log = (  # type: ignore[method-assign]
            lambda event, **fields: build_logs.append(
                {"event": event, **fields}
            )
        )

        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            published = server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))

            ledger.balance_sats = 777
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            monotonic_seconds[0] = 3_701.0
            checked = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )

            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)

            # The mismatch build publishes the repaired balances for the same
            # generation, so the next build resumes byte reuse from the
            # repaired snapshot instead of paying another carry read against
            # a still-stale publication.
            clock_ms[0] = 1_000_030
            monotonic_seconds[0] = 3_702.0
            following = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )

        assert checked is not None and following is not None
        self.assertEqual(checked.window_build_mode, "self_check_match")
        self.assertEqual(checked.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(following.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(ledger.prior_balance_reads, 2)
        self.assertEqual(ledger.full_snapshot_calls, 2)
        repaired = server._published_payout_state.artifact
        assert repaired is not None
        self.assertNotEqual(
            repaired.prior_balances_sha256,
            published.prior_balances_sha256,
        )
        self.assertEqual(
            repaired.prior_balances_sha256,
            checked.prior_balances_sha256,
        )
        self.assertIsNone(
            server._payout_prior_balances_reuse_invalidated_sha256
        )
        self.assertEqual(
            server.payout_artifact_event_counts["balance_check_mismatch"],
            1,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["balance_repair_published"],
            1,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["self_check_match"],
            1,
        )
        built_sources = [
            entry["prior_balances_source"]
            for entry in build_logs
            if entry["event"] == "payout_artifact_built"
        ]
        self.assertEqual(built_sources, ["published", "ledger", "published"])
        mismatch_logs = [
            entry
            for entry in build_logs
            if entry["event"] == "payout_artifact_balance_check_mismatch"
        ]
        self.assertEqual(len(mismatch_logs), 1)
        repair_logs = [
            entry
            for entry in build_logs
            if entry["event"] == "payout_artifact_balance_repair_published"
        ]
        self.assertEqual(len(repair_logs), 1)

    def test_balance_backstop_failure_retains_oracle_repaired_window(
        self,
    ) -> None:
        # The share-window oracle and the carry-balance backstop are
        # independent reads. A backstop failure after a successful oracle
        # must not resurrect the delta window the oracle just refuted.
        class FailingCarryOmittingDeltaLedger(IncrementalRecordingLedger):
            def __init__(self) -> None:
                super().__init__()
                self.fail_balance_read = False
                self.balance_read_attempts = 0

            def snapshot_between_job_issues(
                self,
                previous_anchor_job_issued_at_ms: int,
                anchor_job_issued_at_ms: int,
            ) -> list[object]:
                del previous_anchor_job_issued_at_ms, anchor_job_issued_at_ms
                self.delta_snapshot_calls += 1
                return []

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.balance_read_attempts += 1
                if self.fail_balance_read:
                    raise RuntimeError("carry aggregate unavailable")
                return [
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": 546,
                    }
                ]

        ledger = FailingCarryOmittingDeltaLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))

            ledger.fail_balance_read = True
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            monotonic_seconds[0] = 3_701.0
            checked = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )

            append_incremental_share(
                ledger,
                share_seq=5,
                accepted_at_ms=1_000_021,
            )
            clock_ms[0] = 1_000_030
            monotonic_seconds[0] = 3_702.0
            following = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
            )

        assert checked is not None and following is not None
        self.assertEqual(checked.window_build_mode, "self_check_mismatch")
        self.assertEqual(
            checked.window_full_rescan_reason,
            "periodic_self_check_balance_check_failed",
        )
        # The omitting delta produced three retained shares; the oracle's
        # four-share window must be the one served and cached.
        self.assertEqual(len(checked.shares_json), 4)
        # One read published the priming state; the only other attempt is
        # the backstop that failed.
        self.assertEqual(ledger.balance_read_attempts, 2)
        # The unverified balances keep reusing the published snapshot.
        self.assertEqual(
            checked.prior_balances_sha256,
            initial.prior_balances_sha256,
        )
        self.assertIsNone(
            server._payout_prior_balances_reuse_invalidated_sha256
        )
        self.assertEqual(
            server.payout_artifact_event_counts["self_check_mismatch"],
            1,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["self_check_failed"],
            0,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["balance_check_mismatch"],
            0,
        )
        # The failed backstop still stamps the attempt: the next build stays
        # on the delta path instead of retrying the oracle immediately.
        self.assertEqual(following.window_build_mode, "incremental")
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_periodic_self_check_repair_arms_and_serves(self) -> None:
        # The repaired publication is what lets the mismatch build's fresh
        # balances actually reach miners: its artifact must arm against the
        # repaired digest and the reuse probe must serve it, instead of the
        # armed-artifact fence clearing the repair against a stale published
        # snapshot until an unrelated generation change.
        class DriftingCarryLedger(IncrementalRecordingLedger):
            def __init__(self) -> None:
                super().__init__()
                self.balance_sats = 546

            def current_prior_balances(self) -> list[dict[str, object]]:
                return [
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": self.balance_sats,
                    }
                ]

        ledger = DriftingCarryLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 3_600.0
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]

        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))

            ledger.balance_sats = 777
            clock_ms[0] = 1_000_020
            monotonic_seconds[0] = 3_701.0
            checked = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert checked is not None
            self.assertEqual(checked.prior_balances[0]["balance_sats"], 777)

            self.assertTrue(server._install_payout_ledger_artifact(checked))
            served = server._usable_payout_ledger_artifact(
                0,
                artifacts.network_difficulty,
            )

        assert served is not None
        self.assertEqual(served.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(
            served.prior_balances_sha256,
            checked.prior_balances_sha256,
        )
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)

    def test_self_check_repair_supersedes_active_jobs(self) -> None:
        # The in-place repair changes neither the payout generation nor the
        # append epoch, so an already-stamped job passes every admission
        # fence while carrying the refuted balances. The repair must
        # schedule a refresh wave, and the wave's reselection must identify
        # such jobs by their payout digest, or they stay mineable (and a
        # block solve commits the stale allocation) until an unrelated
        # generation change.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            bundle = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
            context = server.stamp_job_for_client(
                state,
                bundle,
                clean_jobs=True,
            )
        state.active_job = context
        snapshot = SimpleNamespace(
            bestblockhash=str(bundle.template["previousblockhash"]),
            previousblockhash=str(bundle.template["previousblockhash"]),
            template_fingerprint=bundle.template_fingerprint,
        )
        with server._job_cache_lock:
            published = server._published_payout_state.artifact
        assert published is not None
        self.assertEqual(
            context.payout_artifact_sha256,
            published.prior_balances_sha256,
        )
        self.assertFalse(
            server.client_needs_tip_template_refresh(state, snapshot)
        )

        self.assertTrue(
            server._publish_self_check_repaired_balances(
                0,
                stale_prior_balances_sha256=published.prior_balances_sha256,
                balances=[
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": 999,
                    }
                ],
            )
        )

        self.assertTrue(
            server.client_needs_tip_template_refresh(state, snapshot)
        )
        self.assertTrue(server._tip_refresh_pending_event.is_set())
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_self_check_repair_retires_refuted_jobs_clean(self) -> None:
        # Reselection alone is not enough: the wave stamps the replacement
        # with clean_jobs from client_tip_changed_for_snapshot, and a
        # clean_jobs=False delivery leaves the refuted job ID admissible in
        # active_job_ids. The digest mismatch must force clean-job
        # retirement, or a late block submission on the old job still
        # commits the refuted allocation after corrected work is delivered.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            bundle = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
            context = server.stamp_job_for_client(
                state,
                bundle,
                clean_jobs=True,
            )
        state.active_job = context
        snapshot = SimpleNamespace(
            bestblockhash=str(bundle.template["previousblockhash"]),
            previousblockhash=str(bundle.template["previousblockhash"]),
            template_fingerprint=bundle.template_fingerprint,
        )
        with server._job_cache_lock:
            published = server._published_payout_state.artifact
        assert published is not None
        self.assertFalse(
            server.client_tip_changed_for_snapshot(state, snapshot)
        )

        self.assertTrue(
            server._publish_self_check_repaired_balances(
                0,
                stale_prior_balances_sha256=published.prior_balances_sha256,
                balances=[
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": 999,
                    }
                ],
            )
        )

        self.assertTrue(
            server.client_tip_changed_for_snapshot(state, snapshot)
        )

    def test_late_append_invalidation_supersedes_active_jobs(self) -> None:
        # A late-visible append advances only the append epoch: the payout
        # generation and published digest both survive, so an active job
        # mining the pre-append window passes every other fence. The
        # invalidation must schedule a refresh wave, the reselection must
        # identify the job by its stamped epoch, and the replacement must
        # retire it clean -- otherwise miners keep working (and can solve a
        # block from) a window that omitted the late share until some
        # unrelated tip event.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            bundle = server.build_shared_job_bundle(
                server.current_template_artifacts(),
                worker(),
            )
            context = server.stamp_job_for_client(
                state,
                bundle,
                clean_jobs=True,
            )
        state.active_job = context
        snapshot = SimpleNamespace(
            bestblockhash=str(bundle.template["previousblockhash"]),
            previousblockhash=str(bundle.template["previousblockhash"]),
            template_fingerprint=bundle.template_fingerprint,
        )
        self.assertFalse(
            server.client_needs_tip_template_refresh(state, snapshot)
        )
        self.assertFalse(
            server.client_tip_changed_for_snapshot(state, snapshot)
        )

        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            token = server._expose_inflight_scan_anchor(999_999)
        try:
            server._invalidate_incremental_payout_window_for_append(
                stamped_pending_share(999_950)
            )
        finally:
            server._retire_inflight_scan_anchor(token)
        with server._job_cache_lock:
            self.assertEqual(
                server._payout_ledger_append_invalidation_epoch, 1
            )

        self.assertTrue(
            server.client_needs_tip_template_refresh(state, snapshot)
        )
        self.assertTrue(
            server.client_tip_changed_for_snapshot(state, snapshot)
        )
        self.assertTrue(server._tip_refresh_pending_event.is_set())
        self.assertTrue(server._tip_refresh_retry.is_set())

    def test_direct_delivery_rechecks_published_digest_at_commit(self) -> None:
        # A periodic self-check repair swaps the published balance snapshot
        # in place: neither the payout generation nor the append epoch moves,
        # so only the digest identifies a direct-delivery context keyed to
        # the refuted balances. When the repair lands after the build but
        # before the commit boundary, the delivery must re-check the digest
        # or the miner's active job carries the refuted allocation.
        server, _ledger, _artifacts = self.configured_server()
        install_fake_bundle_builder(server)
        server._ensure_tip_refresh_state()
        state = client(1)
        sent: list[dict[str, object]] = []
        state.send = sent.append  # type: ignore[method-assign]
        original_build_job_for_client = server.build_job_for_client
        repaired = False

        def repair_after_build(*args: object, **kwargs: object) -> object:
            nonlocal repaired
            context = original_build_job_for_client(*args, **kwargs)  # type: ignore[arg-type]
            if not repaired:
                repaired = True
                with server._job_cache_lock:
                    published = server._published_payout_state.artifact
                assert published is not None
                self.assertEqual(
                    context.payout_artifact_sha256,
                    published.prior_balances_sha256,
                )
                self.assertTrue(
                    server._publish_self_check_repaired_balances(
                        0,
                        stale_prior_balances_sha256=(
                            published.prior_balances_sha256
                        ),
                        balances=[
                            {
                                "recipient_id": "carry",
                                "order_key": "01:carry",
                                "p2mr_program_hex": "66" * 32,
                                "balance_sats": 999,
                            }
                        ],
                    )
                )
            return context

        server.build_job_for_client = repair_after_build  # type: ignore[method-assign]

        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: 1_000_000,
        ):
            self.assertFalse(server.maybe_send_job(state, clean_jobs=True))
        self.assertEqual(sent, [])
        self.assertIsNone(state.active_job)
        self.assertEqual(state.active_job_ids, set())
        self.assertEqual(server.jobs, {})

    def test_incremental_and_forced_full_artifacts_match_with_carry_boundaries(
        self,
    ) -> None:
        class CarryLedger(IncrementalRecordingLedger):
            def current_prior_balances(self) -> list[dict[str, object]]:
                return [
                    {
                        "recipient_id": "below",
                        "order_key": "01:below",
                        "p2mr_program_hex": "11" * 32,
                        "balance_sats": 545,
                    },
                    {
                        "recipient_id": "at",
                        "order_key": "02:at",
                        "p2mr_program_hex": "22" * 32,
                        "balance_sats": 546,
                    },
                    {
                        "recipient_id": "above",
                        "order_key": "03:above",
                        "p2mr_program_hex": "33" * 32,
                        "balance_sats": 547,
                    },
                ]

        ledger = CarryLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            incremental = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )

        assert incremental is not None and forced is not None
        incremental_bytes = canonical_json_text(
            {
                "shares": list(incremental.shares_json),
                "prior_balances": list(incremental.prior_balances),
            }
        ).encode()
        forced_bytes = canonical_json_text(
            {
                "shares": list(forced.shares_json),
                "prior_balances": list(forced.prior_balances),
            }
        ).encode()
        self.assertEqual(incremental_bytes, forced_bytes)
        self.assertEqual(
            incremental.share_snapshot_sha256,
            forced.share_snapshot_sha256,
        )
        self.assertEqual(
            incremental.prior_balances_sha256,
            forced.prior_balances_sha256,
        )

    def test_normal_refresh_reuses_generation_owned_prior_balances(self) -> None:
        class CountingCarryLedger(IncrementalRecordingLedger):
            prior_balance_reads = 0

            def current_prior_balances(self) -> list[dict[str, object]]:
                self.prior_balance_reads += 1
                return [
                    {
                        "recipient_id": "carry",
                        "order_key": "01:carry",
                        "p2mr_program_hex": "66" * 32,
                        "balance_sats": 545,
                    }
                ]

        ledger = CountingCarryLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            published = server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            incremental = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )

        assert initial is not None and incremental is not None and forced is not None
        self.assertEqual(ledger.prior_balance_reads, 2)
        self.assertEqual(
            initial.prior_balances_sha256,
            published.prior_balances_sha256,
        )
        self.assertEqual(
            incremental.prior_balances_sha256,
            published.prior_balances_sha256,
        )

    def test_fifteen_second_generation_bumps_refresh_once_per_minute(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        modes: list[str | None] = []
        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            modes.append(initial.window_build_mode)
            for bump in range(1, 5):
                monotonic_seconds[0] = 100.0 + (15.0 * bump)
                clock_ms[0] = 1_000_000 + (15_000 * bump)
                append_incremental_share(
                    ledger,
                    share_seq=3 + bump,
                    accepted_at_ms=clock_ms[0] - 2,
                )
                built = server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
                assert built is not None
                modes.append(built.window_build_mode)

        self.assertEqual(
            modes,
            ["full_rescan", "debounced", "debounced", "debounced", "incremental"],
        )
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 1)

    def test_found_block_candidate_bypasses_normal_interval(self) -> None:
        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            # Even an overdue routine oracle must be deferred from the
            # immediate found-block publication path.
            server.payout_artifact_full_rescan_seconds = 0.0
            clock_ms[0] = 1_000_020
            candidate = server._prepared_payout_state_candidate(
                (
                    0,
                    1,
                    str(server.rpc.tip),
                    "accepted_block_preview",
                    time.monotonic(),
                )
            )
            candidate = server._accepted_block_preview_candidate(
                candidate,
                block_hash="44" * 32,
                preview=(("winner", "01:winner", "55" * 32, 546),),
            )

        artifact = candidate.ledger_artifact
        assert artifact is not None
        self.assertEqual(artifact.window_build_mode, "incremental")
        self.assertEqual(artifact.window_delta_rows, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 1)
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(len(artifact.shares_json), 4)

    def test_failed_periodic_oracle_is_spaced_from_next_delta_build(self) -> None:
        class FailingOracleLedger(IncrementalRecordingLedger):
            fail_full_snapshot = False

            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[object]:
                if self.fail_full_snapshot:
                    self.full_snapshot_calls += 1
                    raise RuntimeError("oracle unavailable")
                return super().snapshot_at_job_issue(
                    anchor_job_issued_at_ms,
                    window_weight=window_weight,
                )

        ledger = FailingOracleLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 10.0
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            self.assertIsNotNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
            ledger.fail_full_snapshot = True
            append_incremental_share(
                ledger,
                share_seq=4,
                accepted_at_ms=1_000_010,
            )
            clock_ms[0] = 1_000_020
            monotonic_seconds[0] = 110.0
            failed_check = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert failed_check is not None
            self.assertEqual(
                failed_check.window_build_mode,
                "incremental_self_check_failed",
            )

            append_incremental_share(
                ledger,
                share_seq=5,
                accepted_at_ms=1_000_021,
            )
            clock_ms[0] = 1_000_030
            monotonic_seconds[0] = 111.0
            next_delta = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert next_delta is not None
        self.assertEqual(next_delta.window_build_mode, "incremental")
        self.assertEqual(ledger.full_snapshot_calls, 2)

    def test_found_block_reuses_armed_window_when_prepare_lock_is_busy(
        self,
    ) -> None:
        class BusyPrepareLock:
            def acquire(self, blocking: bool = True) -> bool:
                self.assert_nonblocking(blocking)
                return False

            @staticmethod
            def assert_nonblocking(blocking: bool) -> None:
                if blocking:
                    raise AssertionError("found-block path attempted to wait")

            def release(self) -> None:
                raise AssertionError("unacquired lock was released")

        server, ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            server._payout_state_prepare_lock = BusyPrepareLock()  # type: ignore[assignment]
            candidate = server._prepared_payout_state_candidate(
                (
                    0,
                    1,
                    str(server.rpc.tip),
                    "accepted_block_preview",
                    time.monotonic(),
                )
            )
            candidate = server._accepted_block_preview_candidate(
                candidate,
                block_hash="44" * 32,
                preview=(("winner", "01:winner", "55" * 32, 546),),
            )

        artifact = candidate.ledger_artifact
        assert artifact is not None
        self.assertEqual(artifact.window_build_mode, "found_block_cached")
        self.assertEqual(artifact.payout_state_generation, 1)
        self.assertEqual(
            list(artifact.prior_balances),
            [
                {
                    "recipient_id": "winner",
                    "order_key": "01:winner",
                    "p2mr_program_hex": "55" * 32,
                    "balance_sats": 546,
                }
            ],
        )
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 0)
        self.assertEqual(
            server.payout_artifact_event_counts["found_block_cached"],
            1,
        )

    def test_found_block_build_failure_is_visible_and_reuses_exact_window(
        self,
    ) -> None:
        class FailingDeltaLedger(IncrementalRecordingLedger):
            fail_delta = False

            def snapshot_between_job_issues(
                self,
                previous_anchor_job_issued_at_ms: int,
                anchor_job_issued_at_ms: int,
            ) -> list[object]:
                if self.fail_delta:
                    self.delta_snapshot_calls += 1
                    raise RuntimeError("delta unavailable")
                return super().snapshot_between_job_issues(
                    previous_anchor_job_issued_at_ms,
                    anchor_job_issued_at_ms,
                )

        ledger = FailingDeltaLedger()
        for share_seq in range(1, 4):
            append_incremental_share(
                ledger,
                share_seq=share_seq,
                accepted_at_ms=999_900 + share_seq,
            )
        server, rpc = coordinator(ledger=ledger)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            ledger.fail_delta = True
            clock_ms[0] = 1_000_020
            candidate = server._prepared_payout_state_candidate(
                (
                    0,
                    1,
                    str(server.rpc.tip),
                    "accepted_block_preview",
                    time.monotonic(),
                )
            )

        artifact = candidate.ledger_artifact
        assert artifact is not None
        self.assertEqual(artifact.window_build_mode, "found_block_cached")
        self.assertEqual(
            server.payout_artifact_event_counts["build_aborted"],
            1,
        )
        self.assertEqual(
            server.payout_artifact_event_counts["found_block_cached"],
            1,
        )

    def test_old_anchor_does_not_rearm_before_window_cadence(self) -> None:
        class RecordingExecutor:
            def __init__(self) -> None:
                self.submissions = 0

            def submit(self, _fn: object) -> Future[None]:
                self.submissions += 1
                return Future()

        server, _ledger, artifacts = self.configured_server()
        clock_ms = [1_000_000]
        monotonic_seconds = [100.0]
        with (
            patch(
                "lab.prism.prism_coordinator.now_ms",
                side_effect=lambda: clock_ms[0],
            ),
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                side_effect=lambda: monotonic_seconds[0],
            ),
        ):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            executor = RecordingExecutor()
            with server._payout_artifact_executor_lock:
                server._payout_artifact_executor = executor  # type: ignore[assignment]

            # A pending-share floor can leave the declared anchor old even
            # though its exact pages were just re-proved. Past-floor probes
            # must not enqueue five-second debounced balance-only builds.
            clock_ms[0] = 1_061_000
            monotonic_seconds[0] = 105.0
            self.assertIsNotNone(
                server._usable_payout_ledger_artifact(
                    0,
                    artifacts.network_difficulty,
                )
            )
            with server._payout_artifact_executor_lock:
                self.assertIsNone(server._payout_artifact_requested)

            monotonic_seconds[0] = 160.0
            self.assertIsNotNone(
                server._usable_payout_ledger_artifact(
                    0,
                    artifacts.network_difficulty,
                )
            )

        with server._payout_artifact_executor_lock:
            self.assertEqual(
                server._payout_artifact_requested,
                (0, int(artifacts.network_difficulty)),
            )
            self.assertEqual(executor.submissions, 1)


def mark_progress_healthy(server: PrismCoordinator) -> None:
    snapshot = server.fetch_qbit_tip_template_snapshot()
    server._record_progress_tip_poll(snapshot)
    server._record_progress_publication(
        snapshot,
        int(getattr(server, "_payout_state_generation", 0)),
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

    def test_preview_publication_degrades_when_artifact_preparation_times_out(
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

        def timing_out_preparation(*_args: object, **_kwargs: object) -> object:
            raise TimeoutError("postgres operation exceeded 30s")

        server._prepared_payout_state_candidate = (  # type: ignore[method-assign]
            timing_out_preparation
        )
        server._begin_accepted_block_payout_preview(parent_hash)
        with patch("builtins.print"):
            self.assertEqual(
                server._publish_accepted_block_payout_preview(parent_hash, preview),
                preview,
            )

        # Children of the pending parent receive the immutable compact
        # preview even though no payout-state generation was published.
        transition = server._accepted_block_payout_previews[parent_hash]
        self.assertIsNone(transition.published_generation)
        self.assertTrue(transition.landed)
        self.assertEqual(
            server._prior_balances_for_job_parent(parent_hash),
            preview,
        )
        # Payout-state publication itself stays gated until the landing
        # reaches a terminal durable state.
        with self.assertRaisesRegex(
            _PayoutStatePublicationBlocked, "still pending"
        ):
            with server._payout_balance_mutation():
                pass
        server._clear_accepted_block_payout_preview(parent_hash)

    def test_unresolved_accepted_parent_depth_cap_fails_closed(self) -> None:
        server, _rpc = coordinator()
        server.accepted_parent_unresolved_depth_max = 2
        server.accepted_block_payout_preview_wait_seconds = 0.01
        hashes = ["a1" * 32, "a2" * 32, "a3" * 32]
        for height, block_hash in enumerate(hashes, start=10):
            server._begin_accepted_block_payout_preview(
                block_hash, block_height=height
            )
            server._mark_accepted_block_payout_landed(
                block_hash, block_height=height
            )
        self.assertEqual(len(server.accepted_parent_unresolved_ages_seconds()), 3)
        with self.assertRaisesRegex(TemplateRefreshBlocked, "exceeds cap"):
            server._await_pending_parent_payout_preview(hashes[0])
        # At the cap issuance still blocks: a child issued now could land
        # and create a (cap + 1)th unresolved transition, so the configured
        # maximum would no longer bound the prospective balance chain.
        server._clear_accepted_block_payout_preview(hashes[2])
        with self.assertRaisesRegex(TemplateRefreshBlocked, "exceeds cap"):
            server._await_pending_parent_payout_preview(hashes[0])
        # Below the cap the ordinary bounded wait applies instead.
        server._clear_accepted_block_payout_preview(hashes[1])
        with self.assertRaisesRegex(TemplateRefreshBlocked, "not ready yet"):
            server._await_pending_parent_payout_preview(hashes[0])
        server._clear_accepted_block_payout_preview(hashes[0])
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])

    def test_landing_observability_metrics_exposition(self) -> None:
        import contextlib as _contextlib
        import time as _time

        server, _rpc = coordinator()
        server.block_landing_db_timeout_seconds = 45.0
        server.block_landing_db_timeout_max_seconds = 90.0

        @_contextlib.contextmanager
        def statement_timeout(seconds: float):
            yield

        original_ledger = server.ledger
        server.ledger = SimpleNamespace(
            statement_timeout=statement_timeout,
            prior_balances_read_stats=lambda: {
                "reads_total": 3,
                "last_seconds": 0.004,
                "max_seconds": 0.021,
            },
        )
        block_hash = "ab" * 32
        with self.assertRaises(TimeoutError):
            with server._block_landing_ledger_statement_timeout_scope(block_hash):
                raise LedgerOperationTimeout("postgres operation exceeded 45s")
        server.ledger = original_ledger

        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        server.accepted_block_payout_preview_wait_seconds = 0.0
        with self.assertRaisesRegex(TemplateRefreshBlocked, "not ready yet"):
            server._await_pending_parent_payout_preview(block_hash)
        server._startup_phase_origin_monotonic = _time.monotonic()
        server._record_startup_phase_once("audit_listener_bound")

        prior_stats = {
            "reads_total": 3,
            "last_seconds": 0.004,
            "max_seconds": 0.021,
        }
        server.ledger.prior_balances_read_stats = (  # type: ignore[attr-defined]
            lambda: prior_stats
        )
        lines = "\n".join(server.landing_observability_metrics_lines())
        self.assertIn(
            'qbit_prism_block_ledger_call_timeouts_total{call_class="landing"} 1',
            lines,
        )
        self.assertIn(
            'qbit_prism_block_ledger_call_budget_seconds{call_class="landing"} 45.000000',
            lines,
        )
        self.assertIn(
            "qbit_prism_accepted_parent_unresolved_transitions 1",
            lines,
        )
        self.assertIn(
            "qbit_prism_accepted_parent_preview_wait_timeouts_total 1",
            lines,
        )
        self.assertNotIn(
            "qbit_prism_accepted_parent_unresolved_oldest_seconds -1",
            lines,
        )
        self.assertIn("qbit_prism_prior_balances_reads_total 3", lines)
        self.assertIn(
            "qbit_prism_prior_balances_read_max_seconds 0.021000",
            lines,
        )
        self.assertIn(
            'qbit_prism_startup_phase_seconds{phase="audit_listener_bound"}',
            lines,
        )
        server._clear_accepted_block_payout_preview(block_hash)

    def test_startup_replay_gate_blocks_job_builds_until_enumeration(self) -> None:
        server, _rpc = coordinator()
        enumerated: list[int] = []

        class EnumLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                enumerated.append(limit)
                return []

        server.ledger = EnumLedger()
        server._note_block_replay_enumeration_owed()
        with self.assertRaisesRegex(TemplateRefreshBlocked, "has not enumerated"):
            server._await_pending_parent_payout_preview("ab" * 32)
        self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(enumerated, [32])
        self.assertFalse(server._block_replay_enumeration_owed())
        self.assertIsNone(server._await_pending_parent_payout_preview("ab" * 32))

    def test_owed_enumeration_bypasses_live_candidate_short_circuits(self) -> None:
        server, _rpc = coordinator()
        enumerated: list[int] = []

        class EnumLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                enumerated.append(limit)
                return []

        server.ledger = EnumLedger()
        server.block_landing_db_timeout_seconds = 7.5
        server._retry_block_candidate = object()
        # Steady state: a retained retry short-circuits the outbox poll.
        self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(enumerated, [])
        # Owed enumeration is correctness-critical and bypasses the
        # short-circuits, running with the landing-class budget and
        # recording under the landing metrics class.
        server._note_block_replay_enumeration_owed()
        self.assertEqual(server.replay_pending_block_candidates(), 0)
        self.assertEqual(enumerated, [32])
        self.assertFalse(server._block_replay_enumeration_owed())
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["last_budget_seconds"], 7.5)
        self.assertNotIn("fast", metrics)

    def test_worker_reported_deadline_counts_as_landing_timeout(self) -> None:
        """A server-side cancellation completes the ledger worker with a
        timeout error before the coordinator-side wait expires; the
        completion path must still count it for the landing-timeout alert."""
        server, _rpc = coordinator()

        class CancelledLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                raise LedgerOperationTimeout("postgres statement deadline expired")

        server.ledger = CancelledLedger()
        server._note_block_replay_enumeration_owed()
        with patch("builtins.print"):
            self.assertTrue(server._run_startup_block_candidate_replay())
        self.assertTrue(server._block_replay_enumeration_owed())
        metrics = server.block_ledger_call_class_metrics()
        self.assertEqual(metrics["landing"]["calls_total"], 1)
        self.assertEqual(metrics["landing"]["timeouts_total"], 1)
        self.assertNotIn("fast", metrics)

    def test_startup_enumeration_escalates_past_truncated_batches(self) -> None:
        """A full first batch must not end enumeration: rows beyond the batch
        window (potentially the active parent) still need their payout
        barriers armed before job builds unblock."""
        server, _rpc = coordinator()
        requested: list[int] = []
        total_pending = 33

        class TruncatingLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                requested.append(limit)
                return [
                    durable_candidate_row(index)
                    for index in range(min(limit, total_pending))
                ]

        server.ledger = TruncatingLedger()
        server._note_block_replay_enumeration_owed()
        self.assertEqual(server.replay_pending_block_candidates(), total_pending)
        self.assertEqual(requested, [32, 64])
        self.assertFalse(server._block_replay_enumeration_owed())
        self.assertEqual(
            server._block_replay_candidate_queue.qsize(), total_pending
        )
        # Every restored row armed its payout barrier before the gate opened.
        self.assertEqual(
            len(server._accepted_block_payout_previews), total_pending
        )

    def test_startup_enumeration_stays_owed_at_truncation_cap(self) -> None:
        """An outbox larger than the enumeration cap keeps job builds blocked
        instead of silently declaring enumeration complete."""
        server, _rpc = coordinator()
        requested: list[int] = []

        class EndlessLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                requested.append(limit)
                return [durable_candidate_row(index) for index in range(limit)]

        server.ledger = EndlessLedger()
        server._note_block_replay_enumeration_owed()
        with patch("builtins.print"):
            queued = server.replay_pending_block_candidates()
        self.assertEqual(queued, 1024)
        self.assertEqual(requested, [32, 64, 128, 256, 512, 1024])
        self.assertTrue(server._block_replay_enumeration_owed())
        with self.assertRaisesRegex(TemplateRefreshBlocked, "has not enumerated"):
            server._await_pending_parent_payout_preview("ab" * 32)

    def test_steady_state_poll_restores_a_single_batch(self) -> None:
        """Without an owed enumeration, a full batch stays a single query;
        the next poll picks up later rows after the queue drains."""
        server, _rpc = coordinator()
        requested: list[int] = []

        class FullBatchLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                requested.append(limit)
                return [durable_candidate_row(index) for index in range(limit)]

        server.ledger = FullBatchLedger()
        self.assertEqual(server.replay_pending_block_candidates(), 32)
        self.assertEqual(requested, [32])
        self.assertFalse(server._block_replay_enumeration_owed())

    def test_startup_replay_timeout_keeps_job_builds_blocked(self) -> None:
        server, _rpc = coordinator()
        release = threading.Event()

        class StallLedger(FakeLedger):
            def pending_block_candidate_rows(
                self, *, limit: int = 32
            ) -> list[dict[str, object]]:
                release.wait(5)
                return []

        server.ledger = StallLedger()
        server.block_landing_db_timeout_seconds = 0.01
        server.block_landing_db_timeout_max_seconds = 0.01
        try:
            with patch("builtins.print"):
                self.assertTrue(server._run_startup_block_candidate_replay())
            self.assertTrue(server._block_replay_enumeration_owed())
            with self.assertRaisesRegex(
                TemplateRefreshBlocked, "has not enumerated"
            ):
                server._await_pending_parent_payout_preview("ab" * 32)
            # The startup enumeration ran with the landing budget, so its
            # timeout fires the documented landing-timeout alert instead of
            # hiding inside the fast-call series.
            metrics = server.block_ledger_call_class_metrics()
            self.assertEqual(metrics["landing"]["timeouts_total"], 1)
            self.assertNotIn("fast", metrics)
        finally:
            release.set()

    def test_startup_phase_seconds_records_first_occurrence_only(self) -> None:
        import time as _time

        server, _rpc = coordinator()
        self.assertEqual(server.startup_phase_seconds(), {})
        server._record_startup_phase_once("audit_listener_bound")
        self.assertEqual(server.startup_phase_seconds(), {})
        server._startup_phase_origin_monotonic = _time.monotonic() - 1.0
        server._record_startup_phase_once("audit_listener_bound")
        first = server.startup_phase_seconds()["audit_listener_bound"]
        self.assertGreaterEqual(first, 1.0)
        server._record_startup_phase_once("audit_listener_bound")
        self.assertEqual(
            server.startup_phase_seconds()["audit_listener_bound"], first
        )

    def test_job_issuance_continues_during_padded_landing(self) -> None:
        """Acceptance shape for issue #188: a landing padded past its normal
        duration must not stop child job issuance, and payout-state
        publication stays gated until the landing reaches a terminal state.
        The pad is logical (the barrier simply stays unresolved) so the test
        is instant while pinning the same ordering a 10s landing produces."""
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
        server._begin_accepted_block_payout_preview(parent_hash, block_height=10)
        server._mark_accepted_block_payout_landed(parent_hash, block_height=10)
        server._publish_accepted_block_payout_preview(parent_hash, preview)
        # The landing bookkeeping is still unresolved; every child build keeps
        # receiving the immutable preview instead of blocking or reading
        # confirmed balances.
        for _ in range(5):
            self.assertEqual(
                server._prior_balances_for_job_parent(parent_hash),
                preview,
            )
        ages = server.accepted_parent_unresolved_ages_seconds()
        self.assertEqual(len(ages), 1)
        with self.assertRaisesRegex(
            _PayoutStatePublicationBlocked, "still pending"
        ):
            with server._payout_balance_mutation():
                pass
        # Landing completes: the transition clears and confirmed reads resume.
        server._clear_accepted_block_payout_preview(parent_hash)
        self.assertEqual(
            server._prior_balances_for_job_parent(
                parent_hash, fallback_balances=[]
            ),
            [],
        )
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])

    def test_unmark_landed_clears_unresolved_age(self) -> None:
        server, _rpc = coordinator()
        block_hash = "cd" * 32
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)
        ages = server.accepted_parent_unresolved_ages_seconds()
        self.assertEqual(len(ages), 1)
        self.assertGreaterEqual(ages[0], 0.0)
        server._unmark_accepted_block_payout_landed(block_hash)
        self.assertEqual(server.accepted_parent_unresolved_ages_seconds(), [])
        server._clear_accepted_block_payout_preview(block_hash)

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

    def test_preview_publication_survives_window_build_timeout(self) -> None:
        """Issue #188 acceptance: when the payout window build dies with the
        slow ledger during found-block publication, the preview must still
        cross the atomic publication boundary. Admission then reopens and
        child jobs build against the verified compact preview instead of
        stalling behind the global publication fence until the ledger
        recovers."""
        accepted_hash = "c4" * 32
        server, rpc = coordinator(
            template=base_template(height=12, prevhash=accepted_hash)
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

        def timing_out_window_build(
            *_args: object, **_kwargs: object
        ) -> PayoutLedgerArtifact:
            raise LedgerOperationTimeout("postgres statement deadline expired")

        server._build_payout_ledger_artifact = (  # type: ignore[method-assign]
            timing_out_window_build
        )
        server._begin_accepted_block_payout_preview(accepted_hash, block_height=11)
        server._mark_accepted_block_payout_landed(accepted_hash, block_height=11)
        generation_before = server._payout_state_generation
        with patch("builtins.print"):
            published = server._publish_accepted_block_payout_preview(
                accepted_hash, preview
            )
        self.assertEqual(published, preview)
        # The publication crossed the atomic boundary: a generation was
        # installed and delivery admission reopened despite the dead build.
        self.assertFalse(server._payout_state_publication_fenced())
        self.assertEqual(server._payout_state_generation, generation_before + 1)
        transition = server._accepted_block_payout_previews[accepted_hash]
        self.assertEqual(transition.published_generation, generation_before + 1)
        # Child job construction is admitted and consumes the preview.
        bundle = server.shared_job_bundle(artifacts, mode="ready")
        self.assertIsNotNone(bundle)
        self.assertEqual(recorded["calls"], 1)
        last_kwargs = recorded["last_kwargs"]
        assert isinstance(last_kwargs, dict)
        self.assertEqual(list(last_kwargs["prior_balances"]), preview)
        server._clear_accepted_block_payout_preview(accepted_hash)

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

        with self.assertRaisesRegex(
            _PayoutStatePublicationBlocked,
            "confirmation is still pending",
        ):
            with server._payout_balance_mutation():
                self.fail("landed transition must bar payout reconciliation")
        with self.assertRaisesRegex(
            _PayoutStatePublicationBlocked,
            "confirmation is still pending",
        ):
            server.reconcile_prism_pool_blocks_once()

        server._clear_accepted_block_payout_preview(block_hash)
        with server._payout_balance_mutation():
            pass

    def test_landed_transition_poll_uses_coordination_blocked_budget(self) -> None:
        server, _rpc = coordinator()
        server.reorg_reconciler_enabled = True
        block_hash = "af" * 32
        server._begin_accepted_block_payout_preview(block_hash, block_height=10)
        server._mark_accepted_block_payout_landed(block_hash, block_height=10)

        with (
            patch(
                "lab.prism.prism_coordinator.time.monotonic",
                return_value=100.0,
            ),
            self.assertRaisesRegex(
                _PayoutStatePublicationBlocked,
                "confirmation is still pending",
            ),
        ):
            server.poll_qbit_tip_template_once()

        self.assertIsNone(
            getattr(server, "template_refresh_failure_started_monotonic", None)
        )
        self.assertEqual(
            server.coordination_blocked_streak_age_seconds(105.0),
            5.0,
        )

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
            server._note_reorg_reconcile_outcome(tip_hash, trusted=True)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(len(reconcile_calls), 1)

        rpc.tip = "33" * 32
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(len(reconcile_calls), 2)
        self.assertEqual(reconcile_calls[-1], "33" * 32)

    def test_untrusted_outcome_for_other_tip_keeps_memo_armed(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server._note_reorg_reconcile_outcome(rpc.tip, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            server._note_reorg_reconcile_outcome(tip_hash, trusted=True)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        # A superseded/untrusted pass for a different tip must not unarm the
        # cached trusted outcome for the current tip.
        server._note_reorg_reconcile_outcome("44" * 32, trusted=False)
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [])

        # An untrusted outcome for the current tip itself unarms only it.
        server._note_reorg_reconcile_outcome(rpc.tip, trusted=False)
        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [rpc.tip])

    def test_reconcile_error_outcome_clears_every_memo_entry(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server._note_reorg_reconcile_outcome(rpc.tip, trusted=True)
        server._note_reorg_reconcile_outcome("55" * 32, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            server._note_reorg_reconcile_outcome(tip_hash, trusted=True)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        server._note_reorg_reconcile_outcome(
            "55" * 32,
            trusted=False,
            clear_memo=True,
        )

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [rpc.tip])

    def test_tip_flip_back_within_ttl_requires_fresh_reconcile(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        tip_a = rpc.tip
        self.addCleanup(server.shutdown_tip_refresh_executor)
        server.observe_tip_for_refresh(tip_a)
        server._note_reorg_reconcile_outcome(tip_a, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [])

        # Detecting an intervening tip ends tip A's memo epoch: when the
        # chain flips back to A within the cache TTL, pool-block chain state
        # must be re-proven by a fresh pass, not assumed from the pre-flip
        # outcome.
        server.observe_tip_for_refresh("bb" * 32)

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [tip_a])

    def test_stale_pass_cannot_rearm_memo_after_newer_tip_detected(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        tip_a = rpc.tip
        self.addCleanup(server.shutdown_tip_refresh_executor)
        server.observe_tip_for_refresh(tip_a)
        server.observe_tip_for_refresh("bb" * 32)

        # A pass for tip A finishing after tip B was detected must not arm
        # the memo for A: its epoch already ended at the newer observation.
        server._note_reorg_reconcile_outcome(tip_a, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            return {"untrusted": False}

        server.reconcile_prism_pool_blocks_once = fake_reconcile  # type: ignore[method-assign]

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
        self.assertEqual(reconcile_calls, [tip_a])

    def test_reorg_cache_rechecks_chain_view_before_reuse(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        rpc.blockchain_info["headers"] = 101
        server._note_reorg_reconcile_outcome(rpc.tip, trusted=True)
        reconcile_calls: list[str | None] = []

        def fake_reconcile(*, tip_hash: str | None = None) -> dict[str, object]:
            reconcile_calls.append(tip_hash)
            server._note_reorg_reconcile_outcome(tip_hash, trusted=False)
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

    def test_sync_ledger_snapshot_publishes_reusable_artifact(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        # Pin distinct per-generation anchors near the live clock so the
        # armed window stays anchored inside the audit ceiling.
        first_anchor_ms = now_ms() - 20
        with server._job_cache_lock:
            server._job_build_issued_at_ms[first.generation] = first_anchor_ms

        inline = server.shared_job_bundle(first, mode="ready")

        self.assertEqual(inline.payout_artifact_generation, 0)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertGreater(artifact.generation, 0)
        self.assertEqual(
            artifact.payout_state_generation,
            server._payout_state_generation,
        )
        self.assertEqual(
            artifact.accepted_share_count,
            len(server.ledger.miners),
        )
        self.assertEqual(list(artifact.shares_json), inline.shares_json)
        self.assertEqual(artifact.snapshot_anchor_ms, inline.issued_at_ms)

        second = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert second is not None
        with server._job_cache_lock:
            server._job_build_issued_at_ms[second.generation] = (
                first_anchor_ms + 10
            )

        reused = server.shared_job_bundle(second, mode="ready")

        self.assertEqual(reused.payout_artifact_generation, artifact.generation)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        self.assertEqual(reused.shares_json, inline.shares_json)
        self.assertNotEqual(reused.issued_at_ms, inline.issued_at_ms)
        self.assertEqual(
            reused.found_block["anchor_job_issued_at_ms"],
            artifact.snapshot_anchor_ms,
        )

    def test_sync_artifact_publication_survives_share_commit_mid_read(
        self,
    ) -> None:
        class MidReadCommitLedger(FakeLedger):
            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[FakeShare]:
                result = super().snapshot_at_job_issue(
                    anchor_job_issued_at_ms,
                    window_weight=window_weight,
                )
                # A share becomes durable while the window read is in
                # flight. The read is scoped by the frozen anchor -- the
                # commit lands above it and belongs to the next window -- so
                # anchor-scoped publication proceeds.
                self.miners = [*self.miners, f"mid-read-{self.snapshot_calls}"]
                return result

        server, rpc = coordinator(ledger=MidReadCommitLedger())
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertFalse(bundle.collection_only)
        self.assertEqual(bundle.payout_artifact_generation, 0)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, bundle.issued_at_ms)
        self.assertEqual(list(artifact.shares_json), bundle.shares_json)

        # The armed window keeps serving under continuous commits: the live
        # durable count has moved past the artifact's, and reuse must not
        # care.
        second = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert second is not None
        reused = server.shared_job_bundle(second, mode="ready")
        self.assertEqual(reused.payout_artifact_generation, artifact.generation)
        self.assertEqual(server.ledger.snapshot_calls, 1)
        self.assertEqual(
            reused.found_block["anchor_job_issued_at_ms"],
            artifact.snapshot_anchor_ms,
        )

    def test_sync_publication_allowed_when_anchor_predates_durable_share(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        first = server.shared_job_bundle(artifacts, mode="ready")
        self.assertFalse(first.collection_only)
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)
            # A share becomes durable after this generation's anchor was
            # frozen. Force the synchronous path to run again for the same
            # frozen anchor.
            server._payout_ledger_artifact = None
            server._job_bundle_cache.clear()
        server.ledger.miners = [*server.ledger.miners, "post-anchor-share"]

        second = server.shared_job_bundle(artifacts, mode="ready")

        self.assertEqual(second.payout_artifact_generation, 0)
        # The window read at the frozen anchor deterministically excludes
        # the later durable share -- it belongs to the next window -- so the
        # anchor-scoped publication proceeds even though the live count has
        # moved past the frozen anchor.
        with server._job_cache_lock:
            republished = server._payout_ledger_artifact
        self.assertIsNotNone(republished)
        assert republished is not None
        self.assertEqual(republished.snapshot_anchor_ms, second.issued_at_ms)
        self.assertEqual(second.issued_at_ms, first.issued_at_ms)

    def test_rearmed_artifact_is_not_shadowed_by_stale_cached_bundle(
        self,
    ) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        stale = server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(len(stale.shares_json), 3)

        # A share lands and the debounced re-arm rebuilds a fresher window.
        server.ledger.miners = [*server.ledger.miners, "late-share"]
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            fresh_artifact = server._payout_ledger_artifact
        assert fresh_artifact is not None
        self.assertEqual(fresh_artifact.accepted_share_count, 4)

        rebuilt = server.shared_job_bundle(artifacts, mode="ready")

        # The pre-re-arm bundle is still inside its cache TTL under the
        # no-artifact key; its older window must not shadow the fresher
        # artifact.
        self.assertEqual(
            rebuilt.payout_artifact_generation,
            fresh_artifact.generation,
        )
        self.assertEqual(len(rebuilt.shares_json), 4)
        self.assertEqual(recorded["calls"], 2)

    def test_racing_commit_does_not_disable_sync_artifact_seeding(self) -> None:
        class RacingStatsLedger(FakeLedger):
            def __init__(self) -> None:
                super().__init__(
                    miners=["miner-a", "miner-b", "miner-c", "miner-d"]
                )
                self._pending_counts = iter([3, 4])

            def accepted_share_stats(self) -> dict[str, int]:
                self.stats_calls += 1
                try:
                    count = next(self._pending_counts)
                except StopIteration:
                    count = 4
                return {
                    "accepted_share_count": count,
                    "distinct_miner_count": 4,
                }

        server, rpc = coordinator(ledger=RacingStatsLedger())
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        # A commit races the informational count read. The window is scoped
        # by the frozen anchor regardless of which side of the read the
        # commit landed on, so seeding proceeds; the count is diagnostics,
        # not a fence.
        bundle = server.shared_job_bundle(artifacts, mode="ready")

        self.assertFalse(bundle.collection_only)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        self.assertIsNotNone(artifact)
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, bundle.issued_at_ms)
        self.assertEqual(list(artifact.shares_json), bundle.shares_json)

    def test_same_window_background_rebuild_keeps_artifact_generation(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None

        server.shared_job_bundle(artifacts, mode="ready")
        with server._job_cache_lock:
            installed = server._payout_ledger_artifact
        assert installed is not None

        # No share committed since: the speculative rebuild reads the same
        # window under a fresh anchor and must not spin the generation,
        # which would re-key bundle lookups for nothing -- but it is still a
        # successful preparation, so an accumulated re-arm backoff releases
        # and the fresher anchor advances the staleness clock in place.
        with server._payout_artifact_executor_lock:
            server._payout_artifact_rearm_backoff = 4
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            refreshed = server._payout_ledger_artifact
        assert refreshed is not None
        self.assertEqual(refreshed.generation, installed.generation)
        self.assertEqual(
            refreshed.share_snapshot_sha256,
            installed.share_snapshot_sha256,
        )
        assert installed.snapshot_anchor_ms is not None
        assert refreshed.snapshot_anchor_ms is not None
        self.assertGreaterEqual(
            int(refreshed.snapshot_anchor_ms),
            int(installed.snapshot_anchor_ms),
        )
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 1)

        # A new durable share makes the rebuild a genuine replacement.
        server.ledger.miners = [*server.ledger.miners, "late-share"]
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            replaced = server._payout_ledger_artifact
        assert replaced is not None
        self.assertGreater(replaced.generation, installed.generation)
        self.assertEqual(replaced.accepted_share_count, 4)

    def test_delayed_older_snapshot_does_not_regress_artifact(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.shared_job_bundle(artifacts, mode="ready")
        with server._job_cache_lock:
            current = server._payout_ledger_artifact
        assert current is not None
        assert current.snapshot_anchor_ms is not None

        # A snapshot taken at an earlier anchor finishes its window
        # conversion late, so its preparation timestamp is newer than the
        # installed artifact's; the anchor still proves it is the older
        # window.
        older_window = [
            {"share_seq": seq, "miner_id": "miner-a"} for seq in (1, 2)
        ]
        delayed = PayoutLedgerArtifact(
            generation=0,
            payout_state_generation=server._payout_state_generation,
            network_difficulty=int(current.network_difficulty),
            accepted_share_count=2,
            shares_json=tuple(older_window),
            prior_balances=(),
            prepared_monotonic=time.monotonic(),
            snapshot_anchor_ms=int(current.snapshot_anchor_ms) - 5,
            share_snapshot_sha256=canonical_json_sha256(older_window),
        )
        server._install_payout_ledger_artifact(delayed)
        with server._job_cache_lock:
            self.assertIs(server._payout_ledger_artifact, current)

        # A window snapshotted at a fresher anchor replaces regardless of
        # its preparation timestamp ordering.
        newer_window = [
            {"share_seq": seq, "miner_id": "miner-a"} for seq in (1, 2, 3, 4)
        ]
        fresher = dataclass_replace(
            delayed,
            accepted_share_count=4,
            shares_json=tuple(newer_window),
            prepared_monotonic=current.prepared_monotonic - 1.0,
            snapshot_anchor_ms=int(current.snapshot_anchor_ms) + 5,
            share_snapshot_sha256=canonical_json_sha256(newer_window),
        )
        server._install_payout_ledger_artifact(fresher)
        with server._job_cache_lock:
            replaced = server._payout_ledger_artifact
        assert replaced is not None
        self.assertGreater(replaced.generation, current.generation)
        self.assertEqual(replaced.accepted_share_count, 4)

    def test_difficulty_change_replaces_same_window_artifact(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.shared_job_bundle(artifacts, mode="ready")
        with server._job_cache_lock:
            installed = server._payout_ledger_artifact
        assert installed is not None

        # A retarget moves the live template to a new difficulty while the
        # share window stays identical. The rebuild at the live difficulty
        # must replace the artifact: keeping the old-difficulty artifact
        # would fail the reuse fence on every build while the same-window
        # skip kept discarding its replacement.
        retarget_difficulty = int(installed.network_difficulty) + 1
        with server._job_cache_lock:
            live = server._template_artifacts
            assert live is not None
            server._template_artifacts = dataclass_replace(
                live,
                network_difficulty=retarget_difficulty,
            )
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            retarget_difficulty,
        )
        with server._job_cache_lock:
            replaced = server._payout_ledger_artifact
        assert replaced is not None
        self.assertGreater(replaced.generation, installed.generation)
        self.assertEqual(replaced.network_difficulty, retarget_difficulty)

        # A pre-retarget build delayed after its snapshot carries the same
        # window at the old difficulty; whether its anchor trails or ties,
        # the live-difficulty artifact must stay.
        delayed = dataclass_replace(
            installed,
            generation=0,
            prepared_monotonic=time.monotonic(),
        )
        server._install_payout_ledger_artifact(delayed)
        with server._job_cache_lock:
            self.assertIs(server._payout_ledger_artifact, replaced)

        # Even a delayed pre-retarget build that clamped a fresher anchor
        # must not displace it: a wrong-difficulty install would fail every
        # reuse probe on the difficulty check, which never re-arms.
        assert replaced.snapshot_anchor_ms is not None
        leading = dataclass_replace(
            installed,
            generation=0,
            prepared_monotonic=time.monotonic(),
            snapshot_anchor_ms=int(replaced.snapshot_anchor_ms) + 5,
        )
        server._install_payout_ledger_artifact(leading)
        with server._job_cache_lock:
            self.assertIs(server._payout_ledger_artifact, replaced)

    def test_fence_failure_rearms_artifact_preparation_with_debounce(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.payout_artifact_rearm_min_seconds = 5.0
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )

        class RecordingExecutor:
            def __init__(self) -> None:
                self.submissions = 0

            def submit(self, _fn: object) -> Future[None]:
                # Recorded but never run: the request slot stays observable.
                self.submissions += 1
                return Future()

        executor = RecordingExecutor()
        with server._payout_artifact_executor_lock:
            server._payout_artifact_executor = executor  # type: ignore[assignment]

        usable = server._usable_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        self.assertIsNotNone(usable)
        with server._payout_artifact_executor_lock:
            self.assertIsNone(server._payout_artifact_requested)

        # Age the armed window past the re-anchor floor: the probe keeps
        # serving the artifact while the debounced background re-anchor is
        # scheduled.
        server.payout_artifact_reanchor_seconds = 10.0
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 11_000,
            )

        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        with server._payout_artifact_executor_lock:
            self.assertEqual(
                server._payout_artifact_requested,
                (
                    server._payout_state_generation,
                    int(artifacts.network_difficulty),
                ),
            )
            self.assertEqual(executor.submissions, 1)
            # Every past-floor probe counts, even when the rearm itself is
            # debounced -- the canary needs "serving past the floor" to be
            # visible independently of rearm_scheduled.
            self.assertEqual(
                server.payout_artifact_event_counts["probe_past_floor"],
                1,
            )
            # The one-slot worker dequeues the request; a served probe inside
            # the interval must not slip a duplicate into the emptied slot.
            server._payout_artifact_requested = None

        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        with server._payout_artifact_executor_lock:
            self.assertIsNone(server._payout_artifact_requested)

        # Once the interval has elapsed the next served probe re-arms.
        with server._payout_artifact_executor_lock:
            server._payout_artifact_last_schedule_monotonic = (
                time.monotonic() - 6.0
            )
        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        with server._payout_artifact_executor_lock:
            self.assertEqual(
                server._payout_artifact_requested,
                (
                    server._payout_state_generation,
                    int(artifacts.network_difficulty),
                ),
            )

    def test_aborted_speculative_rebuilds_back_off_and_reset(self) -> None:
        ledger = AnchorRecordingLedger()
        server, rpc = coordinator(ledger=ledger)
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server.payout_artifact_rearm_min_seconds = 5.0

        # A pending-commit floor older than the audit anchor ceiling aborts
        # the rebuild before the window walk (the artifact would arm born
        # expired); each abort doubles the re-arm interval instead of
        # retrying the reward-window walk at the floor forever.
        share = stamped_pending_share(now_ms() - 400_000)
        server._ensure_pending_share_commit_state()
        with server._pending_share_commit_lock:
            server._pending_share_commit_floor[id(share)] = [
                share,
                time.monotonic(),
                False,
            ]
        server._prepare_payout_ledger_artifact(0, artifacts.network_difficulty)
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 2)
        server._prepare_payout_ledger_artifact(0, artifacts.network_difficulty)
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 4)
        self.assertEqual(ledger.snapshot_calls, 0)

        # Elapsed time beyond the floor but inside the scaled interval must
        # not re-arm.
        with server._payout_artifact_executor_lock:
            server._payout_artifact_last_schedule_monotonic = (
                time.monotonic() - 10.0
            )
        server._rearm_payout_ledger_artifact_after_fence_failure(
            0,
            artifacts.network_difficulty,
        )
        with server._payout_artifact_executor_lock:
            self.assertIsNone(server._payout_artifact_requested)

        # A rebuild that finally arms resets the backoff to the floor.
        server._finish_pending_share_commit(share)
        server._prepare_payout_ledger_artifact(0, artifacts.network_difficulty)
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 1)

    def test_payout_publication_resets_rearm_backoff(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        with server._payout_artifact_executor_lock:
            server._payout_artifact_rearm_backoff = 8

        # The accepted-block preview publishes a new payout generation whose
        # candidate artifact installs through the atomic publication pointer
        # swap; the accumulated backoff must release with it.
        parent_hash = str(rpc.template["previousblockhash"])
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        server._publish_accepted_block_payout_preview(
            parent_hash,
            [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 25,
                }
            ],
        )

        self.assertGreater(server._payout_state_generation, 0)
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 1)

    def test_landed_preview_suppresses_fence_failure_rearm(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        scheduled: list[tuple[int, int]] = []
        server._schedule_payout_ledger_artifact_preparation = (  # type: ignore[method-assign]
            lambda generation, difficulty, **_kwargs: scheduled.append(
                (generation, difficulty)
            )
        )
        accepted_hash = "d0" * 32
        server._begin_accepted_block_payout_preview(
            accepted_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        server._mark_accepted_block_payout_landed(
            accepted_hash,
            block_height=int(rpc.template["height"]) - 1,
        )

        # Age the armed window past the re-anchor floor: the probe keeps
        # serving while attempting the background re-anchor.
        server.payout_artifact_reanchor_seconds = 10.0
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 11_000,
            )

        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        # A speculative rebuild would read database balances the landed
        # prospective state supersedes; preparation resumes only through the
        # durable-confirmation call site.
        self.assertEqual(scheduled, [])

    def test_artifact_reuse_serves_past_install_age_to_anchor_ceiling(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
        assert armed is not None
        assert armed.snapshot_anchor_ms is not None
        # The balances digest is memoized at construction so the serving
        # probe and the reused-build fences never re-canonicalize the
        # tuple on the request path.
        self.assertEqual(
            armed.prior_balances_sha256,
            canonical_json_sha256(armed.prior_balances),
        )

        # A window whose anchor is past the re-anchor floor still serves --
        # post-anchor shares belong to the next window by construction, and
        # validity is carried by the generation/balances fences, never by
        # wall-clock age below the ceiling. (The re-anchor and
        # anchor-ceiling env knobs wire these same attributes in __init__.)
        server.payout_artifact_reanchor_seconds = 10.0
        server.ledger.miners = [*server.ledger.miners, "late-1", "late-2"]
        with server._job_cache_lock:
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 15_000,
            )
        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
                rearm_on_fence_failure=False,
            )
        )

        # 2026-07-30 brownout regression: install age must never retire the
        # window. A 10s install budget at the production build cadence (a
        # shared build every 10-14s, a 4-8s walk) expired between
        # consecutive builds, sending every build back through the
        # synchronous reward-window walk. Only payout events and the audit
        # ceiling end reuse.
        with server._job_cache_lock:
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                prepared_monotonic=float(armed.prepared_monotonic) - 3600.0,
            )
        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
                rearm_on_fence_failure=False,
            )
        )

        # The wall-clock anchor ceiling stays as the audit-facing backstop.
        server.payout_artifact_max_anchor_age_seconds = 5.0
        with server._job_cache_lock:
            server._payout_ledger_artifact = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 6_000,
            )
        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
                rearm_on_fence_failure=False,
            )
        )

    def test_reuse_kill_switch_disables_probe_and_background_walks(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            assert server._payout_ledger_artifact is not None

        # Off: the probe refuses a perfectly valid armed artifact and the
        # scheduler runs no background reward-window walks -- the disabled
        # state restores pre-reuse delivery economics (the synchronous
        # path keeps the re-landed anchor-selection semantics), so a
        # delivery regression is an env flip instead of a rollback.
        server.payout_artifact_reuse_enabled = False
        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )
        server._schedule_payout_ledger_artifact_preparation(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._payout_artifact_executor_lock:
            self.assertIsNone(server._payout_artifact_requested)
            self.assertIsNone(server._payout_artifact_future)

        # Back on: the armed artifact serves again without a rebuild.
        server.payout_artifact_reuse_enabled = True
        self.assertIsNotNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )

        # Off also gates arming itself: a prepared window is discarded at
        # install, so nothing churns install events or re-keys the idle
        # bundle fast path to an unusable generation while disabled.
        server.payout_artifact_reuse_enabled = False
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)

    def test_anchor_ceiling_must_clear_reanchor_floor(self) -> None:
        # One env var used to be able to re-create the 2026-07-30 incident:
        # a ceiling the background re-anchor cannot beat (floor + debounce +
        # the window walk) rejects every probe while the rebuild runs
        # debounced in parallel -- walk-per-build. __init__ refuses the
        # configuration at startup through this validation.
        with self.assertRaises(SystemExit):
            prism_coordinator_module.validate_payout_artifact_age_bounds(
                60.0,
                60.0,
            )
        with self.assertRaises(SystemExit):
            prism_coordinator_module.validate_payout_artifact_age_bounds(
                120.0,
                60.0,
            )
        # The same margin applies to a custom build cadence; validating only
        # the re-anchor floor allowed 299s builds under a 300s ceiling.
        with self.assertRaises(SystemExit):
            prism_coordinator_module.validate_payout_artifact_age_bounds(
                300.0,
                299.0,
            )
        # The default split (300s ceiling, 60s floor) passes.
        prism_coordinator_module.validate_payout_artifact_age_bounds(
            300.0,
            60.0,
        )

    def test_in_flight_build_keeps_aged_artifact_it_already_selected(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        server.payout_artifact_max_anchor_age_seconds = 5.0
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            aged = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 6_000,
            )
            server._payout_ledger_artifact = aged

        # New reuse decisions reject a window past the audit ceiling...
        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
                rearm_on_fence_failure=False,
            )
        )
        # ...but a build that selected it while fresh completes with it: the
        # in-build re-validation checks supersession and the balances fence
        # only, so queue delay past the bound cannot scrap the reuse into
        # the full snapshot it was armed to avoid.
        server.ledger.snapshot_calls = 0
        bundle = server.build_shared_job_bundle(
            artifacts,
            worker(),
            payout_artifact=aged,
        )
        self.assertEqual(server.ledger.snapshot_calls, 0)
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"],
            aged.snapshot_anchor_ms,
        )

    def test_in_flight_build_survives_same_window_anchor_refresh(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            # An in-flight build selected the armed artifact while it
            # carried an older anchor.
            selected = dataclass_replace(
                armed,
                snapshot_anchor_ms=int(armed.snapshot_anchor_ms) - 5,
            )
            server._payout_ledger_artifact = selected

        # A same-window rebuild refreshes the anchor in place: the stored
        # instance swaps while the generation -- the re-key authority --
        # and the window bytes stay identical.
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            refreshed = server._payout_ledger_artifact
        assert refreshed is not None
        self.assertIsNot(refreshed, selected)
        self.assertEqual(refreshed.generation, selected.generation)
        self.assertEqual(
            refreshed.share_snapshot_sha256,
            selected.share_snapshot_sha256,
        )

        # The build holding the pre-refresh instance completes its reuse
        # instead of being scrapped into a full snapshot by the swap.
        server.ledger.snapshot_calls = 0
        bundle = server.build_shared_job_bundle(
            artifacts,
            worker(),
            payout_artifact=selected,
        )
        self.assertEqual(server.ledger.snapshot_calls, 0)
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"],
            selected.snapshot_anchor_ms,
        )

    def test_generation_current_cached_bundle_serves_past_reanchor_floor(
        self,
    ) -> None:
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        with server._payout_artifact_executor_lock:
            server._payout_artifact_executor_shutdown = True
        # Re-anchor floor below the bundle-cache TTL so the two gates are
        # distinguishable inside the TTL window.
        server.payout_artifact_reanchor_seconds = 5.0

        server.shared_job_bundle(first, mode="ready")
        self.assertEqual(recorded["calls"], 1)
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
        assert armed is not None

        # A bundle keyed to the currently armed artifact generation carries
        # exactly the armed window: it keeps serving past the re-anchor
        # floor (its window is as fresh as reuse itself), bounded upstream
        # by the cache TTL.
        second = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert second is not None
        reused = server.shared_job_bundle(second, mode="ready")
        self.assertEqual(reused.payout_artifact_generation, armed.generation)
        self.assertEqual(recorded["calls"], 2)
        with server._job_cache_lock:
            for key, entry in list(server._job_bundle_cache.items()):
                if entry.payout_artifact_generation == armed.generation:
                    server._job_bundle_cache[key] = dataclass_replace(
                        entry,
                        built_monotonic=float(entry.built_monotonic) - 7.0,
                    )
        server.shared_job_bundle(second, mode="ready")
        self.assertEqual(recorded["calls"], 2)

    def test_cached_no_artifact_bundle_gates_on_build_age_not_anchor(
        self,
    ) -> None:
        # 2026-07-29 regression companion: the gate must follow build age
        # (freshness of the work) inside the audit ceiling, never the
        # wall-clock anchor, which is frozen per template generation.
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        with server._payout_artifact_executor_lock:
            server._payout_artifact_executor_shutdown = True
        server.payout_artifact_reanchor_seconds = 5.0

        seeded = server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)

        # Fresh build age serves from cache once the armed artifact is
        # gone.
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
        server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)

        # Build age past the re-anchor floor rebuilds even though the
        # bundle-cache TTL has not lapsed.
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
            for key, entry in list(server._job_bundle_cache.items()):
                server._job_bundle_cache[key] = dataclass_replace(
                    entry,
                    built_monotonic=float(entry.built_monotonic) - 7.0,
                )
        server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 2)

        # The audit ceiling on the declared anchor stays as the backstop
        # even for a freshly built entry.
        aged_anchor_ms = (
            int(seeded.found_block["anchor_job_issued_at_ms"]) - 400_000
        )
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
            for key, entry in list(server._job_bundle_cache.items()):
                aged_found_block = dict(entry.found_block)
                aged_found_block["anchor_job_issued_at_ms"] = aged_anchor_ms
                server._job_bundle_cache[key] = dataclass_replace(
                    entry,
                    found_block=aged_found_block,
                )
        server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 3)

    def test_slow_window_walk_still_arms_reusable_artifact(self) -> None:
        # 2026-07-29 regression: the reward-window walk takes multiple
        # seconds at production volume, and freshness measured from the
        # wall-clock anchor rejected every slow build on arrival. The walk
        # here advances the wall clock past the freshness budget; the
        # artifact must still arm, serve reuse, and reset the re-arm
        # backoff.
        clock = [now_ms()]

        class SlowWalkLedger(FakeLedger):
            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[FakeShare]:
                result = super().snapshot_at_job_issue(
                    anchor_job_issued_at_ms,
                    window_weight=window_weight,
                )
                clock[0] += 15_000
                return result

        server, rpc = coordinator(ledger=SlowWalkLedger())
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        with server._payout_artifact_executor_lock:
            server._payout_artifact_rearm_backoff = 4
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock[0],
        ):
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._job_cache_lock:
                armed = server._payout_ledger_artifact
            self.assertIsNotNone(armed)
            assert armed is not None
            assert armed.snapshot_anchor_ms is not None
            self.assertGreater(
                clock[0] - int(armed.snapshot_anchor_ms),
                10_000,
            )
            self.assertIs(
                server._usable_payout_ledger_artifact(
                    server._payout_state_generation,
                    artifacts.network_difficulty,
                ),
                armed,
            )
        with server._payout_artifact_executor_lock:
            self.assertEqual(server._payout_artifact_rearm_backoff, 1)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="installed"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="built"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="incremental"}',
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="self_check_failed"}',
            metrics,
        )
        self.assertIn(
            'qbit_prism_payout_artifact_events_total{event="found_block_cached"}',
            metrics,
        )

    def test_born_expired_install_paces_backoff_instead_of_resetting(
        self,
    ) -> None:
        # A walk that outlives the audit ceiling arms nothing. Crediting it
        # as a success reset the re-arm backoff to the floor and re-walked
        # the reward window continuously -- the 2026-07-29 livelock. It
        # must pace exactly like a failed preparation.
        clock = [now_ms()]

        class GlacialWalkLedger(FakeLedger):
            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[FakeShare]:
                result = super().snapshot_at_job_issue(
                    anchor_job_issued_at_ms,
                    window_weight=window_weight,
                )
                clock[0] += 400_000
                return result

        server, rpc = coordinator(ledger=GlacialWalkLedger())
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock[0],
        ):
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._job_cache_lock:
                self.assertIsNone(server._payout_ledger_artifact)
            with server._payout_artifact_executor_lock:
                self.assertEqual(server._payout_artifact_rearm_backoff, 2)
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._payout_artifact_executor_lock:
                self.assertEqual(server._payout_artifact_rearm_backoff, 4)
        with server._payout_artifact_executor_lock:
            self.assertEqual(
                server.payout_artifact_event_counts["born_expired"],
                2,
            )

    def test_equal_anchor_reprove_credits_freshness(self) -> None:
        # A pending-commit floor pins the snapshot anchor, so a fence-failure
        # rebuild re-proves the same window at the SAME anchor. That re-prove
        # must advance the freshness clock: no fresher window is
        # constructible while the floor holds, and rejecting the credit
        # would re-walk the reward window (with backoff reset each round)
        # until the audit ceiling -- the livelock shape this re-land
        # removes.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        share = stamped_pending_share(now_ms() - 5)
        server._ensure_pending_share_commit_state()
        with server._pending_share_commit_lock:
            server._pending_share_commit_floor[id(share)] = [
                share,
                time.monotonic(),
                False,
            ]
        try:
            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._job_cache_lock:
                armed = server._payout_ledger_artifact
                assert armed is not None
                server._payout_ledger_artifact = dataclass_replace(
                    armed,
                    prepared_monotonic=float(armed.prepared_monotonic) - 11.0,
                )
            # Install age no longer gates the probe (2026-07-30 brownout
            # regression); the armed window keeps serving while pinned.
            self.assertIsNotNone(
                server._usable_payout_ledger_artifact(
                    server._payout_state_generation,
                    artifacts.network_difficulty,
                    rearm_on_fence_failure=False,
                )
            )

            server._prepare_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
            with server._job_cache_lock:
                reproved = server._payout_ledger_artifact
            assert reproved is not None
            self.assertEqual(reproved.generation, armed.generation)
            self.assertEqual(
                reproved.snapshot_anchor_ms,
                armed.snapshot_anchor_ms,
            )
            self.assertIsNotNone(
                server._usable_payout_ledger_artifact(
                    server._payout_state_generation,
                    artifacts.network_difficulty,
                )
            )
        finally:
            server._finish_pending_share_commit(share)

    def test_publication_restamps_candidate_artifact_freshness(self) -> None:
        # A payout-state candidate builds its artifact before the atomic
        # publication, and the delivery-gate drain between the two can
        # outlive the reuse budget. The install must restamp freshness so a
        # freshly published generation never arms an already-stale artifact.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        real_build = server._build_payout_ledger_artifact

        def slow_publication_build(*args: object) -> object:
            built = real_build(*args)
            if built is None:
                return None
            return dataclass_replace(
                built,
                prepared_monotonic=time.monotonic() - 11.0,
            )

        server._build_payout_ledger_artifact = slow_publication_build  # type: ignore[method-assign]
        parent_hash = str(rpc.template["previousblockhash"])
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        server._publish_accepted_block_payout_preview(
            parent_hash,
            [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 25,
                }
            ],
        )

        self.assertGreater(server._payout_state_generation, 0)
        usable = server._usable_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        self.assertIsNotNone(usable)

    def test_usable_probe_survives_equal_window_freshness_restamp(
        self,
    ) -> None:
        # An equal-window re-prove restamps the armed artifact by replacing
        # the object while a reuse probe is hashing balances outside the
        # cache lock. The probe must treat the restamped copy as the same
        # armed window instead of failing closed into a synchronous
        # reward-window walk: under a pinned pending-commit floor the
        # restamp IS the intentional recovery path, so the race is routine.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            armed = server._payout_ledger_artifact
            assert armed is not None
            # Strip the memoized balances digest: the race window this test
            # drives opens while the probe hashes balances outside the cache
            # lock, which only happens on the fallback path for artifacts
            # without a memo (legacy or test-constructed). The restamp
            # admission below must keep protecting that path.
            armed = dataclass_replace(armed, prior_balances_sha256=None)
            server._payout_ledger_artifact = armed

        real_sha256 = prism_coordinator_module.canonical_json_sha256
        restamped_once = [False]

        def restamp_during_hash(value: object) -> str:
            if not restamped_once[0]:
                restamped_once[0] = True
                self.assertTrue(
                    server._install_payout_ledger_artifact(
                        dataclass_replace(
                            armed,
                            generation=0,
                            prepared_monotonic=time.monotonic(),
                        )
                    )
                )
            return real_sha256(value)

        with patch.object(
            prism_coordinator_module,
            "canonical_json_sha256",
            restamp_during_hash,
        ):
            usable = server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        self.assertTrue(restamped_once[0])
        with server._job_cache_lock:
            restamped = server._payout_ledger_artifact
        assert restamped is not None
        self.assertIsNot(restamped, armed)
        self.assertIsNotNone(usable)
        self.assertIs(usable, restamped)
        self.assertEqual(restamped.generation, armed.generation)

    def test_publication_discards_born_expired_candidate_artifact(
        self,
    ) -> None:
        # Candidate construction plus the delivery-gate drain can push the
        # candidate artifact's declared anchor past the audit ceiling. The
        # atomic publication must apply the same born-expired admission rule
        # as _install_payout_ledger_artifact: arming the artifact would fail
        # every reuse probe on anchor age -- with the re-arm suppressed
        # while the accepted preview awaits durability -- instead of letting
        # the post-publication probe schedule recovery.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        real_build = server._build_payout_ledger_artifact
        ceiling_ms = server._payout_artifact_max_anchor_age_ms()

        def expired_anchor_build(*args: object) -> object:
            built = real_build(*args)
            if built is None:
                return None
            assert built.snapshot_anchor_ms is not None
            return dataclass_replace(
                built,
                snapshot_anchor_ms=int(built.snapshot_anchor_ms)
                - int(ceiling_ms)
                - 1_000,
            )

        server._build_payout_ledger_artifact = expired_anchor_build  # type: ignore[method-assign]
        parent_hash = str(rpc.template["previousblockhash"])
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        generation_before = server._payout_state_generation
        server._publish_accepted_block_payout_preview(
            parent_hash,
            [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 25,
                }
            ],
        )
        self.assertGreater(server._payout_state_generation, generation_before)
        with server._job_cache_lock:
            self.assertIsNone(server._payout_ledger_artifact)
        with server._payout_artifact_executor_lock:
            born_expired = int(
                server.payout_artifact_event_counts.get("born_expired", 0)
            )
        self.assertGreaterEqual(born_expired, 1)
        self.assertIsNone(
            server._usable_payout_ledger_artifact(
                server._payout_state_generation,
                artifacts.network_difficulty,
            )
        )

    def test_publication_records_installed_event(self) -> None:
        # The atomic publication arms the candidate artifact through its own
        # pointer swap, not _install_payout_ledger_artifact. It must still
        # count in the installed lifecycle event family, or every
        # publication-path install is invisible to the observability the
        # event counter exists for.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._pool_ready_latched = True
        with server._payout_artifact_executor_lock:
            installed_before = int(
                server.payout_artifact_event_counts.get("installed", 0)
            )
        parent_hash = str(rpc.template["previousblockhash"])
        server._begin_accepted_block_payout_preview(
            parent_hash,
            block_height=int(rpc.template["height"]) - 1,
        )
        server._publish_accepted_block_payout_preview(
            parent_hash,
            [
                {
                    "recipient_id": "miner-a",
                    "order_key": "miner-a",
                    "p2mr_program_hex": "11" * 32,
                    "balance_sats": 25,
                }
            ],
        )
        with server._job_cache_lock:
            self.assertIsNotNone(server._payout_ledger_artifact)
        with server._payout_artifact_executor_lock:
            installed_after = int(
                server.payout_artifact_event_counts.get("installed", 0)
            )
        self.assertEqual(installed_after, installed_before + 1)

    def test_generation_bump_does_not_scrap_in_flight_build(self) -> None:
        # 2026-07-29 regression: every differing window bumps the armed
        # artifact generation, and the in-build re-validation scrapped any
        # build whose selected artifact lost the slot -- a rebuild storm at
        # production share rates. A build must finish on the copy it
        # selected: its window stays audit-reproducible at its declared
        # anchor while the payout generation and published balances hold.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        server._prepare_payout_ledger_artifact(
            server._payout_state_generation,
            artifacts.network_difficulty,
        )
        with server._job_cache_lock:
            selected = server._payout_ledger_artifact
        assert selected is not None
        assert selected.snapshot_anchor_ms is not None

        fresher_window = [
            {"share_seq": seq, "miner_id": "miner-a"} for seq in (1, 2, 3, 4)
        ]
        fresher = dataclass_replace(
            selected,
            generation=0,
            accepted_share_count=4,
            shares_json=tuple(fresher_window),
            snapshot_anchor_ms=int(selected.snapshot_anchor_ms) + 5,
            share_snapshot_sha256=canonical_json_sha256(fresher_window),
        )
        self.assertTrue(server._install_payout_ledger_artifact(fresher))
        with server._job_cache_lock:
            bumped = server._payout_ledger_artifact
        assert bumped is not None
        self.assertGreater(bumped.generation, selected.generation)

        server.ledger.snapshot_calls = 0
        bundle = server.build_shared_job_bundle(
            artifacts,
            worker(),
            payout_artifact=selected,
        )
        self.assertEqual(server.ledger.snapshot_calls, 0)
        self.assertEqual(
            bundle.found_block["anchor_job_issued_at_ms"],
            selected.snapshot_anchor_ms,
        )
        self.assertEqual(bundle.shares_json, list(selected.shares_json))

    def test_rebuilt_bundle_serves_under_aged_template_generation(
        self,
    ) -> None:
        # 2026-07-29 regression: issued_at_ms is frozen per template
        # generation and predates the walk, so a wall-clock anchor gate on
        # cached bundles declared every rebuilt bundle dead on arrival once
        # its template generation outlived the bound. Below the audit
        # ceiling, cache service must follow build age, not anchor age.
        server, rpc = coordinator()
        recorded = install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        with server._payout_artifact_executor_lock:
            server._payout_artifact_executor_shutdown = True
        with server._job_cache_lock:
            server._job_build_issued_at_ms[artifacts.generation] = (
                now_ms() - 20_000
            )

        built = server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(
            built.found_block["anchor_job_issued_at_ms"],
            built.issued_at_ms,
        )

        served = server.shared_job_bundle(artifacts, mode="ready")
        self.assertEqual(recorded["calls"], 1)
        self.assertEqual(served.issued_at_ms, built.issued_at_ms)

    def test_admission_deadline_fails_fast_and_visibly(self) -> None:
        server, _rpc = coordinator()
        server.routine_admission_deadline_seconds = 0.05
        server._publication_priority_scheduled_locked = (  # type: ignore[method-assign]
            lambda: True
        )

        with self.assertRaises(JobBuildAdmissionDeadlineExceeded) as raised:
            server._begin_routine_job_build_preparation(
                request_source="initial",
                cancelled=None,
            )

        # The deadline is coordination churn, not a failure budget event:
        # callers retry with their normal pacing.
        self.assertIsInstance(raised.exception, TemplateRefreshBlocked)
        with server._job_build_scheduler_lock:
            self.assertEqual(
                server.initial_job_prepared_work_counts["admission_deadline"],
                1,
            )

    def test_reused_anchor_bundle_preview_matches_fresh_build_through_guard(
        self,
    ) -> None:
        # The #50 payout-preview guard compares the preview computed from
        # the issued job against one recomputed at landing. A reused-anchor
        # bundle must therefore reproduce, byte for byte, what a fresh
        # ledger read at the same anchor produces -- the audit
        # reproducibility contract the artifact's snapshot_anchor_ms
        # declaration documents.
        programs = {
            "miner-a": "aa" * 32,
            "miner-b": "bb" * 32,
            "miner-c": "cc" * 32,
            "miner-d": "dd" * 32,
        }

        class AnchorScopedLedger(FakeLedger):
            """Window reads respect the anchor like the real ledger."""

            def __init__(self) -> None:
                super().__init__(miners=[])
                self.stamped: list[tuple[int, FakeShare]] = []

            def add_share(self, miner_id: str, accepted_at_ms: int) -> None:
                self.stamped.append(
                    (
                        int(accepted_at_ms),
                        FakeShare(
                            miner_id=miner_id,
                            share_seq=len(self.stamped) + 1,
                        ),
                    )
                )

            def accepted_share_stats(self) -> dict[str, int]:
                self.stats_calls += 1
                return {
                    "accepted_share_count": len(self.stamped),
                    "distinct_miner_count": len(
                        {share.miner_id for _, share in self.stamped}
                    ),
                }

            def snapshot_at_job_issue(
                self,
                anchor_job_issued_at_ms: int,
                *,
                window_weight: int | None = None,
            ) -> list[FakeShare]:
                self.snapshot_calls += 1
                return [
                    share
                    for stamp, share in self.stamped
                    if stamp <= int(anchor_job_issued_at_ms)
                ]

        anchor_ms = now_ms() - 50
        ledger = AnchorScopedLedger()
        ledger.add_share("miner-a", anchor_ms - 30)
        ledger.add_share("miner-b", anchor_ms - 20)
        ledger.add_share("miner-c", anchor_ms - 10)
        server, rpc = coordinator(ledger=ledger)

        def preview_bundle_builder(**kwargs: object) -> dict[str, object]:
            suffix_hex = str(kwargs["coinbase_script_sig_suffix_hex"])
            weights: dict[str, int] = {}
            for share in kwargs["shares"]:  # type: ignore[union-attr]
                miner = str(share["miner_id"])  # type: ignore[index]
                weights[miner] = weights.get(miner, 0) + 1
            return {
                "found_block": dict(kwargs["found_block"]),  # type: ignore[call-overload]
                "payout_policy_manifest": {
                    "accounts": [
                        {
                            "account_type": "miner",
                            "recipient_id": miner,
                            "order_key": miner,
                            "p2mr_program_hex": programs[miner],
                            "carry_forward_balance_sats": 1_000 * weight,
                        }
                        for miner, weight in sorted(weights.items())
                    ]
                },
                "signed_coinbase_manifest": {
                    "manifest": {
                        "coinbase_tx_hex": synthetic_manifest_coinbase_hex(
                            suffix_hex
                        ),
                    }
                },
            }

        server.build_audit_bundle = preview_bundle_builder  # type: ignore[method-assign]

        first = server.store_template_artifacts(dict(rpc.template))
        assert first is not None
        with server._job_cache_lock:
            server._job_build_issued_at_ms[first.generation] = anchor_ms

        seeded_bundle = server.shared_job_bundle(first, mode="ready")
        self.assertEqual(seeded_bundle.payout_artifact_generation, 0)
        with server._job_cache_lock:
            artifact = server._payout_ledger_artifact
        assert artifact is not None
        self.assertEqual(artifact.snapshot_anchor_ms, anchor_ms)

        # Shares keep landing after the anchor; the durable count moves past
        # the artifact's, which must not matter for reuse.
        ledger.add_share("miner-a", anchor_ms + 20)
        ledger.add_share("miner-d", anchor_ms + 25)

        second = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert second is not None
        reused = server.shared_job_bundle(second, mode="ready")
        self.assertEqual(reused.payout_artifact_generation, artifact.generation)
        self.assertEqual(
            reused.found_block["anchor_job_issued_at_ms"],
            anchor_ms,
        )

        # Control: a fresh synchronous build pinned to the same anchor pays
        # its own window read and must reproduce the reused output exactly,
        # excluding the post-anchor shares deterministically.
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
            server._job_bundle_cache.clear()
        third = server.store_template_artifacts(
            base_template(height=12, prevhash="33" * 32)
        )
        assert third is not None
        with server._job_cache_lock:
            server._job_build_issued_at_ms[third.generation] = anchor_ms
        snapshot_calls_before = ledger.snapshot_calls
        fresh = server.shared_job_bundle(third, mode="ready")
        self.assertEqual(ledger.snapshot_calls, snapshot_calls_before + 1)
        self.assertEqual(fresh.payout_artifact_generation, 0)

        self.assertEqual(reused.shares_json, fresh.shares_json)
        self.assertEqual(
            fresh.found_block["anchor_job_issued_at_ms"],
            anchor_ms,
        )
        assert reused.prospective_prior_balances is not None
        assert fresh.prospective_prior_balances is not None
        self.assertEqual(
            canonical_json_text(list(reused.prospective_prior_balances)),
            canonical_json_text(list(fresh.prospective_prior_balances)),
        )

        # A window at a fresher anchor covers the post-anchor shares and
        # produces a different preview; built now to prove the guard
        # equality below is load-bearing.
        with server._job_cache_lock:
            server._payout_ledger_artifact = None
            server._job_bundle_cache.clear()
        fourth = server.store_template_artifacts(
            base_template(height=13, prevhash="44" * 32)
        )
        assert fourth is not None
        with server._job_cache_lock:
            server._job_build_issued_at_ms[fourth.generation] = anchor_ms + 30
        divergent = server.shared_job_bundle(fourth, mode="ready")
        assert divergent.prospective_prior_balances is not None
        self.assertNotEqual(
            canonical_json_text(list(divergent.prospective_prior_balances)),
            canonical_json_text(list(reused.prospective_prior_balances)),
        )

        # The #50 guard sequence at landing: the issued preview (from the
        # reused-anchor job) publishes first; the verified preview
        # recomputed at the same anchor must then publish idempotently.
        block_hash = "d1" * 32
        server._begin_accepted_block_payout_preview(
            block_hash,
            block_height=int(rpc.template["height"]),
        )
        issued = server._materialize_prior_balance_preview(
            reused.prospective_prior_balances
        )
        server._publish_accepted_block_payout_preview(block_hash, issued)
        generation_after_issued = server._payout_state_generation
        verified = server._materialize_prior_balance_preview(
            fresh.prospective_prior_balances
        )
        server._publish_accepted_block_payout_preview(block_hash, verified)
        self.assertEqual(
            server._payout_state_generation,
            generation_after_issued,
        )
        with server._accepted_block_payout_preview_condition:
            transition = server._accepted_block_payout_previews[block_hash]
        self.assertEqual(
            transition.preview,
            server._serialize_prior_balance_preview(issued),
        )

        # A preview from any other anchor's window trips the guard exactly
        # as submit_block_candidate's landing rebuild would.
        with self.assertRaisesRegex(RuntimeError, "changed during retry"):
            server._publish_accepted_block_payout_preview(
                block_hash,
                server._materialize_prior_balance_preview(
                    divergent.prospective_prior_balances
                ),
            )

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

    def test_publication_critical_build_cannot_be_displaced_by_initial_work(
        self,
    ) -> None:
        server, rpc = coordinator()
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        payout_generation = server._payout_state_generation

        def request_for(
            artifacts: object,
            *,
            publication_critical: bool,
            request_source: str,
        ) -> object:
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
                publication_critical=publication_critical,
                request_source=request_source,
            )

        latest = request_for(
            new_artifacts,
            publication_critical=True,
            request_source="tip_refresh",
        )
        reconnect = request_for(
            old_artifacts,
            publication_critical=False,
            request_source="initial",
        )
        same_tip_reconnect = request_for(
            new_artifacts,
            publication_critical=False,
            request_source="initial",
        )
        latest_flight = SimpleNamespace(request=latest)
        server._job_build_active = latest_flight
        server._job_build_retiring = None
        server._job_build_pending = None

        deferred = server._request_job_build(reconnect)  # type: ignore[arg-type]
        coalesced = server._request_job_build(  # type: ignore[arg-type]
            same_tip_reconnect
        )

        self.assertIs(server._job_build_active, latest_flight)
        self.assertIs(coalesced, latest.promise)  # type: ignore[union-attr]
        self.assertFalse(latest.cancellation.is_set())  # type: ignore[union-attr]
        self.assertFalse(deferred.done())
        self.assertEqual(
            server.job_build_priority_counts["routine_deferred"],
            1,
        )
        self.assertEqual(
            server.initial_job_prepared_work_counts["deferred"],
            1,
        )
        self.assertEqual(
            server.initial_job_prepared_work_counts["singleflight"],
            1,
        )

        latest.promise.set_result(object())  # type: ignore[union-attr]
        with self.assertRaises(JobBuildSuperseded):
            deferred.result()

        metrics = "\n".join(server.job_build_metrics_lines())
        self.assertIn(
            'qbit_prism_job_build_priority_events_total{result="routine_deferred"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_initial_job_prepared_work_total{result="deferred"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_initial_job_prepared_work_total{result="singleflight"} 1',
            metrics,
        )

    def test_publication_critical_build_preempts_routine_builder_capacity(
        self,
    ) -> None:
        server, rpc = coordinator()
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        payout_generation = server._payout_state_generation

        def request_for(
            artifacts: object,
            *,
            publication_critical: bool,
            request_source: str,
        ) -> object:
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
                publication_critical=publication_critical,
                request_source=request_source,
            )

        routine = request_for(
            old_artifacts,
            publication_critical=False,
            request_source="initial",
        )
        latest = request_for(
            new_artifacts,
            publication_critical=True,
            request_source="tip_refresh",
        )
        routine_flight = SimpleNamespace(request=routine)
        server._job_build_active = routine_flight
        server._job_build_retiring = None
        server._job_build_pending = None
        server._start_job_build_locked = (  # type: ignore[method-assign]
            lambda request: SimpleNamespace(request=request, future=None)
        )
        server._arm_job_build_locked = lambda _flight: None  # type: ignore[method-assign]

        promise = server._request_job_build(latest)  # type: ignore[arg-type]

        self.assertIs(promise, latest.promise)  # type: ignore[union-attr]
        self.assertTrue(routine.cancellation.is_set())  # type: ignore[union-attr]
        self.assertIs(server._job_build_retiring, routine_flight)
        assert server._job_build_active is not None
        self.assertIs(server._job_build_active.request, latest)
        self.assertEqual(
            server.job_build_priority_counts["routine_preempted"],
            1,
        )

    def test_publication_critical_build_restarts_unhealthy_exact_flight(
        self,
    ) -> None:
        for unhealthy in ("almost_expired", "stalled"):
            with self.subTest(unhealthy=unhealthy):
                server, rpc = coordinator()
                artifacts = server.store_template_artifacts(dict(rpc.template))
                assert artifacts is not None
                payout_generation = server._payout_state_generation

                def request_for(
                    *,
                    publication_critical: bool,
                    priority_requested_monotonic: float | None = None,
                ) -> object:
                    return server._new_job_build_request(
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
                        publication_critical=publication_critical,
                        request_source=(
                            "tip_refresh"
                            if publication_critical
                            else "initial"
                        ),
                        priority_requested_monotonic=(
                            priority_requested_monotonic
                        ),
                    )

                routine = request_for(publication_critical=False)
                now = time.monotonic()
                if unhealthy == "almost_expired":
                    routine.cancellation.started_monotonic = now - 59.99
                    routine.cancellation.deadline_monotonic = now + 0.01
                    routine.cancellation.last_checkpoint_monotonic = now
                else:
                    routine.cancellation.started_monotonic = now - 1.0
                    routine.cancellation.deadline_monotonic = now + 59.0
                    routine.cancellation.last_checkpoint_monotonic = (
                        now - server.job_build_cancel_grace_seconds - 0.01
                    )
                priority_requested = now - 59.0
                latest = request_for(
                    publication_critical=True,
                    priority_requested_monotonic=priority_requested,
                )
                routine_flight = SimpleNamespace(request=routine)
                server._job_build_active = routine_flight
                server._job_build_retiring = None
                server._job_build_pending = None
                server._start_job_build_locked = (  # type: ignore[method-assign]
                    lambda request: SimpleNamespace(request=request, future=None)
                )
                server._arm_job_build_locked = lambda _flight: None  # type: ignore[method-assign]

                promise = server._request_job_build(latest)  # type: ignore[arg-type]

                self.assertIs(promise, latest.promise)
                self.assertIsNot(promise, routine.promise)
                self.assertTrue(routine.cancellation.is_set())
                self.assertIs(server._job_build_retiring, routine_flight)
                assert server._job_build_active is not None
                self.assertIs(server._job_build_active.request, latest)
                self.assertEqual(
                    latest.requested_monotonic,
                    priority_requested,
                )
                self.assertGreater(
                    latest.cancellation.deadline_monotonic
                    - time.monotonic(),
                    server.job_build_timeout_seconds - 1.0,
                )
                self.assertEqual(
                    server.job_build_priority_counts["routine_preempted"],
                    1,
                )

    def test_publication_priority_precedes_immutable_request_preparation(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        critical_preparation_entered = threading.Event()
        release_critical_preparation = threading.Event()
        initial_preparation_entered = threading.Event()
        original_new_request = server._new_job_build_request

        def observed_new_request(*args: object, **kwargs: object) -> object:
            request_source = str(kwargs.get("request_source", "routine"))
            if request_source == "tip_refresh":
                critical_preparation_entered.set()
                if not release_critical_preparation.wait(5):
                    raise AssertionError("test did not release priority preparation")
            elif request_source == "initial":
                initial_preparation_entered.set()
            return original_new_request(*args, **kwargs)  # type: ignore[arg-type]

        server._new_job_build_request = observed_new_request  # type: ignore[method-assign]
        results: dict[str, list[object]] = {"critical": [], "initial": []}
        errors: list[BaseException] = []

        def build(label: str, *, publication_critical: bool) -> None:
            try:
                results[label].append(
                    server.shared_job_bundle(
                        artifacts,
                        mode="ready",
                        publication_critical=publication_critical,
                        request_source=(
                            "tip_refresh" if publication_critical else "initial"
                        ),
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        critical_thread = threading.Thread(
            target=build,
            args=("critical",),
            kwargs={"publication_critical": True},
        )
        initial_thread = threading.Thread(
            target=build,
            args=("initial",),
            kwargs={"publication_critical": False},
        )
        critical_thread.start()
        try:
            self.assertTrue(critical_preparation_entered.wait(5))
            initial_thread.start()
            self.assertFalse(initial_preparation_entered.wait(0.1))
            metrics = "\n".join(server.job_build_metrics_lines())
            self.assertIn("qbit_prism_job_build_priority_active 1", metrics)
        finally:
            release_critical_preparation.set()
            critical_thread.join(5)
            initial_thread.join(5)

        self.assertFalse(critical_thread.is_alive())
        self.assertFalse(initial_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual([len(results[label]) for label in results], [1, 1])
        self.assertIs(results["critical"][0], results["initial"][0])
        self.assertFalse(initial_preparation_entered.is_set())
        self.assertEqual(
            server.initial_job_prepared_work_counts["deferred"],
            1,
        )
        self.assertEqual(
            server.initial_job_prepared_work_counts["cache_hit"],
            1,
        )
        self.assertEqual(
            server.job_build_priority_admission_seconds["count"],
            1,
        )
        self.assertGreaterEqual(
            server.job_build_priority_admission_seconds["sum"],
            0.1,
        )

    def test_priority_reservation_cancels_admitted_routine_preparation(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        artifacts = server.store_template_artifacts(dict(rpc.template))
        assert artifacts is not None
        routine_in_payout_lookup = threading.Event()
        release_routine_lookup = threading.Event()
        routine_request_constructed = threading.Event()
        original_usable_artifact = server._usable_payout_ledger_artifact
        original_new_request = server._new_job_build_request
        routine_thread: threading.Thread

        def blocked_usable_artifact(*args: object, **kwargs: object) -> object:
            if threading.current_thread() is routine_thread:
                # The pre-admission cache probe also consults the payout
                # artifact; only the lookup made under an admitted routine
                # preparation exercises the cancellation contract here.
                with server._job_build_scheduler_lock:
                    admitted = bool(server._job_build_routine_preparations)
                if admitted:
                    routine_in_payout_lookup.set()
                    if not release_routine_lookup.wait(5):
                        raise AssertionError("test did not release payout lookup")
            return original_usable_artifact(*args, **kwargs)  # type: ignore[arg-type]

        def observed_new_request(*args: object, **kwargs: object) -> object:
            if kwargs.get("request_source") == "initial":
                routine_request_constructed.set()
            return original_new_request(*args, **kwargs)  # type: ignore[arg-type]

        server._usable_payout_ledger_artifact = blocked_usable_artifact  # type: ignore[method-assign]
        server._new_job_build_request = observed_new_request  # type: ignore[method-assign]
        errors: list[BaseException] = []

        def build_routine() -> None:
            try:
                server.shared_job_bundle(
                    artifacts,
                    mode="ready",
                    retry_superseded=False,
                    request_source="initial",
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        routine_thread = threading.Thread(target=build_routine)
        routine_thread.start()
        priority_token: int | None = None
        try:
            self.assertTrue(routine_in_payout_lookup.wait(5))
            with server._job_build_scheduler_lock:
                routine_cancellations = tuple(
                    cancellation_ref()
                    for cancellation_ref in (
                        server._job_build_routine_preparations.values()
                    )
                )
            self.assertEqual(len(routine_cancellations), 1)
            self.assertIsNotNone(routine_cancellations[0])
            priority_token, _requested = (
                server._begin_job_build_priority_preparation()
            )
            assert routine_cancellations[0] is not None
            self.assertTrue(routine_cancellations[0].is_set())
        finally:
            release_routine_lookup.set()
            routine_thread.join(5)
            if priority_token is not None:
                server._finish_job_build_priority_preparation(priority_token)

        self.assertFalse(routine_thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        self.assertFalse(routine_request_constructed.is_set())
        with server._job_build_scheduler_lock:
            self.assertEqual(len(server._job_build_routine_preparations), 0)

    def test_publication_critical_collection_promotes_past_ready_work(self) -> None:
        server, rpc = coordinator(ledger=FakeLedger(miners=["miner-a"]))
        old_artifacts = server.store_template_artifacts(dict(rpc.template))
        new_artifacts = server.store_template_artifacts(
            base_template(height=11, prevhash="22" * 32)
        )
        assert old_artifacts is not None and new_artifacts is not None
        payout_generation = server._payout_state_generation
        ready = server._new_job_build_request(
            old_artifacts,
            None,
            mode="ready",
            payout_state_generation=payout_generation,
            cache_key=server._job_bundle_key(
                old_artifacts,
                mode="ready",
                payout_state_generation=payout_generation,
                worker=None,
            ),
            request_source="initial",
        )
        latest_identity = worker("tq1latest", "tq1latest.rig")
        latest = server._new_job_build_request(
            new_artifacts,
            latest_identity,
            mode="collection",
            payout_state_generation=payout_generation,
            cache_key=server._job_bundle_key(
                new_artifacts,
                mode="collection",
                payout_state_generation=payout_generation,
                worker=latest_identity,
            ),
            publication_critical=True,
            request_source="tip_refresh",
        )
        ready_flight = SimpleNamespace(request=ready)
        server._job_build_active = ready_flight
        server._job_build_retiring = None
        server._job_build_pending = latest
        armed: list[object] = []
        server._start_job_build_locked = (  # type: ignore[method-assign]
            lambda request: SimpleNamespace(request=request, future=None)
        )
        server._arm_job_build_locked = armed.append  # type: ignore[method-assign]

        server._promote_pending_job_build_locked()

        self.assertTrue(ready.cancellation.is_set())
        self.assertIs(server._job_build_retiring, ready_flight)
        self.assertIsNone(server._job_build_pending)
        assert server._job_build_active is not None
        self.assertIs(server._job_build_active.request, latest)
        self.assertEqual(armed, [server._job_build_active])
        self.assertEqual(
            server.job_build_priority_counts["routine_preempted"],
            1,
        )

    def test_routine_pending_never_promotes_over_publication_critical_flight(
        self,
    ) -> None:
        for placement in ("active", "retiring"):
            with self.subTest(placement=placement):
                server, rpc = coordinator()
                critical_artifacts = server.store_template_artifacts(
                    dict(rpc.template)
                )
                routine_artifacts = server.store_template_artifacts(
                    base_template(height=11, prevhash="22" * 32)
                )
                assert critical_artifacts is not None
                assert routine_artifacts is not None
                payout_generation = server._payout_state_generation

                def request_for(
                    artifacts: object,
                    *,
                    publication_critical: bool,
                ) -> object:
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
                        publication_critical=publication_critical,
                        request_source=(
                            "tip_refresh" if publication_critical else "initial"
                        ),
                    )

                critical = request_for(
                    critical_artifacts,
                    publication_critical=True,
                )
                routine = request_for(
                    routine_artifacts,
                    publication_critical=False,
                )
                critical_flight = SimpleNamespace(request=critical)
                server._job_build_active = (
                    critical_flight if placement == "active" else None
                )
                server._job_build_retiring = (
                    critical_flight if placement == "retiring" else None
                )
                server._job_build_pending = routine
                server._start_job_build_locked = (  # type: ignore[method-assign]
                    lambda _request: self.fail(
                        "routine pending work displaced publication-critical work"
                    )
                )

                server._promote_pending_job_build_locked()

                self.assertIs(server._job_build_pending, routine)
                self.assertFalse(critical.cancellation.is_set())
                self.assertIs(
                    (
                        server._job_build_active
                        if placement == "active"
                        else server._job_build_retiring
                    ),
                    critical_flight,
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
        # The poll must observe the same-tip template rotation immediately for
        # the queued-fanout replacement race below; disable the same-tip
        # template reuse window so the rotation is fetched, not deferred.
        server.template_cache_seconds = 0.0
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
        # Same-tip contexts survive the disconnect (detached from the dead
        # connection object) so a same-username reconnect can still submit
        # in-flight work; they age out on the same-tip retention TTL.
        for job_id in (active_id, evicted_id):
            entry = server.evicted_job_graveyard.get(job_id)
            self.assertIsNotNone(entry)
            assert entry is not None
            self.assertIsNone(entry.client)
            self.assertIn(job_id, server._disconnected_evicted_job_ids)

    def test_disconnect_purges_evicted_contexts_when_retention_disabled(
        self,
    ) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.disconnected_job_retention = 0
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
        self.assertNotIn(active_id, server.evicted_job_graveyard)
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

    def test_poll_observes_reorg_reconcile_phase(self) -> None:
        # The reconcile stage runs serially before every refresh build; one
        # poll must record exactly one reorg_reconcile phase observation.
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_tip_refresh_bundle_phase_seconds_count{phase="reorg_reconcile"} 1',
            metrics,
        )

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


def spool_share(seq: int) -> dict[str, object]:
    return {
        "share_seq": seq,
        "share_id": f"share-{seq}",
        "miner_id": "miner-a",
        "order_key": "miner-a",
        "p2mr_program_hex": "22" * 32,
        "share_difficulty": 1,
        "job_issued_at_ms": 1_700_000_000_000 + seq,
        "accepted_at_ms": 1_700_000_000_100 + seq,
        "credit_policy": None,
    }


ECHO_BUILDER_COMMAND = [
    sys.executable,
    "-c",
    "import json,sys; json.dump({'received': json.load(sys.stdin)}, sys.stdout)",
]


class ShareWindowSpoolTests(unittest.TestCase):
    """Spool-file handoff of the serialized share window to the builder."""

    def _coordinator(self) -> PrismCoordinator:
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        self.addCleanup(server.retire_share_window_spool)
        return server

    def _ledger_artifact(
        self,
        shares: list[dict[str, object]],
        *,
        generation: int,
    ) -> PayoutLedgerArtifact:
        return PayoutLedgerArtifact(
            generation=generation,
            payout_state_generation=0,
            network_difficulty=1,
            accepted_share_count=len(shares),
            shares_json=tuple(shares),
            prior_balances=(),
            prepared_monotonic=time.monotonic(),
            snapshot_anchor_ms=None,
        )

    def _build_with_echo_builder(
        self,
        server: PrismCoordinator,
        shares: list[dict[str, object]],
        serialization: object,
        *,
        height: int,
    ) -> dict[str, object]:
        # These tests cover the one-shot spool handoff; pin the persistent
        # builder off so the echo helper is never spawned as a daemon.
        with patch.dict(os.environ, {"PRISM_BUILDER_SERVE": "0"}), patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=list(ECHO_BUILDER_COMMAND),
        ):
            return server.build_audit_bundle(
                shares=shares,
                found_block={
                    "block_height": height,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
                payout_policy={"policy": "day-one"},
                share_serialization=serialization,  # type: ignore[arg-type]
            )

    def _expected_payload(
        self,
        shares: list[dict[str, object]],
        *,
        height: int,
    ) -> dict[str, object]:
        identities, compact_shares = _compact_share_payload(shares)
        return {
            "found_block": {
                "block_height": height,
                "coinbase_value_sats": 50_00000000,
                "network_difficulty": 1,
                "anchor_job_issued_at_ms": 1_700_000_000_000,
            },
            "prior_balances": [],
            "payout_policy": {"policy": "day-one"},
            "coinbase_script_sig_suffix_hex": "00",
            "witness_merkle_leaves_hex": [],
            "compact_share_identities": [
                list(identity) for identity in identities
            ],
            "compact_shares": [list(share) for share in compact_shares],
        }

    def test_spool_feeds_builder_and_is_written_once_per_generation(
        self,
    ) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )
        spool_creations: list[int] = []
        real_spool_file = prism_coordinator_module._share_window_spool_file

        def counting_spool_file() -> object:
            spool_creations.append(1)
            return real_spool_file()

        with patch(
            "lab.prism.prism_coordinator._share_window_spool_file",
            side_effect=counting_spool_file,
        ):
            first = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=10,
            )
            second = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=11,
            )

        self.assertEqual(
            first["received"],
            self._expected_payload(shares, height=10),
        )
        self.assertEqual(
            second["received"],
            self._expected_payload(shares, height=11),
        )
        self.assertEqual(len(spool_creations), 1)

    def test_spool_creation_failure_falls_back_to_pipe_writes(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )

        with patch(
            "lab.prism.prism_coordinator._share_window_spool_file",
            side_effect=OSError("temp filesystem unavailable"),
        ) as spool_factory:
            first = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=10,
            )
            second = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=11,
            )

        self.assertEqual(
            first["received"],
            self._expected_payload(shares, height=10),
        )
        self.assertEqual(
            second["received"],
            self._expected_payload(shares, height=11),
        )
        # The failure is sticky: a broken temp filesystem is not retried on
        # every subsequent build of the same generation.
        self.assertEqual(spool_factory.call_count, 1)

    def test_splice_failure_poisons_spool_and_falls_back_to_pipe(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )

        with patch(
            "lab.prism.prism_coordinator.os.splice",
            side_effect=OSError("splice unsupported"),
        ):
            first = self._build_with_echo_builder(
                server,
                shares,
                serialization,
                height=10,
            )
        second = self._build_with_echo_builder(
            server,
            shares,
            serialization,
            height=11,
        )

        self.assertEqual(
            first["received"],
            self._expected_payload(shares, height=10),
        )
        self.assertEqual(
            second["received"],
            self._expected_payload(shares, height=11),
        )
        self.assertTrue(serialization._spool_failed)
        self.assertIsNone(serialization._spool_file)

    def test_mid_stream_splice_failure_poisons_spool_for_later_builds(
        self,
    ) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )

        # The first chunk moves and the second fails: this build is
        # unrepairable (partial tail already in the pipe), but the spool must
        # be poisoned so later builds stream the cached fragments instead of
        # repeating the failure.
        with patch(
            "lab.prism.prism_coordinator.os.splice",
            side_effect=[16, OSError("splice io error")],
        ):
            with self.assertRaises(OSError):
                self._build_with_echo_builder(
                    server,
                    shares,
                    serialization,
                    height=10,
                )
        self.assertTrue(serialization._spool_failed)

        recovered = self._build_with_echo_builder(
            server,
            shares,
            serialization,
            height=11,
        )
        self.assertEqual(
            recovered["received"],
            self._expected_payload(shares, height=11),
        )

    def test_content_rotation_retires_spool_after_last_lease(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        first = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )
        lease = first.acquire_spooled_tail(shares)
        self.assertIsNotNone(lease)
        assert lease is not None
        spool_file, spool_size = lease
        self.assertGreater(spool_size, 0)

        changed_shares = [*shares, spool_share(4)]
        second = server._share_window_serialization_for_artifact(
            self._ledger_artifact(changed_shares, generation=2),
            changed_shares,
        )
        self.assertIsNot(second, first)
        # Retired: no new leases, but the in-flight transfer keeps the
        # descriptor alive until it releases.
        self.assertIsNone(first.acquire_spooled_tail(shares))
        self.assertFalse(spool_file.closed)
        first.release_spooled_tail()
        self.assertTrue(spool_file.closed)

    def test_generation_retag_reuses_content_keyed_serialization(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]

        first = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )
        second = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=2),
            shares,
        )

        self.assertIs(second, first)

    def test_shutdown_retires_current_spool(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = server._share_window_serialization_for_artifact(
            self._ledger_artifact(shares, generation=1),
            shares,
        )
        lease = serialization.acquire_spooled_tail(shares)
        assert lease is not None
        serialization.release_spooled_tail()
        self.assertFalse(lease[0].closed)

        server.retire_share_window_spool()

        self.assertTrue(lease[0].closed)
        self.assertIsNone(serialization.acquire_spooled_tail(shares))


FAKE_SERVE_BUILDER_COMMAND = [
    sys.executable,
    str(Path(__file__).resolve().parent / "fixtures" / "fake_serve_builder.py"),
]


class ServeBuilderTests(unittest.TestCase):
    """Persistent --serve builder client, its fallback, and supersession."""

    def _coordinator(self) -> PrismCoordinator:
        server, _rpc = coordinator()
        server.signing_seed_hex = "42" * 32
        server.ledger_attestation_signing_seed_hex = "43" * 32
        self.addCleanup(server.shutdown_serve_builder)
        self.addCleanup(server.retire_share_window_spool)
        return server

    def _serialization(
        self,
        server: PrismCoordinator,
        shares: list[dict[str, object]],
        *,
        generation: int = 1,
    ) -> object:
        return server._share_window_serialization_for_artifact(
            PayoutLedgerArtifact(
                generation=generation,
                payout_state_generation=0,
                network_difficulty=1,
                accepted_share_count=len(shares),
                shares_json=tuple(shares),
                prior_balances=(),
                prepared_monotonic=time.monotonic(),
                snapshot_anchor_ms=None,
            ),
            shares,
        )

    def _build(
        self,
        server: PrismCoordinator,
        shares: list[dict[str, object]],
        serialization: object,
        *,
        height: int,
        mode: str = "ok",
        cancellation: object = None,
    ) -> dict[str, object]:
        with patch.dict(
            os.environ,
            {"FAKE_SERVE_BUILDER_MODE": mode},
        ), patch(
            "lab.prism.prism_coordinator.prism_tool_command",
            return_value=list(FAKE_SERVE_BUILDER_COMMAND),
        ):
            return server.build_audit_bundle(
                shares=shares,
                found_block={
                    "block_height": height,
                    "coinbase_value_sats": 50_00000000,
                    "network_difficulty": 1,
                    "anchor_job_issued_at_ms": 1_700_000_000_000,
                },
                prior_balances=[],
                coinbase_script_sig_suffix_hex="00",
                summary_only=True,
                payout_policy={"policy": "day-one"},
                share_serialization=serialization,  # type: ignore[arg-type]
                cancellation=cancellation,  # type: ignore[arg-type]
            )

    def test_cache_miss_uploads_window_then_hits_on_next_request(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        identities, compact_shares = _compact_share_payload(shares)
        expected_window = {
            "compact_share_identities": [
                list(identity) for identity in identities
            ],
            "compact_shares": [list(share) for share in compact_shares],
        }
        try:
            first = self._build(server, shares, serialization, height=10)
            second = self._build(server, shares, serialization, height=11)

            self.assertEqual(first["transport"], "serve")
            self.assertTrue(first["request_had_window"])
            self.assertEqual(first["window"], expected_window)
            self.assertEqual(first["found_block"]["block_height"], 10)

            self.assertEqual(second["transport"], "serve")
            self.assertFalse(second["request_had_window"])
            self.assertEqual(second["window"], expected_window)
            self.assertEqual(second["found_block"]["block_height"], 11)

            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
                window_counts = dict(server.serve_builder_window_cache_counts)
            self.assertEqual(counts["requests"], 2)
            self.assertEqual(counts["spawns"], 1)
            self.assertEqual(counts["fallbacks"], 0)
            self.assertEqual(counts["window_uploads"], 1)
            self.assertEqual(window_counts, {"hits": 1, "misses": 1})
        finally:
            server.shutdown_serve_builder()

    def test_needs_window_bounce_reuploads_and_succeeds(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            first = self._build(server, shares, serialization, height=10)
            self.assertTrue(first["request_had_window"])
            client = server._serve_builder
            assert client is not None
            second_shares = [spool_share(seq) for seq in range(1, 5)]
            second_serialization = self._serialization(
                server,
                second_shares,
                generation=2,
            )
            # Pretend this window was already uploaded: the daemon's
            # needs_window bounce must repair the divergence transparently.
            client.note_uploaded_window(
                second_serialization.share_snapshot_sha256  # type: ignore[attr-defined]
            )

            server._ensure_tip_refresh_state()
            server._job_build_phase_local.tip_refresh_metrics = True
            try:
                second = self._build(
                    server,
                    second_shares,
                    second_serialization,
                    height=11,
                )
            finally:
                server._job_build_phase_local.tip_refresh_metrics = False

            self.assertEqual(second["transport"], "serve")
            self.assertTrue(second["request_had_window"])
            self.assertEqual(second["found_block"]["block_height"], 11)
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
            self.assertEqual(counts["requests"], 2)
            self.assertEqual(counts["fallbacks"], 0)
            # One build observes serialization_copy exactly three times --
            # fragment precompose, the accumulated transport writes, and the
            # daemon's reported timings -- matching the one-shot transport
            # even when a needs_window bounce re-sent the request.
            histogram = server.tip_refresh_build_phase_histograms[
                "serialization_copy"
            ]
            self.assertEqual(histogram["count"], 3)
        finally:
            server.shutdown_serve_builder()

    def test_hit_promotion_keeps_mirror_and_daemon_lru_aligned(self) -> None:
        server = self._coordinator()
        shares_a = [spool_share(seq) for seq in range(1, 4)]
        shares_b = [spool_share(seq) for seq in range(1, 5)]
        shares_c = [spool_share(seq) for seq in range(1, 6)]
        ser_a = self._serialization(server, shares_a, generation=1)
        ser_b = self._serialization(server, shares_b, generation=2)
        ser_c = self._serialization(server, shares_c, generation=3)
        try:
            self.assertTrue(
                self._build(server, shares_a, ser_a, height=10)[
                    "request_had_window"
                ]
            )
            self.assertTrue(
                self._build(server, shares_b, ser_b, height=11)[
                    "request_had_window"
                ]
            )
            # The hit promotes window A on both sides; uploading a third
            # window must therefore evict B everywhere.
            self.assertFalse(
                self._build(server, shares_a, ser_a, height=12)[
                    "request_had_window"
                ]
            )
            self.assertTrue(
                self._build(server, shares_c, ser_c, height=13)[
                    "request_had_window"
                ]
            )

            aligned = self._build(server, shares_a, ser_a, height=14)

            self.assertEqual(aligned["transport"], "serve")
            self.assertFalse(aligned["request_had_window"])
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
            self.assertEqual(counts["requests"], 5)
            self.assertEqual(counts["window_uploads"], 3)
            self.assertEqual(counts["fallbacks"], 0)
        finally:
            server.shutdown_serve_builder()

    def test_daemon_crash_falls_back_to_one_shot(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            result = self._build(
                server,
                shares,
                serialization,
                height=10,
                mode="crash-before-response",
            )

            self.assertEqual(result["transport"], "one-shot")
            self.assertEqual(
                result["received"]["found_block"]["block_height"],
                10,
            )
            self.assertIn("compact_shares", result["received"])
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
                self.assertIsNone(server._serve_builder)
            self.assertEqual(counts["fallbacks"], 1)
            self.assertEqual(counts["requests"], 0)
        finally:
            server.shutdown_serve_builder()

    def test_daemon_splice_failure_poisons_spool_and_one_shot_recovers(
        self,
    ) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            # The daemon upload dies mid-splice; the spool is poisoned before
            # falling back, so the fresh one-shot subprocess streams the
            # cached fragments instead of retrying the same failing transfer.
            with patch(
                "lab.prism.prism_coordinator.os.splice",
                side_effect=[16, OSError("splice io error")],
            ):
                result = self._build(server, shares, serialization, height=10)

            self.assertEqual(result["transport"], "one-shot")
            self.assertIn("compact_shares", result["received"])
            self.assertTrue(serialization._spool_failed)  # type: ignore[attr-defined]
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
            self.assertEqual(counts["fallbacks"], 1)
        finally:
            server.shutdown_serve_builder()

    def test_protocol_mismatch_falls_back_to_one_shot(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            result = self._build(
                server,
                shares,
                serialization,
                height=10,
                mode="protocol-mismatch",
            )

            self.assertEqual(result["transport"], "one-shot")
            with server._serve_builder_metrics_lock:
                counts = dict(server.serve_builder_counts)
                self.assertIsNone(server._serve_builder)
            self.assertEqual(counts["fallbacks"], 1)
            # The mismatched daemon was a real worker launch and must appear
            # in the lifecycle totals alongside the one-shot fallback.
            with server._job_build_scheduler_lock:
                worker_counts = dict(server.job_build_worker_counts)
            self.assertEqual(worker_counts["starts"], 2)
        finally:
            server.shutdown_serve_builder()

    def test_disabled_serve_builder_uses_one_shot_directly(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        with patch.dict(os.environ, {"PRISM_BUILDER_SERVE": "0"}):
            result = self._build(server, shares, serialization, height=10)

        self.assertEqual(result["transport"], "one-shot")
        with server._serve_builder_metrics_lock:
            counts = dict(server.serve_builder_counts)
        self.assertEqual(counts["spawns"], 0)

    def test_daemon_timeout_fallback_shares_the_build_deadline(self) -> None:
        server = self._coordinator()
        # An already-exhausted budget: the daemon attempt must not grant the
        # one-shot fallback a fresh full deadline, and the timeout is counted
        # as one worker failure, not two.
        server.bundle_build_timeout_seconds = 0.0
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)

        with self.assertRaisesRegex(
            RuntimeError,
            "qbit-prism-build-audit-bundle timed out",
        ):
            self._build(
                server,
                shares,
                serialization,
                height=10,
                mode="hang-after-request",
            )

        with server._serve_builder_metrics_lock:
            counts = dict(server.serve_builder_counts)
        self.assertEqual(counts["fallbacks"], 1)
        self.assertEqual(counts["spawns"], 0)
        self.assertEqual(server.tip_refresh_worker_failures, 1)
        # The live daemon killed at the handshake deadline is a recorded
        # termination, and the one-shot replacement consumes the pending
        # restart: two launches, one termination, one restart.
        with server._job_build_scheduler_lock:
            worker_counts = dict(server.job_build_worker_counts)
            self.assertFalse(server._job_build_worker_restart_pending)
        self.assertEqual(worker_counts["starts"], 2)
        self.assertEqual(worker_counts["terminations"], 1)
        self.assertEqual(worker_counts["restarts"], 1)

    def test_idle_daemon_death_counts_crash_and_restart(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            first = self._build(server, shares, serialization, height=10)
            self.assertEqual(first["transport"], "serve")
            client = server._serve_builder
            assert client is not None
            client.process.kill()
            client.process.wait(timeout=5)

            second = self._build(server, shares, serialization, height=11)

            self.assertEqual(second["transport"], "serve")
            with server._job_build_scheduler_lock:
                counts = dict(server.job_build_worker_counts)
                self.assertFalse(server._job_build_worker_restart_pending)
            self.assertEqual(counts["crashes"], 1)
            self.assertEqual(counts["restarts"], 1)
            self.assertEqual(server.tip_refresh_worker_restarts, 1)
            with server._serve_builder_metrics_lock:
                serve_counts = dict(server.serve_builder_counts)
            self.assertEqual(serve_counts["spawns"], 2)
            self.assertEqual(serve_counts["fallbacks"], 0)
        finally:
            server.shutdown_serve_builder()

    def test_daemon_respawn_counts_as_worker_restart(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        try:
            # A prior worker ended abnormally (crash or supersession kill)
            # with no immediate fallback consuming the flag; the replacement
            # daemon spawn must claim the restart instead of leaking it to
            # an unrelated later one-shot build.
            server._ensure_tip_refresh_state()
            with server._job_build_scheduler_lock:
                server._job_build_worker_restart_pending = True

            result = self._build(server, shares, serialization, height=10)

            self.assertEqual(result["transport"], "serve")
            with server._job_build_scheduler_lock:
                counts = dict(server.job_build_worker_counts)
                self.assertFalse(server._job_build_worker_restart_pending)
            self.assertEqual(counts["restarts"], 1)
            self.assertGreaterEqual(counts["starts"], 1)
            self.assertEqual(server.tip_refresh_worker_restarts, 1)
        finally:
            server.shutdown_serve_builder()

    def test_daemon_detaches_from_build_control_after_request(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        control = prism_coordinator_module._JobBundleBuildControl(
            key=("serve-detach",),
            previousblockhash="11" * 32,
            payout_state_generation=0,
            payout_artifact_generation=1,
        )
        with server._job_cache_lock:
            server._active_job_bundle_builds[control.key] = control
        server._job_build_phase_local.bundle_build_control = control
        try:
            result = self._build(server, shares, serialization, height=10)

            self.assertEqual(result["transport"], "serve")
            # The completed request detached the daemon: a late supersession
            # of this build has no process reference left to terminate.
            self.assertIsNone(control.process)
            control.cancel_event.set()
            client = server._serve_builder
            assert client is not None
            self.assertIsNone(client.process.poll())
        finally:
            server._job_build_phase_local.bundle_build_control = None
            control.cancel_event.set()
            with server._job_cache_lock:
                server._active_job_bundle_builds.pop(control.key, None)
            server.shutdown_serve_builder()

    def test_supersession_cancels_in_flight_daemon_request(self) -> None:
        server = self._coordinator()
        shares = [spool_share(seq) for seq in range(1, 4)]
        serialization = self._serialization(server, shares)
        control = prism_coordinator_module._JobBundleBuildControl(
            key=("serve-test",),
            previousblockhash="11" * 32,
            payout_state_generation=0,
            payout_artifact_generation=1,
        )
        with server._job_cache_lock:
            server._active_job_bundle_builds[control.key] = control
        errors: list[BaseException] = []
        results: list[object] = []

        def build() -> None:
            # The bundle-build control travels on the builder thread's local
            # state, exactly as _execute_job_build installs it.
            server._job_build_phase_local.bundle_build_control = control
            try:
                results.append(
                    self._build(
                        server,
                        shares,
                        serialization,
                        height=10,
                        mode="hang-after-request",
                    )
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)
            finally:
                server._job_build_phase_local.bundle_build_control = None

        thread = threading.Thread(target=build)
        thread.start()
        try:
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if control.process is not None:
                    break
                time.sleep(0.005)
            self.assertIsNotNone(control.process)

            control.cancel_event.set()
            thread.join(5.0)
        finally:
            control.cancel_event.set()
            thread.join(1.0)
            server.shutdown_serve_builder()

        self.assertFalse(thread.is_alive())
        self.assertEqual(results, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], JobBuildSuperseded)
        self.assertIsNone(server._serve_builder)


class ReorgReconcileRefreshPathTests(unittest.TestCase):
    """The tip-refresh reconcile stage: memo reuse and fetch overlap."""

    def test_poll_memo_hit_skips_full_reconcile_pass(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        with server.lock:
            server._reorg_reconcile_trusted_memo[rpc.tip] = time.monotonic()

        def unexpected_pass(**_kwargs: object) -> dict[str, object]:
            raise AssertionError(
                "memo-armed poll must not queue a serialized reconcile pass"
            )

        server.reconcile_prism_pool_blocks_once = (  # type: ignore[method-assign]
            unexpected_pass
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        # A memo hit never needs the prefetch worker.
        self.assertIsNone(server._reconcile_prefetch_executor)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="memo_hit"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_tip_refresh_bundle_phase_seconds_count{phase="reorg_reconcile"} 1',
            metrics,
        )

    def test_poll_overlaps_reconcile_with_template_fetch(self) -> None:
        # Deadlock-free only when the reconcile pass and the template fetch
        # genuinely run concurrently: the fetch refuses to return before the
        # reconcile starts, and the reconcile refuses to return before the
        # fetch starts.
        fetch_started = threading.Event()
        reconcile_running = threading.Event()

        class OverlapProbeRpc(FakeRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblocktemplate":
                    fetch_started.set()
                    assert reconcile_running.wait(5.0), (
                        "reconcile prefetch did not start while the template "
                        "fetch was in flight"
                    )
                return super().call(method, params)

        template = base_template()
        rpc = OverlapProbeRpc(template, tip=str(template["previousblockhash"]))
        server, _ = coordinator(template=template)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        reconciled_tips: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            reconciled_tips.append(tip_hash)
            reconcile_running.set()
            assert fetch_started.wait(5.0), (
                "template fetch did not start while the reconcile pass was "
                "in flight"
            )
            # A real trusted pass arms the per-tip memo; the join validates
            # the overlapped result against that armed entry.
            with server.lock:
                server._reorg_reconcile_trusted_memo[tip_hash] = time.monotonic()
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(reconciled_tips, [rpc.tip])
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="overlap"} 1',
            metrics,
        )

    def test_poll_reconciles_snapshot_tip_serially_when_tip_moves_before_fetch(
        self,
    ) -> None:
        # The cheap probe observed a tip that was replaced before the
        # template fetch. The prefetched pass proved the superseded hash, so
        # the poll must reconcile the snapshot's newer tip on its own thread.
        template = base_template()
        new_tip = str(template["previousblockhash"])
        old_tip = "ab" * 32

        class MovingTipRpc(FakeRpc):
            def __init__(self) -> None:
                super().__init__(template, tip=new_tip)
                self.best_tip_calls = 0

            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getbestblockhash":
                    self.best_tip_calls += 1
                    if self.best_tip_calls == 1:
                        self.calls.append(method)
                        return old_tip
                return super().call(method, params)

        rpc = MovingTipRpc()
        server, _ = coordinator(template=template)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        reconcile_calls: list[str] = []
        prefetch_drained = threading.Event()

        def fake_ensure(tip_hash: str) -> bool:
            reconcile_calls.append(tip_hash)
            if tip_hash == old_tip:
                prefetch_drained.set()
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertTrue(prefetch_drained.wait(5.0))
        self.assertIn(new_tip, reconcile_calls)
        self.assertEqual(
            sorted(reconcile_calls),
            sorted([old_tip, new_tip]),
        )
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="serial"} 1',
            metrics,
        )

    def test_poll_reproves_when_memo_evicted_during_template_fetch(self) -> None:
        # A tip that flips away and back during the template fetch evicts
        # the memo entry so post-flip pool-block state is re-proven. The
        # join must judge the memo at join time, never the pre-fetch probe.
        template = base_template()
        server, _ = coordinator(template=template)
        server.reorg_reconciler_enabled = True
        tip = str(template["previousblockhash"])

        class EvictingRpc(FakeRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblocktemplate":
                    # Models a detection during the fetch unarming the memo.
                    with server.lock:
                        server._reorg_reconcile_trusted_memo.clear()
                return super().call(method, params)

        rpc = EvictingRpc(template, tip=tip)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        with server.lock:
            server._reorg_reconcile_trusted_memo[tip] = time.monotonic()
        reconcile_calls: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            reconcile_calls.append(tip_hash)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(reconcile_calls, [tip])
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="serial"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="memo_hit"} 0',
            metrics,
        )

    def test_poll_reproves_when_memo_rearmed_across_detection_epoch(self) -> None:
        # A pass whose execution straddles a flip away and back can re-arm
        # the memo after the flip-back (the latest detected hash matches
        # again), but its proof belongs to the closed epoch. The join must
        # refuse memo entries when the detection epoch moved mid-fetch.
        template = base_template()
        server, _ = coordinator(template=template)
        server.reorg_reconciler_enabled = True
        tip = str(template["previousblockhash"])

        class FlipBackRpc(FakeRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblocktemplate":
                    # Models a flip away and back detected during the fetch
                    # (two epoch bumps) with a straddling pass re-arming the
                    # memo for the same hash afterwards.
                    with server.lock:
                        server.tip_detection_epoch = (
                            int(getattr(server, "tip_detection_epoch", 0)) + 2
                        )
                        server._reorg_reconcile_trusted_memo[tip] = (
                            time.monotonic()
                        )
                return super().call(method, params)

        rpc = FlipBackRpc(template, tip=tip)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        with server.lock:
            server._reorg_reconcile_trusted_memo[tip] = time.monotonic()
        reconcile_calls: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            reconcile_calls.append(tip_hash)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(reconcile_calls, [tip])
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="serial"} 1',
            metrics,
        )

    def test_overlap_join_blocks_when_trust_flips_after_prefetch(self) -> None:
        # The prefetched pass proves trust when it runs, up to a fetch
        # window earlier. Headers running ahead without a best-hash change
        # (an arriving reorg, no detection) must still block the refresh at
        # the join, matching the memo branch's live trust check.
        template = base_template()
        server, _ = coordinator(template=template)
        server.reorg_reconciler_enabled = True
        tip = str(template["previousblockhash"])

        class TrustFlipRpc(FakeRpc):
            def call(
                self,
                method: str,
                params: list[object] | None = None,
            ) -> object:
                if method == "getblocktemplate":
                    # Headers run ahead after the fetch: untrusted view with
                    # no tip detection (epoch unchanged).
                    self.blockchain_info["headers"] = 101
                return super().call(method, params)

        rpc = TrustFlipRpc(template, tip=tip)
        server.rpc = rpc
        install_fake_bundle_builder(server)
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            lambda _tip: True
        )

        try:
            with self.assertRaises(TemplateRefreshBlocked) as raised:
                server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertIn("untrusted", str(raised.exception))
        self.assertIsNone(state.active_job)

    def test_tip_refresh_join_bounds_a_stalled_reconcile_prefetch(self) -> None:
        # The overlap join must never park the poll loop on a crawling
        # prefetched pass: it times out into the normal blocked-retry path
        # within the CONFIGURED budget (not merely eventually), retries
        # re-join the identical still-running future without starting
        # another pass, and the pass completing unblocks the next poll.
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server.reconcile_prefetch_join_timeout_seconds = 0.05
        started = threading.Event()
        release = threading.Event()
        pass_calls = [0]

        def slow_ensure(tip_hash: str) -> bool:
            pass_calls[0] += 1
            started.set()
            assert release.wait(10.0)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            slow_ensure
        )
        try:
            join_started = time.monotonic()
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
            # Generous scheduler tolerance, but far below any hardcoded
            # multi-second budget: the configured 0.05s must be what fired.
            self.assertLess(time.monotonic() - join_started, 1.0)
            self.assertTrue(started.is_set())
            with server._reconcile_prefetch_executor_lock:
                pending = server._reconcile_prefetch_pending
            self.assertIsNotNone(pending)
            assert pending is not None
            first_future = pending[1]
            self.assertFalse(first_future.done())

            # A retry re-joins the same running future: same slot identity,
            # no additional pass started.
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
            with server._reconcile_prefetch_executor_lock:
                pending_again = server._reconcile_prefetch_pending
            assert pending_again is not None
            self.assertIs(pending_again[1], first_future)
            self.assertEqual(pass_calls[0], 1)

            release.set()
            self.assertTrue(first_future.result(5.0))
            server.poll_qbit_tip_template_once()
        finally:
            release.set()
            server.shutdown_reconcile_prefetch_executor()

    def test_serial_reconcile_branch_uses_the_bounded_join(self) -> None:
        # With the memo disabled no overlap prefetch exists, so the pass
        # lands in the serial re-prove branch -- which must route through
        # the same prefetch slot and bounded join instead of parking the
        # poll loop synchronously inside a crawling pass.
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server.reorg_reconcile_cache_seconds = 0.0
        server.reconcile_prefetch_join_timeout_seconds = 0.05
        started = threading.Event()
        release = threading.Event()

        def slow_ensure(tip_hash: str) -> bool:
            started.set()
            assert release.wait(10.0)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            slow_ensure
        )
        try:
            join_started = time.monotonic()
            with self.assertRaises(TemplateRefreshBlocked):
                server.poll_qbit_tip_template_once()
            self.assertLess(time.monotonic() - join_started, 1.0)
            self.assertTrue(started.is_set())
            with server._reconcile_prefetch_executor_lock:
                pending = server._reconcile_prefetch_pending
            self.assertIsNotNone(pending)
            assert pending is not None
            self.assertFalse(pending[1].done())

            release.set()
            self.assertTrue(pending[1].result(5.0))
            server.poll_qbit_tip_template_once()
        finally:
            release.set()
            server.shutdown_reconcile_prefetch_executor()

    def test_pass_raised_timeout_is_not_logged_as_join_expiry(self) -> None:
        # socket.timeout IS builtins.TimeoutError: a pass that raises it
        # completes the future exceptionally and must surface as an ordinary
        # blocked pass -- not as a join expiry, which would misdirect
        # operators toward the join while nothing is running.
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        server.reorg_reconcile_cache_seconds = 0.0
        server.reconcile_prefetch_join_timeout_seconds = 5.0

        def timing_out_ensure(tip_hash: str) -> bool:
            raise TimeoutError("pass-owned timeout")

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            timing_out_ensure
        )
        try:
            captured = io.StringIO()
            join_started = time.monotonic()
            with contextlib.redirect_stdout(captured):
                with self.assertRaises(TemplateRefreshBlocked):
                    server.poll_qbit_tip_template_once()
            self.assertLess(time.monotonic() - join_started, 2.0)
            self.assertNotIn("join exceeded", captured.getvalue())
        finally:
            server.shutdown_reconcile_prefetch_executor()

    def test_reconcile_prefetch_slot_is_reused_across_failed_attempts(self) -> None:
        # A refresh attempt that dies before its join (template-RPC outage)
        # must not queue another serialized pass per retry: the slot holds
        # at most one outstanding prefetch, reused for the same tip and
        # replaced (with the queued task cancelled) on a tip change.
        server, _ = coordinator()
        server.reorg_reconciler_enabled = True
        started = threading.Event()
        release = threading.Event()

        def slow_ensure(tip_hash: str) -> bool:
            started.set()
            assert release.wait(5.0)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            slow_ensure
        )
        try:
            first = server._submit_reconcile_prefetch("aa" * 32)
            assert first is not None
            self.assertTrue(started.wait(5.0))
            second = server._submit_reconcile_prefetch("aa" * 32)
            self.assertIs(second, first)
            third = server._submit_reconcile_prefetch("bb" * 32)
            assert third is not None
            self.assertIsNot(third, first)
            fourth = server._submit_reconcile_prefetch("cc" * 32)
            assert fourth is not None
            self.assertIsNot(fourth, third)
            self.assertTrue(third.cancelled())
            release.set()
            self.assertTrue(first.result(5.0))
            self.assertTrue(fourth.result(5.0))
        finally:
            release.set()
            server.shutdown_reconcile_prefetch_executor()

    def test_job_build_memo_lookups_are_counted(self) -> None:
        server, rpc = coordinator()
        server.reorg_reconciler_enabled = True
        with server.lock:
            server._reorg_reconcile_trusted_memo[rpc.tip] = time.monotonic()

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())

        with server.lock:
            server._reorg_reconcile_trusted_memo[rpc.tip] = (
                time.monotonic() - 100.0
            )
        serial_tips: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            serial_tips.append(tip_hash)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())

        self.assertEqual(serial_tips, [rpc.tip])
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="job_build",source="memo_hit"} 1',
            metrics,
        )
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="job_build",source="serial"} 1',
            metrics,
        )

    def test_poll_falls_back_to_serial_reconcile_after_prefetch_shutdown(
        self,
    ) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.reorg_reconciler_enabled = True
        state = client(1)
        state.send = lambda _payload: None  # type: ignore[method-assign]
        server.clients = {state}
        server.shutdown_reconcile_prefetch_executor()
        reconcile_calls: list[str] = []

        def fake_ensure(tip_hash: str) -> bool:
            reconcile_calls.append(tip_hash)
            return True

        server.ensure_reorg_reconciled_for_tip = (  # type: ignore[method-assign]
            fake_ensure
        )

        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()

        self.assertEqual(refreshed, 1)
        self.assertEqual(reconcile_calls, [rpc.tip])
        self.assertIsNone(server._reconcile_prefetch_executor)
        metrics = server.metrics_payload()
        self.assertIn(
            'qbit_prism_reorg_reconcile_lookups_total{path="tip_refresh",source="serial"} 1',
            metrics,
        )


class JobContextStampTests(unittest.TestCase):
    def test_job_context_stamps_client_version_mask(self) -> None:
        # Cross-connection resumes validate version bits against the mask
        # negotiated on the delivering connection, so the context must
        # record it at stamp time.
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        state.version_mask = 0x1FFFE000

        context = server.build_job_for_client(state, clean_jobs=True)

        self.assertEqual(context.version_mask, 0x1FFFE000)


if __name__ == "__main__":
    unittest.main()
