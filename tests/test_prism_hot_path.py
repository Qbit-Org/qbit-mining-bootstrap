#!/usr/bin/env python3
"""Hot-path regressions behind the 2026-07-16 stale/unknown-job reject outage.

Covers the coordinator-side commitments:
- mining.submit classifies against the poller/blockwait-observed tip instead
  of a blocking per-share getbestblockhash RPC, with a freshness bound that
  falls back to the RPC when tip observation stalls (never fail-open on a
  frozen snapshot);
- per-share / per-job stdout logging is debug-gated;
- prepared fanout passes perform O(1) live validations regardless of client
  count (the base branch's validation-token architecture keeps per-client
  deliveries RPC-free; pinned here as an outage regression guard).
"""

from __future__ import annotations

import contextlib
import io
import threading
import time
import unittest
from decimal import Decimal

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.prism_coordinator import (
    ClientState,
    PRISM_REJECTION_REASON_IDS,
    PRISM_REJECTION_STALE_JOB,
    PrismCoordinator,
    StratumError,
    TemplateRefreshBlocked,
    WorkerIdentity,
    default_prism_coinbase_tag_hex,
)
from lab.prism.share_ledger import AcceptedShareRecord, PendingShare

PAYOUT_ADDRESS = "tq1z70ukpvs96kye6jmgvl3nttevtkrq8uu89snkpm6m8gwqukw8u5dsz32kwa"
EXTRANONCE2_SIZE = 8
OLD_TIP = "11" * 32
NEW_TIP = "33" * 32


class FakeShare:
    def __init__(self, miner_id: str, share_seq: int) -> None:
        self.miner_id = miner_id
        self.share_seq = share_seq

    def to_prism_json(self) -> dict[str, object]:
        return {"share_seq": self.share_seq, "miner_id": self.miner_id}


class FakeAppendLedger:
    backend_name = "fake"

    def __init__(self) -> None:
        self.miners = ["miner-a", "miner-b", "miner-c"]
        self.appended: list[PendingShare] = []

    def accepted_share_stats(self) -> dict[str, int]:
        return {
            "accepted_share_count": len(self.miners) + len(self.appended),
            "distinct_miner_count": len(self.miners),
        }

    def snapshot_at_job_issue(
        self,
        anchor_job_issued_at_ms: int,
        *,
        window_weight: int | None = None,
    ) -> list[FakeShare]:
        return [
            FakeShare(miner_id=miner, share_seq=seq + 1)
            for seq, miner in enumerate(self.miners)
        ]

    def current_prior_balances(self) -> list[dict[str, object]]:
        return []

    def append_batch(
        self,
        entries: list[tuple[PendingShare, dict[str, object] | None]],
    ) -> list[AcceptedShareRecord]:
        records = []
        for pending, _candidate in entries:
            self.appended.append(pending)
            records.append(
                AcceptedShareRecord(
                    share_seq=len(self.appended),
                    share_id=pending.share_id,
                    miner_id=pending.miner_id,
                    order_key=pending.order_key,
                    p2mr_program_hex=pending.p2mr_program_hex,
                    share_difficulty=pending.share_difficulty,
                    network_difficulty=pending.network_difficulty,
                    template_height=pending.template_height,
                    job_id=pending.job_id,
                    job_issued_at_ms=pending.job_issued_at_ms,
                    accepted_at_ms=pending.accepted_at_ms,
                    ntime=pending.ntime,
                    credit_policy=pending.credit_policy,
                )
            )
        return records

    def append(self, pending: PendingShare) -> AcceptedShareRecord:
        return self.append_batch([(pending, None)])[0]

    def metrics(self) -> dict[str, int]:
        return {"blocks": 0, "owed_accounts": 0}


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

    def call(self, method: str, params: list[object] | None = None, **_kwargs: object) -> object:
        self.calls.append(method)
        if method == "getblocktemplate":
            return dict(self.template)
        if method == "getbestblockhash":
            return self.tip
        if method == "getblockchaininfo":
            return dict(self.blockchain_info)
        if method == "getblockcount":
            return int(self.blockchain_info["blocks"])
        if method == "getblock":
            return {"previousblockhash": OLD_TIP}
        raise AssertionError(f"unexpected RPC {method}")

    def count(self, method: str) -> int:
        return sum(1 for name in self.calls if name == method)


class FakeSock:
    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self._lock = threading.Lock()

    def sendall(self, data: bytes) -> None:
        with self._lock:
            self.sent.append(data)

    def shutdown(self, how: int) -> None:
        return None

    def close(self) -> None:
        return None


def base_template(height: int = 10, prevhash: str = OLD_TIP) -> dict[str, object]:
    return {
        "height": height,
        "previousblockhash": prevhash,
        "bits": "1b00ffff",
        "version": 0x20000000,
        "curtime": 1_700_000_000,
        "coinbasevalue": 50_00000000,
        "transactions": [],
    }


def worker(payout: str = PAYOUT_ADDRESS) -> WorkerIdentity:
    return WorkerIdentity(
        username=payout,
        payout_address=payout,
        worker_name=None,
        script_pubkey_hex="5220" + "22" * 32,
        p2mr_program_hex="22" * 32,
    )


def client(connection_id: int) -> ClientState:
    state = ClientState.__new__(ClientState)
    state.sock = FakeSock()
    state.address = ("127.0.0.1", 40_000 + connection_id)
    state.connection_id = connection_id
    state.extranonce1_hex = f"{connection_id:08x}"
    state.subscribed = True
    state.authorized = True
    identity = worker()
    state.username = identity.username
    state.worker = identity
    state.version_mask = 0
    state.active_job = None
    state.share_difficulty = Decimal("0.000000001")
    state.pending_share_difficulty = None
    state.minimum_advertised_difficulty = Decimal("0")
    state.vardiff_config = None
    state.listener_vardiff_config = None
    state.requested_difficulty = None
    state.suggested_difficulty = None
    state.requested_min_difficulty = None
    state.vardiff_window_started_monotonic = time.monotonic()
    state.vardiff_window_accepted = 0
    state.vardiff_window_submitted = 0
    state.vardiff_window_work = Decimal("0")
    state.vardiff_difficulty_estimate = None
    state.active_job_ids = set()
    state.post_accept_refresh_block = None
    state.tip_work_delivered = None
    state.job_update_lock = threading.RLock()
    state.send_lock = threading.Lock()
    return state


def coordinator(
    *,
    template: dict[str, object] | None = None,
) -> tuple[PrismCoordinator, FakeRpc]:
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
    server.latest_bundle = None
    server.tip_template_snapshot = None
    server.recent_share_keys = set()
    server.extranonce2_size = EXTRANONCE2_SIZE
    server.coinbase_tag_hex = default_prism_coinbase_tag_hex()
    server.share_difficulty = Decimal("0.000000001")
    server.vardiff_config = vardiff.VardiffConfig(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("0.000000001"),
        max_difficulty=Decimal("1024"),
        retarget_interval_seconds=Decimal("90"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("0.000000001"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("0.4"),
        retarget_tolerance=Decimal("0.25"),
    )
    server.default_share_weight = 1
    server.share_weights_by_username = {}
    server.min_ready_miners = 3
    server.ledger = FakeAppendLedger()
    server.blockpoll_seconds = 2.0
    server.job_bundle_cache_seconds = 10.0
    server.template_cache_seconds = 2.0
    server.reorg_reconcile_cache_seconds = 5.0
    server.health_refresh_seconds = 5.0
    server.stratum_send_timeout_seconds = 20.0
    server.stale_grace_seconds = 0.0
    server.hot_path_log_enabled = False
    server.share_writer_active = False
    server._ensure_job_cache_state()
    return server, rpc


def synthetic_manifest_coinbase_hex(suffix_hex: str) -> str:
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


def install_fake_bundle_builder(server: PrismCoordinator) -> dict[str, object]:
    recorded: dict[str, object] = {"calls": 0}

    def fake_build_audit_bundle(**kwargs: object) -> dict[str, object]:
        recorded["calls"] = int(recorded["calls"]) + 1
        suffix_hex = str(kwargs["coinbase_script_sig_suffix_hex"])
        return {
            "found_block": dict(kwargs["found_block"]),
            "signed_coinbase_manifest": {
                "manifest": {
                    "coinbase_tx_hex": synthetic_manifest_coinbase_hex(suffix_hex),
                }
            },
        }

    server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
    return recorded


def register_job(server: PrismCoordinator, state: ClientState) -> object:
    context = server.build_job_for_client(state, clean_jobs=True)
    state.active_job = context
    with server.lock:
        server.jobs[context.job.job_id] = context
        state.active_job_ids.add(context.job.job_id)
    return context


def observe_tip(server: PrismCoordinator, tip_hash: str, *, age_seconds: float = 0.0) -> None:
    """Install an observed tip the way blockpoll/blockwait would."""
    with server.lock:
        server.current_tip_first_seen = (tip_hash, None)
        server.current_tip_observed_monotonic = time.monotonic() - age_seconds


def submit_params(state: ClientState, context: object) -> list[object]:
    """Params for a share that deterministically passes the share target
    without also being a block solution."""
    extranonce2_hex = "11" * EXTRANONCE2_SIZE
    for nonce in range(200_000):
        nonce_hex = f"{nonce:08x}"
        submission = direct_stratum.assemble_submission(
            context.job,
            extranonce2_hex=extranonce2_hex,
            ntime_hex=context.job.ntime,
            nonce_hex=nonce_hex,
        )
        if submission.share_pass and not submission.block_pass:
            return [
                state.username,
                context.job.job_id,
                extranonce2_hex,
                context.job.ntime,
                nonce_hex,
            ]
    raise AssertionError("no share-passing nonce found")


class SubmitTipCheckTests(unittest.TestCase):
    def test_submit_uses_observed_tip_without_rpc(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        context = register_job(server, state)
        observe_tip(server, OLD_TIP)
        rpc.calls.clear()

        accepted_and_closed = server.handle_submit(state, submit_params(state, context))

        self.assertFalse(accepted_and_closed)
        self.assertEqual(rpc.count("getbestblockhash"), 0)
        self.assertEqual(len(server.ledger.appended), 1)

    def test_submit_rejects_stale_job_from_observed_tip_without_rpc(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        context = register_job(server, state)
        params = submit_params(state, context)
        observe_tip(server, NEW_TIP)
        rpc.calls.clear()

        with self.assertRaises(StratumError) as caught:
            server.handle_submit(state, params)

        self.assertEqual(caught.exception.reason, PRISM_REJECTION_STALE_JOB)
        self.assertEqual(rpc.count("getbestblockhash"), 0)
        self.assertEqual(server.rejection_counts_by_reason[PRISM_REJECTION_STALE_JOB], 1)
        self.assertEqual(len(server.ledger.appended), 0)

    def test_submit_falls_back_to_rpc_without_observation(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        context = register_job(server, state)
        self.assertIsNone(getattr(server, "current_tip_first_seen", None))
        params = submit_params(state, context)
        rpc.calls.clear()

        server.handle_submit(state, params)

        self.assertEqual(rpc.count("getbestblockhash"), 1)
        self.assertEqual(len(server.ledger.appended), 1)

    def test_submit_falls_back_to_rpc_when_observation_goes_stale(self) -> None:
        # The Codex P1 regression: a poller that stops refreshing after a tip
        # change must not let submits classify against the frozen snapshot
        # forever. Once the observation exceeds its freshness budget the
        # submit path re-reads the live tip.
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.submit_tip_max_age_seconds = 10.0
        state = client(1)
        context = register_job(server, state)
        params = submit_params(state, context)
        # Observation is stale, and the live tip has moved on: the share must
        # be classified against the RPC tip and rejected as stale.
        observe_tip(server, OLD_TIP, age_seconds=11.0)
        rpc.tip = NEW_TIP
        rpc.calls.clear()

        with self.assertRaises(StratumError) as caught:
            server.handle_submit(state, params)

        self.assertEqual(caught.exception.reason, PRISM_REJECTION_STALE_JOB)
        self.assertEqual(rpc.count("getbestblockhash"), 1)
        self.assertEqual(len(server.ledger.appended), 0)

    def test_submit_tip_max_age_zero_restores_per_share_rpc(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.submit_tip_max_age_seconds = 0.0
        state = client(1)
        context = register_job(server, state)
        params = submit_params(state, context)
        observe_tip(server, OLD_TIP)
        rpc.calls.clear()

        server.handle_submit(state, params)

        self.assertEqual(rpc.count("getbestblockhash"), 1)


class HotPathLoggingTests(unittest.TestCase):
    def drive_job_and_share(self, server: PrismCoordinator, rpc: FakeRpc) -> None:
        state = client(1)
        self.assertTrue(server.maybe_send_job(state, clean_jobs=True))
        context = state.active_job
        observe_tip(server, OLD_TIP)
        server.handle_submit(state, submit_params(state, context))

    def test_per_share_and_per_job_prints_off_by_default(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.drive_job_and_share(server, rpc)
        output = stdout.getvalue()
        self.assertNotIn("building job", output)
        self.assertNotIn("sent job", output)
        self.assertNotIn("accepted share", output)

    def test_per_share_and_per_job_prints_enabled_by_flag(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.hot_path_log_enabled = True
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.drive_job_and_share(server, rpc)
        output = stdout.getvalue()
        self.assertIn("building job connection=1", output)
        self.assertIn("sent job connection=1", output)
        self.assertIn("accepted share seq=1", output)


class FanOutRpcEconomyTests(unittest.TestCase):
    """Outage regression guard: a refresh pass's RPC cost must not scale with
    the number of connected clients. The base branch's validation-token
    architecture performs O(1) live validations per pass and keeps the
    per-client delivery path RPC-free; this pins that property against the
    2026-07-16 failure mode (two serialized RPC round trips per client)."""

    def run_pass(self, client_count: int) -> FakeRpc:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        states = [client(index + 1) for index in range(client_count)]
        server.clients = set(states)
        try:
            refreshed = server.poll_qbit_tip_template_once()
        finally:
            server.shutdown_tip_refresh_executor()
        self.assertEqual(refreshed, client_count)
        for state in states:
            self.assertIsNotNone(state.active_job)
        return rpc

    def test_refresh_pass_rpc_count_is_independent_of_client_count(self) -> None:
        small = self.run_pass(1)
        large = self.run_pass(8)
        for method in (
            "getbestblockhash",
            "getblocktemplate",
            "getblockchaininfo",
            "getblockcount",
            "getblock",
        ):
            self.assertEqual(small.count(method), large.count(method), method)

    def test_driver_live_trust_check_aborts_queued_delivery(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 1
        server.reorg_reconciler_enabled = True

        def reconcile_live_chain_view(tip_hash: str) -> bool:
            self.assertEqual(tip_hash, OLD_TIP)
            return not server.qbit_chain_view_untrusted()

        server.ensure_reorg_reconciled_for_tip = reconcile_live_chain_view  # type: ignore[method-assign]
        first = client(1)
        second = client(2)
        server.clients = [first, second]  # type: ignore[assignment]
        first_delivery_started = threading.Event()
        release_first_delivery = threading.Event()
        second_notifications: list[dict[str, object]] = []

        def block_first_delivery(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                first_delivery_started.set()
                self.assertTrue(release_first_delivery.wait(10))

        first.send = block_first_delivery  # type: ignore[method-assign]
        second.send = second_notifications.append  # type: ignore[method-assign]
        refreshed: list[int] = []
        errors: list[BaseException] = []

        def poll() -> None:
            try:
                refreshed.append(server.poll_qbit_tip_template_once())
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        poll_thread = threading.Thread(target=poll)
        poll_thread.start()
        try:
            self.assertTrue(first_delivery_started.wait(5))
            chain_info_calls_before_mismatch = rpc.count("getblockchaininfo")
            rpc.blockchain_info["headers"] = 101
            deadline = time.monotonic() + 5
            while (
                rpc.count("getblockchaininfo") <= chain_info_calls_before_mismatch
                and time.monotonic() < deadline
            ):
                time.sleep(0.02)
            self.assertGreater(
                rpc.count("getblockchaininfo"),
                chain_info_calls_before_mismatch,
                "driver live trust check never ran",
            )
        finally:
            release_first_delivery.set()
            poll_thread.join(10)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(poll_thread.is_alive())
        self.assertEqual(refreshed, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIn("chain view became untrusted", str(errors[0]))
        self.assertEqual(second_notifications, [])
        self.assertIsNone(second.active_job)


if __name__ == "__main__":
    unittest.main()
