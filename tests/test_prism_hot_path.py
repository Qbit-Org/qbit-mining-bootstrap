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
from concurrent.futures import ThreadPoolExecutor
import io
import threading
import time
import unittest
from decimal import Decimal
from unittest import mock

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


class CountingRLock:
    """RLock test double that counts acquisitions by the calling thread."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._local = threading.local()
        self.first_acquisition = threading.Event()

    def acquire(self, *args: object, **kwargs: object) -> bool:
        acquired = self._lock.acquire(*args, **kwargs)
        if acquired:
            self._local.acquisitions = self.acquisitions_for_current_thread() + 1
            self.first_acquisition.set()
        return acquired

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> CountingRLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()

    def acquisitions_for_current_thread(self) -> int:
        return int(getattr(self._local, "acquisitions", 0))


class NotifyingRLock:
    """RLock test double that exposes when another path attempts admission."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.attempted = threading.Event()

    def acquire(self, *args: object, **kwargs: object) -> bool:
        self.attempted.set()
        return self._lock.acquire(*args, **kwargs)

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> NotifyingRLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


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
    state.vardiff_lock = threading.RLock()
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
    server._recent_share_lock = threading.Lock()
    server._share_accounting_lock = threading.Lock()
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
    server._ensure_tip_refresh_service().reconfigure_for_test(
        blockpoll_seconds=server.blockpoll_seconds,
        failure_holdoff_seconds=0.0,
    )
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
    server._ensure_bundle_compiler().build_audit_bundle = (  # type: ignore[method-assign]
        fake_build_audit_bundle
    )
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
    def test_submit_tip_selection_is_atomic_with_observation_update(self) -> None:
        server, rpc = coordinator()
        observed_at = time.monotonic()
        with server.lock:
            server.current_tip_first_seen = (OLD_TIP, None)
            server.current_tip_observed_monotonic = observed_at

        freshness_check_started = threading.Event()
        release_freshness_check = threading.Event()
        tip_update_finished = threading.Event()
        selected: list[str] = []
        errors: list[BaseException] = []
        real_monotonic = time.monotonic

        def controlled_monotonic() -> float:
            if threading.current_thread().name == "submit-tip":
                freshness_check_started.set()
                if not release_freshness_check.wait(5):
                    raise AssertionError("freshness check was not released")
                return observed_at
            return real_monotonic()

        def select_tip() -> None:
            try:
                selected.append(server.submit_stale_check_tip())
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        def update_tip() -> None:
            with server.lock:
                server.current_tip_first_seen = (NEW_TIP, real_monotonic())
                server.current_tip_observed_monotonic = real_monotonic()
            tip_update_finished.set()

        submit_thread = threading.Thread(target=select_tip, name="submit-tip")
        update_thread = threading.Thread(target=update_tip, name="tip-observer")
        with mock.patch(
            "lab.prism.prism_coordinator.time.monotonic",
            side_effect=controlled_monotonic,
        ):
            submit_thread.start()
            try:
                self.assertTrue(freshness_check_started.wait(5))
                update_thread.start()
                # A new observation cannot supersede the selected hash before
                # submit_stale_check_tip returns its point-in-time result.
                self.assertFalse(tip_update_finished.wait(0.1))
            finally:
                release_freshness_check.set()
                submit_thread.join(5)
                if update_thread.ident is not None:
                    update_thread.join(5)

        self.assertFalse(submit_thread.is_alive())
        self.assertFalse(update_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(selected, [OLD_TIP])
        self.assertTrue(tip_update_finished.is_set())
        self.assertEqual(rpc.count("getbestblockhash"), 0)

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
        server._ensure_tip_refresh_service().reconfigure_for_test(
            submit_tip_max_age_seconds=server.submit_tip_max_age_seconds
        )
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

    def test_submit_keeps_published_tip_during_bounded_replacement_build(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.submit_tip_max_age_seconds = 10.0
        server._ensure_tip_refresh_service().reconfigure_for_test(
            submit_tip_max_age_seconds=server.submit_tip_max_age_seconds
        )
        server.template_refresh_failure_exit_seconds = 120.0
        server._ensure_tip_refresh_service().reconfigure_for_test(
            failure_exit_seconds=server.template_refresh_failure_exit_seconds
        )
        state = client(1)
        context = register_job(server, state)
        params = submit_params(state, context)
        # The ordinary 10-second observation freshness has expired, but a new
        # tip is only detected: replacement work has not been published yet.
        observe_tip(server, OLD_TIP, age_seconds=11.0)
        rpc.tip = NEW_TIP
        self.assertTrue(server.observe_tip_for_refresh(NEW_TIP))
        rpc.calls.clear()

        server.handle_submit(state, params)

        self.assertEqual(rpc.count("getbestblockhash"), 0)
        self.assertEqual(len(server.ledger.appended), 1)
        self.assertEqual(server.current_tip_first_seen[0], OLD_TIP)
        self.assertEqual(server.latest_detected_tip[0], NEW_TIP)

    def test_unpublished_divergence_lease_does_not_renew_and_expires(self) -> None:
        server, rpc = coordinator()
        server.submit_tip_max_age_seconds = 10.0
        server._ensure_tip_refresh_service().reconfigure_for_test(
            submit_tip_max_age_seconds=server.submit_tip_max_age_seconds
        )
        server.template_refresh_failure_exit_seconds = 120.0
        server._ensure_tip_refresh_service().reconfigure_for_test(
            failure_exit_seconds=server.template_refresh_failure_exit_seconds
        )
        observe_tip(server, OLD_TIP, age_seconds=11.0)
        rpc.tip = NEW_TIP
        self.assertTrue(server.observe_tip_for_refresh(NEW_TIP))
        divergence_started = server.tip_refresh_divergence_started_monotonic
        self.assertIsNotNone(divergence_started)

        newest_tip = "44" * 32
        rpc.tip = newest_tip
        self.assertTrue(server.observe_tip_for_refresh(newest_tip))
        self.assertEqual(
            server.tip_refresh_divergence_started_monotonic,
            divergence_started,
        )
        with server.lock:
            server.tip_refresh_divergence_started_monotonic = (
                time.monotonic() - 121.0
            )
        rpc.calls.clear()

        self.assertEqual(server.submit_stale_check_tip(), newest_tip)
        self.assertEqual(rpc.count("getbestblockhash"), 1)

    def test_submit_tip_max_age_zero_restores_per_share_rpc(self) -> None:
        server, rpc = coordinator()
        install_fake_bundle_builder(server)
        server.submit_tip_max_age_seconds = 0.0
        server._ensure_tip_refresh_service().reconfigure_for_test(
            submit_tip_max_age_seconds=server.submit_tip_max_age_seconds
        )
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


class HotPathLockIsolationTests(unittest.TestCase):
    def test_first_touch_installs_one_hot_path_lock_set(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.extranonce2_size = EXTRANONCE2_SIZE
        state = ClientState.__new__(ClientState)
        start = threading.Barrier(32)

        def initialize(_index: int) -> tuple[object, object, object, bool]:
            start.wait(timeout=2)
            vardiff_lock = server._client_vardiff_lock(state)
            reserved = server._reserve_recent_share_key(("same", "header"))
            return (
                vardiff_lock,
                server._ensure_share_submission_service().recent_shares,
                server._share_accounting_lock,
                reserved,
            )

        with ThreadPoolExecutor(max_workers=32) as executor:
            results = list(executor.map(initialize, range(32)))

        first = results[0]
        self.assertTrue(all(result[0] is first[0] for result in results))
        self.assertTrue(all(result[1] is first[1] for result in results))
        self.assertTrue(all(result[2] is first[2] for result in results))
        self.assertEqual(sum(result[3] for result in results), 1)

    def test_idle_vardiff_wait_cannot_hold_coordinator_lock(self) -> None:
        server, _rpc = coordinator()
        state = client(1)
        vardiff_lock = NotifyingRLock()
        state.vardiff_lock = vardiff_lock  # type: ignore[assignment]
        active_job = object()
        state.active_job = active_job
        server.clients.add(state)
        server.latest_detected_tip = None
        request = type("IdleRequest", (), {})()
        request.client = state
        request.connection_id = state.connection_id
        request.worker = state.worker
        request.active_job = active_job
        request.current_difficulty = state.share_difficulty
        request.window_started_monotonic = state.vardiff_window_started_monotonic
        attempting = threading.Event()

        def validate_idle_request() -> None:
            attempting.set()
            server._idle_request_skip_reason(request)

        with vardiff_lock:
            vardiff_lock.attempted.clear()
            validator = threading.Thread(target=validate_idle_request)
            validator.start()
            self.assertTrue(attempting.wait(1))
            self.assertTrue(vardiff_lock.attempted.wait(1))
            acquired = server.lock.acquire(timeout=0.25)
            self.assertTrue(acquired)
            if acquired:
                server.lock.release()
        validator.join(1)
        self.assertFalse(validator.is_alive())

    def test_job_delivery_vardiff_wait_cannot_hold_coordinator_lock(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        vardiff_lock = NotifyingRLock()
        state.vardiff_lock = vardiff_lock  # type: ignore[assignment]

        with vardiff_lock:
            vardiff_lock.attempted.clear()
            sender = threading.Thread(
                target=lambda: server.maybe_send_job(state, clean_jobs=True),
            )
            sender.start()
            self.assertTrue(vardiff_lock.attempted.wait(2))
            acquired = server.lock.acquire(timeout=0.25)
            self.assertTrue(acquired)
            if acquired:
                server.lock.release()
        sender.join(2)
        self.assertFalse(sender.is_alive())

    def test_real_submissions_cannot_starve_tip_publication(self) -> None:
        """A 600-client/100-submit burst admits tip publication promptly."""
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        clients = [client(index + 1) for index in range(600)]
        contexts = [register_job(server, state) for state in clients]
        with server.lock:
            server.clients.update(clients)
        params = [
            submit_params(state, context)
            for state, context in zip(clients[:100], contexts[:100], strict=True)
        ]
        counting_lock = CountingRLock()
        server.lock = counting_lock  # type: ignore[assignment]

        # Hold accepted submissions after their single control snapshot so tip
        # publication is guaranteed to compete with a live submit burst, not
        # merely start after fast in-memory ledger commits have all finished.
        ledger_entered = threading.Event()
        release_ledger = threading.Event()
        append_batch = server.ledger.append_batch

        def blocking_append_batch(
            entries: list[tuple[PendingShare, dict[str, object] | None]],
        ) -> list[AcceptedShareRecord]:
            ledger_entered.set()
            if not release_ledger.wait(2):
                raise AssertionError("tip publication did not release share commits")
            return append_batch(entries)

        server.ledger.append_batch = blocking_append_batch  # type: ignore[method-assign]

        start = threading.Event()
        ready = threading.Event()
        active_lock = threading.Lock()
        ready_count = 0
        active_count = 0
        active_during_publication = 0
        publication_wait = float("inf")

        def submit_share(index: int) -> int:
            nonlocal ready_count, active_count
            with active_lock:
                ready_count += 1
                active_count += 1
                if ready_count == 64:
                    ready.set()
            self.assertTrue(start.wait(2))
            before = counting_lock.acquisitions_for_current_thread()
            try:
                try:
                    server.handle_submit(clients[index], params[index])
                except StratumError as exc:
                    # Submits whose admission snapshot follows the publication
                    # correctly observe the old job as stale.
                    if exc.reason != PRISM_REJECTION_STALE_JOB:
                        raise
                return counting_lock.acquisitions_for_current_thread() - before
            finally:
                with active_lock:
                    active_count -= 1

        def publish_tip_state() -> None:
            nonlocal active_during_publication, publication_wait
            try:
                self.assertTrue(ledger_entered.wait(2))
                started = time.monotonic()
                with counting_lock:
                    publication_wait = time.monotonic() - started
                    server.current_tip_first_seen = (NEW_TIP, time.monotonic())
                    server.current_tip_observed_monotonic = time.monotonic()
                    with active_lock:
                        active_during_publication = active_count
            finally:
                release_ledger.set()

        with ThreadPoolExecutor(
            max_workers=64,
            thread_name_prefix="share-submit",
        ) as executor:
            futures = [executor.submit(submit_share, index) for index in range(100)]
            self.assertTrue(ready.wait(2))
            tip_thread = threading.Thread(
                target=publish_tip_state,
                name="tip-publication",
            )
            tip_thread.start()
            start.set()
            acquisitions_per_submit = [future.result(timeout=5) for future in futures]
            tip_thread.join(2)

        self.assertFalse(tip_thread.is_alive())
        self.assertGreater(active_during_publication, 0)
        self.assertLess(publication_wait, 0.5)
        # Before the consolidated snapshot, every normal handle_submit crossed
        # this lock separately for pool state, job lookup, and tip selection.
        self.assertEqual(acquisitions_per_submit, [1] * 100)
        self.assertEqual(server.submitted_share_count, 100)

    def test_concurrent_vardiff_accounting_is_exact_without_coordinator_lock(self) -> None:
        server, _rpc = coordinator()
        state = client(1)
        state.vardiff_window_started_monotonic = time.monotonic()
        fake_job = type("FakeJob", (), {"share_difficulty": Decimal("2")})()

        def account(_index: int) -> None:
            server.note_vardiff_submitted_share(state)
            server.note_vardiff_accepted_share(state, fake_job)  # type: ignore[arg-type]

        with ThreadPoolExecutor(max_workers=32) as executor:
            list(executor.map(account, range(1_000)))

        with state.vardiff_lock:
            self.assertEqual(state.vardiff_window_submitted, 1_000)
            self.assertEqual(state.vardiff_window_accepted, 1_000)
            self.assertEqual(state.vardiff_window_work, Decimal("2000"))
        self.assertEqual(server.submitted_share_count, 1_000)

    def test_slow_stratum_socket_cannot_hold_coordinator_lock(self) -> None:
        server, _rpc = coordinator()
        install_fake_bundle_builder(server)
        state = client(1)
        send_started = threading.Event()
        release_send = threading.Event()

        def slow_send(_payload: dict[str, object]) -> None:
            send_started.set()
            self.assertTrue(release_send.wait(2))

        state.send = slow_send  # type: ignore[method-assign]
        sender = threading.Thread(
            target=lambda: server.maybe_send_job(state, clean_jobs=True)
        )
        sender.start()
        try:
            self.assertTrue(send_started.wait(2))
            acquired = server.lock.acquire(timeout=0.25)
            self.assertTrue(acquired)
            if acquired:
                server.lock.release()
        finally:
            release_send.set()
            sender.join(5)
        self.assertFalse(sender.is_alive())


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
        server._ensure_tip_refresh_service().reconfigure_for_test(
            max_workers=server.tip_refresh_max_workers
        )
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
