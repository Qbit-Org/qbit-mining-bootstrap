#!/usr/bin/env python3
"""Focused security/accounting tests for PRISM fanout transition authority."""

from __future__ import annotations

from decimal import Decimal
import queue
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.prism_coordinator import (
    ClientState,
    DEFAULT_PRISM_FANOUT_TRANSITION_LEASE_SECONDS,
    DEFAULT_PRISM_FANOUT_TRANSITION_MAX_JOBS_PER_CONNECTION,
    PRISM_CREDIT_POLICY_FANOUT_TRANSITION,
    PRISM_REJECTION_DUPLICATE_SHARE,
    PRISM_REJECTION_STALE_JOB,
    PRISM_REJECTION_TRANSITION_LEASE_EXPIRED,
    PRISM_REJECTION_TRANSITION_LEASE_REVOKED,
    PRISM_REJECTION_UNKNOWN_JOB,
    PRISM_REJECTION_REASON_IDS,
    PrismCoordinator,
    PrismJobContext,
    StratumError,
    WorkerIdentity,
    validate_fanout_transition_lease_limits,
)
from lab.prism.share_ledger import SingleWriterShareLedger


TIP_B = "bb" * 32
TIP_C = "cc" * 32
TIP_D = "dd" * 32


class FakeSocket:
    def shutdown(self, _how: int) -> None:
        return None

    def close(self) -> None:
        return None


class TipRpc:
    def __init__(self, tip: str) -> None:
        self.tip = tip
        self.submitblock_calls = 0

    def call(self, method: str, _params: list[object] | None = None) -> object:
        if method == "getbestblockhash":
            return self.tip
        if method == "submitblock":
            self.submitblock_calls += 1
            return None
        raise AssertionError(f"unexpected RPC {method}")


def worker(username: str) -> WorkerIdentity:
    return WorkerIdentity(
        username=username,
        payout_address=username,
        worker_name=None,
        script_pubkey_hex="5220" + "11" * 32,
        p2mr_program_hex="11" * 32,
    )


def client(connection_id: int, username: str) -> ClientState:
    state = ClientState(
        sock=FakeSocket(),  # type: ignore[arg-type]
        address=("127.0.0.1", connection_id),
        connection_id=connection_id,
        extranonce1_hex=f"{connection_id:08x}",
    )
    state.subscribed = True
    state.authorized = True
    state.authorization_generation = 1
    state.username = username
    state.worker = worker(username)
    return state


def context(
    state: ClientState,
    job_id: str,
    tip_hash: str,
    tip_generation: int,
    *,
    clean_jobs: bool = True,
) -> PrismJobContext:
    qbit_target = direct_stratum.difficulty_target(Decimal("1"))
    job = SimpleNamespace(
        job_id=job_id,
        share_target=qbit_target,
        share_difficulty=Decimal("1"),
        transaction_hexes=(),
        clean_jobs=clean_jobs,
    )
    return PrismJobContext(
        job=job,  # type: ignore[arg-type]
        template={
            "previousblockhash": tip_hash,
            "height": 10 + tip_generation,
            "coinbasevalue": 5_000_000_000,
        },
        shares_json=[],
        prior_balances=[],
        found_block={"network_difficulty": 1},
        share_weight=1,
        collection_only=False,
        worker=state.worker,  # type: ignore[arg-type]
        issued_at_ms=1_800_000_000_000 + tip_generation,
        template_fingerprint=f"{tip_generation:064x}",
        template_generation=tip_generation,
        payout_state_generation=tip_generation,
        connection_id=state.connection_id,
        authorization_generation=state.authorization_generation,
        difficulty_generation=tip_generation,
        tip_generation=tip_generation,
    )


def coordinator(
    clients: list[ClientState],
    *,
    lease_seconds: float = 30.0,
    max_jobs: int = 1,
    vardiff_enabled: bool = False,
) -> PrismCoordinator:
    server = PrismCoordinator.__new__(PrismCoordinator)
    server.lock = threading.RLock()
    server.stop_event = threading.Event()
    server.clients = set(clients)
    server.jobs = {}
    server.accepted_block_count = 0
    server.max_blocks = 10
    server.extranonce2_size = 8
    server.stale_grace_seconds = 0.0
    server.submit_tip_max_age_seconds = 1_000.0
    server.same_tip_job_retention_seconds = 30.0
    server.same_tip_job_retention_per_connection = 64
    server.current_tip_first_seen = (TIP_B, None)
    server.current_tip_generation = 1
    server.current_tip_published_at_ms = int(time.time() * 1000)
    server.current_tip_observed_monotonic = time.monotonic()
    server.latest_detected_tip = (TIP_B, 1)
    server.tip_refresh_divergence_started_monotonic = None
    server.current_tip_parent = None
    server.tip_template_snapshot = None
    server.fanout_transition_lease_seconds = lease_seconds
    server.fanout_transition_max_jobs_per_connection = max_jobs
    server.fanout_transition_credited_share_count = 0
    server.fanout_transition_lease_counts = {
        "armed": 0,
        "accepted": 0,
        "expired": 0,
        "revoked_delivery": 0,
        "revoked_authorization": 0,
        "capacity_evicted": 0,
    }
    server.submitted_share_count = 0
    server.stale_share_count = 0
    server.duplicate_share_count = 0
    server.low_difficulty_share_count = 0
    server.collection_block_submission_count = 0
    server.grace_credited_share_count = 0
    server.rejection_counts_by_reason = {
        reason: 0 for reason in PRISM_REJECTION_REASON_IDS
    }
    server.worker_metrics_limit = 100
    server.worker_metrics_lock = threading.Lock()
    server.worker_share_counts = {}
    server.worker_rejection_counts = {}
    server.recent_share_keys = set()
    server.share_weights_by_username = {}
    server.vardiff_config = vardiff.VardiffConfig(
        enabled=vardiff_enabled,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=Decimal("1"),
        max_difficulty=Decimal("1024"),
        retarget_interval_seconds=Decimal("90"),
        max_step_factor=Decimal("4"),
        startup_difficulty=Decimal("1"),
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("0.4"),
        retarget_tolerance=Decimal("0.25"),
    )
    server.ledger = SingleWriterShareLedger()
    server.share_writer_active = False
    server.hot_path_log_enabled = False
    server.block_candidate_queue = queue.Queue(maxsize=8)
    server.rpc = TipRpc(TIP_B)
    server._pool_ready_latched = False
    for state in clients:
        initial = context(state, f"job-b-{state.connection_id}", TIP_B, 1)
        state.active_job = initial
        state.active_job_ids = {initial.job.job_id}
        server.jobs[initial.job.job_id] = initial
        with server.lock:
            server._record_delivered_job_authority_locked(
                state,
                initial,
                time.monotonic(),
            )
    return server


def publish_tip(
    server: PrismCoordinator,
    tip_hash: str,
    *,
    now: float | None = None,
) -> None:
    now = time.monotonic() if now is None else now
    with server.lock:
        source_tip = server.current_tip_first_seen[0]
        target_generation = server.current_tip_generation + 1
        server._arm_fanout_transition_leases_locked(
            source_tip_hash=source_tip,
            target_tip_hash=tip_hash,
            target_tip_generation=target_generation,
            now=now,
        )
        server.current_tip_generation = target_generation
        server.current_tip_published_at_ms = int(time.time() * 1000)
        server.current_tip_first_seen = (tip_hash, now)
        server.current_tip_observed_monotonic = time.monotonic()
        server.latest_detected_tip = (tip_hash, target_generation)
        server.rpc.tip = tip_hash  # type: ignore[attr-defined]


def deliver_replacement(
    server: PrismCoordinator,
    state: ClientState,
    tip_hash: str,
    generation: int,
) -> PrismJobContext:
    replacement = context(
        state,
        f"job-{tip_hash[:2]}-{state.connection_id}",
        tip_hash,
        generation,
    )
    with server.lock:
        for job_id in tuple(state.active_job_ids):
            server.jobs.pop(job_id, None)
        state.active_job_ids = {replacement.job.job_id}
        state.active_job = replacement
        server.jobs[replacement.job.job_id] = replacement
        server._record_delivered_job_authority_locked(
            state,
            replacement,
            time.monotonic(),
        )
    return replacement


def submit(
    server: PrismCoordinator,
    state: ClientState,
    job_id: str,
    *,
    header_byte: str = "aa",
    hash_byte: str = "11",
    block_pass: bool = False,
) -> bool:
    submission = SimpleNamespace(
        header_hex=header_byte * 80,
        block_hash_hex=hash_byte * 32,
        share_pass=True,
        block_pass=block_pass,
    )
    with patch(
        "lab.prism.prism_coordinator.direct_stratum.assemble_submission",
        return_value=submission,
    ):
        return server.handle_submit(
            state,
            [state.username, job_id, "00" * 8, "00000001", "00000002"],
        )


class FanoutTransitionLeaseTests(unittest.TestCase):
    def test_configuration_defaults_disabled_and_requires_bounded_state(self) -> None:
        self.assertEqual(DEFAULT_PRISM_FANOUT_TRANSITION_LEASE_SECONDS, 0)
        self.assertEqual(
            DEFAULT_PRISM_FANOUT_TRANSITION_MAX_JOBS_PER_CONNECTION,
            1,
        )
        validate_fanout_transition_lease_limits(
            lease_seconds=0,
            max_jobs_per_connection=0,
            max_connections=0,
            production=True,
        )
        with self.assertRaisesRegex(SystemExit, "MAX_JOBS_PER_CONNECTION"):
            validate_fanout_transition_lease_limits(
                lease_seconds=1,
                max_jobs_per_connection=0,
                max_connections=1,
                production=False,
            )
        with self.assertRaisesRegex(SystemExit, "MAX_CONNECTIONS"):
            validate_fanout_transition_lease_limits(
                lease_seconds=1,
                max_jobs_per_connection=1,
                max_connections=0,
                production=True,
            )

        state = client(1, "miner-disabled")
        server = coordinator([state], lease_seconds=0)
        old_job_id = state.active_job.job.job_id  # type: ignore[union-attr]
        self.assertEqual(state.delivered_job_authorities, {})
        publish_tip(server, TIP_C)
        self.assertEqual(state.transition_submit_leases, {})
        with self.assertRaises(StratumError) as rejected:
            submit(server, state, old_job_id)
        self.assertEqual(rejected.exception.reason, PRISM_REJECTION_STALE_JOB)

    def test_fast_client_revokes_while_blocked_client_retains_exact_job(self) -> None:
        fast = client(1, "miner-fast")
        blocked = client(2, "miner-blocked")
        server = coordinator([fast, blocked])
        fast_old = fast.active_job.job.job_id  # type: ignore[union-attr]
        blocked_old = blocked.active_job.job.job_id  # type: ignore[union-attr]

        publish_tip(server, TIP_C)
        deliver_replacement(server, fast, TIP_C, 2)
        send_started = threading.Event()
        release_send = threading.Event()

        def blocked_socket_delivery() -> None:
            send_started.set()
            release_send.wait(2)
            deliver_replacement(server, blocked, TIP_C, 2)

        delivery = threading.Thread(target=blocked_socket_delivery)
        delivery.start()
        self.assertTrue(send_started.wait(1))
        with server.lock:
            fast_status, _ = server._transition_submit_lease_locked(
                fast, fast_old, now=time.monotonic()
            )
            blocked_status, _ = server._transition_submit_lease_locked(
                blocked, blocked_old, now=time.monotonic()
            )
        self.assertEqual(fast_status, "revoked")
        self.assertEqual(blocked_status, "eligible")

        release_send.set()
        delivery.join(2)
        self.assertFalse(delivery.is_alive())
        with server.lock:
            blocked_status, _ = server._transition_submit_lease_locked(
                blocked, blocked_old, now=time.monotonic()
            )
        self.assertEqual(blocked_status, "revoked")

    def test_blocked_client_prior_work_is_credited_once_with_receipt(self) -> None:
        state = client(1, "miner-a")
        server = coordinator([state])
        old_job_id = state.active_job.job.job_id  # type: ignore[union-attr]
        publish_tip(server, TIP_C)

        self.assertFalse(
            submit(
                server,
                state,
                old_job_id,
                block_pass=True,
            )
        )

        shares = server.ledger.all_shares()
        self.assertEqual(len(shares), 1)
        self.assertEqual(
            shares[0].credit_policy,
            PRISM_CREDIT_POLICY_FANOUT_TRANSITION,
        )
        receipt = shares[0].transition_receipt
        self.assertIsNotNone(receipt)
        self.assertEqual(receipt["connection_id"], state.connection_id)
        self.assertEqual(receipt["job_id"], old_job_id)
        self.assertEqual(receipt["source_tip_hash"], TIP_B)
        self.assertEqual(receipt["target_tip_hash"], TIP_C)
        self.assertEqual(receipt["classified_tip_hash"], TIP_C)
        self.assertEqual(server.fanout_transition_credited_share_count, 1)
        self.assertEqual(server.fanout_transition_lease_counts["accepted"], 1)
        self.assertEqual(server.rpc.submitblock_calls, 0)  # type: ignore[attr-defined]
        self.assertTrue(server.block_candidate_queue.empty())

    def test_prior_work_rejects_after_delivery_and_absolute_expiry(self) -> None:
        delivered = client(1, "miner-delivered")
        delivered_server = coordinator([delivered])
        delivered_job = delivered.active_job.job.job_id  # type: ignore[union-attr]
        publish_tip(delivered_server, TIP_C)
        deliver_replacement(delivered_server, delivered, TIP_C, 2)
        with self.assertRaises(StratumError) as revoked:
            submit(delivered_server, delivered, delivered_job)
        self.assertEqual(
            revoked.exception.reason,
            PRISM_REJECTION_TRANSITION_LEASE_REVOKED,
        )

        expired = client(2, "miner-expired")
        expired_server = coordinator([expired], lease_seconds=5.0)
        expired_job = expired.active_job.job.job_id  # type: ignore[union-attr]
        publish_tip(expired_server, TIP_C, now=time.monotonic() - 6.0)
        with self.assertRaises(StratumError) as expiry:
            submit(expired_server, expired, expired_job)
        self.assertEqual(
            expiry.exception.reason,
            PRISM_REJECTION_TRANSITION_LEASE_EXPIRED,
        )
        with self.assertRaises(StratumError) as repeated_expiry:
            submit(
                expired_server,
                expired,
                expired_job,
                header_byte="ab",
            )
        self.assertEqual(
            repeated_expiry.exception.reason,
            PRISM_REJECTION_TRANSITION_LEASE_EXPIRED,
        )
        self.assertEqual(expired_server.ledger.all_shares(), [])

    def test_rapid_tip_churn_does_not_renew_or_accumulate(self) -> None:
        state = client(1, "miner-a")
        server = coordinator([state], lease_seconds=20.0)
        old_job_id = state.active_job.job.job_id  # type: ignore[union-attr]
        publish_tip(server, TIP_C, now=100.0)
        first = state.transition_submit_leases[old_job_id]

        publish_tip(server, TIP_D, now=110.0)
        retained = state.transition_submit_leases[old_job_id]
        self.assertEqual(len(state.transition_submit_leases), 1)
        self.assertEqual(retained.armed_monotonic, 100.0)
        self.assertEqual(retained.target_tip_hash, TIP_C)
        self.assertEqual(retained.target_tip_generation, 2)
        self.assertEqual(first.expires_monotonic, retained.expires_monotonic)
        with server.lock:
            status, _ = server._transition_submit_lease_locked(
                state,
                old_job_id,
                now=120.001,
            )
        self.assertEqual(status, "expired")

    def test_late_superseded_delivery_rebases_without_sliding(self) -> None:
        state = client(1, "miner-a")
        server = coordinator([state], lease_seconds=20.0)
        base = time.monotonic() - 2.0
        publish_tip(server, TIP_C, now=base)
        publish_tip(server, TIP_D, now=base + 1.0)

        late_c = deliver_replacement(server, state, TIP_C, 2)
        late_c_job_id = late_c.job.job_id
        with server.lock:
            status, lease = server._transition_submit_lease_locked(
                state,
                late_c_job_id,
                now=base + 2.0,
            )

        self.assertEqual(status, "eligible")
        self.assertIsNotNone(lease)
        self.assertEqual(lease.authority.tip_hash, TIP_C)
        self.assertEqual(lease.authority.tip_generation, 2)
        self.assertEqual(lease.target_tip_hash, TIP_D)
        self.assertEqual(lease.target_tip_generation, 3)
        self.assertEqual(lease.armed_monotonic, base + 1.0)
        self.assertEqual(lease.expires_monotonic, base + 21.0)
        self.assertEqual(len(state.transition_submit_leases), 1)

        self.assertFalse(submit(server, state, late_c_job_id))
        receipt = server.ledger.all_shares()[0].transition_receipt
        self.assertEqual(receipt["source_tip_hash"], TIP_C)
        self.assertEqual(receipt["source_tip_generation"], 2)
        self.assertEqual(receipt["target_tip_hash"], TIP_D)
        self.assertEqual(receipt["target_tip_generation"], 3)

    def test_disconnect_and_reconnect_cannot_reuse_authority(self) -> None:
        original = client(1, "miner-a")
        server = coordinator([original])
        old_job_id = original.active_job.job.job_id  # type: ignore[union-attr]
        publish_tip(server, TIP_C)
        server._cancel_pending_initial_job_locked = (  # type: ignore[method-assign]
            lambda *_args, **_kwargs: None
        )
        server._retain_current_collection_refresh_if_unrepresented = (  # type: ignore[method-assign]
            lambda: None
        )

        server.disconnect_client(original)
        self.assertEqual(original.delivered_job_authorities, {})
        self.assertEqual(original.transition_submit_leases, {})

        reconnect = client(2, "miner-a")
        with server.lock:
            status, lease = server._transition_submit_lease_locked(
                reconnect,
                old_job_id,
                now=time.monotonic(),
            )
        self.assertEqual((status, lease), ("missing", None))

        server.clients.add(reconnect)
        with self.assertRaises(StratumError) as unknown:
            submit(server, reconnect, old_job_id)
        self.assertEqual(unknown.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)

    def test_duplicate_transition_share_is_not_accounted_twice(self) -> None:
        state = client(1, "miner-a")
        server = coordinator([state], vardiff_enabled=True)
        old_job_id = state.active_job.job.job_id  # type: ignore[union-attr]
        publish_tip(server, TIP_C)

        self.assertFalse(submit(server, state, old_job_id))
        with self.assertRaises(StratumError) as duplicate:
            submit(server, state, old_job_id)

        self.assertEqual(duplicate.exception.reason, PRISM_REJECTION_DUPLICATE_SHARE)
        self.assertEqual(len(server.ledger.all_shares()), 1)
        self.assertEqual(server.fanout_transition_credited_share_count, 1)
        self.assertEqual(server.fanout_transition_lease_counts["accepted"], 1)
        self.assertEqual(state.vardiff_window_submitted, 2)
        self.assertEqual(state.vardiff_window_accepted, 1)

    def test_block_worthy_prior_job_never_enters_candidate_state(self) -> None:
        state = client(1, "miner-highdiff")
        server = coordinator([state])
        old_job_id = state.active_job.job.job_id  # type: ignore[union-attr]
        publish_tip(server, TIP_C)

        self.assertFalse(
            submit(
                server,
                state,
                old_job_id,
                header_byte="ab",
                hash_byte="22",
                block_pass=True,
            )
        )

        self.assertEqual(len(server.ledger.all_shares()), 1)
        self.assertTrue(server.block_candidate_queue.empty())
        self.assertEqual(server.rpc.submitblock_calls, 0)  # type: ignore[attr-defined]
        self.assertEqual(server.accepted_block_count, 0)

    def test_delivered_and_transition_state_are_capacity_bounded(self) -> None:
        state = client(1, "miner-a")
        server = coordinator([state], max_jobs=2)
        with server.lock:
            for index in range(3):
                delivered = context(
                    state,
                    f"same-tip-{index}",
                    TIP_B,
                    1,
                    clean_jobs=False,
                )
                server._record_delivered_job_authority_locked(
                    state,
                    delivered,
                    10.0 + index,
                )
        self.assertEqual(
            list(state.delivered_job_authorities),
            ["same-tip-1", "same-tip-2"],
        )

        publish_tip(server, TIP_C, now=20.0)
        publish_tip(server, TIP_D, now=21.0)
        self.assertLessEqual(len(state.delivered_job_authorities), 2)
        self.assertLessEqual(len(state.transition_submit_leases), 2)
        self.assertLessEqual(
            len(state.delivered_job_authorities)
            + len(state.transition_submit_leases),
            4,
        )
        self.assertEqual(
            set(state.transition_submit_leases),
            {"same-tip-1", "same-tip-2"},
        )


if __name__ == "__main__":
    unittest.main()
