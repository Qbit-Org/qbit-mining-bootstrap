#!/usr/bin/env python3
"""Per-template job-build cache, cached health snapshot, and latency metrics."""

from __future__ import annotations

import threading
import time
import unittest
from dataclasses import dataclass, replace as dataclass_replace
from decimal import Decimal

from lab.auxpow import vardiff
from lab.prism import direct_stratum
from lab.prism.prism_coordinator import (
    ClientState,
    PRISM_JOB_EXTRANONCE1_PLACEHOLDER_HEX,
    PRISM_REJECTION_REASON_IDS,
    PrismCoordinator,
    TemplateRefreshBlocked,
    WorkerIdentity,
    default_prism_coinbase_tag_hex,
    qbit_template_fingerprint,
)
from lab.prism.share_ledger import SingleWriterShareLedger

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
    state.job_update_lock = threading.RLock()
    state.send_lock = threading.Lock()
    return state


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
    server.latest_bundle = None
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
            "signed_coinbase_manifest": {
                "manifest": {
                    "coinbase_tx_hex": synthetic_manifest_coinbase_hex(suffix_hex),
                }
            },
        }

    server.build_audit_bundle = fake_build_audit_bundle  # type: ignore[method-assign]
    return recorded


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
        self.assertIs(contexts[0].bundle, contexts[1].bundle)
        self.assertIs(contexts[0].shares_json, contexts[1].shares_json)

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

    def test_zero_ttl_disables_bundle_cache(self) -> None:
        server, _ = coordinator()
        recorded = install_fake_bundle_builder(server)
        server.job_bundle_cache_seconds = 0.0

        server.build_job_for_client(client(1), clean_jobs=True)
        server.build_job_for_client(client(2), clean_jobs=True)

        self.assertEqual(recorded["calls"], 2)

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
        self.assertIs(context_a1.bundle, context_a2.bundle)
        self.assertIsNot(context_a1.bundle, context_b.bundle)

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

    def test_ready_empty_collection_bundle_rebuilds_on_cache_hit(self) -> None:
        ledger = ReadyLedgerWithEmptyFirstSnapshot()
        server, _ = coordinator(ledger=ledger)
        recorded = install_fake_bundle_builder(server)
        state = client(1)

        empty_context = server.build_job_for_client(state, clean_jobs=True)
        ready_context = server.build_job_for_client(state, clean_jobs=True)

        self.assertTrue(empty_context.collection_only)
        self.assertFalse(ready_context.collection_only)
        self.assertEqual(recorded["calls"], 2)
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
        server.tip_refresh_max_workers = 1
        server.reorg_reconciler_enabled = True
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        chain_view_trusted = True
        trust_checks: list[bool] = []

        def current_tip_trusted(*, expected_tip_hash: str | None = None) -> bool:
            self.assertEqual(expected_tip_hash, server.rpc.tip)
            trust_checks.append(chain_view_trusted)
            return chain_view_trusted

        server.ensure_reorg_reconciled_for_current_tip = current_tip_trusted  # type: ignore[method-assign]
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
            chain_view_trusted = False
        finally:
            release_first.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertEqual(refreshed, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIn("chain view became untrusted", str(errors[0]))
        self.assertEqual(trust_checks, [True, False])
        self.assertIsNotNone(first.active_job)
        self.assertIsNone(second.active_job)
        self.assertEqual(second_sent, [])

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
        self.assertIn("tip changed while prepared work was queued", str(errors[0]))
        self.assertIsNotNone(first.active_job)
        self.assertIsNone(second.active_job)
        self.assertEqual(second_sent, [])

    def test_multiworker_cancel_releases_client_lock_while_draining_peer(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        server.tip_refresh_max_workers = 2
        server.reorg_reconciler_enabled = True
        server.ensure_reorg_reconciled_for_tip = lambda _tip: True  # type: ignore[method-assign]
        admitted = client(1)
        invalidating = client(2)
        queued = client(3)
        server.clients = [admitted, invalidating, queued]  # type: ignore[assignment]
        admitted_send_started = threading.Event()
        release_admitted_send = threading.Event()
        invalidation_started = threading.Event()
        invalidation_finished = threading.Event()
        queued_sent: list[dict[str, object]] = []
        worker_local = threading.local()
        original_send_prepared_job = server.send_prepared_job

        def controlled_send_prepared_job(
            state: ClientState,
            *args: object,
            **kwargs: object,
        ) -> object:
            worker_local.client = state
            try:
                return original_send_prepared_job(state, *args, **kwargs)
            except TemplateRefreshBlocked:
                if state is invalidating:
                    invalidation_finished.set()
                raise

        def current_tip_trusted(*, expected_tip_hash: str | None = None) -> bool:
            self.assertEqual(expected_tip_hash, server.rpc.tip)
            state = worker_local.client
            if state is admitted:
                return True
            if state is invalidating:
                self.assertTrue(admitted_send_started.wait(5))
                invalidation_started.set()
                return False
            return True

        def admitted_send(payload: dict[str, object]) -> None:
            if payload["method"] == "mining.notify":
                admitted_send_started.set()
                self.assertTrue(release_admitted_send.wait(5))

        server.send_prepared_job = controlled_send_prepared_job  # type: ignore[method-assign]
        server.ensure_reorg_reconciled_for_current_tip = current_tip_trusted  # type: ignore[method-assign]
        admitted.send = admitted_send  # type: ignore[method-assign]
        invalidating.send = lambda _payload: self.fail("invalidating client received work")  # type: ignore[method-assign]
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
            self.assertTrue(invalidation_started.wait(5))
            self.assertTrue(invalidation_finished.wait(5))
            lock_acquired = invalidating.job_update_lock.acquire(timeout=0.1)
            self.assertTrue(lock_acquired)
            if lock_acquired:
                invalidating.job_update_lock.release()
            # The coordinator still waits for the admitted peer delivery, but
            # queued workers observe cancellation without taking client state.
            self.assertTrue(thread.is_alive())
            self.assertIsNone(queued.active_job)
        finally:
            release_admitted_send.set()
            thread.join(5)
            server.shutdown_tip_refresh_executor()

        self.assertFalse(thread.is_alive())
        self.assertTrue(invalidation_finished.is_set())
        self.assertEqual(refreshed, [])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], TemplateRefreshBlocked)
        self.assertIsNotNone(admitted.active_job)
        self.assertIsNone(invalidating.active_job)
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
        bundle = server.prepare_tip_refresh_bundle(snapshot, clients)
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

    def test_template_fingerprint_race_aborts_before_fanout(self) -> None:
        server, _ = coordinator()
        install_fake_bundle_builder(server)
        states = [client(1), client(2)]
        sent: list[dict[str, object]] = []
        for state in states:
            state.send = sent.append  # type: ignore[method-assign]
        server.clients = set(states)
        original_shared_job_bundle = server.shared_job_bundle

        def race_artifacts(artifacts: object, identity: WorkerIdentity) -> object:
            bundle = original_shared_job_bundle(artifacts, identity)
            with server._job_cache_lock:
                server._template_artifacts = dataclass_replace(
                    server._template_artifacts,
                    fingerprint="ff" * 32,
                )
            return bundle

        server.shared_job_bundle = race_artifacts  # type: ignore[method-assign]

        with self.assertRaisesRegex(TemplateRefreshBlocked, "cache changed"):
            server.poll_qbit_tip_template_once()

        self.assertEqual(sent, [])
        self.assertIsNone(getattr(server, "current_tip_first_seen", None))
        self.assertIsNone(server.tip_template_snapshot)


class HealthSnapshotTests(unittest.TestCase):
    def test_health_payload_uses_aggregate_stats_not_all_shares(self) -> None:
        ledger = FakeLedger()
        server, _ = coordinator(ledger=ledger)
        payload = server.health_payload()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["accepted_share_count"], 3)
        self.assertEqual(payload["ready_miner_count"], 3)
        self.assertGreaterEqual(ledger.stats_calls, 1)

    def test_cached_health_payload_computes_inline_without_refresher(self) -> None:
        server, _ = coordinator()
        status, payload = server.cached_health_payload()
        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])

    def test_cached_health_payload_serves_snapshot_and_flags_staleness(self) -> None:
        server, _ = coordinator()
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

        metrics = server.metrics_payload()

        self.assertIn('qbit_prism_job_build_seconds_bucket{le="0.5"} 1', metrics)
        self.assertIn('qbit_prism_job_build_seconds_bucket{le="+Inf"} 1', metrics)
        self.assertIn("qbit_prism_job_build_seconds_count 1", metrics)
        self.assertIn('qbit_prism_job_cache_hits_total{cache="bundle"} 1', metrics)
        self.assertIn('qbit_prism_job_cache_misses_total{cache="bundle"} 1', metrics)
        self.assertIn('qbit_prism_job_build_phase_seconds_total{phase="bundle"} 0.2', metrics)
        self.assertIn("qbit_prism_connected_clients 0", metrics)


if __name__ == "__main__":
    unittest.main()
