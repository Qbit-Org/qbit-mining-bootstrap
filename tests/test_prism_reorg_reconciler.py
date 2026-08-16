from __future__ import annotations

import inspect
import threading
import time
import unittest
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace

from lab.prism.prism_coordinator import PrismCoordinator
from lab.prism.reorg_reconciler import (
    ReorgPorts,
    ReorgReconcilerService,
    qbit_chain_view_untrusted,
)
from tests import prism_vardiff_test_support as support


class FakeLedger:
    def __init__(self) -> None:
        self.inactivated: list[str] = []
        self.reactivated: list[str] = []

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        assert active_tip_height == 10
        return [
            {"block_height": 8, "block_hash": "aa", "chain_state": "confirmed"},
            {"block_height": 9, "block_hash": "bb", "chain_state": "inactive"},
        ]

    def mark_pool_block_inactive(self, **kwargs: object) -> dict[str, int]:
        self.inactivated.append(str(kwargs["block_hash"]))
        return {"inactive_count": 1}

    def reactivate_pool_block(self, **kwargs: object) -> dict[str, int]:
        self.reactivated.append(str(kwargs["block_hash"]))
        return {"reactivated_count": 1}

    def mark_mature_pool_payouts(self, **_kwargs: object) -> dict[str, int]:
        return {"matured_count": 2}


def make_ports(
    *,
    ledger: object | None = None,
    publish: object = 7,
    chain_untrusted: bool = False,
    ensure_tip: object = None,
) -> tuple[ReorgPorts, list[tuple[str, object]]]:
    events: list[tuple[str, object]] = []
    active_ledger = FakeLedger() if ledger is None else ledger
    state_lock = threading.RLock()

    def rpc_call(method: str, params: object = None) -> object:
        if method == "getbestblockhash":
            return "tip"
        if method == "getblockcount":
            return 10
        if method == "getblockhash":
            return {8: "not-aa", 9: "bb"}[int(params[0])]  # type: ignore[index]
        raise AssertionError(method)

    @contextmanager
    def publication_guard():
        events.append(("guard", "enter"))
        try:
            yield
        finally:
            events.append(("guard", "exit"))

    def prepared_candidate(captured, **kwargs):
        events.append(("prepare", kwargs))
        return SimpleNamespace(captured=captured)

    return (
        ReorgPorts(
            rpc_call=rpc_call,
            ledger=lambda: active_ledger,
            ensure_job_cache_state=lambda: events.append(("ensure", None)),
            state_lock=lambda: state_lock,
            source_tip=lambda: "tip",
            reserve_external_tip=lambda tip: events.append(("reserve", tip)),
            max_supersession_retries=lambda: 1,
            prepare_lock=nullcontext,
            capture_source=lambda: (0, 0, "tip", "test", 0.0),
            prepared_candidate=prepared_candidate,
            captured_publication_required=lambda _captured: False,
            block_publication=lambda **kwargs: events.append(("block", kwargs)),
            publication_guard=publication_guard,
            publish_candidate=lambda _candidate: publish,  # type: ignore[return-value]
            observe_preparation=lambda elapsed: events.append(("observe", elapsed)),
            chain_view_untrusted=lambda: chain_untrusted,
            reorg_proof_snapshot=lambda: ("tip", 0),
            flight_wait_seconds=lambda: 30.0,
            prefetch_join_timeout_seconds=lambda: 20.0,
            reconcile_with_admission=lambda **kwargs: {
                "tip": kwargs.get("tip_hash"),
                "untrusted": chain_untrusted,
            },
            reconcile_serialized=lambda **kwargs: {
                "serialized": kwargs,
                "untrusted": False,
                "superseded": False,
            },
            ensure_tip=(
                ensure_tip
                if ensure_tip is not None
                else lambda tip: events.append(("ensure_tip", tip)) or True
            ),
        ),
        events,
    )


class ReorgReconcilerServiceTests(unittest.TestCase):
    def test_fresh_memo_still_checks_live_chain_trust(self) -> None:
        ports, events = make_ports()
        service = ReorgReconcilerService(ports, cache_seconds=5.0)
        service._reorg_reconcile_trusted_memo["tip"] = time.monotonic()
        self.assertTrue(service.ensure_current())
        # The memo satisfied the caller; no serialized pass ran.
        self.assertNotIn(("ensure_tip", "tip"), events)
        self.assertEqual(
            service.reorg_reconcile_lookup_snapshot()[("job_build", "memo_hit")],
            1,
        )

        untrusted_ports, untrusted_events = make_ports(chain_untrusted=True)
        untrusted = ReorgReconcilerService(untrusted_ports, cache_seconds=5.0)
        untrusted._reorg_reconcile_trusted_memo["tip"] = time.monotonic()
        # An arriving reorg (headers ahead of the validated tip) must force
        # the full pass immediately, memo freshness notwithstanding.
        untrusted.ensure_current()
        self.assertIn(("ensure_tip", "tip"), untrusted_events)
        self.assertEqual(
            untrusted.reorg_reconcile_lookup_snapshot()[("job_build", "serial")],
            1,
        )

    def test_reconcile_owns_chain_mutations_counts_and_publication(self) -> None:
        ledger = FakeLedger()
        ports, events = make_ports(ledger=ledger)
        service = ReorgReconcilerService(ports)

        summary = service.reconcile(tip_hash="tip", force_publish=True)

        self.assertEqual(ledger.inactivated, ["aa"])
        self.assertEqual(ledger.reactivated, ["bb"])
        self.assertEqual(summary["published_generation"], 7)
        self.assertEqual(summary["inactive_blocks"], 1)
        self.assertEqual(summary["reactivated_blocks"], 1)
        self.assertEqual(summary["matured_payouts"], 2)
        self.assertIn(("block", {"force": True}), events)
        # PR 75 publication-order guard wraps reactivation.
        guard_enter = events.index(("guard", "enter"))
        guard_exit = events.index(("guard", "exit"))
        self.assertLess(guard_enter, guard_exit)
        self.assertEqual(ledger.reactivated, ["bb"])
        # Preparation passes the mutation-derived rescan flag (#112).
        self.assertIn(("prepare", {"force_full_window_rescan": True}), events)
        state = service.snapshot()
        self.assertEqual(state.inactive_block_count, 1)
        self.assertEqual(state.reactivated_block_count, 1)
        self.assertEqual(state.matured_payout_count, 2)
        self.assertTrue(state.last_trusted)
        # A trusted pass for the latest detected tip arms the per-tip memo.
        self.assertIn("tip", service._reorg_reconcile_trusted_memo)

    def test_untrusted_pass_records_skip_and_does_not_arm_memo(self) -> None:
        ports, _events = make_ports(chain_untrusted=True)
        service = ReorgReconcilerService(ports)
        summary = service.reconcile(tip_hash="tip")
        self.assertTrue(summary["untrusted"])
        self.assertEqual(service.snapshot().reconcile_skip_count, 1)
        self.assertNotIn("tip", service._reorg_reconcile_trusted_memo)
        self.assertFalse(service.snapshot().last_trusted)

    def test_stale_proof_epoch_refuses_to_arm_memo(self) -> None:
        ports, _events = make_ports()
        service = ReorgReconcilerService(ports)
        # The pass started in epoch 3; detection has since moved to epoch 0
        # (make_ports pins reorg_proof_snapshot at ("tip", 0)).
        service.note_outcome("tip", trusted=True, proof_epoch=3)
        self.assertNotIn("tip", service._reorg_reconcile_trusted_memo)
        service.note_outcome("tip", trusted=True, proof_epoch=0)
        self.assertIn("tip", service._reorg_reconcile_trusted_memo)

    def test_mutating_pass_evicts_other_tips_and_error_clears_memo(self) -> None:
        ports, _events = make_ports()
        service = ReorgReconcilerService(ports)
        service._reorg_reconcile_trusted_memo["other"] = time.monotonic()
        service.note_outcome("tip", trusted=True, evict_others=True, proof_epoch=0)
        self.assertNotIn("other", service._reorg_reconcile_trusted_memo)
        self.assertIn("tip", service._reorg_reconcile_trusted_memo)
        service.note_outcome("tip", trusted=False, clear_memo=True)
        self.assertEqual(dict(service._reorg_reconcile_trusted_memo), {})

    def test_flight_admission_runs_before_the_serialized_adapter(self) -> None:
        ports, _events = make_ports()
        service = ReorgReconcilerService(ports)
        summary = service.reconcile_with_flights(tip_hash="tip")
        self.assertEqual(
            summary["serialized"],
            {"tip_hash": "tip"},
        )
        # Forced/source-reserved callers bypass flight registration entirely.
        forced = service.reconcile_with_flights(tip_hash="tip", _force_publish=True)
        self.assertEqual(
            forced["serialized"],
            {"tip_hash": "tip", "_force_publish": True, "_source_reserved": False},
        )
        self.assertEqual(service._reconcile_flights, {})

    def test_proving_prefetch_satisfies_both_memo_pass_neither(self) -> None:
        release = threading.Event()

        def blocking_ensure_tip(_tip: str) -> bool:
            release.wait(5.0)
            return True

        ports, _events = make_ports(ensure_tip=blocking_ensure_tip)
        service = ReorgReconcilerService(ports)
        try:
            memo_pass = service.submit_prefetch("aa" * 32)
            self.assertIsNotNone(memo_pass)
            # A memo-honoring pending pass may not satisfy a proving caller;
            # the prove request replaces it like a tip change.
            proving = service.submit_prefetch("aa" * 32, prove=True)
            self.assertIsNotNone(proving)
            self.assertIsNot(proving, memo_pass)
            # A proving pending pass satisfies either kind of caller.
            reused = service.submit_prefetch("aa" * 32)
            self.assertIs(reused, proving)
            reused_proving = service.submit_prefetch("aa" * 32, prove=True)
            self.assertIs(reused_proving, proving)
        finally:
            release.set()
            service.shutdown_prefetch_executor()
        self.assertIsNone(service.submit_prefetch("bb" * 32))

    def test_chain_view_validation_fails_closed(self) -> None:
        self.assertTrue(
            qbit_chain_view_untrusted(
                lambda _method: {
                    "initialblockdownload": False,
                    "blocks": 9,
                    "headers": 10,
                },
                "main",
            )
        )
        with self.assertRaisesRegex(RuntimeError, "non-object"):
            qbit_chain_view_untrusted(lambda _method: [], "regtest")


class CoordinatorReorgIntegrationTests(unittest.TestCase):
    def test_compat_fields_adopt_pre_service_writes_and_route_after(self) -> None:
        server = support.coordinator()
        # The bare fixture wrote the legacy names before any service exists;
        # the descriptors parked them in backing slots.
        self.assertIn("_reorg_compat_enabled", server.__dict__)
        self.assertNotIn("_reorg_reconciler_service", server.__dict__)
        server.reorg_inactive_block_count = 5

        service = server._ensure_reorg_reconciler_service()

        # Adoption seeds the service and removes every backing key.
        self.assertFalse(service.enabled)
        self.assertEqual(service.inactive_block_count, 5)
        for key in list(server.__dict__):
            self.assertFalse(key.startswith("_reorg_compat_"), key)
        # Post-service reads and writes route to the single owner.
        server.reorg_reconcile_skip_count = 9
        self.assertEqual(service.reconcile_skip_count, 9)
        service.matured_payout_count = 4
        self.assertEqual(server.matured_payout_count, 4)

    def test_lookup_counts_property_returns_a_copy(self) -> None:
        server = support.coordinator()
        server._record_reorg_reconcile_lookup("job_build", "serial")
        snapshot = server.reorg_reconcile_lookup_counts
        self.assertEqual(snapshot[("job_build", "serial")], 1)
        snapshot[("job_build", "serial")] = 99
        self.assertEqual(
            server.reorg_reconcile_lookup_counts[("job_build", "serial")],
            1,
        )

    def test_serialized_adapter_keeps_writer_payout_preview_order(self) -> None:
        source = inspect.getsource(
            PrismCoordinator._reconcile_prism_pool_blocks_serialized
        )
        # Writer admission decorates the adapter; the direct payout-balance
        # mutation lock and the landed-preview fail-closed check run inside
        # it, before the service core. The old _payout_balance_mutation()
        # wrapper must not reappear.
        self.assertIn("_payout_balance_mutation_lock", source)
        self.assertIn("_accepted_block_payout_preview_condition", source)
        self.assertIn("PayoutStatePublicationBlocked", source)
        self.assertIn("_ensure_reorg_reconciler_service().reconcile(", source)
        self.assertNotIn("with self._payout_balance_mutation()", source)
        wrappers = getattr(
            PrismCoordinator._reconcile_prism_pool_blocks_serialized,
            "__wrapped__",
            None,
        )
        self.assertIsNotNone(wrappers, "writer-admission decorator missing")

    def test_service_wiring_uses_heartbeat_aware_preparation_lock(self) -> None:
        source = inspect.getsource(
            PrismCoordinator._ensure_reorg_reconciler_service
        )
        self.assertIn("_block_submitter_lock", source)
        self.assertIn('"payout-state-prepare"', source)
        # Live config-override reads stay callable ports.
        self.assertIn("reconcile_flight_wait_seconds", source)
        self.assertIn("reconcile_prefetch_join_timeout_seconds", source)
        # The atomic tip/epoch proof comes from the tip-refresh owner.
        self.assertIn("reorg_proof_snapshot", source)


if __name__ == "__main__":
    unittest.main()
