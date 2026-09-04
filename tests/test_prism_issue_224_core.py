"""Issue #224 Wave 1 core: reconciliation rereads balances, never the window.

A reconcile pass mutates pool-block, payout-entry and carry state but never
``qbit_share_ledger`` (the invariant recorded in
``lab.prism.accepted_preview_telemetry``). At a fixed anchor the accepted
share window therefore stays exact after a maturation, an inactive marking
or a reactivation, and only the prior balances need a live reread. Before
this wave every confirmed mutation answered with ``force_full_window_rescan``
and paid an O(window) oracle read under the writer lock on the accepted
preview's critical path.

These tests pin the split between the two intents, the fail-closed paths
that keep the full rescan, and the fixed-cardinality attribution the
telemetry contract defines for reconcile passes, steps, callers and actual
full rescans.
"""

from __future__ import annotations

import threading
import time
import unittest
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import patch

from lab.prism.accepted_preview_telemetry import (
    AcceptedPreviewTelemetry,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS,
    PRISM_REORG_RECONCILE_CALLERS,
    PRISM_REORG_RECONCILE_STEPS,
    ensure_accepted_preview_telemetry,
)
from lab.prism.payout_state import (
    PayoutStatePublicationBlocked,
    _PayoutWindowMaterialization,
)
from lab.prism.reorg_reconciler import ReorgPorts, ReorgReconcilerService
from lab.prism.share_ledger import IncrementalWindowAdvanceStats
from tests.prism_coordinator_test_support import (
    IncrementalRecordingLedger,
    append_incremental_share,
    coordinator,
)


BALANCE_REREAD_INTENT = {
    "force_full_window_rescan": False,
    "force_prior_balances_read": True,
}
PLAIN_INTENT = {"force_full_window_rescan": False}
FULL_RESCAN_INTENT = {"force_full_window_rescan": True}


def full_rescan_total(snapshot: dict[str, object]) -> int:
    rescans = snapshot["full_rescans"]
    assert isinstance(rescans, dict)
    return sum(int(stats["count"]) for stats in rescans.values())


def pass_counts(snapshot: dict[str, object]) -> dict[str, int]:
    passes = snapshot["reconcile_passes"]
    assert isinstance(passes, dict)
    return {caller: int(stats["count"]) for caller, stats in passes.items()}


def step_count(snapshot: dict[str, object], caller: str, step: str) -> int:
    steps = snapshot["reconcile_steps"]
    assert isinstance(steps, dict)
    return int(steps[(caller, step)]["count"])


# --- reconciler-level fixtures -------------------------------------------


class MutatingLedger:
    """One confirmed row that leaves the active chain plus a maturation."""

    def __init__(
        self,
        *,
        rows: list[dict[str, object]] | None = None,
        matured: int = 1,
        lose_mutation_response: bool = False,
    ) -> None:
        self.rows = (
            [{"block_height": 8, "block_hash": "aa", "chain_state": "confirmed"}]
            if rows is None
            else rows
        )
        self.matured = matured
        self.lose_mutation_response = lose_mutation_response
        self.inactivated: list[str] = []

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        return [dict(row) for row in self.rows]

    def mark_pool_block_inactive(self, **kwargs: object) -> dict[str, int]:
        self.inactivated.append(str(kwargs["block_hash"]))
        if self.lose_mutation_response:
            raise ConnectionError("mutation response lost")
        return {"inactive_count": 1}

    def reactivate_pool_block(self, **kwargs: object) -> dict[str, int]:
        return {"reactivated_count": 0}

    def mark_mature_pool_payouts(self, **_kwargs: object) -> dict[str, int]:
        return {"matured_count": self.matured}


def make_ports(
    *,
    ledger: object,
    telemetry: AcceptedPreviewTelemetry | None = None,
    chain_untrusted: bool = False,
    publication_required: bool = False,
    publish: object = 7,
    serialized_refusals: list[BaseException] | None = None,
) -> tuple[ReorgPorts, list[tuple[str, object]], dict[str, ReorgReconcilerService]]:
    """Ports whose coordinator seams route back into the service under test.

    ``reconcile_with_admission`` and ``reconcile_serialized`` stand in for
    the coordinator's public entry and serialized adapter, so the caller
    stamp and the admission stamp cross the same seams they cross in the
    coordinator; ``serialized_refusals`` raises in the adapter before the
    core pass, the way a landed preview refuses a pass.
    """
    events: list[tuple[str, object]] = []
    holder: dict[str, ReorgReconcilerService] = {}
    state_lock = threading.RLock()
    active_chain = {8: "not-aa"}
    refusals = list(serialized_refusals or [])

    def rpc_call(method: str, params: object = None) -> object:
        if method == "getbestblockhash":
            return "tip"
        if method == "getblockcount":
            return 10
        if method == "getblockhash":
            height = int(params[0])  # type: ignore[index]
            events.append(("getblockhash", height))
            return active_chain[height]
        raise AssertionError(method)

    @contextmanager
    def publication_guard():
        yield

    def prepared_candidate(captured, **kwargs):
        events.append(("prepare", kwargs))
        return SimpleNamespace(captured=captured)

    def reconcile_serialized(**kwargs: object) -> dict[str, object]:
        events.append(("serialized", dict(kwargs)))
        if refusals:
            raise refusals.pop(0)
        return holder["service"].reconcile(
            tip_hash=kwargs.get("tip_hash"),  # type: ignore[arg-type]
            force_publish=bool(kwargs.get("_force_publish", False)),
            source_reserved=bool(kwargs.get("_source_reserved", False)),
        )

    ports = ReorgPorts(
        rpc_call=rpc_call,
        ledger=lambda: ledger,
        ensure_job_cache_state=lambda: None,
        state_lock=lambda: state_lock,
        source_tip=lambda: "tip",
        reserve_external_tip=lambda tip: events.append(("reserve", tip)),
        max_supersession_retries=lambda: 1,
        prepare_lock=nullcontext,
        capture_source=lambda: (0, 0, "tip", "test", 0.0),
        prepared_candidate=prepared_candidate,
        captured_publication_required=lambda _captured: publication_required,
        block_publication=lambda **kwargs: events.append(("block", kwargs)),
        publication_guard=publication_guard,
        publish_candidate=lambda _candidate: publish,  # type: ignore[return-value]
        observe_preparation=lambda elapsed: None,
        chain_view_untrusted=lambda: chain_untrusted,
        reorg_proof_snapshot=lambda: ("tip", 0),
        flight_wait_seconds=lambda: 30.0,
        prefetch_join_timeout_seconds=lambda: 20.0,
        reconcile_with_admission=lambda **kwargs: (
            holder["service"].reconcile_with_flights(**kwargs)
        ),
        reconcile_serialized=reconcile_serialized,
        ensure_tip=lambda tip: holder["service"].ensure_tip(tip),
        accepted_preview_telemetry=(
            (lambda: telemetry) if telemetry is not None else None
        ),
    )
    return ports, events, holder


def make_service(**kwargs: object) -> tuple[
    ReorgReconcilerService,
    list[tuple[str, object]],
    AcceptedPreviewTelemetry,
]:
    telemetry = kwargs.pop("telemetry", None) or AcceptedPreviewTelemetry()
    ports, events, holder = make_ports(telemetry=telemetry, **kwargs)  # type: ignore[arg-type]
    service = ReorgReconcilerService(ports, cache_seconds=0.0)
    holder["service"] = service
    return service, events, telemetry


def prepare_intents(events: list[tuple[str, object]]) -> list[object]:
    return [payload for name, payload in events if name == "prepare"]


class ReconcileIntentTests(unittest.TestCase):
    """The confirmed-mutation path asks for balances, not the window."""

    def test_confirmed_mutations_request_a_balance_reread_over_the_window(
        self,
    ) -> None:
        ledger = MutatingLedger()
        service, events, _telemetry = make_service(ledger=ledger)

        summary = service.reconcile(tip_hash="tip")

        self.assertEqual(ledger.inactivated, ["aa"])
        self.assertEqual(summary["inactive_blocks"], 1)
        self.assertEqual(summary["matured_payouts"], 1)
        self.assertEqual(summary["published_generation"], 7)
        # Exactly one preparation, and it carries the narrow #224 intent:
        # the exact window is reused, only the carry aggregate is reread.
        self.assertEqual(prepare_intents(events), [BALANCE_REREAD_INTENT])
        # Publication still fences delivery until the reread balances land.
        self.assertIn(("block", {"force": True}), events)

    def test_maturation_alone_requests_the_reread(self) -> None:
        ledger = MutatingLedger(rows=[], matured=3)
        service, events, _telemetry = make_service(ledger=ledger)

        summary = service.reconcile(tip_hash="tip")

        self.assertEqual(summary["matured_payouts"], 3)
        self.assertEqual(prepare_intents(events), [BALANCE_REREAD_INTENT])

    def test_a_pass_without_mutations_keeps_the_plain_intent(self) -> None:
        ledger = MutatingLedger(rows=[], matured=0)
        service, events, _telemetry = make_service(
            ledger=ledger,
            publication_required=True,
        )

        service.reconcile(tip_hash="tip")

        # No reread is forced (the published balances are still exact) and
        # the pre-#224 keyword shape reaches preparation seams unchanged.
        self.assertEqual(prepare_intents(events), [PLAIN_INTENT])

    def test_no_mutation_and_no_required_publication_prepares_nothing(
        self,
    ) -> None:
        ledger = MutatingLedger(rows=[], matured=0)
        service, events, _telemetry = make_service(ledger=ledger)

        summary = service.reconcile(tip_hash="tip")

        self.assertIsNone(summary["published_generation"])
        self.assertEqual(prepare_intents(events), [])

    def test_lost_mutator_response_keeps_the_forced_full_rescan(self) -> None:
        """Error-after-possible-commit stays fail-closed and unchanged.

        The mutator may have committed before its response was lost, so the
        row counts are unknowable. The pass republishes against the forced
        full rescan exactly as before: that path necessarily rereads the
        current prior balances, so stale balances can never be reused, and
        the narrow reread intent is not what it asks for.
        """
        ledger = MutatingLedger(lose_mutation_response=True)
        service, events, telemetry = make_service(ledger=ledger)

        with self.assertRaisesRegex(ConnectionError, "mutation response lost"):
            service.reconcile(tip_hash="tip")

        self.assertEqual(prepare_intents(events), [FULL_RESCAN_INTENT])
        self.assertIn(("block", {"force": True}), events)
        self.assertEqual(service.snapshot().reconcile_error_count, 1)
        # The error exit still records its pass and the steps it reached,
        # including the error candidate's preparation and publication.
        snapshot = telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["other"], 1)
        self.assertEqual(step_count(snapshot, "other", "mutations"), 1)
        self.assertEqual(step_count(snapshot, "other", "candidate_prepare"), 1)
        self.assertEqual(step_count(snapshot, "other", "publish"), 1)

    def test_a_read_phase_failure_forces_nothing(self) -> None:
        class ReadFailingLedger(MutatingLedger):
            def reorg_watch_blocks(self, *, active_tip_height: int):
                raise TimeoutError("reconcile prefetch join exceeded 20s")

        service, events, telemetry = make_service(ledger=ReadFailingLedger())

        with self.assertRaises(TimeoutError):
            service.reconcile(tip_hash="tip")

        self.assertEqual(prepare_intents(events), [])
        snapshot = telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["other"], 1)
        self.assertEqual(step_count(snapshot, "other", "watch_query"), 1)
        self.assertEqual(step_count(snapshot, "other", "candidate_prepare"), 0)

    def test_untrusted_forced_publication_prepares_without_any_intent(
        self,
    ) -> None:
        service, events, _telemetry = make_service(
            ledger=MutatingLedger(),
            chain_untrusted=True,
        )

        summary = service.reconcile(tip_hash="tip", force_publish=True)

        self.assertTrue(summary["untrusted"])
        self.assertEqual(prepare_intents(events), [{}])


class ReconcileAttributionTests(unittest.TestCase):
    """Pass/step/caller observations through the real entry points."""

    def test_direct_pass_records_pass_and_every_step_it_ran(self) -> None:
        service, _events, telemetry = make_service(ledger=MutatingLedger())

        service.reconcile(tip_hash="tip")

        snapshot = telemetry.snapshot()
        counts = pass_counts(snapshot)
        self.assertEqual(counts["other"], 1)
        for caller in PRISM_REORG_RECONCILE_CALLERS:
            if caller != "other":
                self.assertEqual(counts[caller], 0, caller)
        # getblockcount plus one getblockhash; the watch query; the inactive
        # marking plus the maturation; the preparation; the publication.
        self.assertEqual(step_count(snapshot, "other", "chain_probe"), 2)
        self.assertEqual(step_count(snapshot, "other", "watch_query"), 1)
        self.assertEqual(step_count(snapshot, "other", "mutations"), 2)
        self.assertEqual(step_count(snapshot, "other", "candidate_prepare"), 1)
        self.assertEqual(step_count(snapshot, "other", "publish"), 1)
        # A direct core call never crossed the serialized adapter.
        self.assertEqual(step_count(snapshot, "other", "admission_wait"), 0)
        # Nothing here is a full rescan.
        self.assertEqual(full_rescan_total(snapshot), 0)

    def test_untrusted_pass_records_its_pass_and_no_steps(self) -> None:
        service, _events, telemetry = make_service(
            ledger=MutatingLedger(),
            chain_untrusted=True,
        )

        service.reconcile(tip_hash="tip")

        snapshot = telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["other"], 1)
        for step in PRISM_REORG_RECONCILE_STEPS:
            self.assertEqual(step_count(snapshot, "other", step), 0, step)

    def test_forced_publication_without_context_is_post_confirm(self) -> None:
        service, events, telemetry = make_service(ledger=MutatingLedger())

        service.reconcile_with_flights(
            tip_hash="tip",
            _force_publish=True,
            _source_reserved=True,
        )

        snapshot = telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["post_confirm"], 1)
        self.assertEqual(pass_counts(snapshot)["other"], 0)
        # Crossing the adapter records the admission wait for that caller.
        self.assertEqual(
            step_count(snapshot, "post_confirm", "admission_wait"), 1
        )
        # The adapter seam still receives exactly its historical kwargs.
        self.assertIn(
            (
                "serialized",
                {
                    "tip_hash": "tip",
                    "_force_publish": True,
                    "_source_reserved": True,
                },
            ),
            events,
        )

    def test_direct_forced_core_call_is_post_confirm_too(self) -> None:
        service, _events, telemetry = make_service(ledger=MutatingLedger())
        service.reconcile(tip_hash="tip", force_publish=True)
        self.assertEqual(pass_counts(telemetry.snapshot())["post_confirm"], 1)

    def test_flight_bypass_attributes_the_landing(self) -> None:
        service, events, telemetry = make_service(ledger=MutatingLedger())

        self.assertTrue(service.ensure_tip("tip", _coalesce_same_tip=False))

        snapshot = telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["landing"], 1)
        self.assertEqual(step_count(snapshot, "landing", "admission_wait"), 1)
        self.assertEqual(step_count(snapshot, "landing", "candidate_prepare"), 1)
        self.assertIn(("serialized", {"tip_hash": "tip"}), events)

    def test_coalescing_ensure_tip_without_context_is_other(self) -> None:
        service, _events, telemetry = make_service(ledger=MutatingLedger())
        self.assertTrue(service.ensure_tip("tip"))
        self.assertEqual(pass_counts(telemetry.snapshot())["other"], 1)

    def test_job_build_entry_attributes_job_build(self) -> None:
        service, _events, telemetry = make_service(ledger=MutatingLedger())

        self.assertTrue(service.ensure_current())

        snapshot = telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["job_build"], 1)
        self.assertEqual(step_count(snapshot, "job_build", "admission_wait"), 1)
        # The existing lookup family and the new pass family join on the
        # same spelling.
        self.assertEqual(
            service.reorg_reconcile_lookup_snapshot()[("job_build", "serial")],
            1,
        )

    def test_prefetch_lane_attributes_tip_refresh(self) -> None:
        service, _events, telemetry = make_service(ledger=MutatingLedger())

        self.assertTrue(service.prefetch_pass("tip"))
        self.assertTrue(service.prefetch_pass("tip", prove=True))
        # The bounded re-prove routes through the same lane.
        service.shutdown_prefetch_executor()
        self.assertTrue(service.snapshot_tip_bounded("tip"))

        self.assertEqual(pass_counts(telemetry.snapshot())["tip_refresh"], 3)

    def test_explicit_caller_wins_and_unknown_callers_are_refused(self) -> None:
        service, _events, telemetry = make_service(ledger=MutatingLedger())

        service.ensure_tip("tip", _caller="tip_refresh")
        service.reconcile_with_flights(tip_hash="tip", _caller="job_build")
        service.reconcile(tip_hash="tip", caller="landing")

        counts = pass_counts(telemetry.snapshot())
        self.assertEqual(counts["tip_refresh"], 1)
        self.assertEqual(counts["job_build"], 1)
        self.assertEqual(counts["landing"], 1)
        with self.assertRaisesRegex(ValueError, "unknown reorg reconcile caller"):
            service.reconcile(tip_hash="tip", caller="benchmark-run-42")
        with self.assertRaisesRegex(ValueError, "unknown reorg reconcile caller"):
            service.ensure_tip("tip", _caller="benchmark-run-42")

    def test_outer_entry_point_keeps_its_label_through_nested_seams(self) -> None:
        """A prefetched pass reaching ensure_tip via the coordinator stays tip_refresh."""
        service, _events, telemetry = make_service(ledger=MutatingLedger())

        with service._attributed("tip_refresh"):
            service.reconcile_with_flights(tip_hash="tip", _force_publish=True)

        counts = pass_counts(telemetry.snapshot())
        self.assertEqual(counts["tip_refresh"], 1)
        self.assertEqual(counts["post_confirm"], 0)
        # The stamp is restored once the entry point returns.
        self.assertIsNone(service._attributed_caller())

    def test_adapter_refusal_clears_the_admission_stamp(self) -> None:
        service, _events, telemetry = make_service(
            ledger=MutatingLedger(),
            serialized_refusals=[
                PayoutStatePublicationBlocked("accepted block payout pending")
            ],
        )

        with self.assertRaises(PayoutStatePublicationBlocked):
            service.reconcile_with_flights(tip_hash="tip")
        # A later direct call on this thread must not inherit the stamp.
        service.reconcile(tip_hash="tip")

        snapshot = telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["other"], 1)
        self.assertEqual(step_count(snapshot, "other", "admission_wait"), 0)

    def test_disabled_service_records_nothing(self) -> None:
        ports, _events, holder = make_ports(ledger=MutatingLedger())
        service = ReorgReconcilerService(ports, enabled=False)
        holder["service"] = service
        service.reconcile(tip_hash="tip")
        self.assertTrue(service.ensure_tip("tip", _coalesce_same_tip=False))
        self.assertEqual(
            sum(pass_counts(service.accepted_preview_telemetry.snapshot()).values()),
            0,
        )

    def test_ports_without_an_owner_get_one_private_stable_owner(self) -> None:
        ports, _events, holder = make_ports(ledger=MutatingLedger())
        service = ReorgReconcilerService(ports)
        holder["service"] = service
        owner = service.accepted_preview_telemetry
        self.assertIsInstance(owner, AcceptedPreviewTelemetry)
        self.assertIs(service.accepted_preview_telemetry, owner)

        service.reconcile(tip_hash="tip")
        self.assertEqual(pass_counts(owner.snapshot())["other"], 1)

    def test_port_supplied_owner_is_used_as_is(self) -> None:
        shared = AcceptedPreviewTelemetry()
        service, _events, telemetry = make_service(
            ledger=MutatingLedger(),
            telemetry=shared,
        )
        self.assertIs(telemetry, shared)
        self.assertIs(service.accepted_preview_telemetry, shared)


# --- payout-state level ---------------------------------------------------


class CarryLedger(IncrementalRecordingLedger):
    """Incremental ledger whose carry aggregate moves under the tests' control."""

    def __init__(self) -> None:
        super().__init__()
        self.balance_sats = 546
        self.prior_balance_reads = 0
        self.matured_pending = 0
        self.fail_full_snapshot = False

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

    def snapshot_at_job_issue(self, anchor_job_issued_at_ms, *, window_weight=None):
        if self.fail_full_snapshot:
            self.full_snapshot_calls += 1
            raise TimeoutError("postgres statement deadline expired")
        return super().snapshot_at_job_issue(
            anchor_job_issued_at_ms,
            window_weight=window_weight,
        )

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, int | str]:
        matured, self.matured_pending = self.matured_pending, 0
        if matured:
            # Maturation moves carry balances; the share ledger is untouched.
            self.balance_sats = 777
        return {"backend": "memory", "matured_count": matured}


def configured_coordinator(ledger: CarryLedger | None = None):
    """A ready coordinator over a three-share window on a carry ledger.

    ``ledger`` lets the Wave 2 cadence module supply its subclass without
    re-stating this setup.
    """
    ledger = CarryLedger() if ledger is None else ledger
    append_incremental_share(ledger, share_seq=1, accepted_at_ms=999_900)
    append_incremental_share(ledger, share_seq=2, accepted_at_ms=999_910)
    append_incremental_share(ledger, share_seq=3, accepted_at_ms=999_920)
    server, rpc = coordinator(ledger=ledger)
    artifacts = server.store_template_artifacts(dict(rpc.template))
    assert artifacts is not None
    server._pool_ready_latched = True
    server.payout_artifact_min_build_interval_seconds = 60.0
    server.payout_artifact_full_rescan_seconds = 3_600.0
    return server, rpc, ledger, artifacts


class QuietCoordinatorTestCase(unittest.TestCase):
    """Keep the artifact lifecycle log lines out of the test output."""

    def setUp(self) -> None:
        super().setUp()
        printer = patch("builtins.print")
        printer.start()
        self.addCleanup(printer.stop)


class PayoutWindowIntentTests(QuietCoordinatorTestCase):
    def test_balance_reread_reuses_the_exact_window_and_reads_live_carry(
        self,
    ) -> None:
        server, _rpc, ledger, artifacts = configured_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            published = server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertEqual(initial.window_build_mode, "full_rescan")
            self.assertEqual(initial.window_full_rescan_reason, "cold_start")
            self.assertEqual(ledger.full_snapshot_calls, 1)
            self.assertTrue(server._install_payout_ledger_artifact(initial))

            ledger.balance_sats = 777
            reads_before = ledger.prior_balance_reads
            reused = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert reused is not None
            # Without the intent the published bytes are reused verbatim.
            self.assertEqual(reused.prior_balances[0]["balance_sats"], 546)
            self.assertEqual(
                reused.prior_balances_sha256, published.prior_balances_sha256
            )
            self.assertEqual(ledger.prior_balance_reads, reads_before)

            reread = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                force_prior_balances_read=True,
            )

        assert reread is not None
        # The window is the armed one: no oracle read, same digest, same
        # anchor, and no rescan reason.
        self.assertEqual(reread.window_build_mode, "debounced")
        self.assertIsNone(reread.window_full_rescan_reason)
        self.assertEqual(reread.share_snapshot_sha256, initial.share_snapshot_sha256)
        self.assertEqual(reread.snapshot_anchor_ms, initial.snapshot_anchor_ms)
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 0)
        # The balances are live.
        self.assertEqual(reread.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(ledger.prior_balance_reads, reads_before + 1)
        self.assertNotEqual(
            reread.prior_balances_sha256, published.prior_balances_sha256
        )
        # A balance reread is never an observation of the rescan family.
        snapshot = telemetry.snapshot()
        self.assertEqual(full_rescan_total(snapshot), 1)
        self.assertEqual(snapshot["full_rescans"][("cold_start", "in_process")]["count"], 1)

    def test_balance_reread_rides_the_delta_path_when_shares_landed(self) -> None:
        server, _rpc, ledger, artifacts = configured_coordinator()
        server.payout_artifact_min_build_interval_seconds = 0.0
        clock_ms = [1_000_000]
        with patch(
            "lab.prism.prism_coordinator.now_ms",
            side_effect=lambda: clock_ms[0],
        ):
            server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            append_incremental_share(ledger, share_seq=4, accepted_at_ms=1_000_010)
            clock_ms[0] = 1_000_020
            ledger.balance_sats = 777
            reread = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                force_prior_balances_read=True,
            )

        assert reread is not None
        self.assertEqual(reread.window_build_mode, "incremental")
        self.assertEqual(reread.window_delta_rows, 1)
        self.assertEqual(len(reread.shares_json), 4)
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(ledger.delta_snapshot_calls, 1)
        self.assertEqual(reread.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(
            full_rescan_total(ensure_accepted_preview_telemetry(server).snapshot()),
            1,
        )

    def test_force_full_window_rescan_keeps_its_whole_meaning(self) -> None:
        server, _rpc, ledger, artifacts = configured_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            ledger.balance_sats = 777
            reads_before = ledger.prior_balance_reads
            forced = server._build_payout_ledger_artifact(
                0,
                0,
                artifacts.network_difficulty,
                True,
            )

        assert forced is not None
        self.assertEqual(forced.window_build_mode, "full_rescan")
        self.assertEqual(forced.window_full_rescan_reason, "reconcile_invalidation")
        self.assertEqual(ledger.full_snapshot_calls, 2)
        # A full rescan necessarily obtains the current prior balances.
        self.assertEqual(forced.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(ledger.prior_balance_reads, reads_before + 1)
        rescans = telemetry.snapshot()["full_rescans"]
        self.assertEqual(rescans[("reconcile_invalidation", "in_process")]["count"], 1)
        self.assertEqual(rescans[("cold_start", "in_process")]["count"], 1)

    def test_prepared_candidate_carries_both_intents_distinctly(self) -> None:
        server, rpc, ledger, artifacts = configured_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        captured = (0, 0, str(rpc.tip), "external_tip", time.monotonic())
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            server._current_payout_state_artifact()
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            self.assertTrue(server._install_payout_ledger_artifact(initial))
            ledger.balance_sats = 777

            # Compatibility: the pre-#224 signature still works.
            plain = server._prepared_payout_state_candidate(captured)
            reread = server._prepared_payout_state_candidate(
                captured,
                force_prior_balances_read=True,
            )
            forced = server._prepared_payout_state_candidate(
                captured,
                force_full_window_rescan=True,
            )

        assert plain.ledger_artifact is not None
        assert reread.ledger_artifact is not None
        assert forced.ledger_artifact is not None
        self.assertEqual(plain.ledger_artifact.window_build_mode, "debounced")
        self.assertEqual(plain.ledger_artifact.prior_balances[0]["balance_sats"], 546)
        self.assertEqual(reread.ledger_artifact.window_build_mode, "debounced")
        self.assertEqual(reread.ledger_artifact.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(forced.ledger_artifact.window_build_mode, "full_rescan")
        self.assertEqual(
            forced.ledger_artifact.window_full_rescan_reason,
            "reconcile_invalidation",
        )
        self.assertEqual(forced.ledger_artifact.prior_balances[0]["balance_sats"], 777)
        self.assertEqual(ledger.full_snapshot_calls, 2)
        self.assertEqual(
            telemetry.snapshot()["full_rescans"][
                ("reconcile_invalidation", "in_process")
            ]["count"],
            1,
        )

    def test_narrow_build_seams_are_not_handed_the_new_keyword(self) -> None:
        """Preparation forwards the reread intent only when it applies."""
        server, rpc, _ledger, _artifacts = configured_coordinator()
        seen: list[tuple[object, ...]] = []

        def narrow_build(
            expected_payout_state_generation: int,
            artifact_payout_state_generation: int,
            network_difficulty: int,
            force_full_rescan: bool = False,
            bypass_build_interval: bool = False,
            during_publication: bool = False,
        ) -> None:
            seen.append(
                (
                    expected_payout_state_generation,
                    artifact_payout_state_generation,
                    force_full_rescan,
                )
            )
            return None

        server._build_payout_ledger_artifact = narrow_build  # type: ignore[method-assign]
        captured = (0, 0, str(rpc.tip), "external_tip", time.monotonic())
        server._prepared_payout_state_candidate(captured)
        server._prepared_payout_state_candidate(captured, force_full_window_rescan=True)
        self.assertEqual(seen, [(0, 1, False), (0, 1, True)])
        with self.assertRaises(TypeError):
            server._prepared_payout_state_candidate(
                captured,
                force_prior_balances_read=True,
            )


class FullRescanTelemetryTests(QuietCoordinatorTestCase):
    def test_every_recorded_reason_and_path_is_in_the_contract(self) -> None:
        server, _rpc, _ledger, _artifacts = configured_coordinator()
        snapshot = ensure_accepted_preview_telemetry(server).snapshot()
        for reason, path in snapshot["full_rescans"]:
            self.assertIn(reason, PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS)
            self.assertIn(path, PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS)

    def test_cache_lifecycle_rescans_record_their_fixed_reason(self) -> None:
        server, _rpc, ledger, artifacts = configured_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            # A late-visible append bumps the epoch without waiting for the
            # preparation lock; the next build must fail closed to the oracle.
            with server._job_cache_lock:
                server._payout_ledger_append_invalidation_epoch += 1
            appended = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert appended is not None
            self.assertEqual(appended.window_full_rescan_reason, "late_visible_append")
            # An explicit append-side invalidation leaves its one-shot reason.
            with server._payout_state_prepare_lock:
                server._incremental_payout_artifact_window = None
                server._incremental_payout_artifact_window_invalidation_reason = (
                    "cache_invalidated"
                )
            invalidated = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert invalidated is not None
            self.assertEqual(invalidated.window_full_rescan_reason, "cache_invalidated")

        self.assertEqual(ledger.full_snapshot_calls, 3)
        rescans = telemetry.snapshot()["full_rescans"]
        self.assertEqual(rescans[("cold_start", "in_process")]["count"], 1)
        self.assertEqual(rescans[("late_visible_append", "in_process")]["count"], 1)
        self.assertEqual(rescans[("cache_invalidated", "in_process")]["count"], 1)
        self.assertEqual(full_rescan_total(telemetry.snapshot()), 3)
        for stats in rescans.values():
            self.assertGreaterEqual(stats["sum"], 0.0)
            self.assertGreaterEqual(stats["max"], 0.0)

    def test_daemon_fold_records_the_daemon_path(self) -> None:
        server, _rpc, _ledger, artifacts = configured_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        service = server._ensure_payout_state_service()

        def daemon_fold(*, records, snapshot_anchor_ms, reason, **_kwargs):
            shares_json = tuple(record.to_prism_json() for record in records)
            return _PayoutWindowMaterialization(
                shares_json=shares_json,
                share_snapshot_sha256=service._canonical_json_sha256(shares_json),
                snapshot_anchor_ms=int(snapshot_anchor_ms),
                mode="full_rescan",
                record_count=len(shares_json),
                stats=IncrementalWindowAdvanceStats(0, 0, 0),
                full_rescan_reason=reason,
            )

        with (
            patch.object(service, "_window_pipeline_rust_enabled", return_value=True),
            patch.object(service, "_daemon_full_window_materialization", daemon_fold),
            patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000),
        ):
            built = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert built is not None
        self.assertEqual(built.window_build_mode, "full_rescan")
        rescans = telemetry.snapshot()["full_rescans"]
        self.assertEqual(rescans[("cold_start", "daemon")]["count"], 1)
        self.assertEqual(rescans[("cold_start", "in_process")]["count"], 0)

    def test_periodic_self_check_records_the_periodic_reason_in_process(
        self,
    ) -> None:
        server, _rpc, ledger, artifacts = configured_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 0.0
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            checked = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert checked is not None
        self.assertEqual(checked.window_build_mode, "self_check_match")
        self.assertEqual(ledger.full_snapshot_calls, 2)
        rescans = telemetry.snapshot()["full_rescans"]
        self.assertEqual(rescans[("cold_start", "in_process")]["count"], 1)
        self.assertEqual(rescans[("periodic_self_check", "in_process")]["count"], 1)

    def test_failed_self_check_records_its_failure_reason(self) -> None:
        server, _rpc, ledger, artifacts = configured_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        server.payout_artifact_min_build_interval_seconds = 0.0
        server.payout_artifact_full_rescan_seconds = 0.0
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            initial = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )
            assert initial is not None
            ledger.fail_full_snapshot = True
            checked = server._build_payout_ledger_artifact(
                0, 0, artifacts.network_difficulty
            )

        assert checked is not None
        self.assertEqual(checked.window_build_mode, "incremental_self_check_failed")
        rescans = telemetry.snapshot()["full_rescans"]
        self.assertEqual(
            rescans[("periodic_self_check_failed", "in_process")]["count"], 1
        )

    def test_a_rescan_that_dies_with_the_ledger_still_owns_its_time(self) -> None:
        server, _rpc, ledger, artifacts = configured_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        ledger.fail_full_snapshot = True
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            self.assertIsNone(
                server._build_payout_ledger_artifact(
                    0, 0, artifacts.network_difficulty
                )
            )
        rescans = telemetry.snapshot()["full_rescans"]
        self.assertEqual(rescans[("cold_start", "in_process")]["count"], 1)


# --- coordinator integration ------------------------------------------------


class CoordinatorReconcileIntegrationTests(QuietCoordinatorTestCase):
    """The real adapters: writer admission, mutation lock, service core."""

    def reconciling_coordinator(self):
        server, rpc, ledger, artifacts = configured_coordinator()
        server.reorg_reconciler_enabled = True
        # Every entry point must run a real pass, not a memo hit.
        server.reorg_reconcile_cache_seconds = 0.0
        return server, rpc, ledger, artifacts

    def test_maturation_reuses_the_window_and_publishes_reread_balances(
        self,
    ) -> None:
        server, rpc, ledger, _artifacts = self.reconciling_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        tip = str(rpc.tip)
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            first = server.reconcile_prism_pool_blocks_once(tip_hash=tip)
            self.assertEqual(first["published_generation"], 1)
            self.assertEqual(ledger.full_snapshot_calls, 1)
            with server._job_cache_lock:
                first_published = server._published_payout_state.artifact
            assert first_published is not None
            self.assertEqual(first_published.prior_balances()[0]["balance_sats"], 546)

            ledger.matured_pending = 1
            reads_before = ledger.prior_balance_reads
            second = server.reconcile_prism_pool_blocks_once(tip_hash=tip)

        self.assertEqual(second["matured_payouts"], 1)
        self.assertEqual(second["published_generation"], 2)
        # The exact window was reused: no second oracle read anywhere.
        self.assertEqual(ledger.full_snapshot_calls, 1)
        self.assertEqual(server.payout_artifact_event_counts["full_rescan"], 1)
        # The balances the maturation moved were reread and published.
        self.assertEqual(ledger.prior_balance_reads, reads_before + 1)
        with server._job_cache_lock:
            published = server._published_payout_state.artifact
        assert published is not None
        self.assertEqual(published.generation, 2)
        self.assertEqual(published.prior_balances()[0]["balance_sats"], 777)
        self.assertNotEqual(
            published.prior_balances_sha256, first_published.prior_balances_sha256
        )
        # Attribution: two ``other`` passes that crossed the real adapter,
        # each preparing and publishing; one cold-start rescan, and no
        # reconcile_invalidation rescan at all.
        snapshot = telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["other"], 2)
        self.assertEqual(step_count(snapshot, "other", "admission_wait"), 2)
        # Per pass: the watch query plus the stranded-prepared sweep read.
        self.assertEqual(step_count(snapshot, "other", "watch_query"), 4)
        self.assertEqual(step_count(snapshot, "other", "chain_probe"), 2)
        self.assertEqual(step_count(snapshot, "other", "mutations"), 2)
        self.assertEqual(step_count(snapshot, "other", "candidate_prepare"), 2)
        self.assertEqual(step_count(snapshot, "other", "publish"), 2)
        rescans = snapshot["full_rescans"]
        self.assertEqual(rescans[("cold_start", "in_process")]["count"], 1)
        self.assertEqual(rescans[("reconcile_invalidation", "in_process")]["count"], 0)
        self.assertEqual(rescans[("reconcile_invalidation", "daemon")]["count"], 0)
        self.assertEqual(full_rescan_total(snapshot), 1)

    def test_entry_points_attribute_through_the_real_adapters(self) -> None:
        server, rpc, _ledger, _artifacts = self.reconciling_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        tip = str(rpc.tip)
        with patch("lab.prism.prism_coordinator.now_ms", return_value=1_000_000):
            self.assertTrue(
                server.ensure_reorg_reconciled_for_tip(tip, _coalesce_same_tip=False)
            )
            confirmed = server.reconcile_prism_pool_blocks_once(
                tip_hash=tip,
                _force_publish=True,
                _source_reserved=True,
            )
            self.assertIsNotNone(confirmed["published_generation"])
            self.assertTrue(server.ensure_reorg_reconciled_for_current_tip())
            prefetch = server._submit_reconcile_prefetch(tip)
            assert prefetch is not None
            try:
                self.assertTrue(prefetch.result(timeout=10.0))
            finally:
                server.shutdown_reconcile_prefetch_executor()
            self.assertTrue(server.ensure_reorg_reconciled_for_tip(tip))

        counts = pass_counts(telemetry.snapshot())
        self.assertEqual(counts["landing"], 1)
        self.assertEqual(counts["post_confirm"], 1)
        self.assertEqual(counts["job_build"], 1)
        self.assertEqual(counts["tip_refresh"], 1)
        self.assertEqual(counts["other"], 1)
        snapshot = telemetry.snapshot()
        for caller in PRISM_REORG_RECONCILE_CALLERS:
            self.assertEqual(step_count(snapshot, caller, "admission_wait"), 1, caller)
        # The service records into the same owner the renderer reads.
        self.assertIs(
            server._ensure_reorg_reconciler_service().accepted_preview_telemetry,
            telemetry,
        )

    def test_landed_preview_refusal_records_no_pass(self) -> None:
        server, rpc, _ledger, _artifacts = self.reconciling_coordinator()
        telemetry = ensure_accepted_preview_telemetry(server)
        tip = str(rpc.tip)
        server._begin_accepted_block_payout_preview("aa" * 32, block_height=11)
        server._mark_accepted_block_payout_landed("aa" * 32, block_height=11)
        with self.assertRaises(PayoutStatePublicationBlocked):
            server.reconcile_prism_pool_blocks_once(tip_hash=tip)
        self.assertEqual(sum(pass_counts(telemetry.snapshot()).values()), 0)


if __name__ == "__main__":
    unittest.main()
