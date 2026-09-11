"""Issue #224 Wave 2: a dense reconcile cadence over one exact share window.

Wave 1 taught a confirmed payout mutation to ask candidate preparation for
a live prior-balances reread instead of the O(window) oracle rescan, on the
invariant that a reconcile pass never writes ``qbit_share_ledger``
(recorded in ``lab.prism.accepted_preview_telemetry``). Its tests pin one
mutation at a time. The alert hour was a dense accepted-tip cadence, so
this module drives several consecutive trusted passes over one fixed
accepted-share window through the real coordinator adapters while payout
state moves through the reconciliation-owned mutations the ledger actually
performs -- a maturation, an inactive marking, a reactivation -- interleaved
with passes that change nothing and with the post-confirm and landing entry
points.

Per pass it records the intent handed to candidate preparation, the ledger
artifact that came back, the balances that were published and the ledger
reads that were paid for, and pins them against each other and against the
fixed-cardinality telemetry: a mutating pass rereads balances over the
armed window and never rescans it, a non-mutating pass invents neither, and
a lost mutator response mid-cadence still fails closed to the full rescan.
Everything runs on a frozen anchor clock; nothing here sleeps or compares
elapsed time.
"""

from __future__ import annotations

import re
import unittest
from collections import Counter
from typing import Callable
from unittest.mock import patch

from lab.prism.accepted_preview_telemetry import (
    PRISM_ACCEPTED_LANDING_PHASES,
    PRISM_LEDGER_READ_OPERATIONS,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS,
    PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS,
    PRISM_REORG_RECONCILE_CALLERS,
    PRISM_REORG_RECONCILE_STEPS,
    ensure_accepted_preview_telemetry,
    fold_ledger_read_stats,
)
from lab.prism.metrics import MetricsRenderer
from lab.prism.share_ledger import PsqlShareLedger
from tests.test_prism_issue_224_core import (
    BALANCE_REREAD_INTENT,
    FULL_RESCAN_INTENT,
    PLAIN_INTENT,
    CarryLedger,
    QuietCoordinatorTestCase,
    configured_coordinator,
    full_rescan_total,
    pass_counts,
    step_count,
)


# One confirmed pool block below the fake node's tip (height 100). The
# cadence flips the active chain at its height between its own hash and a
# competitor to drive the inactive and reactivated transitions.
POOL_BLOCK_HASH = "ab" * 32
POOL_BLOCK_HEIGHT = 90
COMPETING_BLOCK_HASH = "cd" * 32
FIXED_NOW_MS = 1_000_000

# The carry aggregate after each mutation. The values are arbitrary but
# pairwise distinct, so a publication that reused an earlier generation's
# balances cannot equal the live read by coincidence. ``CarryLedger`` owns
# the initial 546 and the post-maturation 777.
INITIAL_BALANCE = 546
MATURED_BALANCE = 777
INACTIVE_BALANCE = 101
REACTIVATED_BALANCE = 909
LOST_RESPONSE_BALANCE = 4321


class CadenceLedger(CarryLedger):
    """A carry ledger whose pool-block state feeds the watch loop.

    ``SingleWriterShareLedger`` already models pool-block chain state (the
    prepared/confirmed/inactive transitions with their audit ordinals); the
    memory backend just never exposes those rows to ``reorg_watch_blocks``.
    This fixture exposes them, and moves the carry aggregate on every
    mutation that reports a row changed, the way the PostgreSQL functions
    move ``qbit_payout_carry_forward`` -- and, exactly as in PostgreSQL,
    never touches an accepted share.

    The prior-balances read is attributed with the recorder borrowed from
    ``PsqlShareLedger``, so the operation name, the cell shape and the
    rendering are the shipped ones; the fixture supplies only the trigger.
    """

    _ensure_ledger_read_timings = PsqlShareLedger._ensure_ledger_read_timings
    _note_ledger_read_timing = PsqlShareLedger._note_ledger_read_timing
    ledger_read_gate_stats = PsqlShareLedger.ledger_read_gate_stats

    def __init__(self) -> None:
        super().__init__()
        self.lose_next_maturation_response = False

    def current_prior_balances(self) -> list[dict[str, object]]:
        balances = super().current_prior_balances()
        self._note_ledger_read_timing(
            "current_prior_balances",
            gate_wait_seconds=0.0,
            execute_seconds=0.0,
            timed_out=False,
        )
        return balances

    def reorg_watch_blocks(self, *, active_tip_height: int) -> list[dict[str, object]]:
        with self._lock:
            return [
                {
                    "block_hash": block_hash,
                    "block_height": block[0],
                    "chain_state": block[1],
                }
                for block_hash, block in self._memory_pool_blocks.items()
                if block[1] in {"confirmed", "inactive"}
            ]

    def mark_pool_block_inactive(self, **kwargs: object) -> dict[str, int | str]:
        result = super().mark_pool_block_inactive(**kwargs)  # type: ignore[arg-type]
        if result["inactive_count"]:
            self.balance_sats = INACTIVE_BALANCE
        return result

    def reactivate_pool_block(self, **kwargs: object) -> dict[str, int | str]:
        result = super().reactivate_pool_block(**kwargs)  # type: ignore[arg-type]
        if result["reactivated_count"]:
            self.balance_sats = REACTIVATED_BALANCE
        return result

    def mark_mature_pool_payouts(self, *, active_tip_height: int) -> dict[str, int | str]:
        if self.lose_next_maturation_response:
            # The statement committed server-side; only its response was
            # lost, so the aggregate moved and the caller cannot know it.
            self.lose_next_maturation_response = False
            self.matured_pending = 0
            self.balance_sats = LOST_RESPONSE_BALANCE
            raise ConnectionError("mutation response lost")
        return super().mark_mature_pool_payouts(active_tip_height=active_tip_height)


class CandidateRecorder:
    """Every candidate preparation: the intent asked for, the artifact built.

    The reconciler reaches ``_prepared_payout_state_candidate`` through a
    port resolved at call time, so a per-instance wrapper sees exactly the
    kwargs it forwarded and the real candidate the coordinator prepared.
    """

    def __init__(self, server) -> None:
        self.records: list[tuple[dict[str, bool], object]] = []
        original = server._prepared_payout_state_candidate

        def prepared(captured, **kwargs):
            candidate = original(captured, **kwargs)
            self.records.append((dict(kwargs), candidate.ledger_artifact))
            return candidate

        server._prepared_payout_state_candidate = prepared


def cadence_coordinator():
    """A reconciling coordinator over one confirmed pool block.

    The debounce decision compares ``time.monotonic()`` against the minimum
    build interval; it is held far above any test's wall-clock so the
    cadence rides the armed window by construction rather than by racing an
    elapsed-time threshold.
    """
    ledger = CadenceLedger()
    server, rpc, _ledger, _artifacts = configured_coordinator(ledger)
    server.reorg_reconciler_enabled = True
    server.reorg_reconcile_cache_seconds = 0.0
    server.payout_artifact_min_build_interval_seconds = 3_600.0
    active_chain = {POOL_BLOCK_HEIGHT: POOL_BLOCK_HASH}
    original_call = rpc.call

    def call(method: str, params: list[object] | None = None) -> object:
        if method == "getblockhash":
            rpc.calls.append(method)
            return active_chain[int(params[0])]  # type: ignore[index]
        return original_call(method, params)

    rpc.call = call  # type: ignore[method-assign]
    ledger.persist_accepted_block(
        block_hash=POOL_BLOCK_HASH,
        block_height=POOL_BLOCK_HEIGHT,
        parent_hash="11" * 32,
        final_bundle={},
        audit_report={},
    )
    ledger.confirm_accepted_block(
        block_hash=POOL_BLOCK_HASH,
        active_tip_height=POOL_BLOCK_HEIGHT,
    )
    return server, rpc, ledger, active_chain


def label_values(lines: list[str]) -> dict[str, set[str]]:
    values: dict[str, set[str]] = {}
    for line in lines:
        for name, value in re.findall(r'(\w+)="([^"]*)"', line):
            values.setdefault(name, set()).add(value)
    return values


def sample(lines: list[str], series: str) -> int:
    """The integer value of one exact series line, which must exist once."""
    matches = [
        line for line in lines if line.startswith(series + " ")
    ]
    if len(matches) != 1:
        raise AssertionError(f"{series}: {len(matches)} lines")
    return int(float(matches[0].split(" ", 1)[1]))


class DenseCadenceTests(QuietCoordinatorTestCase):
    """Consecutive trusted passes over one fixed window, through the real adapters."""

    def setUp(self) -> None:
        super().setUp()
        self.server, self.rpc, self.ledger, self.active_chain = cadence_coordinator()
        self.telemetry = ensure_accepted_preview_telemetry(self.server)
        self.recorder = CandidateRecorder(self.server)
        self.tip = str(self.rpc.tip)
        # What the telemetry must report once the cadence is over, kept as
        # the cadence runs so the story is told by the passes themselves.
        self.expected_passes: Counter[str] = Counter()
        self.expected_steps: Counter[tuple[str, str]] = Counter()
        self.window_identity: tuple[object, ...] | None = None
        self.armed_window: object = None
        clock = patch("lab.prism.prism_coordinator.now_ms", return_value=FIXED_NOW_MS)
        clock.start()
        self.addCleanup(clock.stop)

    # -- helpers -----------------------------------------------------------

    def published(self):
        with self.server._job_cache_lock:
            published = self.server._published_payout_state.artifact
        return published

    def published_generation(self) -> int:
        published = self.published()
        return 0 if published is None else int(published.generation)

    def run_pass(
        self,
        label: str,
        *,
        intent: dict[str, bool] | None,
        reads: int,
        mutations: int,
        window_mode: str | None = None,
        caller: str = "other",
        run: Callable[[], object] | None = None,
    ) -> object:
        """Run one pass and pin its per-pass contract.

        ``intent`` is the preparation kwargs the pass must forward, or
        ``None`` when it must prepare nothing; ``reads`` is how many live
        prior-balances reads it may pay for; ``mutations`` how many ledger
        mutators it runs (the maturation sweep always runs once).
        """
        reads_before = self.ledger.prior_balance_reads
        records_before = len(self.recorder.records)
        generation_before = self.published_generation()

        result = (
            run()
            if run is not None
            else self.server.reconcile_prism_pool_blocks_once(tip_hash=self.tip)
        )

        prepared = intent is not None
        self.expected_passes[caller] += 1
        self.expected_steps[(caller, "admission_wait")] += 1
        # getblockcount plus one getblockhash for the watched row.
        self.expected_steps[(caller, "chain_probe")] += 2
        # The watch rows plus the stranded-prepared sweep.
        self.expected_steps[(caller, "watch_query")] += 2
        self.expected_steps[(caller, "mutations")] += mutations
        self.expected_steps[(caller, "candidate_prepare")] += int(prepared)
        self.expected_steps[(caller, "publish")] += int(prepared)

        records = self.recorder.records[records_before:]
        self.assertEqual(
            self.ledger.prior_balance_reads, reads_before + reads, label
        )
        if not prepared:
            self.assertEqual(records, [], label)
            self.assertEqual(self.published_generation(), generation_before, label)
            return result

        self.assertEqual(len(records), 1, label)
        kwargs, artifact = records[0]
        self.assertEqual(kwargs, intent, label)
        self.assertIsNotNone(artifact, label)
        self.assertEqual(artifact.window_build_mode, window_mode, label)
        # The publication carries exactly the live carry aggregate.
        published = self.published()
        self.assertIsNotNone(published, label)
        self.assertEqual(published.generation, generation_before + 1, label)
        self.assertEqual(
            [row["balance_sats"] for row in published.prior_balances()],
            [self.ledger.balance_sats],
            label,
        )
        identity = (
            artifact.snapshot_anchor_ms,
            artifact.share_snapshot_sha256,
            tuple(artifact.shares_json),
        )
        if self.window_identity is None:
            self.window_identity = identity
            self.armed_window = self.server._incremental_payout_artifact_window
            self.assertIsNotNone(self.armed_window, label)
            return result
        # The same window: same anchor, same digest, same shares, and the
        # very object the cold start armed -- never replaced, never
        # rescanned.
        self.assertEqual(identity, self.window_identity, label)
        self.assertIs(
            self.server._incremental_payout_artifact_window,
            self.armed_window,
            label,
        )
        self.assertIsNone(artifact.window_full_rescan_reason, label)
        return result

    def assert_no_window_rescan_since_cold_start(self) -> None:
        self.assertEqual(self.ledger.full_snapshot_calls, 1)
        self.assertEqual(self.ledger.delta_snapshot_calls, 0)
        self.assertEqual(self.server.payout_artifact_event_counts["full_rescan"], 1)
        snapshot = self.telemetry.snapshot()
        self.assertEqual(full_rescan_total(snapshot), 1)
        self.assertEqual(snapshot["full_rescans"][("cold_start", "in_process")]["count"], 1)

    # -- the cadence --------------------------------------------------------

    def test_confirmed_mutations_reread_balances_over_one_exact_window(self) -> None:
        ledger, chain = self.ledger, self.active_chain
        self.assertEqual(ledger.balance_sats, INITIAL_BALANCE)

        # 1. Cold start: the tip is reserved, so the pass publishes; the
        #    window comes from the oracle once and the balances from the
        #    ledger once, with the plain pre-#224 intent.
        first = self.run_pass(
            "cold start",
            intent=PLAIN_INTENT,
            reads=1,
            mutations=1,
            window_mode="full_rescan",
        )
        self.assertEqual(first["published_generation"], 1)
        self.assertEqual(first["watched_blocks"], 1)

        # 2. A maturation moves the aggregate: reread, same window.
        ledger.matured_pending = 1
        second = self.run_pass(
            "maturation",
            intent=BALANCE_REREAD_INTENT,
            reads=1,
            mutations=1,
            window_mode="debounced",
        )
        self.assertEqual(second["matured_payouts"], 1)
        self.assertEqual(ledger.balance_sats, MATURED_BALANCE)

        # 3. Nothing moved and nothing requires publication: no
        #    preparation, no read, no generation.
        third = self.run_pass("quiet", intent=None, reads=0, mutations=1)
        self.assertIsNone(third["published_generation"])

        # 4. The block leaves the active chain: inactive, reread.
        chain[POOL_BLOCK_HEIGHT] = COMPETING_BLOCK_HASH
        fourth = self.run_pass(
            "inactive",
            intent=BALANCE_REREAD_INTENT,
            reads=1,
            mutations=2,
            window_mode="debounced",
        )
        self.assertEqual(fourth["inactive_blocks"], 1)
        self.assertEqual(ledger.balance_sats, INACTIVE_BALANCE)

        # 5. Still off the active chain: the inactive row is left alone.
        fifth = self.run_pass("quiet while inactive", intent=None, reads=0, mutations=1)
        self.assertEqual(fifth["inactive_blocks"], 0)
        self.assertEqual(fifth["reactivated_blocks"], 0)

        # 6. The chain flips back: reactivated, reread.
        chain[POOL_BLOCK_HEIGHT] = POOL_BLOCK_HASH
        sixth = self.run_pass(
            "reactivated",
            intent=BALANCE_REREAD_INTENT,
            reads=1,
            mutations=2,
            window_mode="debounced",
        )
        self.assertEqual(sixth["reactivated_blocks"], 1)
        self.assertEqual(ledger.balance_sats, REACTIVATED_BALANCE)

        # 7. A post-confirm forced publication with nothing to mutate keeps
        #    the plain intent and reuses the published balances verbatim.
        seventh = self.run_pass(
            "post-confirm republish",
            caller="post_confirm",
            run=lambda: self.server.reconcile_prism_pool_blocks_once(
                tip_hash=self.tip,
                _force_publish=True,
                _source_reserved=True,
            ),
            intent=PLAIN_INTENT,
            reads=0,
            mutations=1,
            window_mode="debounced",
        )
        self.assertIsNotNone(seventh["published_generation"])

        # 8. The landing's in-lock pass with a second maturation.
        ledger.matured_pending = 1
        self.run_pass(
            "landing maturation",
            caller="landing",
            run=lambda: self.assertTrue(
                self.server.ensure_reorg_reconciled_for_tip(
                    self.tip, _coalesce_same_tip=False
                )
            ),
            intent=BALANCE_REREAD_INTENT,
            reads=1,
            mutations=1,
            window_mode="debounced",
        )
        self.assertEqual(ledger.balance_sats, MATURED_BALANCE)

        # The window was read from the oracle exactly once, for the cold
        # start, and never rescanned; every later read was a balance read
        # owned by a payout-changing pass.
        self.assert_no_window_rescan_since_cold_start()
        self.assertEqual(ledger.prior_balance_reads, 5)
        self.assertEqual(self.published_generation(), 6)
        self.assert_telemetry_tells_the_same_story()

    def assert_telemetry_tells_the_same_story(self) -> None:
        snapshot = self.telemetry.snapshot()
        self.assertEqual(
            pass_counts(snapshot),
            {caller: self.expected_passes[caller] for caller in PRISM_REORG_RECONCILE_CALLERS},
        )
        for caller in PRISM_REORG_RECONCILE_CALLERS:
            for step in PRISM_REORG_RECONCILE_STEPS:
                self.assertEqual(
                    step_count(snapshot, caller, step),
                    self.expected_steps[(caller, step)],
                    (caller, step),
                )
        # Closed products, and nothing the cadence did grew them.
        self.assertEqual(
            set(snapshot["reconcile_steps"]),
            {
                (caller, step)
                for caller in PRISM_REORG_RECONCILE_CALLERS
                for step in PRISM_REORG_RECONCILE_STEPS
            },
        )
        self.assertEqual(
            set(snapshot["full_rescans"]),
            {
                (reason, path)
                for reason in PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS
                for path in PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS
            },
        )
        # The reads the passes paid for are the reads the ledger attributed,
        # under the one contract operation, and nothing else.
        folded = fold_ledger_read_stats(self.ledger.ledger_read_gate_stats())
        self.assertEqual(list(folded), ["current_prior_balances"])
        self.assertIn("current_prior_balances", PRISM_LEDGER_READ_OPERATIONS)
        self.assertEqual(
            folded["current_prior_balances"]["calls_total"],
            self.ledger.prior_balance_reads,
        )
        # And the operator sees the same story on /metrics with only
        # closed-vocabulary labels.
        renderer = MetricsRenderer(self.server)
        lines = (
            renderer.accepted_preview_attribution_metrics_lines()
            + renderer._ledger_read_gate_metric_lines()
        )
        labels = label_values(lines)
        self.assertEqual(labels["phase"], set(PRISM_ACCEPTED_LANDING_PHASES))
        self.assertEqual(labels["caller"], set(PRISM_REORG_RECONCILE_CALLERS))
        self.assertEqual(labels["step"], set(PRISM_REORG_RECONCILE_STEPS))
        self.assertEqual(labels["reason"], set(PRISM_PAYOUT_WINDOW_FULL_RESCAN_REASONS))
        self.assertEqual(labels["path"], set(PRISM_PAYOUT_WINDOW_FULL_RESCAN_PATHS))
        self.assertEqual(labels["operation"], {"current_prior_balances"})
        self.assertEqual(
            sample(lines, 'qbit_prism_ledger_read_calls_total{operation="current_prior_balances"}'),
            self.ledger.prior_balance_reads,
        )
        for caller in PRISM_REORG_RECONCILE_CALLERS:
            self.assertEqual(
                sample(lines, f'qbit_prism_reorg_reconcile_pass_seconds_count{{caller="{caller}"}}'),
                self.expected_passes[caller],
            )
        self.assertEqual(
            sample(
                lines,
                'qbit_prism_payout_window_full_rescan_seconds_count'
                '{reason="reconcile_invalidation",path="in_process"}',
            ),
            0,
        )
        self.assertEqual(
            sample(
                lines,
                'qbit_prism_payout_window_full_rescan_seconds_count'
                '{reason="cold_start",path="in_process"}',
            ),
            1,
        )

    def test_a_lost_mutator_response_mid_cadence_keeps_the_full_rescan(self) -> None:
        """The fail-closed path is unchanged under the same cadence.

        The maturation may have committed before its response was lost, so
        the row counts are unknowable. The pass republishes against the
        forced full rescan, which necessarily rereads the current balances,
        and records the rescan under its own reason.
        """
        ledger = self.ledger
        self.run_pass(
            "cold start",
            intent=PLAIN_INTENT,
            reads=1,
            mutations=1,
            window_mode="full_rescan",
        )
        ledger.matured_pending = 1
        self.run_pass(
            "maturation",
            intent=BALANCE_REREAD_INTENT,
            reads=1,
            mutations=1,
            window_mode="debounced",
        )
        self.assert_no_window_rescan_since_cold_start()
        generation_before = self.published_generation()
        armed_before = self.server._incremental_payout_artifact_window

        ledger.lose_next_maturation_response = True
        with self.assertRaisesRegex(ConnectionError, "mutation response lost"):
            self.server.reconcile_prism_pool_blocks_once(tip_hash=self.tip)

        kwargs, artifact = self.recorder.records[-1]
        self.assertEqual(kwargs, FULL_RESCAN_INTENT)
        self.assertEqual(artifact.window_build_mode, "full_rescan")
        self.assertEqual(artifact.window_full_rescan_reason, "reconcile_invalidation")
        # The oracle ran again and re-armed a fresh window whose content is
        # the same (no share landed) but which is no longer the cold-start
        # object.
        self.assertEqual(ledger.full_snapshot_calls, 2)
        assert self.window_identity is not None
        self.assertEqual(artifact.share_snapshot_sha256, self.window_identity[1])
        self.assertIsNot(self.server._incremental_payout_artifact_window, armed_before)
        # The republication carries the balances the lost mutation left.
        self.assertEqual(ledger.prior_balance_reads, 3)
        published = self.published()
        assert published is not None
        self.assertEqual(published.generation, generation_before + 1)
        self.assertEqual(
            [row["balance_sats"] for row in published.prior_balances()],
            [LOST_RESPONSE_BALANCE],
        )
        service = self.server._ensure_reorg_reconciler_service()
        self.assertEqual(service.snapshot().reconcile_error_count, 1)
        snapshot = self.telemetry.snapshot()
        self.assertEqual(pass_counts(snapshot)["other"], 3)
        self.assertEqual(step_count(snapshot, "other", "mutations"), 3)
        self.assertEqual(step_count(snapshot, "other", "candidate_prepare"), 3)
        self.assertEqual(step_count(snapshot, "other", "publish"), 3)
        rescans = snapshot["full_rescans"]
        self.assertEqual(rescans[("reconcile_invalidation", "in_process")]["count"], 1)
        self.assertEqual(rescans[("cold_start", "in_process")]["count"], 1)
        self.assertEqual(full_rescan_total(snapshot), 2)
        self.assertEqual(self.server.payout_artifact_event_counts["full_rescan"], 2)


if __name__ == "__main__":
    unittest.main()
