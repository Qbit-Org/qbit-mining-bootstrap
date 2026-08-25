#!/usr/bin/env python3
"""Fast-arrival initial vardiff convergence (#132).

Credit is target-based: a share is worth the difficulty its job was stamped
at. A rental router that filters submissions at its own, much higher
difficulty therefore under-credits its miner for as long as the stamped
target lags the difficulty the rig is really working at. These tests pin the
incident shape -- a standard-lane session started at 16384 behind a router
filtering at 500000 -- and the guards that keep the fast path from touching
anyone else.
"""
# ruff: noqa: F403, F405

from __future__ import annotations

import unittest

from lab.prism.vardiff_service import (
    PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SECONDS_BUCKETS,
    PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SHARES_BUCKETS,
    PRISM_VARDIFF_INITIAL_RETARGET_OUTCOMES,
)
from tests.prism_vardiff_test_support import *


STANDARD_LANE_START = Decimal("16384")
ROUTER_FILTER_DIFFICULTY = Decimal("500000")
# 9.4 PH/s clearing a 500000 filter: 9.4e15 / (500000 * 2**32) accepted
# shares per second, so one every ~0.2285s.
ROUTER_SHARE_INTERVAL_SECONDS = 0.2285


def standard_lane_config(**overrides: object) -> vardiff.VardiffConfig:
    """The deployed standard lane: 16384 start, 4x steps, 300s retargets."""
    base = dict(
        enabled=True,
        target_share_interval_seconds=Decimal("15"),
        min_difficulty=STANDARD_LANE_START,
        max_difficulty=Decimal("4294967296"),
        retarget_interval_seconds=Decimal("300"),
        max_step_factor=Decimal("4"),
        startup_difficulty=STANDARD_LANE_START,
        max_step_down_factor=Decimal("4"),
        ewma_alpha=Decimal("0.4"),
        retarget_tolerance=Decimal("0.25"),
    )
    base.update(overrides)
    return vardiff.VardiffConfig(**base)  # type: ignore[arg-type]


class FastArrivalHarness:
    """A coordinator, a client, and a clock the test drives by hand."""

    def __init__(
        self,
        config: vardiff.VardiffConfig,
        *,
        difficulty: Decimal = STANDARD_LANE_START,
    ) -> None:
        self.server = coordinator()
        self.server.vardiff_config = config
        self.client = client()
        self.client.share_difficulty = difficulty
        self.now = 1000.0
        self.client.vardiff_window_started_monotonic = self.now
        self.client.vardiff_session_started_monotonic = self.now
        self.sent: list[dict[str, object]] = []
        self.server.maybe_send_job = self._send  # type: ignore[method-assign]

    def _send(self, client: object, clean_jobs: bool) -> bool:
        self.sent.append({"clean": clean_jobs, "at": self.now})
        return True

    @property
    def service(self) -> object:
        return self.server._ensure_vardiff_service()

    @property
    def difficulty(self) -> Decimal:
        return self.client.pending_share_difficulty or self.client.share_difficulty

    def deliver(self, count: int, *, interval: float, difficulty: Decimal | None = None) -> None:
        """Feed ``count`` accepted shares stamped at the client's target."""
        for _ in range(count):
            self.now += interval
            stamped = difficulty if difficulty is not None else self.difficulty
            with patch(
                "lab.prism.vardiff_service.time.monotonic",
                side_effect=lambda: self.now,
            ):
                self.server.note_vardiff_submitted_share(self.client)
                self.server.note_vardiff_accepted_share(
                    self.client,
                    FakeJob(stamped),  # type: ignore[arg-type]
                )

    def metrics(self) -> dict[str, str]:
        lines = self.server.vardiff_idle_metrics_lines()
        return {
            line.rsplit(" ", 1)[0]: line.rsplit(" ", 1)[1]
            for line in lines
            if not line.startswith("#")
        }


class PrismVardiffFastArrivalTests(unittest.TestCase):
    def test_router_filtered_session_reaches_router_difficulty_in_the_initial_burst(
        self,
    ) -> None:
        # The incident: a 9.4 PH/s rental order behind a router that only
        # forwards shares clearing 500000, into a standard lane that stamps
        # 16384. Every share the pool sees is worth ~30x what it is credited,
        # and the ordinary ladder needs three 300s retargets to close that.
        harness = FastArrivalHarness(standard_lane_config())
        self.assertEqual(harness.difficulty, STANDARD_LANE_START)

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertGreaterEqual(harness.difficulty, ROUTER_FILTER_DIFFICULTY)
        self.assertEqual(harness.difficulty, STANDARD_LANE_START * 64)
        # Arrival is bounded by the burst, not by the retarget interval.
        elapsed = harness.now - 1000.0
        self.assertLess(elapsed, 2.0)
        self.assertLess(
            Decimal(str(elapsed)),
            standard_lane_config().retarget_interval_seconds,
        )
        # The step up keeps the miner's in-flight work: one non-clean job.
        self.assertEqual(len(harness.sent), 1)
        self.assertFalse(harness.sent[0]["clean"])

    def test_ordinary_ladder_would_still_be_short_after_the_same_burst(self) -> None:
        # Same burst with the fast path off: the ordinary 4x bound lands at
        # 65536, still 7.6x below what the router is filtering at, and the
        # next move cannot come until the 300s interval elapses.
        harness = FastArrivalHarness(
            standard_lane_config(initial_convergence_enabled=False)
        )

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertEqual(harness.difficulty, STANDARD_LANE_START)
        self.assertEqual(harness.sent, [])

        # Only a full ordinary window of that same cadence retargets, and the
        # 4x bound lands at 65536 -- still 7.6x below what the router filters
        # at, with another 300s before the next move. Three intervals of
        # under-credited shares is what the fast path removes.
        harness.deliver(1305, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertGreaterEqual(harness.now - 1000.0, 300.0)
        self.assertEqual(harness.difficulty, STANDARD_LANE_START * 4)
        self.assertLess(harness.difficulty, ROUTER_FILTER_DIFFICULTY)

    def test_on_cadence_small_miner_is_not_driven_upward(self) -> None:
        # One accepted share per target interval, for well past the point
        # where the share threshold is met. The early path exists but never
        # engages, and no difficulty change is advertised.
        harness = FastArrivalHarness(standard_lane_config())

        harness.deliver(20, interval=15.0)

        self.assertEqual(harness.difficulty, STANDARD_LANE_START)
        self.assertEqual(harness.sent, [])
        self.assertTrue(self.client_initial_pending(harness))
        metrics = harness.metrics()
        self.assertEqual(
            metrics["qbit_prism_vardiff_initial_retarget_attempts_total"], "0"
        )

    def test_burst_shorter_than_the_elapsed_floor_waits(self) -> None:
        # Eight shares read off the socket back to back. The work is real but
        # the window is not: dividing by ~0 would observe ~1e9. The elapsed
        # floor holds the session at its lane start until a window long
        # enough to mean something has passed.
        harness = FastArrivalHarness(standard_lane_config())

        harness.deliver(8, interval=0.001)

        self.assertEqual(harness.difficulty, STANDARD_LANE_START)
        self.assertEqual(harness.sent, [])

        # One more share carries the window past the floor, and now the
        # accumulated burst converges in a single bounded move.
        harness.deliver(1, interval=1.0)

        self.assertEqual(harness.difficulty, STANDARD_LANE_START * 64)

    def test_initial_step_obeys_the_lane_ceiling(self) -> None:
        harness = FastArrivalHarness(
            standard_lane_config(max_difficulty=Decimal("262144"))
        )

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertEqual(harness.difficulty, Decimal("262144"))

    def test_initial_step_stays_within_high_diff_lane_bounds(self) -> None:
        # A rental-scale rig on the high-diff lane, whose floor is a wire
        # guarantee. The initial move is bounded above by the lane ceiling
        # and can never land below the floor.
        high_diff = standard_lane_config(
            min_difficulty=ROUTER_FILTER_DIFFICULTY,
            startup_difficulty=ROUTER_FILTER_DIFFICULTY,
            max_difficulty=Decimal("8000000"),
        )
        harness = FastArrivalHarness(high_diff, difficulty=ROUTER_FILTER_DIFFICULTY)
        harness.client.listener_name = "highdiff"
        harness.client.minimum_advertised_difficulty = ROUTER_FILTER_DIFFICULTY

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertEqual(harness.difficulty, Decimal("8000000"))
        self.assertGreaterEqual(harness.difficulty, ROUTER_FILTER_DIFFICULTY)
        self.assertEqual(
            harness.server.client_minimum_advertised_difficulty(harness.client),
            ROUTER_FILTER_DIFFICULTY,
        )

    def test_only_the_first_qualifying_phase_gets_the_larger_bound(self) -> None:
        harness = FastArrivalHarness(standard_lane_config())

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)
        converged = harness.difficulty
        self.assertEqual(converged, STANDARD_LANE_START * 64)
        self.assertFalse(self.client_initial_pending(harness))

        # Commit the advertised difficulty as delivered, then present an
        # equally decisive window: 600 accepted shares over a full 300s
        # interval observes 30x the current difficulty. The relaxation is
        # spent, so this move is bounded by the ordinary 4x, not by 30x.
        harness.client.share_difficulty = converged
        harness.client.pending_share_difficulty = None
        harness.deliver(600, interval=0.5)

        self.assertEqual(harness.difficulty, converged * 4)
        metrics = harness.metrics()
        self.assertEqual(
            metrics["qbit_prism_vardiff_initial_retarget_attempts_total"], "1"
        )

    def test_a_failed_send_does_not_spend_the_relaxation(self) -> None:
        # The relaxation is spent at the paired-send commit point, so a build
        # the coordinator skipped cannot consume it while leaving the miner
        # on its old difficulty.
        harness = FastArrivalHarness(standard_lane_config())
        harness.server.maybe_send_job = lambda client, clean_jobs: False  # type: ignore[method-assign]

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertEqual(harness.difficulty, STANDARD_LANE_START)
        self.assertIsNone(harness.client.pending_share_difficulty)
        self.assertTrue(self.client_initial_pending(harness))
        metrics = harness.metrics()
        self.assertEqual(
            metrics['qbit_prism_vardiff_initial_retargets_total{outcome="superseded"}'],
            "1",
        )
        self.assertEqual(
            metrics['qbit_prism_vardiff_initial_retargets_total{outcome="applied"}'],
            "0",
        )

        # Delivery recovers, and the session still converges in one move.
        harness.server.maybe_send_job = harness._send  # type: ignore[method-assign]
        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertEqual(harness.difficulty, STANDARD_LANE_START * 64)

    def test_explicit_request_outranks_a_converged_initial_difficulty(self) -> None:
        # The early path measures a session against the difficulty it is
        # currently stamped at, so an explicit d= raises the baseline the
        # evidence is judged against rather than being bypassed by it. And
        # once vardiff has converged, a d= still resolves to exactly what
        # was asked for: the request outranks the converged value.
        harness = FastArrivalHarness(standard_lane_config())
        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)
        self.assertEqual(harness.difficulty, STANDARD_LANE_START * 64)

        harness.client.requested_difficulty = Decimal("262144")
        target = harness.server.apply_client_difficulty_requests(harness.client)

        self.assertEqual(target, Decimal("262144"))
        self.assertEqual(
            harness.client.vardiff_config.startup_difficulty,
            Decimal("262144"),
        )

    def test_md_floor_is_never_undercut_by_an_initial_step(self) -> None:
        # md= raises the client's own minimum. The initial step clamps to the
        # specialized policy's bounds like any other step.
        harness = FastArrivalHarness(standard_lane_config())
        harness.client.requested_min_difficulty = Decimal("262144")
        harness.server.apply_client_difficulty_requests(harness.client)
        self.assertEqual(
            harness.client.vardiff_config.min_difficulty,
            Decimal("262144"),
        )

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertGreaterEqual(harness.difficulty, Decimal("262144"))

    def test_request_landing_mid_retarget_wins_over_the_initial_step(self) -> None:
        # The race: the early retarget computed its step from 16384, but an
        # explicit request moved the client before the paired send. The
        # retarget yields rather than overriding the request.
        harness = FastArrivalHarness(standard_lane_config())
        harness.client.pending_share_difficulty = ROUTER_FILTER_DIFFICULTY

        applied = harness.service.retarget_locked(
            harness.client,
            current_difficulty=STANDARD_LANE_START,
            accepted_shares=8,
            submitted_shares=8,
            accepted_difficulty=STANDARD_LANE_START * 8,
            elapsed_seconds=Decimal("1.828"),
            initial_convergence=True,
        )

        self.assertFalse(applied)
        self.assertEqual(harness.client.pending_share_difficulty, ROUTER_FILTER_DIFFICULTY)
        self.assertEqual(harness.sent, [])
        metrics = harness.metrics()
        self.assertEqual(
            metrics['qbit_prism_vardiff_initial_retargets_total{outcome="superseded"}'],
            "1",
        )

    def test_idle_step_down_never_takes_the_initial_bound(self) -> None:
        # An idle window carries no accepted shares, so it can never be the
        # evidence the relaxation is granted for, even if a caller asks.
        harness = FastArrivalHarness(standard_lane_config(), difficulty=Decimal("1048576"))
        # The client is never admitted, so the idle preconditions reject this
        # retarget outright. What matters is that require_idle stripped the
        # relaxation before any of that: a zero-share window is not evidence.
        applied = harness.service.retarget_locked(
            harness.client,
            current_difficulty=Decimal("1048576"),
            accepted_shares=0,
            submitted_shares=0,
            accepted_difficulty=Decimal("0"),
            elapsed_seconds=Decimal("300"),
            initial_convergence=True,
            require_idle=True,
            prepared_bundle=object(),  # type: ignore[arg-type]
        )

        self.assertFalse(applied)
        metrics = harness.metrics()
        self.assertEqual(
            metrics["qbit_prism_vardiff_initial_retarget_attempts_total"], "0"
        )

    def test_arrival_metrics_are_bounded_and_observed_once(self) -> None:
        harness = FastArrivalHarness(standard_lane_config())

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        metrics = harness.metrics()
        self.assertEqual(
            metrics["qbit_prism_vardiff_initial_retarget_attempts_total"], "1"
        )
        self.assertEqual(
            metrics['qbit_prism_vardiff_initial_retargets_total{outcome="applied"}'],
            "1",
        )
        self.assertEqual(
            metrics["qbit_prism_vardiff_high_diff_arrival_seconds_count"], "1"
        )
        self.assertEqual(
            metrics["qbit_prism_vardiff_high_diff_arrival_shares_count"], "1"
        )
        # Eight accepted shares over ~1.8s of connection life.
        self.assertEqual(
            metrics["qbit_prism_vardiff_high_diff_arrival_shares_sum"], "8.000000"
        )
        self.assertEqual(
            metrics['qbit_prism_vardiff_high_diff_arrival_shares_bucket{le="8"}'], "1"
        )
        self.assertEqual(
            metrics['qbit_prism_vardiff_high_diff_arrival_shares_bucket{le="4"}'], "0"
        )
        self.assertEqual(
            metrics['qbit_prism_vardiff_high_diff_arrival_seconds_bucket{le="2.5"}'],
            "1",
        )

        # A later retarget across the same threshold does not re-observe.
        harness.client.share_difficulty = harness.difficulty
        harness.client.pending_share_difficulty = None
        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertEqual(
            harness.metrics()["qbit_prism_vardiff_high_diff_arrival_shares_count"], "1"
        )

    def test_session_starting_above_the_threshold_never_contributes(self) -> None:
        high_diff = standard_lane_config(
            min_difficulty=ROUTER_FILTER_DIFFICULTY,
            startup_difficulty=ROUTER_FILTER_DIFFICULTY,
        )
        harness = FastArrivalHarness(high_diff, difficulty=ROUTER_FILTER_DIFFICULTY)

        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        self.assertGreater(harness.difficulty, ROUTER_FILTER_DIFFICULTY)
        self.assertEqual(
            harness.metrics()["qbit_prism_vardiff_high_diff_arrival_shares_count"], "0"
        )

    def test_metric_label_sets_are_fixed(self) -> None:
        harness = FastArrivalHarness(standard_lane_config())
        harness.deliver(8, interval=ROUTER_SHARE_INTERVAL_SECONDS)

        rendered = harness.metrics()
        outcomes = [
            name
            for name in rendered
            if name.startswith("qbit_prism_vardiff_initial_retargets_total")
        ]
        self.assertEqual(len(outcomes), len(PRISM_VARDIFF_INITIAL_RETARGET_OUTCOMES))
        for outcome in PRISM_VARDIFF_INITIAL_RETARGET_OUTCOMES:
            self.assertIn(
                f'qbit_prism_vardiff_initial_retargets_total{{outcome="{outcome}"}}',
                rendered,
            )
        for metric, buckets in (
            (
                "qbit_prism_vardiff_high_diff_arrival_seconds",
                PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SECONDS_BUCKETS,
            ),
            (
                "qbit_prism_vardiff_high_diff_arrival_shares",
                PRISM_VARDIFF_HIGH_DIFF_ARRIVAL_SHARES_BUCKETS,
            ),
        ):
            emitted = [
                name for name in rendered if name.startswith(f"{metric}_bucket")
            ]
            self.assertEqual(len(emitted), len(buckets) + 1)
        # No worker or username ever reaches a label.
        self.assertNotIn("miner", " ".join(rendered))

    @staticmethod
    def client_initial_pending(harness: FastArrivalHarness) -> bool:
        return bool(harness.client.vardiff_initial_convergence_pending)


if __name__ == "__main__":
    unittest.main()
