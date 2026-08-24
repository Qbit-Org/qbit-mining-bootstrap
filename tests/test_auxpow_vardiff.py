#!/usr/bin/env python3

from __future__ import annotations

import unittest
from decimal import Decimal

from lab.auxpow import vardiff


class AuxPowVardiffTests(unittest.TestCase):
    def config(self) -> vardiff.VardiffConfig:
        return vardiff.VardiffConfig(
            enabled=True,
            target_share_interval_seconds=Decimal("15"),
            min_difficulty=Decimal("0.01"),
            max_difficulty=Decimal("1024"),
            retarget_interval_seconds=Decimal("90"),
            max_step_factor=Decimal("4"),
            startup_difficulty=Decimal("1"),
            max_step_down_factor=Decimal("4"),
            ewma_alpha=Decimal("1"),
            retarget_tolerance=Decimal("0"),
        )

    def test_raises_difficulty_toward_observed_share_rate(self) -> None:
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("2"),
            accepted_shares=12,
            elapsed_seconds=Decimal("60"),
            config=self.config(),
        )

        self.assertEqual(next_difficulty, Decimal("6"))

    def test_limits_single_retarget_step_up(self) -> None:
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("2"),
            accepted_shares=100,
            elapsed_seconds=Decimal("60"),
            config=self.config(),
        )

        self.assertEqual(next_difficulty, Decimal("8"))

    def test_zero_share_window_steps_down(self) -> None:
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("2"),
            accepted_shares=0,
            elapsed_seconds=Decimal("90"),
            config=self.config(),
        )

        self.assertEqual(next_difficulty, Decimal("0.5"))

    def test_absolute_bounds_are_applied(self) -> None:
        low = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("0.02"),
            accepted_shares=0,
            elapsed_seconds=Decimal("90"),
            config=self.config(),
        )
        high = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("512"),
            accepted_shares=100,
            elapsed_seconds=Decimal("60"),
            config=self.config(),
        )

        self.assertEqual(low, Decimal("0.01"))
        self.assertEqual(high, Decimal("1024"))

    def test_share_weighted_work_drives_observed_difficulty(self) -> None:
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("8"),
            accepted_shares=4,
            accepted_difficulty=Decimal("80"),
            elapsed_seconds=Decimal("60"),
            config=self.config(),
        )

        self.assertEqual(next_difficulty, Decimal("20"))

    def test_hysteresis_suppresses_small_retargets(self) -> None:
        self.assertFalse(vardiff.should_retarget(Decimal("100"), Decimal("119"), Decimal("0.20")))
        self.assertTrue(vardiff.should_retarget(Decimal("100"), Decimal("120"), Decimal("0.20")))


class AuxPowVardiffInitialConvergenceTests(unittest.TestCase):
    """Fast-arrival initial convergence math (#132)."""

    def config(self, **overrides: object) -> vardiff.VardiffConfig:
        base = dict(
            enabled=True,
            target_share_interval_seconds=Decimal("15"),
            min_difficulty=Decimal("16384"),
            max_difficulty=Decimal("4294967296"),
            retarget_interval_seconds=Decimal("300"),
            max_step_factor=Decimal("4"),
            startup_difficulty=Decimal("16384"),
            max_step_down_factor=Decimal("4"),
            ewma_alpha=Decimal("0.4"),
            retarget_tolerance=Decimal("0.25"),
        )
        base.update(overrides)
        return vardiff.VardiffConfig(**base)  # type: ignore[arg-type]

    def ready(self, **overrides: object) -> bool:
        # The incident window: eight router-filtered shares stamped at the
        # 16384 lane start, arriving in the ~1.83s a 9.4 PH/s rig needs to
        # produce eight shares that clear a 500000 filter.
        call = dict(
            current_difficulty=Decimal("16384"),
            accepted_shares=8,
            accepted_difficulty=Decimal("16384") * 8,
            elapsed_seconds=Decimal("1.828"),
            config=self.config(),
        )
        call.update(overrides)
        return vardiff.initial_convergence_ready(**call)  # type: ignore[arg-type]

    def test_router_filtered_burst_is_ready(self) -> None:
        self.assertTrue(self.ready())

    def test_too_few_shares_is_not_ready(self) -> None:
        self.assertFalse(
            self.ready(
                accepted_shares=7,
                accepted_difficulty=Decimal("16384") * 7,
            )
        )

    def test_near_zero_elapsed_window_is_not_ready(self) -> None:
        # Without the elapsed floor this window divides real work by an
        # almost-zero interval. note_accepted clamps elapsed at 1ms, and at
        # that clamp eight lane-start shares observe ~1.97e9 -- a 120000x
        # gap that the wider initial bound would happily act on.
        self.assertFalse(self.ready(elapsed_seconds=Decimal("0.001")))
        self.assertGreater(
            vardiff.observed_difficulty(
                accepted_difficulty=Decimal("16384") * 8,
                elapsed_seconds=Decimal("0.001"),
                target_share_interval_seconds=Decimal("15"),
            ),
            Decimal("16384") * 100000,
        )

    def test_on_target_miner_is_not_ready(self) -> None:
        # One accepted share per target interval: eight shares over eight
        # intervals observes exactly the current difficulty, so the early
        # path must not engage no matter how much evidence accumulates.
        for shares in (8, 64, 512):
            with self.subTest(shares=shares):
                self.assertFalse(
                    self.ready(
                        accepted_shares=shares,
                        accepted_difficulty=Decimal("16384") * shares,
                        elapsed_seconds=Decimal("15") * shares,
                    )
                )

    def test_gap_below_the_confidence_floor_is_not_ready(self) -> None:
        # 3.9x observed against a 4x floor: inside the noise a small sample
        # can manufacture, so it waits for the ordinary interval.
        self.assertFalse(
            self.ready(
                accepted_difficulty=Decimal("16384") * 8,
                elapsed_seconds=Decimal("15") * 8 / Decimal("3.9"),
            )
        )
        self.assertTrue(
            self.ready(
                accepted_difficulty=Decimal("16384") * 8,
                elapsed_seconds=Decimal("15") * 8 / Decimal("4.1"),
            )
        )

    def test_disabled_policy_is_never_ready(self) -> None:
        self.assertFalse(self.ready(config=self.config(initial_convergence_enabled=False)))
        self.assertFalse(self.ready(config=self.config(enabled=False)))

    def test_initial_step_crosses_the_router_difficulty_in_one_move(self) -> None:
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("16384"),
            accepted_shares=8,
            accepted_difficulty=Decimal("16384") * 8,
            elapsed_seconds=Decimal("1.828"),
            config=self.config(),
            initial_convergence=True,
        )

        self.assertEqual(next_difficulty, Decimal("16384") * 64)
        self.assertGreaterEqual(next_difficulty, Decimal("500000"))

    def test_same_window_without_the_initial_flag_keeps_the_ordinary_bound(self) -> None:
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("16384"),
            accepted_shares=8,
            accepted_difficulty=Decimal("16384") * 8,
            elapsed_seconds=Decimal("1.828"),
            config=self.config(),
        )

        self.assertEqual(next_difficulty, Decimal("16384") * 4)
        self.assertLess(next_difficulty, Decimal("500000"))

    def test_initial_step_obeys_the_lane_ceiling(self) -> None:
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("16384"),
            accepted_shares=8,
            accepted_difficulty=Decimal("16384") * 8,
            elapsed_seconds=Decimal("1.828"),
            config=self.config(max_difficulty=Decimal("262144")),
            initial_convergence=True,
        )

        self.assertEqual(next_difficulty, Decimal("262144"))

    def test_initial_step_obeys_the_high_diff_floor(self) -> None:
        # The high-diff lane's floor is a wire guarantee, so even a step
        # computed under the wider initial bound clamps up to it.
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("500000"),
            accepted_shares=8,
            accepted_difficulty=Decimal("500000") * 8,
            elapsed_seconds=Decimal("1200"),
            config=self.config(
                min_difficulty=Decimal("500000"),
                startup_difficulty=Decimal("500000"),
            ),
            initial_convergence=True,
        )

        self.assertEqual(next_difficulty, Decimal("500000"))

    def test_initial_flag_does_not_widen_the_step_down_bound(self) -> None:
        next_difficulty = vardiff.calculate_next_difficulty(
            current_difficulty=Decimal("1048576"),
            accepted_shares=0,
            elapsed_seconds=Decimal("300"),
            config=self.config(),
            initial_convergence=True,
        )

        self.assertEqual(next_difficulty, Decimal("1048576") / 4)

    def test_inverted_step_bounds_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.config(
                max_step_factor=Decimal("128"),
                initial_max_step_factor=Decimal("64"),
            )
        with self.assertRaises(ValueError):
            self.config(initial_min_accepted_shares=0)
        with self.assertRaises(ValueError):
            self.config(initial_min_elapsed_seconds=Decimal("0"))


if __name__ == "__main__":
    unittest.main()
