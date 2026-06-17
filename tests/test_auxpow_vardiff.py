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


if __name__ == "__main__":
    unittest.main()
