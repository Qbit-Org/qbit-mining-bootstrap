#!/usr/bin/env python3
"""Settlement-equality comparison for payout previews and ledger snapshots.

A miner may re-authorize the same payout address under any valid bech32
spelling (all-lowercase and all-uppercase encode the same program), which
changes the recipient/order-key identity strings without changing the
committed P2MR payout program. Ledger-confirmation gates must therefore
judge settlement equality on (program, amount) alone: a pure relabel is
equal, while any amount or membership divergence still fails closed.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace

from lab.prism.prism_coordinator import PrismCoordinator

PROGRAM_HEX = "cb" * 32
OTHER_PROGRAM_HEX = "ad" * 32
ADDRESS_LOWER = "tq1z70ukpvs96kye6jmgvl3nttevtkrq8uu89snkpm6m8gwqukw8u5dsz32kwa"
ADDRESS_UPPER = ADDRESS_LOWER.upper()


def balance_row(
    *,
    recipient_id: str,
    order_key: str | None = None,
    program_hex: str = PROGRAM_HEX,
    balance_sats: int,
) -> dict[str, object]:
    return {
        "recipient_id": recipient_id,
        "order_key": order_key if order_key is not None else recipient_id,
        "p2mr_program_hex": program_hex,
        "balance_sats": balance_sats,
    }


class SettlementBalancesByProgramTest(unittest.TestCase):
    def test_case_variant_identity_is_settlement_equal(self) -> None:
        published = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1234)]
        durable = [balance_row(recipient_id=ADDRESS_UPPER, balance_sats=1234)]
        self.assertNotEqual(published, durable)
        self.assertEqual(
            PrismCoordinator.settlement_balances_by_program(published),
            PrismCoordinator.settlement_balances_by_program(durable),
        )

    def test_program_hex_case_is_normalized(self) -> None:
        lower = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=5)]
        upper = [
            balance_row(
                recipient_id=ADDRESS_LOWER,
                program_hex=PROGRAM_HEX.upper(),
                balance_sats=5,
            )
        ]
        self.assertEqual(
            PrismCoordinator.settlement_balances_by_program(lower),
            PrismCoordinator.settlement_balances_by_program(upper),
        )

    def test_amount_divergence_still_detected(self) -> None:
        published = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1234)]
        durable = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1235)]
        self.assertNotEqual(
            PrismCoordinator.settlement_balances_by_program(published),
            PrismCoordinator.settlement_balances_by_program(durable),
        )

    def test_membership_divergence_still_detected(self) -> None:
        published = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1234)]
        durable = published + [
            balance_row(
                recipient_id="qb1zother",
                program_hex=OTHER_PROGRAM_HEX,
                balance_sats=7,
            )
        ]
        self.assertNotEqual(
            PrismCoordinator.settlement_balances_by_program(published),
            PrismCoordinator.settlement_balances_by_program(durable),
        )

    def test_zero_totals_are_dropped(self) -> None:
        with_zero = [
            balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1234),
            balance_row(
                recipient_id="qb1zother",
                program_hex=OTHER_PROGRAM_HEX,
                balance_sats=0,
            ),
        ]
        without_zero = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1234)]
        self.assertEqual(
            PrismCoordinator.settlement_balances_by_program(with_zero),
            PrismCoordinator.settlement_balances_by_program(without_zero),
        )

    def test_split_identities_aggregate_per_program(self) -> None:
        split = [
            balance_row(recipient_id=ADDRESS_UPPER, balance_sats=1000),
            balance_row(recipient_id=ADDRESS_LOWER, balance_sats=234),
        ]
        merged = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1234)]
        self.assertEqual(
            PrismCoordinator.settlement_balances_by_program(split),
            PrismCoordinator.settlement_balances_by_program(merged),
        )


class PriorBalancesMatchCurrentTest(unittest.TestCase):
    def match_current(
        self,
        prior_balances: list[dict[str, object]],
        current: list[dict[str, object]],
    ) -> bool:
        coordinator = SimpleNamespace(
            settlement_balances_by_program=PrismCoordinator.settlement_balances_by_program,
            ledger=SimpleNamespace(current_prior_balances=lambda: current),
        )
        return PrismCoordinator.prior_balances_match_current(
            coordinator, prior_balances
        )

    def test_relabeled_identity_matches_current(self) -> None:
        prior = [balance_row(recipient_id=ADDRESS_UPPER, balance_sats=1234)]
        current = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1234)]
        self.assertTrue(self.match_current(prior, current))

    def test_changed_amount_does_not_match_current(self) -> None:
        prior = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=1234)]
        current = [balance_row(recipient_id=ADDRESS_LOWER, balance_sats=4321)]
        self.assertFalse(self.match_current(prior, current))


if __name__ == "__main__":
    unittest.main()
