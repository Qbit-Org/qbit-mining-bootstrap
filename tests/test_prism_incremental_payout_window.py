#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import math
import random
import unittest
from dataclasses import replace
from typing import Iterable, Sequence

from lab.prism.share_ledger import (
    AcceptedShareRecord,
    IncrementalShareWindow,
    IncrementalWindowFallback,
)


RANDOM_MASTER_SEED = 0x51A7_E2026
RANDOM_SEQUENCE_COUNT = 10_000


def accepted_share(
    share_seq: int,
    *,
    share_difficulty: int,
    job_issued_at_ms: int,
    accepted_at_ms: int,
    credit_policy: str | None = None,
) -> AcceptedShareRecord:
    return AcceptedShareRecord(
        share_seq=share_seq,
        share_id=f"share-{share_seq}",
        miner_id=f"miner-{share_seq % 11}",
        order_key=f"{share_seq % 11:02d}:miner-{share_seq % 11}",
        p2mr_program_hex=f"{share_seq % 256:02x}" * 32,
        share_difficulty=share_difficulty,
        network_difficulty=1_000_000 + (share_seq % 17),
        template_height=800_000 + (share_seq // 32),
        job_id=f"job-{share_seq}",
        job_issued_at_ms=job_issued_at_ms,
        accepted_at_ms=accepted_at_ms,
        ntime=1_700_000_000 + share_seq,
        credit_policy=credit_policy,
    )


def full_rescan_oracle(
    records: Iterable[AcceptedShareRecord],
    *,
    anchor_job_issued_at_ms: int,
    window_weight: int,
) -> tuple[AcceptedShareRecord, ...]:
    """Model qbit_share_ledger's bounded snapshot without sharing cache code."""
    eligible = sorted(
        (
            record
            for record in records
            if record.job_issued_at_ms <= anchor_job_issued_at_ms
            and record.accepted_at_ms <= anchor_job_issued_at_ms
        ),
        key=lambda record: record.share_seq,
    )
    retained_newest_first: list[AcceptedShareRecord] = []
    cumulative_weight = 0
    for record in reversed(eligible):
        if cumulative_weight >= window_weight:
            break
        retained_newest_first.append(record)
        cumulative_weight += record.share_difficulty
    retained_newest_first.reverse()
    return tuple(retained_newest_first)


def canonical_json_text(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_text(value).encode()).hexdigest()


def carry_forward_near_floor(
    rng: random.Random,
) -> tuple[int, tuple[tuple[str, str, str, int], ...]]:
    floor_sats = rng.choice((1, 2, 10, 546, 1_000, 10_000))
    boundary_amounts = (
        0,
        max(0, floor_sats - 1),
        floor_sats,
        floor_sats + 1,
        (2 * floor_sats) - 1,
    )
    carry = tuple(
        (
            f"carry-miner-{index}",
            f"{index:02d}:carry-miner-{index}",
            f"{(index + 192) % 256:02x}" * 32,
            rng.choice(boundary_amounts),
        )
        for index in range(rng.randint(0, 4))
    )
    return floor_sats, carry


def artifact_equivalence_projection(
    records: Sequence[AcceptedShareRecord],
    *,
    carry_forward: tuple[tuple[str, str, str, int], ...],
    payout_floor_sats: int,
) -> tuple[tuple[dict[str, object], ...], str, str]:
    """Project the money-path inputs whose ordering and bytes must not change.

    The boundary classification deliberately includes balances immediately
    below, at, and above a payout-like floor. Incremental maintenance does not
    own payout math, but it must hand exactly the same share and carry tuples to
    that math as a full rebuild.
    """
    shares_json = tuple(record.to_prism_json() for record in records)
    balances_json = tuple(
        {
            "recipient_id": recipient_id,
            "order_key": order_key,
            "p2mr_program_hex": p2mr_program_hex,
            "balance_sats": balance_sats,
        }
        for recipient_id, order_key, p2mr_program_hex, balance_sats in carry_forward
    )
    settlement_boundary = tuple(
        {
            "recipient_id": recipient_id,
            "paid_sats": balance_sats if balance_sats >= payout_floor_sats else 0,
            "carry_forward_sats": balance_sats if balance_sats < payout_floor_sats else 0,
        }
        for recipient_id, _order_key, _program, balance_sats in carry_forward
    )
    artifact_json = canonical_json_text(
        {
            "shares": shares_json,
            "prior_balances": balances_json,
            "payout_floor_sats": payout_floor_sats,
            "settlement_boundary": settlement_boundary,
        }
    )
    return shares_json, artifact_json, hashlib.sha256(artifact_json.encode()).hexdigest()


class IncrementalShareWindowGoldenTests(unittest.TestCase):
    def assert_window_parity(
        self,
        window: IncrementalShareWindow,
        history: Sequence[AcceptedShareRecord],
        *,
        anchor_job_issued_at_ms: int,
        window_weight: int,
        rng: random.Random,
    ) -> None:
        incremental_records = window.records()
        oracle_records = full_rescan_oracle(
            history,
            anchor_job_issued_at_ms=anchor_job_issued_at_ms,
            window_weight=window_weight,
        )
        self.assertIsInstance(incremental_records, tuple)
        self.assertEqual(incremental_records, oracle_records)
        self.assertEqual(
            tuple(record.share_seq for record in incremental_records),
            tuple(sorted(record.share_seq for record in incremental_records)),
        )

        incremental_json = tuple(record.to_prism_json() for record in incremental_records)
        oracle_json = tuple(record.to_prism_json() for record in oracle_records)
        self.assertEqual(incremental_json, oracle_json)
        self.assertEqual(
            canonical_json_sha256(incremental_json),
            canonical_json_sha256(oracle_json),
        )

        payout_floor_sats, carry_forward = carry_forward_near_floor(rng)
        incremental_artifact = artifact_equivalence_projection(
            incremental_records,
            carry_forward=carry_forward,
            payout_floor_sats=payout_floor_sats,
        )
        oracle_artifact = artifact_equivalence_projection(
            oracle_records,
            carry_forward=carry_forward,
            payout_floor_sats=payout_floor_sats,
        )
        self.assertEqual(incremental_artifact, oracle_artifact)

    def assert_stats_shape(self, stats: object) -> None:
        for name in ("added_rows", "expired_rows", "touched_pages"):
            value = getattr(stats, name)
            self.assertIsInstance(value, int)
            self.assertGreaterEqual(value, 0)

    def test_full_snapshot_keeps_exact_crossing_row_and_ascending_order(self) -> None:
        anchor_ms = 10_000
        records = [
            accepted_share(
                1,
                share_difficulty=2,
                job_issued_at_ms=anchor_ms - 4,
                accepted_at_ms=anchor_ms - 3,
            ),
            accepted_share(
                2,
                share_difficulty=3,
                job_issued_at_ms=anchor_ms - 2,
                accepted_at_ms=anchor_ms - 1,
                credit_policy="stale-grace",
            ),
            accepted_share(
                3,
                share_difficulty=8,
                job_issued_at_ms=anchor_ms,
                accepted_at_ms=anchor_ms,
            ),
            accepted_share(
                4,
                share_difficulty=100,
                job_issued_at_ms=anchor_ms + 1,
                accepted_at_ms=anchor_ms,
            ),
        ]

        window = IncrementalShareWindow.from_full_snapshot(
            records,
            anchor_job_issued_at_ms=anchor_ms,
            window_weight=10,
            page_size=2,
        )

        self.assertEqual(tuple(record.share_seq for record in window.records()), (2, 3))
        self.assertEqual(window.records()[0].credit_policy, "stale-grace")
        expected_json = tuple(record.to_prism_json() for record in records[1:3])
        self.assertEqual(
            tuple(record.to_prism_json() for record in window.records()),
            expected_json,
        )
        self.assertEqual(
            canonical_json_sha256(tuple(record.to_prism_json() for record in window.records())),
            canonical_json_sha256(expected_json),
        )

    def test_advance_rejects_non_append_deltas_without_mutating_the_window(self) -> None:
        anchor_ms = 20_000
        records = [
            accepted_share(
                share_seq,
                share_difficulty=1,
                job_issued_at_ms=anchor_ms,
                accepted_at_ms=anchor_ms,
            )
            for share_seq in range(1, 7)
        ]
        window = IncrementalShareWindow.from_full_snapshot(
            records,
            anchor_job_issued_at_ms=anchor_ms,
            window_weight=5,
            page_size=2,
        )
        before = window.records()
        corrected = replace(records[-1], share_difficulty=9)
        unordered = (
            accepted_share(
                8,
                share_difficulty=1,
                job_issued_at_ms=anchor_ms + 1,
                accepted_at_ms=anchor_ms,
            ),
            accepted_share(
                7,
                share_difficulty=1,
                job_issued_at_ms=anchor_ms + 1,
                accepted_at_ms=anchor_ms,
            ),
        )

        for delta, next_anchor_ms in (
            ((corrected,), anchor_ms),
            ((records[-2],), anchor_ms),
            (unordered, anchor_ms + 1),
            ((), anchor_ms - 1),
        ):
            with self.subTest(delta=tuple(record.share_seq for record in delta), anchor=next_anchor_ms):
                with self.assertRaises(IncrementalWindowFallback):
                    window.advance(
                        delta,
                        anchor_job_issued_at_ms=next_anchor_ms,
                    )
                self.assertEqual(window.records(), before)

    def test_10_000_seeded_randomized_sequences_match_full_rescan(self) -> None:
        for sequence_number in range(RANDOM_SEQUENCE_COUNT):
            seed = RANDOM_MASTER_SEED + sequence_number
            rng = random.Random(seed)
            with self.subTest(seed=seed):
                anchor_ms = 1_000_000 + (sequence_number * 100)
                history: list[AcceptedShareRecord] = []
                next_share_seq = 1

                for _ in range(rng.randint(1, 8)):
                    history.append(
                        accepted_share(
                            next_share_seq,
                            share_difficulty=rng.choice((1, 2, 3, 7, 19, 101, 10_000)),
                            job_issued_at_ms=anchor_ms - rng.randint(0, 30),
                            accepted_at_ms=anchor_ms - rng.randint(0, 30),
                            credit_policy=(
                                "stale-grace" if rng.randrange(4) == 0 else None
                            ),
                        )
                    )
                    next_share_seq += 1

                recent_weight = sum(record.share_difficulty for record in history[-3:])
                window_weight = rng.choice(
                    (
                        1,
                        2,
                        10,
                        max(1, recent_weight - 1),
                        max(1, recent_weight),
                        recent_weight + 1,
                        100_000,
                    )
                )
                page_size = rng.randint(2, 8)
                window = IncrementalShareWindow.from_full_snapshot(
                    history,
                    anchor_job_issued_at_ms=anchor_ms,
                    window_weight=window_weight,
                    page_size=page_size,
                )
                self.assert_window_parity(
                    window,
                    history,
                    anchor_job_issued_at_ms=anchor_ms,
                    window_weight=window_weight,
                    rng=rng,
                )

                immediate_anchor_ms = anchor_ms + 1
                immediate_batch: list[AcceptedShareRecord] = []
                for _ in range(rng.randint(1, 4)):
                    record = accepted_share(
                        next_share_seq,
                        share_difficulty=rng.choice((1, 2, 5, 31, 5_000, 50_000)),
                        job_issued_at_ms=immediate_anchor_ms,
                        accepted_at_ms=anchor_ms - rng.randint(0, 3),
                        credit_policy="stale-grace" if rng.randrange(3) == 0 else None,
                    )
                    immediate_batch.append(record)
                    history.append(record)
                    next_share_seq += 1
                previous_count = len(window.records())
                window, stats = window.advance(
                    immediate_batch,
                    anchor_job_issued_at_ms=immediate_anchor_ms,
                )
                anchor_ms = immediate_anchor_ms
                self.assert_stats_shape(stats)
                self.assertEqual(stats.added_rows, len(immediate_batch))
                self.assertEqual(
                    stats.expired_rows,
                    previous_count + len(immediate_batch) - len(window.records()),
                )
                self.assert_window_parity(
                    window,
                    history,
                    anchor_job_issued_at_ms=anchor_ms,
                    window_weight=window_weight,
                    rng=rng,
                )

                transition_anchor_ms = anchor_ms + rng.randint(2, 30)
                delayed_batch: list[AcceptedShareRecord] = []
                for index in range(rng.randint(1, 3)):
                    transition_on_job_time = (index + sequence_number) % 2 == 0
                    record = accepted_share(
                        next_share_seq,
                        share_difficulty=rng.choice((1, 3, 13, 1_000, 100_000)),
                        job_issued_at_ms=(
                            transition_anchor_ms
                            if transition_on_job_time
                            else anchor_ms - rng.randint(0, 2)
                        ),
                        accepted_at_ms=(
                            anchor_ms - rng.randint(0, 2)
                            if transition_on_job_time
                            else transition_anchor_ms
                        ),
                        credit_policy="stale-grace" if rng.randrange(2) == 0 else None,
                    )
                    delayed_batch.append(record)
                    history.append(record)
                    next_share_seq += 1

                window, stats = window.advance(
                    (),
                    anchor_job_issued_at_ms=transition_anchor_ms - 1,
                )
                self.assert_stats_shape(stats)
                self.assertEqual((stats.added_rows, stats.expired_rows), (0, 0))
                self.assert_window_parity(
                    window,
                    history,
                    anchor_job_issued_at_ms=transition_anchor_ms - 1,
                    window_weight=window_weight,
                    rng=rng,
                )

                previous_count = len(window.records())
                window, stats = window.advance(
                    delayed_batch,
                    anchor_job_issued_at_ms=transition_anchor_ms,
                )
                anchor_ms = transition_anchor_ms
                self.assert_stats_shape(stats)
                self.assertEqual(stats.added_rows, len(delayed_batch))
                self.assertEqual(
                    stats.expired_rows,
                    previous_count + len(delayed_batch) - len(window.records()),
                )
                self.assert_window_parity(
                    window,
                    history,
                    anchor_job_issued_at_ms=anchor_ms,
                    window_weight=window_weight,
                    rng=rng,
                )

                correction_index = rng.randrange(len(history))
                original = history[correction_index]
                corrected = replace(
                    original,
                    share_difficulty=original.share_difficulty + rng.randint(1, 101),
                    credit_policy=(
                        None if original.credit_policy == "stale-grace" else "stale-grace"
                    ),
                )
                before_fallback = window.records()
                with self.assertRaises(IncrementalWindowFallback):
                    window.advance(
                        (corrected,),
                        anchor_job_issued_at_ms=anchor_ms,
                    )
                self.assertEqual(window.records(), before_fallback)
                history[correction_index] = corrected
                window = IncrementalShareWindow.from_full_snapshot(
                    history,
                    anchor_job_issued_at_ms=anchor_ms,
                    window_weight=window_weight,
                    page_size=page_size,
                )
                self.assert_window_parity(
                    window,
                    history,
                    anchor_job_issued_at_ms=anchor_ms,
                    window_weight=window_weight,
                    rng=rng,
                )

                post_rebuild_anchor_ms = anchor_ms + 1
                post_rebuild_batch: list[AcceptedShareRecord] = []
                for _ in range(rng.randint(1, 3)):
                    record = accepted_share(
                        next_share_seq,
                        share_difficulty=rng.choice((1, 2, 11, 257, 25_000)),
                        job_issued_at_ms=post_rebuild_anchor_ms,
                        accepted_at_ms=anchor_ms,
                        credit_policy="stale-grace" if rng.randrange(3) == 0 else None,
                    )
                    post_rebuild_batch.append(record)
                    history.append(record)
                    next_share_seq += 1
                window, stats = window.advance(
                    post_rebuild_batch,
                    anchor_job_issued_at_ms=post_rebuild_anchor_ms,
                )
                anchor_ms = post_rebuild_anchor_ms
                self.assert_stats_shape(stats)
                self.assertEqual(stats.added_rows, len(post_rebuild_batch))
                self.assert_window_parity(
                    window,
                    history,
                    anchor_job_issued_at_ms=anchor_ms,
                    window_weight=window_weight,
                    rng=rng,
                )

    def test_90_000_share_window_plus_500_delta_touches_only_edge_pages(self) -> None:
        anchor_ms = 2_000_000
        initial_count = 90_000
        delta_count = 500
        page_size = 4_096
        initial = tuple(
            accepted_share(
                share_seq,
                share_difficulty=1,
                job_issued_at_ms=anchor_ms,
                accepted_at_ms=anchor_ms,
                credit_policy="stale-grace" if share_seq % 101 == 0 else None,
            )
            for share_seq in range(1, initial_count + 1)
        )
        window = IncrementalShareWindow.from_full_snapshot(
            initial,
            anchor_job_issued_at_ms=anchor_ms,
            window_weight=initial_count,
            page_size=page_size,
        )
        delta = tuple(
            accepted_share(
                share_seq,
                share_difficulty=1,
                job_issued_at_ms=anchor_ms + 1,
                accepted_at_ms=anchor_ms + 1,
                credit_policy="stale-grace" if share_seq % 101 == 0 else None,
            )
            for share_seq in range(initial_count + 1, initial_count + delta_count + 1)
        )

        window, stats = window.advance(
            delta,
            anchor_job_issued_at_ms=anchor_ms + 1,
        )

        self.assert_stats_shape(stats)
        self.assertEqual(stats.added_rows, delta_count)
        self.assertEqual(stats.expired_rows, delta_count)
        # Only the partially retained head and append-edge pages may be
        # inspected or rewritten. Fully expired and newly allocated pages do
        # not count; any larger value exposes retained-interior page work.
        self.assertLessEqual(stats.touched_pages, 2)
        expected = full_rescan_oracle(
            (*initial, *delta),
            anchor_job_issued_at_ms=anchor_ms + 1,
            window_weight=initial_count,
        )
        self.assertEqual(window.records(), expected)
        incremental_json = tuple(record.to_prism_json() for record in window.records())
        expected_json = tuple(record.to_prism_json() for record in expected)
        self.assertEqual(incremental_json, expected_json)
        self.assertEqual(
            canonical_json_sha256(incremental_json),
            canonical_json_sha256(expected_json),
        )
        self.assertEqual(len(window.records()), initial_count)
        self.assertEqual(window.records()[0].share_seq, delta_count + 1)
        self.assertEqual(window.records()[-1].share_seq, initial_count + delta_count)
        self.assertLess(
            stats.touched_pages,
            math.ceil(initial_count / page_size),
        )


if __name__ == "__main__":
    unittest.main()
