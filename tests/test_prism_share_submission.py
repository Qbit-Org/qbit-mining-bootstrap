#!/usr/bin/env python3
"""Direct tests for pure PRISM share-submission decisions."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import SimpleNamespace
import threading
import unittest
from unittest.mock import patch

from lab.prism.job_delivery import PRISM_CREDIT_POLICY_STALE_GRACE
from lab.prism.prism_coordinator import PrismCoordinator
from lab.prism import prism_coordinator
from lab.prism.share_submission import (
    PRISM_REJECTION_INVALID_EXTRANONCE,
    PRISM_REJECTION_LOW_DIFFICULTY,
    PRISM_REJECTION_MALFORMED_SUBMIT,
    PRISM_REJECTION_STALE_JOB,
    PRISM_REJECTION_UNAUTHORIZED_WORKER,
    PRISM_REJECTION_UNKNOWN_JOB,
    RecentShareIndex,
    SubmitContextInput,
    SubmitRejected,
    classify_submit_context,
    classify_submit_work,
    parse_submit_request,
    validate_submit_request,
)


def context(*, parent: str = "tip", username: str = "miner-a") -> object:
    return SimpleNamespace(
        template={"previousblockhash": parent},
        worker=SimpleNamespace(username=username),
    )


class SubmitRequestTests(unittest.TestCase):
    def test_parse_returns_immutable_wire_values(self) -> None:
        request = parse_submit_request(
            ["miner-a", "job-1", "00" * 8, "01020304", "05060708", "1fffe000"]
        )

        self.assertEqual(request.job_id, "job-1")
        self.assertEqual(request.version_bits_hex, "1fffe000")
        with self.assertRaises(FrozenInstanceError):
            request.job_id = "job-2"  # type: ignore[misc]

    def test_parse_rejects_incomplete_params(self) -> None:
        with self.assertRaises(SubmitRejected) as raised:
            parse_submit_request(["miner-a", "job-1"])

        self.assertEqual(raised.exception.reason, PRISM_REJECTION_MALFORMED_SUBMIT)

    def test_validation_preserves_protocol_order(self) -> None:
        request = parse_submit_request(
            ["miner-b", "job-1", "00", "01020304", "05060708"]
        )
        with self.assertRaises(SubmitRejected) as unauthorized:
            validate_submit_request(
                request,
                authorized_username="miner-a",
                pool_open=True,
                extranonce2_size=8,
            )
        self.assertEqual(
            unauthorized.exception.reason,
            PRISM_REJECTION_UNAUTHORIZED_WORKER,
        )

        request = parse_submit_request(
            ["miner-a", "job-1", "00", "01020304", "05060708"]
        )
        with self.assertRaises(SubmitRejected) as invalid_width:
            validate_submit_request(
                request,
                authorized_username="miner-a",
                pool_open=True,
                extranonce2_size=8,
            )
        self.assertEqual(
            invalid_width.exception.reason,
            PRISM_REJECTION_INVALID_EXTRANONCE,
        )


class RecentShareIndexTests(unittest.TestCase):
    def test_coordinator_initialization_is_single_flight(self) -> None:
        server = PrismCoordinator.__new__(PrismCoordinator)
        server.extranonce2_size = 8
        seed = {("miner-a", "header-a")}
        server.recent_share_keys = seed
        constructor_started = threading.Event()
        release_constructor = threading.Event()
        constructor_calls = 0
        results: list[object] = []
        errors: list[BaseException] = []
        real_constructor = prism_coordinator.ShareSubmissionService

        def delayed_constructor(*args: object, **kwargs: object) -> object:
            nonlocal constructor_calls
            constructor_calls += 1
            constructor_started.set()
            release_constructor.wait(2)
            return real_constructor(*args, **kwargs)  # type: ignore[arg-type]

        def initialize() -> None:
            try:
                results.append(server._ensure_share_submission_service())
            except BaseException as exc:  # noqa: BLE001 - surfaced below
                errors.append(exc)

        with patch.object(
            prism_coordinator,
            "ShareSubmissionService",
            side_effect=delayed_constructor,
        ):
            first = threading.Thread(target=initialize)
            second = threading.Thread(target=initialize)
            first.start()
            self.assertTrue(constructor_started.wait(1))
            second.start()
            release_constructor.set()
            first.join(2)
            second.join(2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(constructor_calls, 1)
        self.assertEqual(len(results), 2)
        self.assertIs(results[0], results[1])
        self.assertEqual(results[0].recent_shares.snapshot(), tuple(seed))

    def test_capacity_evicts_only_the_oldest_insertion(self) -> None:
        index = RecentShareIndex(capacity=2)
        first = ("miner", "header-a")
        second = ("miner", "header-b")
        third = ("miner", "header-c")

        self.assertTrue(index.reserve(first))
        self.assertTrue(index.reserve(second))
        self.assertFalse(index.reserve(first))
        self.assertTrue(index.reserve(third))

        self.assertEqual(index.snapshot(), (second, third))
        self.assertFalse(index.reserve(second))
        self.assertTrue(index.reserve(first))
        self.assertEqual(index.snapshot(), (third, first))

    def test_release_allows_exact_retry_without_disturbing_order(self) -> None:
        index = RecentShareIndex(capacity=2)
        first = ("miner", "header-a")
        second = ("miner", "header-b")
        index.reserve(first)
        index.reserve(second)

        index.release(first)

        self.assertTrue(index.reserve(first))
        self.assertEqual(index.snapshot(), (second, first))


class SubmitClassificationTests(unittest.TestCase):
    def test_active_and_retained_current_tip_are_normal_credit(self) -> None:
        active = context()
        active_decision = classify_submit_context(
            SubmitContextInput(active, None, "tip", False)  # type: ignore[arg-type]
        )
        self.assertEqual(active_decision.source, "active")
        self.assertIsNone(active_decision.credit_policy)

        retained = SimpleNamespace(context=context())
        retained_decision = classify_submit_context(
            SubmitContextInput(None, retained, "tip", False)  # type: ignore[arg-type]
        )
        self.assertEqual(retained_decision.source, "retained")
        self.assertIsNone(retained_decision.credit_policy)

    def test_missing_and_stale_contexts_have_distinct_rejections(self) -> None:
        with self.assertRaises(SubmitRejected) as missing:
            classify_submit_context(SubmitContextInput(None, None, "tip", False))
        self.assertEqual(missing.exception.reason, PRISM_REJECTION_UNKNOWN_JOB)

        with self.assertRaises(SubmitRejected) as stale:
            classify_submit_context(
                SubmitContextInput(context(parent="old"), None, "tip", False)  # type: ignore[arg-type]
            )
        self.assertEqual(stale.exception.reason, PRISM_REJECTION_STALE_JOB)

    def test_stale_grace_suppresses_block_route(self) -> None:
        decision = classify_submit_context(
            SubmitContextInput(context(parent="old"), None, "tip", True)  # type: ignore[arg-type]
        )
        self.assertEqual(decision.credit_policy, PRISM_CREDIT_POLICY_STALE_GRACE)

        submission = SimpleNamespace(
            share_pass=True,
            block_pass=True,
            header_hex="aa",
        )
        work = classify_submit_work(
            decision.context,
            submission,
            credit_policy=decision.credit_policy,
        )
        self.assertEqual(work.route, "share")
        self.assertFalse(work.block_worthy)

    def test_work_routes_cover_share_and_both_block_paths(self) -> None:
        value = context()
        ordinary = classify_submit_work(
            value,  # type: ignore[arg-type]
            SimpleNamespace(share_pass=True, block_pass=False, header_hex="a"),
            credit_policy=None,
        )
        asynchronous = classify_submit_work(
            value,  # type: ignore[arg-type]
            SimpleNamespace(share_pass=True, block_pass=True, header_hex="b"),
            credit_policy=None,
        )
        synchronous = classify_submit_work(
            value,  # type: ignore[arg-type]
            SimpleNamespace(share_pass=False, block_pass=True, header_hex="c"),
            credit_policy=None,
        )

        self.assertEqual(ordinary.route, "share")
        self.assertEqual(asynchronous.route, "async_block")
        self.assertEqual(synchronous.route, "synchronous_block")
        self.assertTrue(synchronous.credit_share_on_accept)

        with self.assertRaises(SubmitRejected) as low:
            classify_submit_work(
                value,  # type: ignore[arg-type]
                SimpleNamespace(share_pass=False, block_pass=False, header_hex="d"),
                credit_policy=None,
            )
        self.assertEqual(low.exception.reason, PRISM_REJECTION_LOW_DIFFICULTY)


if __name__ == "__main__":
    unittest.main()
