#!/usr/bin/env python3
"""Self-tests for the deterministic concurrency harness.

A harness that lies is worse than no harness: every scenario built on it
inherits the lie, and a green suite then argues *against* fixing a real
defect. These tests pin the three properties the lease scenarios depend on.

1. **Classification is anchored to production SQL.** ``FakePostgres`` answers
   statements by recognising them, so a statement it recognises wrongly is a
   silent divergence. The classifier is driven from real ``PsqlShareLedger``
   calls, and anything unrecognised raises rather than returning a plausible
   answer.

2. **The ports match production.** The fakes stand in for
   ``_NativePostgresClient`` and ``_NativePostgresLeaseGuard``; if those
   signatures move, the fakes must break loudly rather than silently stop
   modelling something.

3. **Order is an input, not an accident.** The scheduler must produce an
   identical trace every run, must refuse to step an actor that cannot
   progress, and must fail fast when an actor blocks on something the
   harness does not model instead of hanging the suite.
"""

from __future__ import annotations

import inspect
import threading
import unittest

from lab.prism import share_ledger as share_ledger_module
from tests.prism_concurrency_harness import (
    HarnessError,
    LeaseHarness,
    LeaseOp,
    NotRunnable,
    SchedulerStall,
    UnsupportedStatement,
    VirtualClock,
    advisory_lock_pair,
    assert_deterministic,
    classify,
)


class StatementClassificationTests(unittest.TestCase):
    """The classifier is pinned to the SQL PsqlShareLedger actually emits."""

    def test_every_lease_statement_is_recognised_from_real_calls(self) -> None:
        """Drive each lease operation through production code and classify it.

        This is the pin that matters: the fragments in ``_SIGNATURES`` are
        lifted from ``share_ledger.py``, and this test proves they still
        match what that module generates rather than what it generated when
        the harness was written.
        """
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            alpha.call(alpha.ledger.renew_writer_lease, label="renew")
            alpha.call(
                alpha.ledger.verify_writer_lease_guard_session,
                label="verify",
            )
            alpha.call(alpha.ledger.release_writer_lease, label="release")

            # Adoption only runs against a silent same-identity predecessor,
            # so it needs a second coordinator rather than another call.
            beta = harness.coordinator(
                "beta",
                session_token="heartbeat-v1:beta-successor",
            )
            beta.start()
            harness.run_until(beta, "done:startup")

            self.assertEqual(
                harness.statement_kinds(),
                ["acquire", "renew", "verify", "release", "acquire"],
            )
            # Every statement above reached FakePostgres and was classified;
            # an unrecognised one would have raised inside the ledger call.
            self.assertTrue(
                all(
                    isinstance(statement.kind, LeaseOp)
                    for statement in harness.server.statements
                )
            )

    def test_adoption_statement_classifies(self) -> None:
        """The adoption CAS is the one statement startup alone never emits."""
        ledger_sql = share_ledger_module.PsqlShareLedger.__dict__[
            "_try_adopt_writer_lease"
        ]
        self.assertIn("observed_writer_session_token", inspect.getsource(ledger_sql))

        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            sql = alpha.ledger._try_adopt_writer_lease.__wrapped__ if False else None
            del sql
            # Build the statement through the real method, capturing it at the
            # port rather than reconstructing it here.
            observed = {
                "writer_session_token": "heartbeat-v1:predecessor",
                "lease_updated_at": "2026-06-26 19:49:22.233718+00",
            }
            alpha.call(
                lambda: alpha.ledger._try_adopt_writer_lease(observed),
                label="adopt",
            )
            self.assertEqual(harness.server.statements[-1].kind, LeaseOp.ADOPT)
            self.assertEqual(
                harness.server.statements[-1].payload[
                    "observed_writer_session_token"
                ],
                "heartbeat-v1:predecessor",
            )

    def test_unknown_statement_fails_loudly(self) -> None:
        with self.assertRaises(UnsupportedStatement) as caught:
            classify("SELECT 1")
        self.assertIn("does not model this statement", str(caught.exception))

    def test_deferred_state_machines_name_their_follow_up(self) -> None:
        for sql, expected in (
            ("INSERT INTO qbit_share_ledger (share_id)", "share submission"),
            ("UPDATE qbit_block_candidate_outbox SET state", "landing"),
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(UnsupportedStatement) as caught:
                    classify(sql)
                self.assertIn(expected, str(caught.exception))
                self.assertIn("#128", str(caught.exception))

    def test_classifier_extracts_the_configured_ttl(self) -> None:
        with LeaseHarness(lease_ttl_seconds=17.5) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            self.assertEqual(
                harness.server.statements[0].lease_ttl_seconds,
                17.5,
            )
            row = harness.lease_row()
            assert row is not None
            self.assertEqual(
                (row.lease_expires_at - row.updated_at).total_seconds(),
                17.5,
            )

    def test_advisory_lock_pair_matches_production_splitting(self) -> None:
        key = share_ledger_module._writer_lease_advisory_lock_key(
            "prism-coordinator",
            1,
        )
        classid, objid = advisory_lock_pair(key)
        self.assertEqual(classid, (key >> 32) & 0xFFFFFFFF)
        self.assertEqual(objid, key & 0xFFFFFFFF)


class PortContractTests(unittest.TestCase):
    """The fakes must keep matching the production classes they replace."""

    def test_sql_backend_matches_native_client_signature(self) -> None:
        from tests.prism_concurrency_harness import FakeSqlBackend

        production = inspect.signature(
            share_ledger_module._NativePostgresClient.run_json
        )
        fake = inspect.signature(FakeSqlBackend.run_json)
        self.assertEqual(list(production.parameters), list(fake.parameters))

    def test_lease_guard_matches_native_guard_signature(self) -> None:
        from tests.prism_concurrency_harness import FakeLeaseGuard

        production = inspect.signature(
            share_ledger_module._NativePostgresLeaseGuard.run_json
        )
        fake = inspect.signature(FakeLeaseGuard.run_json)
        self.assertEqual(list(production.parameters), list(fake.parameters))

    def test_ledger_accepts_the_fakes_through_its_constructor(self) -> None:
        """No bound method is reassigned to install the harness."""
        parameters = inspect.signature(
            share_ledger_module.PsqlShareLedger.__init__
        ).parameters
        for name in ("sql_backend_factory", "lease_guard_factory", "monotonic"):
            self.assertIn(name, parameters)


class VirtualClockTests(unittest.TestCase):
    def test_time_only_moves_when_advanced(self) -> None:
        clock = VirtualClock()
        first = clock.monotonic()
        for _ in range(1000):
            clock.now()
        self.assertEqual(clock.monotonic(), first)
        clock.advance(2.5)
        self.assertEqual(clock.monotonic(), first + 2.5)

    def test_time_cannot_move_backwards(self) -> None:
        clock = VirtualClock()
        with self.assertRaises(HarnessError):
            clock.advance(-1)

    def test_timestamp_text_is_postgres_shaped(self) -> None:
        clock = VirtualClock()
        self.assertEqual(
            VirtualClock.timestamp_text(clock.now()),
            "2026-06-26 19:49:22.233718+00",
        )


class SchedulerTests(unittest.TestCase):
    def test_stepping_a_blocked_actor_is_refused(self) -> None:
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            def sleep_forever() -> None:
                harness.sleep(30.0)

            alpha.submit(sleep_forever, label="sleep")
            harness.run_until(alpha, "sleep:30")

            with self.assertRaises(NotRunnable):
                harness.step(alpha)

            # Time is the only thing that can release it, and the harness can
            # say exactly when.
            self.assertEqual(
                harness.scheduler.next_deadline(),
                harness.clock.monotonic() + 30.0,
            )
            harness.advance_to_next_deadline()
            harness.run_until(alpha, "done:sleep")

    def test_blocking_outside_the_model_is_reported_not_hung(self) -> None:
        """An actor waiting on something unmodelled must fail, not hang."""
        with LeaseHarness(baton_timeout_seconds=0.4) as harness:
            alpha = harness.coordinator("alpha")
            unmodelled = threading.Lock()
            unmodelled.acquire()
            alpha.submit(unmodelled.acquire, label="wedge")

            with self.assertRaises(SchedulerStall) as caught:
                harness.run_until(alpha, "done:wedge")
            self.assertIn("does not model", str(caught.exception))
            unmodelled.release()

    def test_repeated_runs_produce_an_identical_schedule(self) -> None:
        def scenario(harness: LeaseHarness) -> dict[str, object]:
            # Distinct epochs: each computes a different advisory-guard key,
            # so both reach the lease upsert and genuinely race for the row.
            alpha = harness.coordinator("alpha", writer_epoch=1)
            beta = harness.coordinator("beta", writer_epoch=2)
            alpha.start()
            beta.start()
            # Both statements are in flight before either executes.
            harness.run_until(alpha, "alpha.begin:acquire")
            harness.run_until(beta, "beta.begin:acquire")
            harness.drain([alpha, beta])
            return {
                "holder": harness.lease_holder_session(),
                "kinds": harness.statement_kinds(),
                "alpha_failed": alpha.actor.calls[0].error is not None,
                "beta_failed": beta.actor.calls[0].error is not None,
            }

        run = assert_deterministic(self, scenario, repeats=25)
        # The schedule decides the winner, and it decides it the same way
        # every run. That reproducibility is the property under test; the
        # identity of the winner is just what this schedule happens to pick.
        self.assertEqual(run.outcome["holder"], "heartbeat-v1:alpha-1")
        self.assertEqual(run.outcome["kinds"], ["acquire", "acquire"])
        self.assertFalse(run.outcome["alpha_failed"])
        self.assertTrue(run.outcome["beta_failed"])

    def test_trace_names_the_actor_and_the_stop(self) -> None:
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            self.assertEqual(
                harness.trace,
                [
                    "alpha@alpha.begin:acquire",
                    "alpha@alpha.done:acquire",
                    "alpha@done:startup",
                ],
            )


class PostgresModelTests(unittest.TestCase):
    """The PostgreSQL behaviours the lease lifecycle actually relies on."""

    def test_advisory_lock_is_exclusive_and_session_scoped(self) -> None:
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha", writer_epoch=1)
            alpha.start()
            harness.run_until(alpha, "done:startup")
            assert alpha.guard is not None
            self.assertTrue(alpha.guard.held)

            # A same-identity twin computes the same key and cannot take it.
            key = alpha.guard.advisory_lock_key
            other = harness.server.connect(application_name="twin")
            self.assertFalse(harness.server.try_advisory_lock(other, key))

            # Closing the holder's session releases it server-side.
            alpha.guard.close()
            self.assertTrue(harness.server.try_advisory_lock(other, key))

    def test_tuple_lock_is_held_for_the_life_of_a_transaction(self) -> None:
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            def deadline_scoped_write() -> object:
                with alpha.ledger.operation_timeout(30.0):
                    return alpha.ledger.renew_writer_lease()

            alpha.submit(deadline_scoped_write, label="write")
            harness.run_until(alpha, "alpha.done:renew")
            # Statement executed, COMMIT not yet sent: the lock is held and
            # the committed row still carries the pre-write value.
            self.assertIsNotNone(harness.server.lease_lock_holder)
            row = harness.lease_row()
            assert row is not None
            self.assertIsNotNone(row.xmax)

            harness.run_until(alpha, "done:write")
            self.assertIsNone(harness.server.lease_lock_holder)
            row = harness.lease_row()
            assert row is not None
            self.assertIsNone(row.xmax)

    def test_vanishing_during_an_autocommit_statement_leaves_no_orphan(self) -> None:
        """The model must not offer #123's shape on a path that cannot produce it.

        A client that vanishes mid-statement leaves an orphan only when the
        COMMIT is a separate message it never sends. Autocommit has no such
        message: PostgreSQL finishes the statement and commits it. #123 says
        the orphan shape did not exist before the deadline plumbing arrived,
        and a harness that let a scenario build it anyway would argue for a
        fix to a defect that path cannot reach.
        """
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "alpha.done:acquire")

            alpha.vanish()

            self.assertIsNone(
                harness.server.lease_lock_holder,
                "the server finished and committed the autocommit statement",
            )
            row = harness.lease_row()
            assert row is not None
            self.assertEqual(row.writer_session_token, "heartbeat-v1:alpha-1")

    def test_vanishing_inside_a_deadline_scoped_statement_does_orphan(self) -> None:
        """The contrast: a separate COMMIT the client never sends."""
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            def deadline_scoped_write() -> object:
                with alpha.ledger.operation_timeout(30.0):
                    return alpha.ledger.renew_writer_lease()

            alpha.submit(deadline_scoped_write, label="write")
            harness.run_until(alpha, "alpha.precommit")
            alpha.vanish()

            orphan = harness.server.lease_lock_holder
            self.assertIsNotNone(orphan)
            assert orphan is not None
            self.assertTrue(orphan.orphaned)

    def test_autocommit_statements_never_leave_a_transaction_open(self) -> None:
        """The shape #123 says did not exist before deadline plumbing."""
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "alpha.done:acquire")
            # No caller deadline, so no explicit transaction and no precommit
            # checkpoint to vanish inside.
            self.assertNotIn("alpha@alpha.precommit", harness.trace)
            harness.run_until(alpha, "done:startup")
            self.assertIsNone(harness.server.lease_lock_holder)


if __name__ == "__main__":
    unittest.main()
