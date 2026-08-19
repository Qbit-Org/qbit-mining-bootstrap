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
from typing import Any

from lab.prism import share_ledger as share_ledger_module
from lab.prism.share_ledger import DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS
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

            # A second coordinator, to pin the acquire path a successor takes.
            # This one finds the lease released and takes it through the
            # ordinary expiry CAS rather than adopting; the adoption CAS is
            # pinned separately by test_adoption_statement_classifies, which
            # is the only place that statement is emitted here.
            beta = harness.coordinator(
                "beta",
                session_token="heartbeat-v1:beta-successor",
            )
            beta.start()
            harness.run_until(beta, "done:startup")

            # Every statement above reached FakePostgres and was classified
            # as the operation it performs; an unrecognised one would have
            # raised UnsupportedStatement inside the ledger call.
            self.assertEqual(
                harness.statement_kinds(),
                ["acquire", "renew", "verify", "release", "acquire"],
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
            ("INSERT INTO qbit_ctv_fanouts (block_hash)", "CTV fanout"),
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(UnsupportedStatement) as caught:
                    classify(sql)
                self.assertIn(expected, str(caught.exception))
                self.assertIn("#128", str(caught.exception))

    def test_a_landing_statement_shape_that_moved_fails_loudly(self) -> None:
        """A landing table alone is not a classification.

        The deferred marker for the outbox is gone now that landing is
        modelled, so an outbox statement whose shape the classifier does not
        recognise has to reach the unmodelled-statement error rather than
        being answered by whichever landing evaluator matched loosest.
        """
        with self.assertRaises(UnsupportedStatement) as caught:
            classify("UPDATE qbit_block_candidate_outbox SET quarantined = true")
        self.assertIn("does not model this statement", str(caught.exception))
        self.assertIn("_LANDING_SIGNATURES", str(caught.exception))

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
    """The fakes must keep matching the production classes they replace.

    The repository runs no mypy — CI is `compileall` plus `unittest discover`
    — so structural typing is never checked statically. Without these the two
    protocols would be documentation: nothing would notice a fake that
    stopped implementing one, or a production class that outgrew it.
    """

    def _guard_stub(self) -> Any:
        guard = share_ledger_module._NativePostgresLeaseGuard.__new__(
            share_ledger_module._NativePostgresLeaseGuard
        )
        guard._connection = None
        guard._advisory_lock_key = 0
        guard._query_lock = threading.Lock()
        guard._closed = False
        guard._held = True
        return guard

    def test_production_classes_satisfy_their_own_ports(self) -> None:
        client = share_ledger_module._NativePostgresClient.__new__(
            share_ledger_module._NativePostgresClient
        )
        self.assertIsInstance(client, share_ledger_module.LedgerSqlPort)
        self.assertIsInstance(
            self._guard_stub(),
            share_ledger_module.LeaseGuardPort,
        )

    def test_fakes_satisfy_the_ports_they_stand_in_for(self) -> None:
        from tests.prism_concurrency_harness import FakeLeaseGuard, FakeSqlBackend

        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            self.assertIsInstance(alpha.sql_backend, FakeSqlBackend)
            self.assertIsInstance(alpha.guard, FakeLeaseGuard)
            self.assertIsInstance(
                alpha.sql_backend,
                share_ledger_module.LedgerSqlPort,
            )
            self.assertIsInstance(
                alpha.guard,
                share_ledger_module.LeaseGuardPort,
            )

    def test_every_port_method_matches_production_signature(self) -> None:
        """Full signatures, not just parameter names.

        Comparing `list(signature.parameters)` alone would stay green if a
        keyword-only argument became positional or lost its default, which is
        exactly the kind of drift that makes a fake silently stop modelling
        the thing it replaces.
        """
        from tests.prism_concurrency_harness import FakeLeaseGuard, FakeSqlBackend

        for production, fake, methods in (
            (
                share_ledger_module._NativePostgresClient,
                FakeSqlBackend,
                ("run_json", "run_script", "close"),
            ),
            (
                share_ledger_module._NativePostgresLeaseGuard,
                FakeLeaseGuard,
                ("run_json", "try_acquire", "close"),
            ),
        ):
            for method in methods:
                with self.subTest(production=production.__name__, method=method):
                    self.assertEqual(
                        inspect.signature(getattr(production, method)),
                        inspect.signature(getattr(fake, method)),
                    )

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
            beta_error = beta.actor.calls[0].error
            return {
                "holder": harness.lease_holder_session(),
                "kinds": harness.statement_kinds(),
                "alpha_failed": alpha.actor.calls[0].error is not None,
                "beta_error": None if beta_error is None else str(beta_error),
            }

        run = assert_deterministic(self, scenario, repeats=25)
        # The schedule decides the winner, and it decides it the same way
        # every run. That reproducibility is the property under test; the
        # identity of the winner is just what this schedule happens to pick.
        self.assertEqual(run.outcome["holder"], "heartbeat-v1:alpha-1")
        self.assertEqual(run.outcome["kinds"], ["acquire", "acquire"])
        self.assertFalse(run.outcome["alpha_failed"])
        # The loser does not get a clean refusal. Its DO UPDATE matches
        # nothing, and the COALESCE fallback reads a snapshot taken before the
        # winner's row existed, so the whole statement evaluates to SQL NULL.
        # See test_first_acquire_race_loser_gets_an_unhelpful_error.
        self.assertEqual(
            run.outcome["beta_error"],
            "postgres query returned no JSON",
        )

    def test_first_acquire_race_loser_gets_an_unhelpful_error(self) -> None:
        """Two coordinators racing the first-ever acquire: the loser dies badly.

        The losing ``INSERT ... ON CONFLICT DO UPDATE`` affects zero rows, and
        the ``COALESCE`` arm that would report the holder reads this
        statement's own snapshot — taken before the winner's row was
        committed — so it finds nothing and the statement returns SQL NULL.
        ``parse_single_json_value`` turns that into ``postgres query returned
        no JSON``, which ``_ensure_writer_lease`` does not catch, so
        ``PsqlShareLedger.__init__`` dies with an error naming nothing about
        leases or about the coordinator that holds it.

        Pinned here rather than left implicit because it is a real production
        outcome that reads like a driver fault, and because it is the shape a
        fix would have to change: the operator-facing message is the whole
        difference between this and the ordinary "lease is held by" refusal.
        """
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha", writer_epoch=1)
            beta = harness.coordinator("beta", writer_epoch=2)
            alpha.start()
            beta.start()
            harness.run_until(alpha, "alpha.begin:acquire")
            harness.run_until(beta, "beta.begin:acquire")
            harness.drain([alpha, beta])

            alpha.actor.calls[0].value()
            with self.assertRaisesRegex(
                RuntimeError,
                "postgres query returned no JSON",
            ):
                beta.actor.calls[0].value()

            # Contrast: once the winner's row is visible in the loser's
            # snapshot, the same race produces the ordinary refusal.
            gamma = harness.coordinator("gamma", writer_epoch=3)
            gamma.start()
            harness.run_until(gamma, "done:startup")
            with self.assertRaisesRegex(
                RuntimeError,
                "qbit ledger writer lease is held by",
            ):
                gamma.actor.calls[0].value()

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
                    # The acquire runs under #123's acquisition deadline, so
                    # it commits explicitly and offers a precommit stop.
                    "alpha@alpha.precommit",
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
        message: PostgreSQL finishes the statement and commits it. A harness
        that let a scenario build the orphan shape on such a path would argue
        for a fix to a defect that path cannot reach.

        This used to drive the startup lease acquire, which was autocommit
        because no caller armed a deadline on it. #123's fix arms one, so the
        acquire is no longer an example of this property — see
        ``test_vanishing_mid_acquire_orphans_now_that_it_is_deadline_scoped``
        for what that path does instead. An unscoped renewal is still a plain
        autocommit statement and demonstrates the property unchanged; the
        property under test never was about the acquire in particular.
        """
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            # No operation_timeout: run_json gets timeout_seconds=None, so
            # the model runs it the way _NativePostgresClient does, without
            # an enclosing conn.transaction().
            alpha.submit(lambda: alpha.ledger.renew_writer_lease(), label="renew")
            harness.run_until(alpha, "alpha.done:renew")
            self.assertIsNotNone(
                harness.server.lease_lock_holder,
                "the statement is mid-flight and holds the tuple lock",
            )

            alpha.vanish()

            self.assertIsNone(
                harness.server.lease_lock_holder,
                "the server finished and committed the autocommit statement",
            )
            row = harness.lease_row()
            assert row is not None
            self.assertEqual(row.writer_session_token, "heartbeat-v1:alpha-1")
            self.assertIsNone(
                row.xmax,
                "no transaction is left holding the row",
            )

    def test_vanishing_mid_acquire_orphans_now_that_it_is_deadline_scoped(
        self,
    ) -> None:
        """#123's fix widens the orphan shape onto the startup acquire itself.

        ``_run_lease_acquisition_json`` wraps the acquire in
        ``statement_timeout(...)`` so that a successor cannot queue forever
        on the lease tuple. That deadline is also what makes
        ``_NativePostgresClient`` run the statement inside an explicit
        transaction with a separate COMMIT — so a coordinator that vanishes
        mid-acquire now leaves exactly the orphan #123 is about, on a path
        that previously could not produce one.

        This is an accepted trade rather than a regression, and it is pinned
        here so it stays deliberate. The orphan #123 describes was unbounded;
        this one is bounded by ``idle_in_transaction_session_timeout``
        (``PRISM_POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS``, 15s by
        default), which the same fix sets on every coordinator session. That
        is why the session guard is not optional, and
        docs/prism-ledger-ops.md records the trade.

        The reap itself is not asserted here: this harness models no
        idle-in-transaction reaper, and faking one would pin the model's
        opinion rather than PostgreSQL's. That the option is actually set on
        every connection path is pinned directly against the shipped client
        by ``PostgresSessionGuardTests`` in tests/test_prism_share_ledger.py.
        What this test pins is what the model does support: the orphan is
        real, it holds the lease lock, and a successor that meets it is
        bounded rather than stuck.
        """
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha", writer_epoch=1)
            alpha.start()
            # A checkpoint that did not exist on this path before the fix.
            harness.run_until(alpha, "alpha.precommit")
            alpha.vanish()

            orphan = harness.server.lease_lock_holder
            self.assertIsNotNone(orphan)
            assert orphan is not None
            self.assertTrue(orphan.orphaned)
            self.assertTrue(orphan.explicit, "the deadline is what opened it")
            self.assertIsNone(
                harness.lease_row(),
                "the acquire never committed, so no lease row exists at all",
            )

            # The successor still gets out, which is the half that matters:
            # this orphan costs a bounded startup delay, not an outage.
            beta = harness.coordinator("beta", writer_epoch=2)
            beta.start()
            harness.run_until_blocked(beta, "beta.lockwait:acquire")
            self.assertEqual(
                beta.actor.wake_at,
                harness.clock.monotonic()
                + DEFAULT_LEASE_ACQUIRE_LOCK_TIMEOUT_SECONDS,
            )

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
        """No caller deadline, so no explicit transaction to be caught inside.

        Also drove the startup acquire until #123's fix armed a deadline on
        it. Asserting the absence of a precommit stop only means something on
        a statement that genuinely has no caller deadline, so this drives an
        unscoped renewal; on the acquire the assertion would now be checking
        the wrong path and passing for the wrong reason.
        """
        with LeaseHarness() as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")

            mark = len(harness.trace)
            alpha.submit(lambda: alpha.ledger.renew_writer_lease(), label="renew")
            harness.run_until(alpha, "done:renew")

            self.assertNotIn("alpha@alpha.precommit", harness.trace[mark:])
            self.assertIsNone(harness.server.lease_lock_holder)


if __name__ == "__main__":
    unittest.main()
