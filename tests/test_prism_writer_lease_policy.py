#!/usr/bin/env python3
"""The writer-lease heartbeat timing policy, as one inequality (issue #212).

The five heartbeat timings were previously five constants derived from the
adoption silence by hand (``silence / 4``, ``silence * 0.75``) and checked at
startup by a print statement.  That is how ``union-mainnet`` came to restart
69 times in one rapid-block burst while every component was healthy: the
derived envelope covered a heartbeat's *idle interval* but not the *tail* of
a lawful verification, and nothing in the code said so.

These tests hold the policy to three separate obligations:

``PolicyInequalityTests``
    Safety — the terms that make "this process is gone before a replacement
    may adopt" true — and the refusal of combinations that break it.

``ProductionEvidenceTests``
    The shipped defaults, checked against the numbers issue #212 actually
    reported from production. The old policy is *shown* to reject those
    observations and the new one to accept them, so the regression is
    pinned to evidence rather than to a hand-picked constant.

``VerificationAttributionTests``
    The phase accumulator: what an operator reads when a heartbeat does
    exit. Driven by an injected clock, so every duration in the assertions
    is exact rather than approximate.
"""

from __future__ import annotations

import unittest

from lab.prism.coordinator_config import (
    DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_EXIT_TIMEOUT_SECONDS,
    DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_FAILURE_SECONDS,
    DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_MONITOR_SECONDS,
    DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_SECONDS,
)
import lab.prism.writer_lease_timing as writer_lease_timing
from lab.prism.writer_lease_timing import (
    DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
    DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY,
    LEASE_HEARTBEAT_MODE_PROOF,
    LEASE_HEARTBEAT_OUTCOME_PROVEN,
    LEASE_HEARTBEAT_PHASES,
    LEASE_HEARTBEAT_POLICY_TERMS,
    OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227,
    WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS,
    WriterLeaseHeartbeatPolicy,
    WriterLeaseHeartbeatPolicyError,
    WriterLeaseVerificationAttempt,
)

# The policy that shipped before this change, reconstructed from the
# constants it derived. Referenced rather than described so the regression
# assertions below are about a real prior configuration.
PRE_212_POLICY = WriterLeaseHeartbeatPolicy(
    adoption_silence_seconds=1.0,
    heartbeat_interval_seconds=0.25,
    failure_budget_seconds=0.75,
    monitor_interval_seconds=0.05,
    exit_margin_seconds=0.1,
)

# The staleness cap the pre-#212 code actually enforced, spelled out rather
# than recomputed: it reserved only `exit_margin + 2 * monitor_interval`
# inside the silence and made no room at all for the monitor thread's own
# lateness. Evaluating PRE_212_POLICY through today's corrected envelope
# would report a different (and safer) number than production ran, so the
# evidence assertions below use this constant.
PRE_212_SERVER_PROVEN_CAP_SECONDS = 0.8

# The activity budget the same code enforced.
PRE_212_FAILURE_BUDGET_SECONDS = 0.75

# The downstream operational mitigation issue #212 describes as temporary:
# it stopped the restart loop but added ~3s to genuine writer failover and
# explained nothing.
DOWNSTREAM_MITIGATION_POLICY = WriterLeaseHeartbeatPolicy(
    adoption_silence_seconds=4.0,
    heartbeat_interval_seconds=1.0,
    failure_budget_seconds=3.0,
    monitor_interval_seconds=0.05,
    exit_margin_seconds=0.1,
)

# Server-proven ages, in seconds, reported by representative protective
# exits during the union-mainnet burst on 2026-08-31 (bootstrap pin
# c27a1f5336e661360a86f1df70f2a769e7bcaa14). Every one of these came from a
# coordinator that still held its PostgreSQL advisory guard.
OBSERVED_HEALTHY_SERVER_PROVEN_AGES = (0.76, 0.81, 0.87, 0.91)

# Activity ages from the same exits.
OBSERVED_HEALTHY_ACTIVITY_AGES = (0.54, 0.61, 0.70, 0.78)


class PolicyInequalityTests(unittest.TestCase):
    def test_shipped_defaults_satisfy_every_term(self) -> None:
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        self.assertEqual(policy.violations(), ())
        self.assertEqual(policy.advisories(), ())
        # (1) exit before adoption, stated as the worst-case timeline
        # rather than as a rearranged constant: the newest server-proven
        # edge cannot postdate the session's death, the monitor observes
        # the stale edge at most one poll plus its whole budgeted lateness
        # after the cap elapses, and the hard exit then spends its margin.
        # All of that must finish strictly before a successor — which
        # cannot start its silence until the guard session is released —
        # becomes eligible.
        worst_case_exit = (
            policy.server_proven_cap_seconds
            + policy.monitor_interval_seconds
            + policy.scheduler_slack_seconds
            + policy.exit_margin_seconds
        )
        self.assertLess(worst_case_exit, policy.adoption_silence_seconds)
        # The reserve that makes it strict is one monitor poll.
        self.assertAlmostEqual(
            policy.adoption_silence_seconds - worst_case_exit,
            policy.monitor_interval_seconds,
        )
        self.assertLessEqual(
            policy.server_proven_cap_seconds + policy.exit_envelope_seconds,
            policy.adoption_silence_seconds,
        )
        # The startup check is stated on the failure budget, which must
        # leave the same envelope strictly free.
        self.assertLess(
            policy.failure_budget_seconds + policy.exit_envelope_seconds,
            policy.adoption_silence_seconds,
        )
        # (2) one idle wait cannot exhaust the liveness budget.
        self.assertLess(
            policy.heartbeat_interval_seconds,
            policy.failure_budget_seconds,
        )
        # (3) the cap covers the largest gap a healthy coordinator produces.
        self.assertGreaterEqual(
            policy.server_proven_cap_seconds,
            policy.max_healthy_server_gap_seconds,
        )

    def test_configuration_defaults_are_the_policy(self) -> None:
        """The coordinator config must not carry a second copy of the numbers."""
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        self.assertEqual(
            (
                DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_SECONDS,
                DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_FAILURE_SECONDS,
                DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_MONITOR_SECONDS,
                DEFAULT_PRISM_LEDGER_LEASE_HEARTBEAT_EXIT_TIMEOUT_SECONDS,
                DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
            ),
            (
                policy.heartbeat_interval_seconds,
                policy.failure_budget_seconds,
                policy.monitor_interval_seconds,
                policy.exit_margin_seconds,
                policy.adoption_silence_seconds,
            ),
        )

    def test_exit_envelope_reserves_the_monitors_own_lateness(self) -> None:
        """The monitor is a thread in the same process as the heartbeat.

        Regression for the review finding on the first cut of this policy:
        the envelope reserved only poll granularity and the exit margin, so
        a monitor delayed by exactly the slack the policy budgets for the
        heartbeat observed the stale edge after the successor's adoption
        edge — a stall the policy called acceptable produced two live
        writers.
        """
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        self.assertAlmostEqual(
            policy.exit_envelope_seconds,
            policy.exit_margin_seconds
            + 2.0 * policy.monitor_interval_seconds
            + policy.scheduler_slack_seconds,
        )
        # The unreserved spelling is unsafe at the shipped numbers, which
        # is why it is not the spelling.
        unreserved_envelope = (
            policy.exit_margin_seconds + 2.0 * policy.monitor_interval_seconds
        )
        unreserved_cap = (
            policy.adoption_silence_seconds - unreserved_envelope
        )
        self.assertGreater(
            unreserved_cap
            + policy.monitor_interval_seconds
            + policy.scheduler_slack_seconds
            + policy.exit_margin_seconds,
            policy.adoption_silence_seconds,
        )

    def test_healthy_gap_is_derived_from_the_guard_statement_bound(self) -> None:
        """The dominant term is the guard session's own statement timeout.

        The monitor can only measure from completed round trips, and
        PostgreSQL will let one guarded statement run for the whole
        statement_timeout. Any envelope shorter than that plus one idle
        interval is a false-exit generator, whatever else is tuned.
        """
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        self.assertEqual(
            policy.guard_statement_timeout_seconds,
            WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS,
        )
        self.assertEqual(
            policy.max_healthy_server_gap_seconds,
            policy.heartbeat_interval_seconds
            + policy.guard_statement_timeout_seconds
            + policy.scheduler_slack_seconds,
        )

    def test_silence_that_cannot_contain_the_exit_envelope_is_unsafe(self) -> None:
        policy = WriterLeaseHeartbeatPolicy(
            adoption_silence_seconds=1.0,
            heartbeat_interval_seconds=0.25,
            failure_budget_seconds=1.25,
            monitor_interval_seconds=0.05,
            exit_margin_seconds=0.1,
        )
        violations = policy.violations()
        self.assertEqual(len(violations), 1)
        self.assertIn("adoption silence", violations[0])
        with self.assertRaises(WriterLeaseHeartbeatPolicyError):
            policy.validate()

    def test_interval_at_or_above_the_budget_is_unsafe(self) -> None:
        policy = WriterLeaseHeartbeatPolicy(
            adoption_silence_seconds=10.0,
            heartbeat_interval_seconds=1.0,
            failure_budget_seconds=1.0,
            monitor_interval_seconds=0.05,
            exit_margin_seconds=0.1,
        )
        self.assertIn(
            "must exceed the heartbeat interval",
            "".join(policy.violations()),
        )

    def test_non_finite_and_negative_terms_are_unsafe(self) -> None:
        for term in (
            "adoption_silence_seconds",
            "heartbeat_interval_seconds",
            "failure_budget_seconds",
            "monitor_interval_seconds",
            "exit_margin_seconds",
        ):
            for bad in (float("nan"), float("inf"), -1.0):
                with self.subTest(term=term, value=bad):
                    policy = WriterLeaseHeartbeatPolicy(
                        **{
                            **{
                                "adoption_silence_seconds": 2.0,
                                "heartbeat_interval_seconds": 0.25,
                                "failure_budget_seconds": 1.25,
                                "monitor_interval_seconds": 0.05,
                                "exit_margin_seconds": 0.1,
                            },
                            term: bad,
                        }
                    )
                    self.assertTrue(policy.violations())

    def test_safe_but_tight_policy_advises_instead_of_refusing(self) -> None:
        """Stability is a warning; safety is a refusal.

        A lab or test policy may legitimately run a silence far below the
        guard's statement timeout. That cannot produce two writers — the
        exit still precedes adoption — so refusing to start would turn a
        deliberate trade into an outage. Its scheduler slack is scaled with
        the rest of it: the slack is a term of the safety inequality, so a
        scaled policy that left it at the production value would be
        genuinely unsafe rather than merely tight.
        """
        policy = WriterLeaseHeartbeatPolicy(
            adoption_silence_seconds=0.5,
            heartbeat_interval_seconds=0.01,
            failure_budget_seconds=0.2,
            monitor_interval_seconds=0.005,
            exit_margin_seconds=0.01,
            scheduler_slack_seconds=0.005,
        )
        self.assertEqual(policy.violations(), ())
        self.assertEqual(len(policy.advisories()), 1)
        self.assertIn("issue #212", policy.advisories()[0])
        self.assertLess(policy.stability_surplus_seconds, 0.0)

    def test_describe_names_every_term_of_the_inequality(self) -> None:
        described = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY.describe()
        for term in (
            "adoption_silence=",
            "interval=",
            "failure_budget=",
            "monitor=",
            "exit_margin=",
            "guard_statement_timeout=",
            "scheduler_slack=",
            "server_proven_cap=",
            "healthy_gap=",
            "surplus=",
        ):
            self.assertIn(term, described)


class ProductionEvidenceTests(unittest.TestCase):
    """The shipped policy, judged against issue #212's production numbers."""

    def test_pre_212_policy_bounds_fall_inside_healthy_production_ranges(
        self,
    ) -> None:
        """The old envelope hard-exits coordinators that were fine.

        This is the defect, stated as arithmetic. Both of the old bounds
        sit *inside* the ranges the burst reported from coordinators that
        still held their PostgreSQL advisory guard, so healthy operation
        crosses them: a protective exit stops being evidence of a problem
        and becomes a function of ordinary latency. The observations are
        reported as ranges rather than paired samples, so the claim under
        test is about the ranges reaching the bounds, not about which of
        the two bounds any individual exit tripped.
        """
        self.assertGreaterEqual(
            max(OBSERVED_HEALTHY_SERVER_PROVEN_AGES),
            PRE_212_SERVER_PROVEN_CAP_SECONDS,
        )
        self.assertGreaterEqual(
            max(OBSERVED_HEALTHY_ACTIVITY_AGES),
            PRE_212_FAILURE_BUDGET_SECONDS,
        )
        self.assertEqual(
            PRE_212_POLICY.failure_budget_seconds,
            PRE_212_FAILURE_BUDGET_SECONDS,
        )
        # And the old policy could not have been fixed by tuning alone. It
        # does not merely lack tail-latency headroom: judged by the
        # corrected exit envelope it never satisfied exit-before-adoption
        # at all, because a one-second silence cannot hold a 0.75s budget
        # plus the monitor's own budgeted lateness.
        self.assertTrue(PRE_212_POLICY.violations())
        self.assertLess(PRE_212_POLICY.stability_surplus_seconds, 0.0)

    def test_shipped_policy_accepts_every_healthy_observation(self) -> None:
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        self.assertGreater(
            policy.server_proven_cap_seconds,
            PRE_212_SERVER_PROVEN_CAP_SECONDS,
        )
        for age in OBSERVED_HEALTHY_SERVER_PROVEN_AGES:
            with self.subTest(server_proven_age=age):
                self.assertLess(age, policy.server_proven_cap_seconds)
        for age in OBSERVED_HEALTHY_ACTIVITY_AGES:
            with self.subTest(activity_age=age):
                self.assertLess(age, policy.failure_budget_seconds)

    def test_shipped_policy_fails_over_faster_than_the_mitigation(self) -> None:
        """The point of the upstream fix over the 4-second pin.

        Both are safe; the shipped one costs half the failover latency and
        keeps a heartbeat interval short enough to prove ownership several
        times inside one silence window.
        """
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        self.assertEqual(DOWNSTREAM_MITIGATION_POLICY.violations(), ())
        self.assertLess(
            policy.adoption_silence_seconds,
            DOWNSTREAM_MITIGATION_POLICY.adoption_silence_seconds,
        )
        self.assertGreaterEqual(
            policy.adoption_silence_seconds / policy.heartbeat_interval_seconds,
            4.0,
        )

    def test_shipped_silence_is_the_smallest_round_safe_value(self) -> None:
        """No unexplained slack: one step down breaks the inequality.

        Guards the derivation itself. The silence is not a comfort value —
        1.95s is the exact floor for these phase budgets, so anything at or
        below it must be refused, and the shipped 2.0s is the next round
        value above it.
        """
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        floor = policy.failure_budget_seconds + policy.exit_envelope_seconds
        self.assertAlmostEqual(floor, 1.95)
        at_the_floor = WriterLeaseHeartbeatPolicy(
            adoption_silence_seconds=floor,
            heartbeat_interval_seconds=policy.heartbeat_interval_seconds,
            failure_budget_seconds=policy.failure_budget_seconds,
            monitor_interval_seconds=policy.monitor_interval_seconds,
            exit_margin_seconds=policy.exit_margin_seconds,
        )
        self.assertTrue(at_the_floor.violations())


class VerificationAttributionTests(unittest.TestCase):
    """Phase attribution: the answer to "which phase consumed the envelope?"."""

    class Clock:
        def __init__(self) -> None:
            self.now = 1_000.0

        def __call__(self) -> float:
            return self.now

        def advance(self, seconds: float) -> float:
            self.now += seconds
            return self.now

    def test_phases_separate_queue_wait_sql_and_scheduler_delay(self) -> None:
        clock = self.Clock()
        attempt = WriterLeaseVerificationAttempt(
            LEASE_HEARTBEAT_MODE_PROOF,
            monotonic=clock,
        )
        clock.advance(0.30)  # queued behind another guard caller
        attempt.slot_acquired()
        clock.advance(0.40)  # the statement itself
        attempt.statement_completed()
        clock.advance(0.05)  # this process could not get scheduled
        phases = attempt.finish(LEASE_HEARTBEAT_OUTCOME_PROVEN)

        self.assertAlmostEqual(phases.slot_wait_seconds, 0.30)
        self.assertAlmostEqual(phases.guard_sql_seconds, 0.40)
        self.assertAlmostEqual(phases.scheduler_delay_seconds, 0.05)
        self.assertAlmostEqual(phases.total_seconds, 0.75)
        self.assertEqual(phases.statement_count, 1)
        # No server-reported time: the conservative reading, with the
        # whole statement span in guard_sql and nothing in client resume.
        self.assertFalse(phases.server_reported)
        self.assertAlmostEqual(phases.client_resume_seconds, 0.0)
        self.assertEqual(
            set(phases.phase_seconds()),
            {
                "guard_slot_wait",
                "guard_sql",
                "guard_client_resume",
                "scheduler_delay",
                "total",
            },
        )
        self.assertEqual(tuple(phases.phase_seconds()), LEASE_HEARTBEAT_PHASES)

    def test_summary_names_the_phase_that_consumed_the_time(self) -> None:
        clock = self.Clock()
        attempt = WriterLeaseVerificationAttempt(
            LEASE_HEARTBEAT_MODE_PROOF,
            monotonic=clock,
        )
        attempt.slot_acquired()
        clock.advance(0.45)
        attempt.statement_completed()
        summary = attempt.finish(LEASE_HEARTBEAT_OUTCOME_PROVEN).summary()
        self.assertIn("mode=proof", summary)
        self.assertIn("outcome=proven", summary)
        self.assertIn("guard_sql=0.450s(1 stmt)", summary)

    def test_proven_edge_is_the_send_time_not_the_receipt_time(self) -> None:
        """A response proves liveness no later than when the request left.

        Stamping receipt time would let scheduler delay between the answer
        arriving and the stamp make the session look fresher than
        PostgreSQL proved, spending adoption envelope the exit ordering
        argument budgets for the monitor's own poll.
        """
        clock = self.Clock()
        attempt = WriterLeaseVerificationAttempt(
            LEASE_HEARTBEAT_MODE_PROOF,
            monotonic=clock,
        )
        attempt.slot_acquired()
        sent_at = clock.now
        clock.advance(0.40)
        proven = attempt.statement_completed()
        self.assertEqual(proven, sent_at)
        self.assertEqual(attempt.proven_edge_monotonic, sent_at)

    def test_second_statement_proves_from_the_first_response(self) -> None:
        """The attribution recheck's send edge is the previous answer.

        A followup leaves for PostgreSQL as soon as the previous result is
        in hand, so that instant — not the recheck's completion — is the
        conservative edge it proves.
        """
        clock = self.Clock()
        attempt = WriterLeaseVerificationAttempt("renew", monotonic=clock)
        attempt.slot_acquired()
        clock.advance(0.20)
        attempt.statement_completed()
        first_response_at = clock.now
        clock.advance(0.30)
        self.assertEqual(attempt.statement_completed(), first_response_at)
        phases = attempt.finish("renewed")
        self.assertEqual(phases.statement_count, 2)
        self.assertAlmostEqual(phases.guard_sql_seconds, 0.50)
        self.assertAlmostEqual(phases.scheduler_delay_seconds, 0.0)

    def test_unreported_attempt_lands_entirely_in_scheduler_delay(self) -> None:
        """A verifier with no callbacks still yields a truthful total.

        Nothing is invented: with no slot mark and no round trip, the whole
        duration is unattributed to the database, which is itself the
        signal that the attempt never demonstrably reached PostgreSQL.
        """
        clock = self.Clock()
        attempt = WriterLeaseVerificationAttempt("renew", monotonic=clock)
        clock.advance(0.9)
        phases = attempt.finish("renewed")
        self.assertAlmostEqual(phases.slot_wait_seconds, 0.0)
        self.assertAlmostEqual(phases.guard_sql_seconds, 0.0)
        self.assertAlmostEqual(phases.scheduler_delay_seconds, 0.9)
        self.assertEqual(phases.statement_count, 0)


class ProofDocstringTests(unittest.TestCase):
    """The module docstring is the proof; it must not drift from the code.

    Issue #227 found the proof presenting ``1.95 < 2.00`` as though the
    slack term bounded reality, three days after production had exceeded
    it. These tests hold the docstring to the constants, hold the stated
    monitor-lateness bound (inequality (4)) to the policy, and pin the
    production observation the docstring now discusses to the number the
    code carries.
    """

    DOCSTRING = writer_lease_timing.__doc__ or ""

    def _documented_number(self, term: str) -> float:
        """The value a ``term = ... = <number>`` line in the defaults table states."""
        import re

        heading = "The shipped defaults\n"
        self.assertIn(heading, self.DOCSTRING)
        table = self.DOCSTRING.split(heading, 1)[1]
        pattern = re.compile(
            r"^\s+(?:=>\s+)?" + re.escape(term) + r"\s+=\s+(?P<expr>[^(\n]+)",
            re.MULTILINE,
        )
        match = pattern.search(table)
        self.assertIsNotNone(match, f"defaults table has no line for {term}")
        assert match is not None
        numbers = re.findall(r"[0-9]+\.[0-9]+", match.group("expr"))
        self.assertTrue(numbers, f"no number on the {term} line")
        # A derived line spells the arithmetic and ends with its result.
        return float(numbers[-1])

    def test_documented_shipped_numbers_match_the_constants(self) -> None:
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        for term, actual in (
            ("guard_statement_timeout", policy.guard_statement_timeout_seconds),
            ("heartbeat_interval", policy.heartbeat_interval_seconds),
            ("scheduler_slack", policy.scheduler_slack_seconds),
            ("max_healthy_server_gap", policy.max_healthy_server_gap_seconds),
            ("failure_budget", policy.failure_budget_seconds),
            ("monitor_interval", policy.monitor_interval_seconds),
            ("exit_margin", policy.exit_margin_seconds),
            ("exit_envelope", policy.exit_envelope_seconds),
            ("adoption_silence", policy.adoption_silence_seconds),
            ("server_proven_cap", policy.server_proven_cap_seconds),
            ("stability surplus", policy.stability_surplus_seconds),
            (
                "max_guaranteed_monitor_lateness",
                policy.max_guaranteed_monitor_lateness_seconds,
            ),
        ):
            with self.subTest(term=term):
                self.assertAlmostEqual(self._documented_number(term), actual)

    def test_inequality_one_is_still_rejected_when_violated(self) -> None:
        """Guarding the proof: an unsafe silence is refused, not advised."""
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        floor = policy.failure_budget_seconds + policy.exit_envelope_seconds
        for silence in (floor, floor - 0.01, 1.0):
            with self.subTest(adoption_silence=silence):
                unsafe = WriterLeaseHeartbeatPolicy(
                    adoption_silence_seconds=silence,
                    heartbeat_interval_seconds=policy.heartbeat_interval_seconds,
                    failure_budget_seconds=policy.failure_budget_seconds,
                    monitor_interval_seconds=policy.monitor_interval_seconds,
                    exit_margin_seconds=policy.exit_margin_seconds,
                )
                self.assertTrue(unsafe.violations())
                with self.assertRaises(WriterLeaseHeartbeatPolicyError):
                    unsafe.validate()

    def test_monitor_lateness_bound_is_inequality_four(self) -> None:
        """(4): exit precedes adoption iff L < slack + monitor_interval.

        Stated from the worst-case timeline rather than from the property:
        the poll that first sees the stale edge is at most one interval
        plus L after the cap elapsed, the exit spends its margin, and the
        successor is eligible one silence after the proven edge.
        """
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        bound = policy.max_guaranteed_monitor_lateness_seconds
        self.assertAlmostEqual(
            bound,
            policy.scheduler_slack_seconds + policy.monitor_interval_seconds,
        )
        self.assertAlmostEqual(bound, 0.55)
        for lateness, guaranteed in (
            (policy.scheduler_slack_seconds, True),
            (bound - 1e-9, True),
            (bound + 1e-9, False),
            (OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227, False),
        ):
            with self.subTest(lateness=lateness):
                worst_case_exit = (
                    policy.server_proven_cap_seconds
                    + policy.monitor_interval_seconds
                    + lateness
                    + policy.exit_margin_seconds
                )
                self.assertEqual(
                    worst_case_exit < policy.adoption_silence_seconds,
                    guaranteed,
                )
                self.assertEqual(
                    policy.exit_guarantee_overrun_seconds(lateness) == 0.0,
                    guaranteed,
                )

    def test_production_observation_exceeds_the_bound_and_is_documented(
        self,
    ) -> None:
        """The #227 record breaks the assumption, and the docstring says so."""
        policy = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY
        observed = OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227
        self.assertGreater(observed, policy.scheduler_slack_seconds)
        self.assertGreater(observed, policy.max_guaranteed_monitor_lateness_seconds)
        self.assertAlmostEqual(
            policy.exit_guarantee_overrun_seconds(observed),
            observed - 0.55,
        )
        # The sum the spec quotes, with the real lateness in place of slack.
        self.assertAlmostEqual(
            policy.server_proven_cap_seconds
            + policy.monitor_interval_seconds
            + observed
            + policy.exit_margin_seconds,
            2.098,
        )
        self.assertIn(f"{observed:.3f}s", self.DOCSTRING)
        self.assertIn("2.098 > 2.00", self.DOCSTRING)
        self.assertIn("issue #227", self.DOCSTRING)
        # The decision is recorded, not just the observation.
        self.assertIn("(b)", self.DOCSTRING)
        self.assertIn("the shipped response is (b)", self.DOCSTRING)

    def test_policy_terms_and_describe_include_the_lateness_bound(self) -> None:
        self.assertIn("max_guaranteed_monitor_lateness", LEASE_HEARTBEAT_POLICY_TERMS)
        self.assertIn(
            "max_monitor_lateness=0.55s",
            DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY.describe(),
        )


if __name__ == "__main__":
    unittest.main()
