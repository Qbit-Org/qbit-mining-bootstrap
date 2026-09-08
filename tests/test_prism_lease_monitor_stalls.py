#!/usr/bin/env python3
"""In-process lease-monitor stalls: attribution and response (issue #227).

What production showed
----------------------
Three days after PR #214 shipped the writer-lease timing policy,
``union-mainnet`` recorded a heartbeat-monitor wake delay of 0.648s while
the host was 93% idle.  The stall was in-process — a 5.1s payout-window
rescan holding the interpreter — and two things about the telemetry made
it undiagnosable:

* ``guard_sql`` was measured client-side, so the worst attempt showed
  0.714s of "database time" against a server-enforced 0.50s statement
  timeout while ``scheduler_delay`` never exceeded 22 microseconds in 645k
  attempts.  The attribution literally could not see the stall.
* The monitor's lateness was exported only as a lifetime high-water mark,
  so every smaller stall was invisible and the alert could fire once per
  process lifetime.

What is proved here
-------------------
``InRoundTripStallAttributionTests``
    The required scenario: on the #128 deterministic harness (virtual
    clock, baton scheduler, nothing sleeps) a 0.65s scheduler stall is
    injected *inside* a guard round trip — PostgreSQL has answered, the
    heartbeat thread does not get the interpreter back — and the new
    attribution reports it as scheduler time, not ``guard_sql``.  Plus the
    accumulator-level arithmetic of the split.

``MonitorWakeTelemetryTests``
    The point of #227: the rolling-window maximum falls back down once a
    stall ages out and the record's age advances, unlike the lifetime
    gauge.

``GcPauseTelemetryTests``
    Pause durations land in the right generation's series.

``StallProbeTests``
    Repeated over-half-slack wakes produce at most the configured number
    of stack samples per window — a count, never a duration.

``LatenessBeyondSlackResponseTests``
    The decided response (option (b) in the ``writer_lease_timing``
    docstring): the monitor does not hard-exit a healthy coordinator, the
    breach is counted with its overrun past the exit-guarantee bound, and
    one structured warning names it.
"""

from __future__ import annotations

from dataclasses import replace
import gc
import re
import threading
import unittest
from unittest import mock

from lab.prism.background_services import (
    GcPauseTelemetry,
    LedgerLeaseHeartbeatPorts,
    LedgerLeaseHeartbeatService,
    MonitorWakeTelemetry,
    StallProbe,
    verifier_callbacks,
)
from lab.prism.process_telemetry import PRISM_GC_GENERATIONS
from lab.prism.writer_lease_timing import (
    DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY,
    LEASE_HEARTBEAT_MODE_PROOF,
    LEASE_HEARTBEAT_OUTCOME_PROVEN,
    LEASE_MONITOR_LATE_WAKE_SLACK_FRACTIONS,
    LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW,
    LEASE_MONITOR_STALL_PROBE_TRIGGER_SLACK_FRACTION,
    LEASE_MONITOR_STALL_PROBE_WINDOW_SECONDS,
    LEASE_MONITOR_WAKE_DELAY_BUCKETS,
    LEASE_MONITOR_WAKE_DELAY_WINDOW_SECONDS,
    OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227,
    WriterLeaseHeartbeatPolicy,
    WriterLeaseVerificationAttempt,
)
from tests.prism_concurrency_harness import LeaseHarness
from tests.test_prism_lease_heartbeat_tail_latency import (
    HeartbeatUnderTest,
    guard_statement_latency,
)

POLICY = DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY

# The stall issue #227's acceptance criterion names: a scheduler stall of
# this length inside one guard round trip. Above the 0.50s slack and above
# the 0.55s bound at which exit-before-adoption stops being guaranteed.
IN_ROUND_TRIP_STALL_SECONDS = 0.65

# Modelled server time per guard statement in the harness scenario: a
# perfectly ordinary proof, so the whole stall is unambiguously not SQL.
SERVER_EXECUTION_SECONDS = 0.05

LEASE_TTL_SECONDS = 60.0


class Clock:
    """A settable virtual clock for the accumulator- and telemetry-level tests."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now


class InRoundTripStallAttributionTests(unittest.TestCase):
    def test_scheduler_stall_inside_a_guard_round_trip_is_not_charged_to_guard_sql(
        self,
    ) -> None:
        """The required test (issue #227, acceptance criterion 1).

        The heartbeat is driven to the checkpoint at which the fake
        PostgreSQL has finished executing the ownership proof — the
        answer has left the server — and the virtual clock then advances
        0.65s before the heartbeat actor is stepped again, which is what a
        GIL stall between the answer arriving and the heartbeat thread
        resuming looks like.  The statement reported 0.05s of server
        execution, so the attribution must put the 0.65s in scheduler time
        (and in its in-round-trip sub-phase, client resume), not in
        ``guard_sql``.  On the merge base this scenario reports
        ``guard_sql`` of 0.70s and a scheduler delay of zero.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            warnings: list[str] = []
            heartbeat.service._ports = replace(
                heartbeat.service._ports, log=warnings.append
            )
            harness.server.statement_latency_seconds = guard_statement_latency(
                SERVER_EXECUTION_SECONDS
            )
            # The shipped ledger accepts the server-timing callback, so
            # the wiring under test is production's, not a stub's.
            self.assertIn(
                "on_statement_server_seconds",
                verifier_callbacks(alpha.ledger.prove_writer_lease_guard_session),
            )
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            # The first beat renews; settle into the proof cadence.
            heartbeat.run(actors, seconds=1.0)
            self.assertFalse(heartbeat.exited)
            proofs_before = heartbeat.service.attempt_counts[LEASE_HEARTBEAT_MODE_PROOF]

            # Fast-forward to the next beat, then run the heartbeat actor
            # exactly up to the point where the fake server has finished
            # the proof statement (its modelled 0.05s already charged).
            heartbeat.advance_until_beat([heartbeat.monitor])
            harness.run_until(heartbeat.heartbeat, "alpha.guard.done:prove")
            answered_at = harness.clock.monotonic()

            # PostgreSQL has answered. This process does not get back to
            # the heartbeat thread for 0.65s.
            harness.advance(IN_ROUND_TRIP_STALL_SECONDS)
            harness.drain(actors)
            heartbeat.quiesce(actors)

            self.assertFalse(
                heartbeat.exited,
                f"healthy coordinator hard-exited: {heartbeat.exits}",
            )
            self.assertEqual(
                heartbeat.service.attempt_counts[LEASE_HEARTBEAT_MODE_PROOF],
                proofs_before + 1,
            )
            phases = heartbeat.service.last_phases
            self.assertEqual(phases.mode, LEASE_HEARTBEAT_MODE_PROOF)
            self.assertEqual(phases.outcome, LEASE_HEARTBEAT_OUTCOME_PROVEN)
            self.assertTrue(phases.server_reported)
            # The database is charged only what it reported.
            self.assertAlmostEqual(
                phases.guard_sql_seconds, SERVER_EXECUTION_SECONDS, places=6
            )
            self.assertLess(phases.guard_sql_seconds, IN_ROUND_TRIP_STALL_SECONDS)
            # The stall is scheduler time, and specifically the part of it
            # that fell inside the round trip.
            self.assertGreaterEqual(
                phases.scheduler_delay_seconds, IN_ROUND_TRIP_STALL_SECONDS - 1e-6
            )
            self.assertAlmostEqual(
                phases.client_resume_seconds, IN_ROUND_TRIP_STALL_SECONDS, places=6
            )
            self.assertAlmostEqual(
                phases.slot_wait_seconds
                + phases.guard_sql_seconds
                + phases.scheduler_delay_seconds,
                phases.total_seconds,
                places=9,
            )
            # The metrics view carries the same split under fixed keys.
            snapshot = heartbeat.service.snapshot()
            last = snapshot["last_phase_seconds"]
            assert isinstance(last, dict)
            self.assertAlmostEqual(last["guard_sql"], SERVER_EXECUTION_SECONDS, places=6)
            self.assertAlmostEqual(
                last["guard_client_resume"], IN_ROUND_TRIP_STALL_SECONDS, places=6
            )
            # The server-proven edge is still the conservative send edge:
            # the stall neither made the session look fresher nor older
            # than the instant the statement left.
            self.assertAlmostEqual(
                heartbeat.service.last_server_proven_monotonic,
                answered_at - SERVER_EXECUTION_SECONDS,
                places=6,
            )
            self.assertIn("client_resume=0.650s", phases.summary())

    def test_guard_sql_splits_into_server_execution_and_client_resume(self) -> None:
        """Known server time plus a known client gap land where they should.

        ``slot_wait + guard_sql + scheduler_delay == total`` still holds,
        and ``client_resume`` is the in-round-trip share of the residual.
        """
        clock = Clock()
        attempt = WriterLeaseVerificationAttempt(
            LEASE_HEARTBEAT_MODE_PROOF, monotonic=clock
        )
        clock.advance(0.02)  # queue wait for the guard's slot
        attempt.slot_acquired()
        clock.advance(0.10)  # the server executes
        clock.advance(0.65)  # ... and this process does not resume
        attempt.statement_completed()
        attempt.statement_server_seconds(0.10)
        clock.advance(0.03)  # result handling after the round trip
        phases = attempt.finish(LEASE_HEARTBEAT_OUTCOME_PROVEN)

        self.assertTrue(phases.server_reported)
        self.assertAlmostEqual(phases.slot_wait_seconds, 0.02)
        self.assertAlmostEqual(phases.guard_sql_seconds, 0.10)
        self.assertAlmostEqual(phases.client_resume_seconds, 0.65)
        self.assertAlmostEqual(phases.scheduler_delay_seconds, 0.68)
        self.assertAlmostEqual(phases.total_seconds, 0.80)
        self.assertAlmostEqual(
            phases.slot_wait_seconds
            + phases.guard_sql_seconds
            + phases.scheduler_delay_seconds,
            phases.total_seconds,
        )
        self.assertLessEqual(phases.client_resume_seconds, phases.scheduler_delay_seconds)
        self.assertIn("server-timed", phases.summary())

    def test_server_time_accumulates_across_a_two_statement_verification(self) -> None:
        """The recheck's server time adds to the first statement's."""
        clock = Clock()
        attempt = WriterLeaseVerificationAttempt("renew", monotonic=clock)
        attempt.slot_acquired()
        clock.advance(0.20)
        attempt.statement_completed()
        attempt.statement_server_seconds(0.15)
        clock.advance(0.30)
        attempt.statement_completed()
        attempt.statement_server_seconds(0.25)
        phases = attempt.finish("renewed")
        self.assertEqual(phases.statement_count, 2)
        self.assertAlmostEqual(phases.guard_sql_seconds, 0.40)
        self.assertAlmostEqual(phases.client_resume_seconds, 0.10)
        self.assertAlmostEqual(phases.scheduler_delay_seconds, 0.10)

    def test_unreported_or_malformed_server_time_keeps_the_conservative_split(
        self,
    ) -> None:
        for reported in (None, "not a number", float("nan"), -1.0):
            with self.subTest(reported=reported):
                clock = Clock()
                attempt = WriterLeaseVerificationAttempt(
                    LEASE_HEARTBEAT_MODE_PROOF, monotonic=clock
                )
                attempt.slot_acquired()
                clock.advance(0.70)
                attempt.statement_completed()
                attempt.statement_server_seconds(reported)  # type: ignore[arg-type]
                phases = attempt.finish(LEASE_HEARTBEAT_OUTCOME_PROVEN)
                self.assertFalse(phases.server_reported)
                self.assertAlmostEqual(phases.guard_sql_seconds, 0.70)
                self.assertAlmostEqual(phases.client_resume_seconds, 0.0)
                self.assertAlmostEqual(phases.scheduler_delay_seconds, 0.0)

    def test_server_time_cannot_exceed_the_span_it_fits_inside(self) -> None:
        """A server figure larger than the client span is clamped, not trusted."""
        clock = Clock()
        attempt = WriterLeaseVerificationAttempt(
            LEASE_HEARTBEAT_MODE_PROOF, monotonic=clock
        )
        attempt.slot_acquired()
        clock.advance(0.30)
        attempt.statement_completed()
        attempt.statement_server_seconds(0.45)
        phases = attempt.finish(LEASE_HEARTBEAT_OUTCOME_PROVEN)
        self.assertAlmostEqual(phases.guard_sql_seconds, 0.30)
        self.assertAlmostEqual(phases.client_resume_seconds, 0.0)


class MonitorWakeTelemetryTests(unittest.TestCase):
    def test_rolling_window_maximum_decays_and_record_age_advances(self) -> None:
        """Unlike the lifetime gauge, the window maximum comes back down.

        The lifetime record stays at 0.648s forever (that is the dashboard
        continuity the existing gauge keeps), while the rolling maximum
        reports the stall only for one window and the record age says how
        long ago the record was set.
        """
        telemetry = MonitorWakeTelemetry()
        slack = POLICY.scheduler_slack_seconds
        now = 10_000.0
        telemetry.observe(0.001, slack_seconds=slack, now=now)
        stall_at = now + 20.0
        telemetry.observe(
            OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227,
            slack_seconds=slack,
            now=stall_at,
        )
        self.assertAlmostEqual(
            telemetry.lifetime_max_seconds, OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227
        )
        self.assertAlmostEqual(
            telemetry.window_max_seconds(stall_at),
            OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227,
        )
        self.assertAlmostEqual(telemetry.record_age_seconds(stall_at), 0.0)

        # Quiet wakes for the rest of the window: the stall still dominates.
        later = stall_at + LEASE_MONITOR_WAKE_DELAY_WINDOW_SECONDS / 2.0
        telemetry.observe(0.002, slack_seconds=slack, now=later)
        self.assertAlmostEqual(
            telemetry.window_max_seconds(later),
            OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227,
        )
        self.assertAlmostEqual(
            telemetry.record_age_seconds(later),
            LEASE_MONITOR_WAKE_DELAY_WINDOW_SECONDS / 2.0,
        )

        # Once the stall ages out, the window maximum is the recent quiet
        # wake, the lifetime record is unchanged, and its age keeps growing.
        aged_out = stall_at + LEASE_MONITOR_WAKE_DELAY_WINDOW_SECONDS + 10.0
        telemetry.observe(0.003, slack_seconds=slack, now=aged_out)
        self.assertAlmostEqual(telemetry.window_max_seconds(aged_out), 0.003)
        self.assertAlmostEqual(
            telemetry.lifetime_max_seconds, OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227
        )
        self.assertAlmostEqual(
            telemetry.record_age_seconds(aged_out),
            LEASE_MONITOR_WAKE_DELAY_WINDOW_SECONDS + 10.0,
        )
        # And with no wakes at all inside the window, it is zero.
        self.assertAlmostEqual(
            telemetry.window_max_seconds(
                aged_out + 2.0 * LEASE_MONITOR_WAKE_DELAY_WINDOW_SECONDS
            ),
            0.0,
        )

        snapshot = telemetry.snapshot(aged_out)
        self.assertEqual(snapshot["count"], 4)
        self.assertEqual(
            set(snapshot["late_wakes"]), set(LEASE_MONITOR_LATE_WAKE_SLACK_FRACTIONS)
        )
        self.assertEqual(tuple(snapshot["buckets"]), LEASE_MONITOR_WAKE_DELAY_BUCKETS)

    def test_histogram_is_cumulative_and_threshold_counters_are_exact(self) -> None:
        telemetry = MonitorWakeTelemetry()
        slack = 0.5
        for delay in (0.004, 0.26, 0.41, 0.5, 0.648):
            telemetry.observe(delay, slack_seconds=slack, now=1.0)
        snapshot = telemetry.snapshot(1.0)
        buckets = snapshot["buckets"]
        assert isinstance(buckets, dict)
        self.assertEqual(buckets[0.005], 1)
        self.assertEqual(buckets[0.25], 1)
        self.assertEqual(buckets[0.4], 2)
        self.assertEqual(buckets[0.5], 4)
        self.assertEqual(buckets[0.75], 5)
        self.assertEqual(buckets[2.0], 5)
        self.assertEqual(snapshot["count"], 5)
        self.assertAlmostEqual(snapshot["sum"], 0.004 + 0.26 + 0.41 + 0.5 + 0.648)
        late = snapshot["late_wakes"]
        assert isinstance(late, dict)
        # >= 0.25, >= 0.4, >= 0.5 respectively.
        self.assertEqual(late, {"0.5": 4, "0.8": 3, "1.0": 2})


class GcPauseTelemetryTests(unittest.TestCase):
    def test_gc_pause_histogram_records_per_generation(self) -> None:
        """A pause lands in the series of the generation that paused."""
        clock = Clock()
        telemetry = GcPauseTelemetry(clock=clock)
        telemetry._callback("start", {"generation": 2})
        clock.advance(0.30)
        telemetry._callback("stop", {"generation": 2, "collected": 5, "uncollectable": 0})
        telemetry._callback("start", {"generation": 0})
        clock.advance(0.002)
        telemetry._callback("stop", {"generation": 0, "collected": 1, "uncollectable": 0})

        snapshot = telemetry.snapshot()
        self.assertEqual(set(snapshot), set(PRISM_GC_GENERATIONS))
        self.assertEqual(snapshot["2"]["count"], 1)
        self.assertAlmostEqual(snapshot["2"]["last_seconds"], 0.30)
        self.assertAlmostEqual(snapshot["2"]["max_seconds"], 0.30)
        self.assertEqual(snapshot["0"]["count"], 1)
        self.assertAlmostEqual(snapshot["0"]["last_seconds"], 0.002)
        self.assertEqual(snapshot["1"]["count"], 0)
        buckets_2 = snapshot["2"]["buckets"]
        assert isinstance(buckets_2, dict)
        self.assertEqual(buckets_2[0.25], 0)
        self.assertEqual(buckets_2[0.5], 1)
        worst_generation, worst_seconds = telemetry.worst()
        self.assertEqual(worst_generation, "2")
        self.assertAlmostEqual(worst_seconds, 0.30)
        # An unknown generation never grows the label set.
        telemetry.record("3", 5.0)
        self.assertEqual(set(telemetry.snapshot()), set(PRISM_GC_GENERATIONS))

    def test_installed_collector_callbacks_record_a_real_collection(self) -> None:
        telemetry = GcPauseTelemetry()
        telemetry.install()
        telemetry.install()  # idempotent
        self.addCleanup(telemetry.uninstall)
        self.assertEqual(gc.callbacks.count(telemetry._callback), 1)
        gc.collect()  # a full collection reports generation 2
        self.assertGreaterEqual(telemetry.snapshot()["2"]["count"], 1)
        telemetry.uninstall()
        self.assertNotIn(telemetry._callback, gc.callbacks)
        self.assertFalse(telemetry.installed)


class StallProbeTests(unittest.TestCase):
    def test_stall_probe_is_rate_limited(self) -> None:
        """Repeated over-half-slack wakes yield at most N samples per window."""
        probe = StallProbe()
        self.assertEqual(
            probe.max_samples_per_window, LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW
        )
        self.assertEqual(probe.window_seconds, LEASE_MONITOR_STALL_PROBE_WINDOW_SECONDS)
        triggers = 10 * LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW
        sampled = [
            probe.sample(now=100.0 + index, wake_delay_seconds=0.3)
            for index in range(triggers)
        ]
        self.assertEqual(probe.samples_total, LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW)
        self.assertEqual(
            probe.suppressed_total,
            triggers - LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW,
        )
        self.assertEqual(
            sum(1 for text in sampled if text is not None),
            LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW,
        )
        # The next window re-arms the budget.
        self.assertIsNotNone(
            probe.sample(
                now=100.0 + LEASE_MONITOR_STALL_PROBE_WINDOW_SECONDS,
                wake_delay_seconds=0.3,
            )
        )
        self.assertEqual(
            probe.samples_total, LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW + 1
        )

    def test_probe_samples_other_threads_not_itself(self) -> None:
        parked = threading.Event()
        release = threading.Event()

        def park() -> None:
            parked.set()
            release.wait(5.0)

        worker = threading.Thread(target=park, name="stall-probe-target", daemon=True)
        worker.start()
        self.addCleanup(release.set)
        self.assertTrue(parked.wait(5.0))
        stacks = StallProbe().capture_stacks()
        self.assertIn("stall-probe-target", stacks)
        self.assertIn("park@", stacks)
        self.assertNotIn(threading.current_thread().name + " (", stacks)
        # Bounded output: every frame names a bare file, never a path.
        frame_files = re.findall(r"@([^:\s]+):\d+", stacks)
        self.assertTrue(frame_files)
        for frame_file in frame_files:
            self.assertNotIn("/", frame_file)

    def test_service_level_probe_holds_the_limit_under_a_stall_storm(self) -> None:
        """Through the monitor's own entry point, the count still holds."""
        clock = Clock()
        service, logged, exits = _service_with_fake_ports(clock)
        trigger = LEASE_MONITOR_STALL_PROBE_TRIGGER_SLACK_FRACTION * POLICY.scheduler_slack_seconds
        for _ in range(50):
            clock.advance(0.05)
            _wake(service, trigger)
        self.assertEqual(
            service.stall_probe.samples_total,
            LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW,
        )
        self.assertEqual(
            service.stall_probe.suppressed_total,
            50 - LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW,
        )
        # One probe line per sample, none for suppressed triggers, no exit.
        self.assertEqual(len(logged), LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW)
        self.assertEqual(exits, [])
        self.assertEqual(service.exit_guarantee_breaches, 0)


def _service_with_fake_ports(
    clock: Clock,
    *,
    policy: WriterLeaseHeartbeatPolicy = POLICY,
) -> tuple[LedgerLeaseHeartbeatService, list[str], list[str]]:
    """A heartbeat service on a virtual clock with a recording log and exit."""
    logged: list[str] = []
    exits: list[str] = []

    class Ledger:
        _lease_adoption_silence_seconds = policy.adoption_silence_seconds

    ports = LedgerLeaseHeartbeatPorts(
        ledger=lambda: Ledger(),
        heartbeat_seconds=lambda: policy.heartbeat_interval_seconds,
        failure_seconds=lambda: policy.failure_budget_seconds,
        monitor_seconds=lambda: policy.monitor_interval_seconds,
        exit_timeout_seconds=lambda: policy.exit_margin_seconds,
        external_fence_timeout_seconds=lambda: 0.5,
        lease_hard_exit=lambda message, *, include_traceback: exits.append(message),
        watchdog_hard_exit=lambda reason, *, timeout_seconds: exits.append(reason),
        heartbeat_loop=lambda: None,
        monitor_loop=lambda: None,
        monotonic=clock,
        scheduler_slack_seconds=lambda: policy.scheduler_slack_seconds,
        log=logged.append,
    )
    service = LedgerLeaseHeartbeatService(ports)
    service.last_success_monotonic = clock()
    service.last_server_proven_monotonic = clock()
    return service, logged, exits


def _wake(
    service: LedgerLeaseHeartbeatService,
    wake_delay: float,
    policy: WriterLeaseHeartbeatPolicy = POLICY,
):
    """One monitor wake through the shipped accounting, diagnostics run inline.

    The monitor loop hands the returned job to a diagnostics thread after
    the exit decision; the tier tests here run it synchronously so their
    assertions are deterministic. The threading itself is pinned by
    ``InstrumentCostAndOrderingTests``.
    """
    job = service._observe_monitor_wake(wake_delay, policy)
    if job is not None:
        job()
    return job


class LatenessBeyondSlackResponseTests(unittest.TestCase):
    def test_lateness_beyond_slack_triggers_the_documented_response(self) -> None:
        """Option (b), pinned: no exit, a counted breach, one structured warning.

        Driven at the production number.  A 0.648s wake is beyond the
        0.50s slack *and* beyond the 0.55s bound of inequality (4), so it
        is an exit-guarantee breach with a 0.098s overrun.  The monitor
        accepts it — the fences carry the residual — and says so.
        """
        clock = Clock()
        service, logged, exits = _service_with_fake_ports(clock)
        clock.advance(1.0)
        _wake(service, OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227)

        self.assertEqual(exits, [])
        self.assertEqual(service.exit_guarantee_breaches, 1)
        self.assertAlmostEqual(
            service.worst_exit_guarantee_overrun_seconds,
            OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227
            - POLICY.max_guaranteed_monitor_lateness_seconds,
        )
        self.assertEqual(len(logged), 1)
        warning = logged[0]
        self.assertIn("WARNING", warning)
        self.assertIn("monitor wake delay 0.648s exceeded the 0.500s scheduler slack", warning)
        self.assertIn("issue #227", warning)
        self.assertIn("exit-before-adoption was NOT guaranteed for this beat", warning)
        self.assertIn("0.098s after the adoption edge", warning)
        self.assertIn("session-token fenced", warning)
        self.assertIn("Last attempt:", warning)
        self.assertIn("Policy:", warning)
        # A stack sample rode along with the first warning.
        self.assertIn("Threads:", warning)
        self.assertEqual(service.stall_probe.samples_total, 1)
        breach = service.last_slack_breach
        assert breach is not None
        self.assertAlmostEqual(breach["wake_delay_seconds"], 0.648)
        self.assertAlmostEqual(breach["overrun_seconds"], 0.098)

        # The metric surface reports it under fixed keys.
        snapshot = service.snapshot()
        self.assertEqual(snapshot["exit_guarantee_breaches"], 1)
        self.assertAlmostEqual(snapshot["worst_exit_guarantee_overrun_seconds"], 0.098)
        wakes = snapshot["monitor_wakes"]
        assert isinstance(wakes, dict)
        self.assertEqual(wakes["late_wakes"], {"0.5": 1, "0.8": 1, "1.0": 1})
        self.assertAlmostEqual(wakes["lifetime_max_seconds"], 0.648)
        self.assertAlmostEqual(wakes["window_max_seconds"], 0.648)
        policy_seconds = snapshot["policy_seconds"]
        assert isinstance(policy_seconds, dict)
        self.assertAlmostEqual(policy_seconds["max_guaranteed_monitor_lateness"], 0.55)

    def test_lateness_inside_the_reserve_warns_without_a_guarantee_breach(self) -> None:
        clock = Clock()
        service, logged, exits = _service_with_fake_ports(clock)
        clock.advance(1.0)
        _wake(service, 0.52)
        self.assertEqual(exits, [])
        self.assertEqual(service.exit_guarantee_breaches, 0)
        self.assertEqual(service.worst_exit_guarantee_overrun_seconds, 0.0)
        self.assertEqual(len(logged), 1)
        self.assertIn("exit-before-adoption still held for this beat", logged[0])
        self.assertIn("strictness reserve consumed", logged[0])

    def test_wakes_within_slack_are_counted_but_never_warned(self) -> None:
        clock = Clock()
        service, logged, exits = _service_with_fake_ports(clock)
        for delay in (0.0, 0.01, 0.2, 0.24):
            clock.advance(0.05)
            self.assertIsNone(_wake(service, delay))
        self.assertEqual(logged, [])
        self.assertEqual(exits, [])
        self.assertEqual(service.exit_guarantee_breaches, 0)
        self.assertEqual(service.stall_probe.samples_total, 0)
        self.assertEqual(service.snapshot()["monitor_wakes"]["count"], 4)

    def test_breach_warnings_are_rate_limited_like_the_probe(self) -> None:
        """A stall storm produces bounded warnings, every breach still counted."""
        clock = Clock()
        service, logged, exits = _service_with_fake_ports(clock)
        storm = 4 * LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW
        for _ in range(storm):
            clock.advance(0.7)
            _wake(service, 0.7)
        self.assertEqual(service.exit_guarantee_breaches, storm)
        self.assertEqual(len(logged), LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW)
        self.assertEqual(
            service.suppressed_slack_breach_warnings,
            storm - LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW,
        )
        # Suppressed warnings are exported, so a quiet period and a
        # rate-limited flood do not look the same on /metrics.
        self.assertEqual(
            service.snapshot()["slack_breach_warnings_suppressed"],
            storm - LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW,
        )
        self.assertEqual(exits, [])

    def test_late_monitor_beyond_slack_survives_on_the_harness(self) -> None:
        """The real loops: a 0.648s-late monitor over a healthy heartbeat.

        The tail-latency suite proves survival at 0.4s (inside the slack).
        This is the #227 case, beyond the slack: the monitor must not
        hard-exit — the heartbeat kept proving, so nothing tripped a bound
        — and must have recorded the exit-guarantee breach.
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            warnings: list[str] = []
            heartbeat.service._ports = replace(
                heartbeat.service._ports, log=warnings.append
            )
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)

            harness.advance(
                heartbeat.policy.monitor_interval_seconds
                + OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227
            )
            while heartbeat.monitor.runnable():
                harness.step(heartbeat.monitor)
            heartbeat.run(actors, seconds=1.0)
            heartbeat.quiesce(actors)

            self.assertFalse(heartbeat.exited, heartbeat.exits)
            service = heartbeat.service
            self.assertGreaterEqual(service.monitor_wake_delay_seconds, 0.648)
            self.assertEqual(service.exit_guarantee_breaches, 1)
            self.assertGreaterEqual(service.worst_exit_guarantee_overrun_seconds, 0.098)
            # The warning is written by the diagnostics thread, after the
            # monitor's decision; wait for it before reading the sink.
            diagnostics = service.diagnostics_thread
            self.assertIsNotNone(diagnostics)
            assert diagnostics is not None
            diagnostics.join(5.0)
            self.assertFalse(diagnostics.is_alive())
            self.assertEqual(len(warnings), 1)
            self.assertIn("NOT guaranteed", warnings[0])
            snapshot = service.snapshot()
            self.assertEqual(snapshot["monitor_wakes"]["late_wakes"]["1.0"], 1)

    def test_hard_exit_reason_quotes_window_max_record_age_and_gc(self) -> None:
        """The exit message no longer quotes only a possibly hours-old record."""
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            heartbeat.service._ports = replace(
                heartbeat.service._ports, log=lambda line: None
            )
            heartbeat.service.gc_pauses.record("2", 0.123)
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)
            # Deschedule the heartbeat for good: the monitor must age it
            # out and exit, quoting the new figures.
            heartbeat.stall_heartbeat(heartbeat.policy.server_proven_cap_seconds + 1.0)
            self.assertTrue(heartbeat.exited)
            reason = heartbeat.exit_reason
            self.assertIn("Monitor wake delay:", reason)
            self.assertIn("lifetime record", reason)
            self.assertIn(f"in the last {LEASE_MONITOR_WAKE_DELAY_WINDOW_SECONDS:g}s", reason)
            self.assertIn("exit-guarantee breach(es)", reason)
            self.assertIn("Worst GC pause: 0.123s (generation 2)", reason)


class _SnapshotOnWrite(list):
    """A bucket list that takes a snapshot the instant any bucket is bumped.

    Stands in for a scrape interleaving with a writer between two of the
    writer's stores; the invariant under test is that no interleaving can
    render a bucket above the histogram's count.
    """

    def __init__(self, values, on_write) -> None:
        super().__init__(values)
        self._on_write = on_write

    def __setitem__(self, index, value) -> None:
        super().__setitem__(index, value)
        self._on_write()


DIAGNOSTICS_THREAD_NAME = "prism-ledger-lease-monitor-diagnostics"


class InstrumentCostAndOrderingTests(unittest.TestCase):
    """The instrument must not extend the path it measures.

    The monitor thread decides the hard exit. Everything this package adds
    to that thread has to be O(1) and has to come *after* the decision, or
    it consumes the very envelope it reports on — and work done between
    polls is not measured as lateness at all. These tests pin the
    mechanism (which thread, in which order, which calls are never made),
    not a duration.
    """

    def test_observing_a_wake_does_no_capture_or_logging_itself(self) -> None:
        """Accounting is O(1); the frame walk and the log line are deferred."""
        clock = Clock()
        service, logged, exits = _service_with_fake_ports(clock)
        captures: list[str] = []

        def capture_stacks() -> str:
            captures.append(threading.current_thread().name)
            return "stacks"

        service.stall_probe.capture_stacks = capture_stacks  # type: ignore[method-assign]
        clock.advance(1.0)
        job = service._observe_monitor_wake(OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227, POLICY)
        # Counted and admitted, but nothing walked and nothing written yet.
        self.assertEqual(service.exit_guarantee_breaches, 1)
        self.assertEqual(captures, [])
        self.assertEqual(logged, [])
        self.assertEqual(service.stall_probe.samples_total, 0)
        self.assertIsNotNone(job)
        assert job is not None
        # The shipped runner hands the job to a named daemon thread.
        service._run_diagnostics(job)
        thread = service.diagnostics_thread
        assert thread is not None
        thread.join(5.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual(captures, [DIAGNOSTICS_THREAD_NAME])
        self.assertEqual(len(logged), 1)
        self.assertIn("Threads:\nstacks", logged[0])
        self.assertEqual(service.stall_probe.samples_total, 1)
        self.assertEqual(exits, [])

    def test_probe_and_warning_run_after_the_exit_decision_and_off_the_monitor_thread(
        self,
    ) -> None:
        """On the real loops: decide first, diagnose after, never on the monitor.

        First a healthy heartbeat with a 0.648s-late monitor: the sample and
        the warning both happen, on the diagnostics thread, after the
        monitor has decided not to exit. Then a dead heartbeat with a wake
        far beyond the slack: the monitor exits, on its own thread, and no
        capture or log line runs ahead of that exit (nor after it — the
        exit reason carries the figures).
        """
        with LeaseHarness(lease_ttl_seconds=LEASE_TTL_SECONDS) as harness:
            alpha = harness.coordinator("alpha")
            alpha.start()
            harness.run_until(alpha, "done:startup")
            heartbeat = HeartbeatUnderTest(harness, alpha)
            service = heartbeat.service
            monitor_thread_name = heartbeat.monitor._thread.name
            events: list[tuple[str, str]] = []

            def capture_stacks() -> str:
                events.append(("capture", threading.current_thread().name))
                return "stacks"

            service.stall_probe.capture_stacks = capture_stacks  # type: ignore[method-assign]
            original_exit = service._ports.lease_hard_exit

            def lease_hard_exit(message: str, *, include_traceback: bool) -> None:
                events.append(("exit", threading.current_thread().name))
                original_exit(message, include_traceback=include_traceback)

            service._ports = replace(
                service._ports,
                log=lambda line: events.append(("log", threading.current_thread().name)),
                lease_hard_exit=lease_hard_exit,
            )
            actors = [heartbeat.heartbeat, heartbeat.monitor]
            heartbeat.run(actors, seconds=1.0)

            # Healthy heartbeat, late monitor.
            harness.advance(
                heartbeat.policy.monitor_interval_seconds
                + OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227
            )
            while heartbeat.monitor.runnable():
                harness.step(heartbeat.monitor)
            self.assertFalse(heartbeat.exited)
            diagnostics = service.diagnostics_thread
            self.assertIsNotNone(diagnostics)
            assert diagnostics is not None
            diagnostics.join(5.0)
            self.assertFalse(diagnostics.is_alive())
            self.assertEqual([kind for kind, _ in events], ["capture", "log"])
            self.assertEqual(
                {name for _, name in events}, {DIAGNOSTICS_THREAD_NAME}
            )
            self.assertNotEqual(DIAGNOSTICS_THREAD_NAME, monitor_thread_name)
            self.assertEqual(service.exit_guarantee_breaches, 1)
            events.clear()

            # Dead heartbeat: only the monitor is stepped, after sleeping
            # through the whole cap. Its wake is far beyond the slack, so
            # both tiers are due — and neither may run ahead of the exit.
            harness.advance(heartbeat.policy.server_proven_cap_seconds + 1.0)
            while heartbeat.monitor.runnable() and not heartbeat.exited:
                harness.step(heartbeat.monitor)
            self.assertTrue(heartbeat.exited)
            self.assertEqual(events, [("exit", monitor_thread_name)])
            # The wake was still accounted (O(1), before the decision).
            self.assertEqual(service.exit_guarantee_breaches, 2)
            self.assertIn("exit-guarantee breach(es)", heartbeat.exit_reason)
            heartbeat.quiesce(actors)

    def test_stack_capture_never_touches_linecache(self) -> None:
        """No stat, no source read: the walk uses frame objects only.

        ``traceback.extract_stack`` stats every file and reads every line;
        even ``StackSummary.extract(lookup_lines=False)`` still calls
        ``linecache.checkcache`` per file. The sample prints function
        names, basenames and line numbers, none of which need the source.
        """
        parked = threading.Event()
        release = threading.Event()

        def park() -> None:
            parked.set()
            release.wait(5.0)

        worker = threading.Thread(target=park, name="linecache-probe-target", daemon=True)
        worker.start()
        self.addCleanup(release.set)
        self.assertTrue(parked.wait(5.0))
        with mock.patch("linecache.checkcache") as checkcache, mock.patch(
            "linecache.lazycache"
        ) as lazycache, mock.patch("linecache.getline") as getline, mock.patch(
            "linecache.getlines"
        ) as getlines, mock.patch("linecache.updatecache") as updatecache:
            stacks = StallProbe().capture_stacks()
        for patched in (checkcache, lazycache, getline, getlines, updatecache):
            patched.assert_not_called()
        self.assertIn("linecache-probe-target", stacks)
        self.assertIn("park@", stacks)

    def test_gc_pause_buckets_never_exceed_count_mid_record(self) -> None:
        """A scrape between two of record()'s stores sees bucket <= count."""
        telemetry = GcPauseTelemetry(clock=Clock())
        seen: list[dict[str, object]] = []
        telemetry._bucket_counts["2"] = _SnapshotOnWrite(
            telemetry._bucket_counts["2"],
            lambda: seen.append(telemetry.snapshot()["2"]),
        )
        telemetry.record("2", 0.3)
        self.assertTrue(seen)
        for mid in seen:
            buckets = mid["buckets"]
            assert isinstance(buckets, dict)
            self.assertTrue(
                all(value <= mid["count"] for value in buckets.values()), mid
            )

    def test_wake_buckets_never_exceed_count_mid_observe(self) -> None:
        telemetry = MonitorWakeTelemetry()
        seen: list[dict[str, object]] = []
        telemetry._bucket_counts = _SnapshotOnWrite(
            telemetry._bucket_counts,
            lambda: seen.append(telemetry.snapshot(1.0)),
        )
        telemetry.observe(0.3, slack_seconds=0.5, now=1.0)
        self.assertTrue(seen)
        for mid in seen:
            buckets = mid["buckets"]
            assert isinstance(buckets, dict)
            self.assertTrue(
                all(value <= mid["count"] for value in buckets.values()), mid
            )


if __name__ == "__main__":
    unittest.main()
