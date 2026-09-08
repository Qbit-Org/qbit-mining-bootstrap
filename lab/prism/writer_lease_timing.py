"""One validated timing policy for the PRISM writer-lease heartbeat.

Why this module exists
----------------------
The heartbeat's five timing knobs — adoption silence, heartbeat interval,
failure budget, monitor interval, exit margin — are not five independent
tunables. They encode one safety argument:

    a coordinator that has lost its guarded session must be *gone* before
    any replacement becomes eligible to compare-and-swap the lease row.

Historically the numbers were derived from the adoption silence alone
(``silence / 4`` and ``silence * 0.75``) and the coupling was re-derived by
hand at each call site, with only a printed warning when an override broke
it.  Issue #212 is the failure mode that produces: the derived envelope was
sized for a heartbeat's *idle interval* but not for the *tail* of a lawful
verification, so ordinary rapid-block verification latency exhausted the
server-proven envelope and hard-exited healthy coordinators (69 restarts on
``union-mainnet``, activity ages 0.54-0.78s, server-proven ages 0.76-0.91s
against a 0.75s budget and a 0.80s cap).

The fix is to state the whole inequality once, in terms of phases that are
actually measurable in production, and to reject a configuration that
breaks it instead of printing and continuing.

The phase budgets
-----------------
Three inputs are physical properties of the deployment rather than free
choices:

``guard_statement_timeout_seconds``
    The server-side ``statement_timeout`` on the dedicated guard session
    (:class:`~lab.prism.share_ledger._NativePostgresLeaseGuard`).  It is a
    hard bound: no single guarded round trip can take longer, because
    PostgreSQL cancels it.

``heartbeat_interval_seconds``
    The idle gap the heartbeat waits between proofs.

``scheduler_slack_seconds``
    The process-side scheduling allowance: thread wake-up and GIL
    contention behind a rapid-block burst.  It is applied in *both*
    inequalities below, because it describes the process rather than one
    thread — once to the heartbeat's stamping path, and once to the
    monitor's own poll.  Budgeting a stall for the heartbeat while
    assuming the monitor wakes punctually is the mistake that made the
    first cut of this policy unsafe.

From those, the largest gap a *healthy* coordinator can leave between two
completed server round trips is

    max_healthy_server_gap = interval + guard_statement_timeout + slack

and nothing shorter is a safe staleness bound.  The argument is that at
every instant either a guarded statement is in flight — and it must answer
or be cancelled within the statement timeout — or none is, in which case
the heartbeat starts one within the remaining interval.  Queueing behind a
concurrent external-effect fence does not extend the gap: the fence runs on
the same guard session and stamps at each of its own round trips.

The safety inequality
---------------------
The monitor measures staleness from completed round trips (client-side
marks can postdate a silent session death, so they cannot carry the
adoption guarantee).  Write ``p`` for the newest server-proven edge and
``t_d`` for the instant the guarded session died; the conservative send-edge
stamping below guarantees ``p <= t_d``.  The monitor polls every
``monitor_interval`` and may itself be ``scheduler_slack`` late, so it
observes ``age >= cap`` no later than

    p + cap + monitor_interval + scheduler_slack

and the hard exit is then budgeted ``exit_margin``.  A successor cannot
begin its silence window before the guard session is released, so it is
eligible no earlier than ``t_d + adoption_silence >= p + adoption_silence``.
Exit-before-adoption is therefore

    cap + exit_envelope                       <= adoption_silence     (1)
      where exit_envelope = exit_margin + 2 * monitor_interval
                            + scheduler_slack
    heartbeat_interval                        <  failure_budget       (2)
    max_healthy_server_gap                    <= cap                  (3)

The second ``monitor_interval`` in the envelope is what makes (1) strict:
only one of the two is spent on poll granularity, so the worst-case exit
completes one whole poll interval *before* the adoption edge rather than
on it.

(1) and (2) are *safety*: violating them means a lost coordinator may still
be alive when a replacement adopts, or that one idle wait alone exhausts
the liveness budget.  They are rejected at startup.  (3) is *stability*:
violating it does not permit two writers, it just guarantees that ordinary
tail latency will eventually hard-exit a healthy coordinator — exactly
issue #212.  It is reported as an advisory, because a deliberately tiny
test or lab policy is allowed to trade stability away, and refusing to
start would be worse than restarting.

What (1) actually guarantees
----------------------------
(1) is a statement about a *budget*, not about the process.  Write ``L``
for the monitor's real lateness on the poll that first observes the stale
edge.  That poll happens no later than ``p + cap + monitor_interval + L``
and the exit completes ``exit_margin`` after it, so exit-before-adoption
holds for that beat if and only if

    L  <  adoption_silence - cap - monitor_interval - exit_margin
       =  scheduler_slack + monitor_interval                         (4)

(``max_guaranteed_monitor_lateness_seconds`` below; the identity on the
right holds whenever the cap is the silence minus the exit envelope, which
it is for every policy that is not floored at the failure budget).  Three
regimes follow:

``L <= scheduler_slack``
    the case (1) budgets: the exit completes one whole monitor poll before
    the adoption edge.
``scheduler_slack < L < scheduler_slack + monitor_interval``
    the strictness reserve is consumed: the exit still completes before
    the adoption edge, but by less than one poll.
``L >= scheduler_slack + monitor_interval``
    exit-before-adoption is **not guaranteed** for that beat; the process
    may still be alive up to ``L - (scheduler_slack + monitor_interval)``
    after the adoption edge.

Production exceeded the assumption (issue #227)
-----------------------------------------------
Three days after this policy shipped, ``union-mainnet`` (bootstrap pin
``9c41894``, coordinator up 45h, zero restarts, no heartbeat exit) recorded
a monitor wake delay of **0.648s** — above the 0.50s slack and above the
0.55s bound (4).  For the duration of that stall the sum on the left of
(1), evaluated with the real lateness, was ``1.30 + 0.05 + 0.648 + 0.10 =
2.098 > 2.00``.  No guard loss coincided, so nothing happened; but the
assumption the proof rests on was violated in production.

The stall was in-process, not on the host (32 cores 93% idle, PSI near
zero, no cgroup throttling).  The window held a 5.1s
``reconcile_invalidation`` full rescan of a 52k-share payout window, and
the phase attribution of the same period showed why the old telemetry
could not see it: the worst ``guard_sql`` was 0.714s against a
server-enforced 0.50s statement timeout while ``scheduler_delay`` never
exceeded 22 microseconds in 645k attempts.  ``guard_sql`` was measured
client-side, so a GIL stall between PostgreSQL answering and the heartbeat
thread resuming was booked as database time.

Attribution that survives GIL stalls
------------------------------------
Each guard statement now returns its own server-side execution time
(``clock_timestamp() - statement_timestamp()``, computed and returned by
the statement itself, so no extra round trip and no extra lock).  The
attempt accumulator splits the client-measured statement span into
``guard_sql`` (server execution, when the statement reports it) and
``guard_client_resume`` (the remainder of the round trip: network transit
plus the time this process took to resume after the answer arrived).  The
residual ``scheduler_delay = total - slot_wait - guard_sql`` therefore
absorbs every process-side delay, inside or outside a round trip;
``guard_client_resume`` is the part of it that fell inside one.  A
statement that reports no server time keeps the old, deliberately
conservative attribution (the whole span is ``guard_sql``).  The
server-proven edge is untouched: it is still the conservative send edge,
never a server-reported timestamp, because the ``p <= t_d`` step above
depends on it.

The response to lateness beyond slack (the decision)
----------------------------------------------------
Two responses were on the table.  (a) Treat lateness beyond slack as
envelope consumed for that beat and tighten the cap for that beat.  (b)
Accept the residual and rely on the fences.

(a) is refused, on the arithmetic rather than on taste.  On the late wake
``w`` the monitor can only exit *now*, completing at ``w + exit_margin``;
that bound does not move whether the threshold it compares the age
against is ``cap`` or ``cap - (L - slack)``.  Tightening changes behaviour
only for server-proven ages in ``[cap - (L - slack), cap)`` — a band that a
healthy coordinator occupies whenever a slow statement coincides with the
stall (with the observed 0.648s the tightened threshold is 1.152s, below
the 1.25s a healthy gap may reach).  So (a) hard-exits healthy
coordinators — issue #212's failure mode — and buys nothing against the
breach it is reacting to.

(b) is safe because the un-covered interval permits no mutation.  Three
observations:

* A successor cannot begin its silence until the predecessor's *guard
  session* is gone (``PsqlShareLedger._writer_lease_adoption_wait_seconds``
  cannot even be reached while the advisory guard is held).  A stalled but
  live process is therefore never adopted over; the hazard is the compound
  event of a guard-session death *and* a monitor lateness beyond (4) in the
  same window.
* During a GIL stall no Python thread runs, so nothing escapes *during* the
  stall.  What could escape is work done in the interval between the
  adoption edge and the (late) exit, at most ``L - (slack +
  monitor_interval)`` long.
* Every ledger write is fenced at the row: share appends and every landing
  statement open with the exact-session ``lease AS (UPDATE
  qbit_ledger_writer_lease ... WHERE writer_session_token = ...)`` CTE and
  join it, and adoption rewrites ``writer_session_token``, so a stale
  writer's statement matches zero lease rows and reports "writer lease is
  not active".  Every external effect (submitblock, CTV fanout, wallet)
  runs behind ``require_fresh_lease_for_external_side_effect``, a
  synchronous exact-session verification on the guard session that raises
  — and hard-exits — the moment the committed row names another session.
  The fence-to-RPC residual is the documented preflight residual between
  independent systems and is not widened by the monitor's lateness.  The
  heartbeat's own next proof fails the same way.

So the shipped response is (b), with the breach made loud instead of
invisible: the monitor's lateness feeds a fixed-bucket histogram, three
threshold counters (0.5x, 0.8x and 1.0x slack), a rolling-window maximum
and the age of the lifetime record; a wake beyond (4) increments an
exit-guarantee breach counter, records the worst overrun, and emits one
structured warning naming the breach; a wake beyond half the slack takes a
rate-limited stack sample of the running threads.  The operator action is
in ``docs/prism-ledger-ops.md``.  The durable fix — an exit path that does
not depend on the interpreter being scheduled — is an out-of-process
watchdog fed by the server-proven edge, scoped in issue #130.

The shipped defaults
--------------------
    guard_statement_timeout = 0.50   (server-enforced, unchanged)
    heartbeat_interval      = 0.25
    scheduler_slack         = 0.50   (~3x the worst delay observed in the
                                      union-mainnet burst behind #212;
                                      exceeded once since, by a 0.648s
                                      in-process stall, see #227 above)
    max_healthy_server_gap  = 0.25 + 0.50 + 0.50 = 1.25
    failure_budget          = 1.25   (the same bound; one number, not two)
    monitor_interval        = 0.05
    exit_margin             = 0.10
    exit_envelope           = 0.10 + 2 x 0.05 + 0.50 = 0.70
    adoption_silence        = 2.00   (> 1.25 + 0.70 = 1.95)
    => server_proven_cap    = 2.00 - 0.70 = 1.30
    => stability surplus    = 1.30 - 1.25 = 0.05
    => max_guaranteed_monitor_lateness = 0.50 + 0.05 = 0.55

Checking (1) at the shipped numbers, with a monitor exactly as late as the
policy budgets:

    1.30 + 0.05 + 0.50 + 0.10 = 1.95  <  2.00

so the old coordinator is gone 0.05s — one monitor poll — before the
successor may compare-and-swap, *provided* the monitor's real lateness is
within the 0.55s bound (4).  That proviso is an assumption about the
process, not a property of the policy; production has exceeded it once.

Genuine writer failover therefore costs 2.0s of adoption silence rather
than 1.0s.  That is the price of the guard session's own 0.5s statement
timeout: the monitor can only trust completed round trips, so the envelope
must contain one whole statement plus one idle interval plus process
scheduling — and then, separately, room for the monitor itself to be that
late in noticing.  The stability surplus is deliberately thin (0.05s):
inequality (1) is the safety bound and comes first, while (3) is already a
worst-case-on-worst-case (a statement running its whole server-side
timeout *and* the process stalling for its whole slack allowance in the
same beat).  Production's worst observed server-proven age during the
issue #212 burst was 0.91s, well under the 1.30s cap, and the ownership
proof that now runs on most beats is a single non-blocking read.  It is half of the 4.0s downstream mitigation this replaces,
and unlike that mitigation it is derived rather than dialled in.

Lowering it further requires shrinking a phase, not the envelope: a shorter
statement timeout for the frequent ownership proof, or a shorter heartbeat
interval.  The proof/renewal split (see
``PsqlShareLedger.prove_writer_lease_guard_session``) already removed the
*typical* cost — the frequent statement no longer scans
``pg_stat_activity`` or touches the lease tuple's row lock — but the
*bound* is still the guard session's statement timeout, and the envelope
must be sized for the bound.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import time
from typing import Callable

# The server-side statement_timeout carried by the dedicated writer-lease
# guard session. It is the hard ceiling on one guarded round trip, so it is
# also the dominant term in the heartbeat's staleness envelope. Declared here
# rather than inline in the connection options so the policy below and the
# connection that enforces it cannot drift apart.
WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS = 0.5

# Statements verify_writer_lease_guard_session may lawfully run inside one
# guarded slot: the verification statement plus its single attribution
# recheck. Callers that budget the verification's execution wall-clock must
# cover this many server-side statement timeouts, or a lawful recheck under
# moderate database latency is killed by the caller's deadline instead of
# rescuing the coordinator.
WRITER_LEASE_VERIFICATION_MAX_STATEMENTS = 2

# Process-side delay the envelope must absorb between PostgreSQL answering
# and the monitor observing that answer: heartbeat thread wake-up, GIL
# contention behind a rapid-block burst, result handling, and the monitor's
# own late wake. Sized at roughly three times the worst delay observed in the
# union-mainnet burst that produced issue #212. Issue #227 recorded a 0.648s
# monitor wake delay on union-mainnet, above this allowance; the module
# docstring states what is and is not guaranteed when that happens.
WRITER_LEASE_HEARTBEAT_SCHEDULER_SLACK_SECONDS = 0.5

WRITER_LEASE_HEARTBEAT_INTERVAL_SECONDS = 0.25
WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS = 0.05
WRITER_LEASE_HEARTBEAT_EXIT_MARGIN_SECONDS = 0.1

# Derived, not chosen: the largest gap between completed guard round trips a
# healthy coordinator can produce (see the module docstring).
WRITER_LEASE_HEARTBEAT_FAILURE_SECONDS = (
    WRITER_LEASE_HEARTBEAT_INTERVAL_SECONDS
    + WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS
    + WRITER_LEASE_HEARTBEAT_SCHEDULER_SLACK_SECONDS
)

# The smallest adoption silence that satisfies inequality (1) is just above
# 1.95s with the budgets above. 2.0s is the next round value and leaves 0.05s
# of stability surplus above the healthy-gap bound.
DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS = 2.0

# The monitor wake-delay lateness observed on union-mainnet at 2026-09-04
# (issue #227), the production evidence that the slack allowance is an
# assumption rather than a bound. Kept as a named constant so the tests that
# judge the policy against it cannot drift from the docstring.
OBSERVED_MONITOR_WAKE_DELAY_SECONDS_227 = 0.648

# Heartbeat modes, and the phase names the attribution and metrics use.
# Fixed vocabularies: metric label cardinality must not grow with traffic.
LEASE_HEARTBEAT_MODE_PROOF = "proof"
LEASE_HEARTBEAT_MODE_RENEW = "renew"
LEASE_HEARTBEAT_MODE_FENCE = "fence"
LEASE_HEARTBEAT_MODES = (
    LEASE_HEARTBEAT_MODE_PROOF,
    LEASE_HEARTBEAT_MODE_RENEW,
    LEASE_HEARTBEAT_MODE_FENCE,
)

# guard_slot_wait + guard_sql + scheduler_delay = total. guard_client_resume
# is the part of scheduler_delay that fell inside a round trip (the answer
# had left PostgreSQL but this process had not resumed); it is reported
# separately because it is exactly the delay issue #227's client-side
# attribution could not see.
LEASE_HEARTBEAT_PHASES = (
    "guard_slot_wait",
    "guard_sql",
    "guard_client_resume",
    "scheduler_delay",
    "total",
)

# The policy terms the metrics surface exports, in derivation order. Closed
# set: adding a term is a deliberate schema change, not a runtime accident.
LEASE_HEARTBEAT_POLICY_TERMS = (
    "adoption_silence",
    "heartbeat_interval",
    "failure_budget",
    "monitor_interval",
    "exit_margin",
    "guard_statement_timeout",
    "scheduler_slack",
    "exit_envelope",
    "server_proven_cap",
    "max_healthy_server_gap",
    "stability_surplus",
    "max_guaranteed_monitor_lateness",
)

LEASE_HEARTBEAT_OUTCOME_PROVEN = "proven"
LEASE_HEARTBEAT_OUTCOME_RENEWED = "renewed"
LEASE_HEARTBEAT_OUTCOME_RENEWAL_DUE = "renewal_due"
LEASE_HEARTBEAT_OUTCOME_DEFERRED = "deferred"
LEASE_HEARTBEAT_OUTCOME_FAILED = "failed"
LEASE_HEARTBEAT_OUTCOMES = (
    LEASE_HEARTBEAT_OUTCOME_PROVEN,
    LEASE_HEARTBEAT_OUTCOME_RENEWED,
    LEASE_HEARTBEAT_OUTCOME_RENEWAL_DUE,
    LEASE_HEARTBEAT_OUTCOME_DEFERRED,
    LEASE_HEARTBEAT_OUTCOME_FAILED,
)

# Monitor wake-delay telemetry (issue #227). Every set below is closed, so
# the rendered cardinality is fixed: the histogram has these buckets and
# +Inf, the late-wake counters carry exactly these slack fractions, and the
# rolling window is a fixed number of slices.
#
# The buckets straddle the shipped policy's decision points: 0.25 (half the
# slack, the stall-probe trigger), 0.4 (0.8x slack), 0.5 (the slack itself),
# and 0.75 / 1.0 (the region the 0.648s production record fell in).
LEASE_MONITOR_WAKE_DELAY_BUCKETS = (
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.4,
    0.5,
    0.75,
    1.0,
    2.0,
)
# Label values are the fraction text, so a PromQL author can read
# `slack_fraction="1.0"` as "wakes at least a whole scheduler_slack late".
LEASE_MONITOR_LATE_WAKE_SLACK_FRACTIONS = ("0.5", "0.8", "1.0")
# The rolling-window maximum falls back once a stall ages out of the window,
# unlike the lifetime gauge. Thirty 10s slices give a 5-minute window, the
# same horizon the qbit-tools alert's changes[5m] clause reasons over.
LEASE_MONITOR_WAKE_DELAY_WINDOW_SECONDS = 300.0
LEASE_MONITOR_WAKE_DELAY_WINDOW_SLICES = 30
# The stall probe samples the running threads' stacks when a wake is at
# least this fraction of the slack late. Rate-limited so it can never turn a
# stall into a stall storm: at most MAX_SAMPLES per WINDOW, the rest are
# counted as suppressed.
LEASE_MONITOR_STALL_PROBE_TRIGGER_SLACK_FRACTION = 0.5
LEASE_MONITOR_STALL_PROBE_MAX_SAMPLES_PER_WINDOW = 3
LEASE_MONITOR_STALL_PROBE_WINDOW_SECONDS = 60.0


class WriterLeaseHeartbeatPolicyError(ValueError):
    """An unsafe heartbeat timing policy, refused at startup."""


@dataclass(frozen=True, slots=True)
class WriterLeaseHeartbeatPolicy:
    """The five coupled heartbeat timings plus the phases that justify them.

    Constructed from live configuration at startup and from the ledger's
    own adoption-silence value, so an operator override reaches the same
    inequality the compiled defaults satisfy.  Never mutated: a policy is
    resolved once per heartbeat start and once per monitor loop.
    """

    adoption_silence_seconds: float
    heartbeat_interval_seconds: float
    failure_budget_seconds: float
    monitor_interval_seconds: float
    exit_margin_seconds: float
    guard_statement_timeout_seconds: float = (
        WRITER_LEASE_GUARD_STATEMENT_TIMEOUT_SECONDS
    )
    scheduler_slack_seconds: float = (
        WRITER_LEASE_HEARTBEAT_SCHEDULER_SLACK_SECONDS
    )

    @property
    def exit_envelope_seconds(self) -> float:
        """Wall-clock the monitor needs to notice and finish a hard exit.

        Four terms, and leaving any of them out is a split-brain window:

        ``exit_margin``
            the hard exit's own budget once the decision is taken.
        one ``monitor_interval``
            poll granularity. The monitor can be a full period past the
            instant the cap actually elapsed before it looks.
        ``scheduler_slack``
            the monitor thread's own budgeted lateness. The monitor is a
            Python thread in the same process, behind the same GIL, as the
            heartbeat whose stalls this policy explicitly budgets for; it
            cannot be assumed to wake on time when the heartbeat is not.
            Omitting this term is what made the first cut of this policy
            unsafe: with the shipped budgets a maximally late monitor
            observed the stale edge 0.45s *after* the successor's adoption
            edge, so a stall the policy said was acceptable produced two
            live writers.
        one further ``monitor_interval``
            the strictness reserve. Without it the worst-case exit lands
            exactly on the adoption edge rather than before it, and the
            guarantee is "not after" instead of "before".

        Inequality (1) requires all of this to fit inside the adoption
        silence alongside the staleness cap.
        """
        return (
            self.exit_margin_seconds
            + 2.0 * self.monitor_interval_seconds
            + self.scheduler_slack_seconds
        )

    @property
    def server_proven_cap_seconds(self) -> float:
        """Staleness cap the monitor enforces on completed round trips.

        The whole remaining silence budget after the exit envelope, floored
        at the failure budget so a deliberately tiny (advised) silence
        degrades to the plain liveness bound rather than killing lawful
        single-statement verifications.
        """
        return max(
            self.failure_budget_seconds,
            self.adoption_silence_seconds - self.exit_envelope_seconds,
        )

    @property
    def max_healthy_server_gap_seconds(self) -> float:
        """Largest gap between round trips a healthy coordinator produces."""
        return (
            self.heartbeat_interval_seconds
            + self.guard_statement_timeout_seconds
            + self.scheduler_slack_seconds
        )

    @property
    def stability_surplus_seconds(self) -> float:
        """Headroom the staleness cap keeps above the healthy-gap bound."""
        return (
            self.server_proven_cap_seconds
            - self.max_healthy_server_gap_seconds
        )

    @property
    def max_guaranteed_monitor_lateness_seconds(self) -> float:
        """Largest real monitor lateness for which exit precedes adoption.

        Inequality (4) of the module docstring. The monitor that first
        observes the stale edge does so no later than ``p + cap +
        monitor_interval + L`` and exits ``exit_margin`` later; the
        successor is eligible no earlier than ``p + adoption_silence``. So
        the exit is guaranteed to precede adoption exactly when ``L`` is
        below this value, which equals ``scheduler_slack +
        monitor_interval`` whenever the cap is not floored at the failure
        budget. A wake later than this is an exit-guarantee breach for
        that beat, whether or not a guard loss coincided.
        """
        return max(
            0.0,
            self.adoption_silence_seconds
            - self.server_proven_cap_seconds
            - self.monitor_interval_seconds
            - self.exit_margin_seconds,
        )

    def exit_guarantee_overrun_seconds(self, monitor_lateness_seconds: float) -> float:
        """How far one monitor wake pushed the worst-case exit past adoption.

        Zero while the wake is within
        :attr:`max_guaranteed_monitor_lateness_seconds`; otherwise the time
        by which the process could still have been alive after the
        successor's adoption edge on that beat, had its guard session died.
        """
        return max(
            0.0,
            float(monitor_lateness_seconds)
            - self.max_guaranteed_monitor_lateness_seconds,
        )

    def violations(self) -> tuple[str, ...]:
        """Reasons this policy cannot guarantee exit before adoption."""
        reasons: list[str] = []
        for name in (
            "adoption_silence_seconds",
            "heartbeat_interval_seconds",
            "failure_budget_seconds",
            "monitor_interval_seconds",
            "exit_margin_seconds",
            "guard_statement_timeout_seconds",
            "scheduler_slack_seconds",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0.0:
                reasons.append(f"{name} must be finite and non-negative, got {value!r}")
        if reasons:
            return tuple(reasons)
        if self.heartbeat_interval_seconds <= 0.0:
            reasons.append("heartbeat_interval_seconds must be positive")
        if self.monitor_interval_seconds <= 0.0:
            reasons.append("monitor_interval_seconds must be positive")
        if self.adoption_silence_seconds <= 0.0:
            reasons.append("adoption_silence_seconds must be positive")
        if self.failure_budget_seconds <= self.heartbeat_interval_seconds:
            reasons.append(
                "failure budget "
                f"{self.failure_budget_seconds:g}s must exceed the heartbeat "
                f"interval {self.heartbeat_interval_seconds:g}s, or one idle "
                "wait alone exhausts the liveness budget"
            )
        if (
            self.adoption_silence_seconds
            <= self.failure_budget_seconds + self.exit_envelope_seconds
        ):
            reasons.append(
                "adoption silence "
                f"{self.adoption_silence_seconds:g}s must exceed the failure "
                f"budget {self.failure_budget_seconds:g}s plus the exit "
                f"envelope {self.exit_envelope_seconds:g}s (exit margin "
                f"{self.exit_margin_seconds:g}s + 2 x monitor interval "
                f"{self.monitor_interval_seconds:g}s + scheduler slack "
                f"{self.scheduler_slack_seconds:g}s), or a coordinator that "
                "lost its guarded session may still be live when a "
                "replacement becomes adoption-eligible"
            )
        return tuple(reasons)

    def advisories(self) -> tuple[str, ...]:
        """Reasons this policy is safe but will false-exit under tail latency."""
        if self.violations():
            return ()
        if self.stability_surplus_seconds >= 0.0:
            return ()
        return (
            "server-proven staleness cap "
            f"{self.server_proven_cap_seconds:g}s is below the largest gap a "
            "healthy coordinator can leave between completed guard round "
            f"trips ({self.max_healthy_server_gap_seconds:g}s = heartbeat "
            f"interval {self.heartbeat_interval_seconds:g}s + guard statement "
            f"timeout {self.guard_statement_timeout_seconds:g}s + scheduler "
            f"slack {self.scheduler_slack_seconds:g}s); ordinary verification "
            "tail latency will hard-exit a healthy coordinator (issue #212). "
            "Raise the adoption silence or lower the heartbeat interval",
        )

    def validate(self) -> None:
        """Raise :class:`WriterLeaseHeartbeatPolicyError` when unsafe."""
        reasons = self.violations()
        if reasons:
            raise WriterLeaseHeartbeatPolicyError(
                "writer lease heartbeat timing policy is unsafe: "
                + "; ".join(reasons)
            )

    def describe(self) -> str:
        """One line naming every term of the inequality, for logs and exits."""
        return (
            f"adoption_silence={self.adoption_silence_seconds:g}s "
            f"interval={self.heartbeat_interval_seconds:g}s "
            f"failure_budget={self.failure_budget_seconds:g}s "
            f"monitor={self.monitor_interval_seconds:g}s "
            f"exit_margin={self.exit_margin_seconds:g}s "
            f"guard_statement_timeout={self.guard_statement_timeout_seconds:g}s "
            f"scheduler_slack={self.scheduler_slack_seconds:g}s "
            f"exit_envelope={self.exit_envelope_seconds:g}s "
            f"=> server_proven_cap={self.server_proven_cap_seconds:g}s "
            f"healthy_gap={self.max_healthy_server_gap_seconds:g}s "
            f"surplus={self.stability_surplus_seconds:g}s "
            f"max_monitor_lateness={self.max_guaranteed_monitor_lateness_seconds:g}s"
        )


DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY = WriterLeaseHeartbeatPolicy(
    adoption_silence_seconds=DEFAULT_WRITER_LEASE_ADOPTION_SILENCE_SECONDS,
    heartbeat_interval_seconds=WRITER_LEASE_HEARTBEAT_INTERVAL_SECONDS,
    failure_budget_seconds=WRITER_LEASE_HEARTBEAT_FAILURE_SECONDS,
    monitor_interval_seconds=WRITER_LEASE_HEARTBEAT_MONITOR_SECONDS,
    exit_margin_seconds=WRITER_LEASE_HEARTBEAT_EXIT_MARGIN_SECONDS,
)
# The compiled defaults must satisfy the inequality they document. A future
# edit that breaks it fails at import rather than in production.
DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY.validate()
assert not DEFAULT_WRITER_LEASE_HEARTBEAT_POLICY.advisories()


@dataclass(frozen=True, slots=True)
class WriterLeaseVerificationPhases:
    """Where one guard verification's wall-clock actually went.

    The operator question issue #212 could not answer was "which phase
    consumed the safety envelope?".  Every heartbeat attempt now finishes
    with this breakdown, the monitor's hard exit quotes the last one, and
    the metrics surface exposes it with fixed cardinality.

    ``slot_wait + guard_sql + scheduler_delay == total``.
    ``client_resume`` is the part of ``scheduler_delay`` that fell inside a
    statement's round trip (issue #227): PostgreSQL had answered, or the
    answer was in transit, and this process had not yet resumed.  It is
    only non-zero when the statement reported its own server-side
    execution time (``server_reported``); otherwise ``guard_sql`` is the
    whole client-measured span, as it was before #227, which is the
    conservative reading.
    """

    mode: str
    outcome: str
    slot_wait_seconds: float
    guard_sql_seconds: float
    statement_count: int
    scheduler_delay_seconds: float
    total_seconds: float
    client_resume_seconds: float = 0.0
    server_reported: bool = False

    def summary(self) -> str:
        return (
            f"mode={self.mode} outcome={self.outcome} "
            f"slot_wait={self.slot_wait_seconds:.3f}s "
            f"guard_sql={self.guard_sql_seconds:.3f}s"
            f"({self.statement_count} stmt"
            f"{', server-timed' if self.server_reported else ''}) "
            f"client_resume={self.client_resume_seconds:.3f}s "
            f"scheduler={self.scheduler_delay_seconds:.3f}s "
            f"total={self.total_seconds:.3f}s"
        )

    def phase_seconds(self) -> dict[str, float]:
        """Fixed-key phase map; keys are :data:`LEASE_HEARTBEAT_PHASES`."""
        return {
            "guard_slot_wait": self.slot_wait_seconds,
            "guard_sql": self.guard_sql_seconds,
            "guard_client_resume": self.client_resume_seconds,
            "scheduler_delay": self.scheduler_delay_seconds,
            "total": self.total_seconds,
        }


UNATTRIBUTED_PHASES = WriterLeaseVerificationPhases(
    mode="none",
    outcome="none",
    slot_wait_seconds=0.0,
    guard_sql_seconds=0.0,
    statement_count=0,
    scheduler_delay_seconds=0.0,
    total_seconds=0.0,
)


class WriterLeaseVerificationAttempt:
    """Monotonic phase accumulator for one guard verification attempt.

    Written only by the thread running the attempt and read only after
    :meth:`finish` publishes an immutable snapshot, so it needs no lock.
    Marks are best effort: a verifier that reports no slot acquisition and
    no statement completion still yields a truthful total, with the whole
    duration landing in ``scheduler_delay`` — which is itself the signal
    that the attempt never reached PostgreSQL.
    """

    __slots__ = (
        "mode",
        "_monotonic",
        "_started",
        "_slot_acquired",
        "_last_statement_end",
        "_statement_count",
        "_pending_send",
        "_proven_edge",
        "_server_seconds",
        "_server_reported",
    )

    def __init__(
        self,
        mode: str,
        *,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.mode = mode
        # Resolved at construction, not bound as a default argument: tests
        # that drive the lease lifecycle patch ``time.monotonic`` on the
        # module object, and a default captured at import time would keep
        # reading the real clock behind their backs.
        self._monotonic = monotonic or time.monotonic
        self._started = self._monotonic()
        self._slot_acquired: float | None = None
        self._last_statement_end: float | None = None
        self._statement_count = 0
        # The send edge of the statement currently in flight, and the
        # conservative "PostgreSQL was alive no earlier than this" edge of
        # the newest completed round trip. Both default to the attempt's
        # start so a verifier that reports no marks still yields an edge
        # that cannot overstate freshness.
        self._pending_send = self._started
        self._proven_edge = self._started
        # Server-reported execution time, summed over the statements that
        # reported one (issue #227). Kept apart from the client marks: it
        # refines the attribution and never touches the proven edge.
        self._server_seconds = 0.0
        self._server_reported = False

    @property
    def started_monotonic(self) -> float:
        return self._started

    @property
    def proven_edge_monotonic(self) -> float:
        """Conservative server-proven edge of the newest completed round trip."""
        return self._proven_edge

    def slot_acquired(self) -> None:
        """The guard session's serialized query slot was handed over.

        This is also the send edge of the attempt's first statement: the
        guard issues it immediately after handing over the slot.
        """
        if self._slot_acquired is None:
            self._slot_acquired = self._monotonic()
            self._pending_send = self._slot_acquired

    def statement_completed(self) -> float:
        """One guarded statement's round trip returned a result.

        Returns the conservative server-proven edge for that round trip —
        when the statement was sent, not when the answer was handled — and
        arms the next statement's send edge, since a followup leaves for
        PostgreSQL as soon as this result is in hand.
        """
        now = self._monotonic()
        self._proven_edge = self._pending_send
        self._pending_send = now
        self._last_statement_end = now
        self._statement_count += 1
        return self._proven_edge

    def statement_server_seconds(self, seconds: float | None) -> None:
        """One guarded statement reported its own server-side execution time.

        ``seconds`` is ``clock_timestamp() - statement_timestamp()`` as
        evaluated by the statement itself, so it excludes the answer's
        transit and everything this process did after receipt.  ``None``
        (a statement that does not report, or a malformed value) leaves
        the attribution on its conservative client-side reading.  Never
        consulted for the server-proven edge: a server clock reading is
        not a monotonic instant in this process, and the exit-ordering
        proof needs the send edge.
        """
        if seconds is None:
            return
        try:
            value = float(seconds)
        except (TypeError, ValueError):
            return
        if not math.isfinite(value) or value < 0.0:
            return
        self._server_seconds += value
        self._server_reported = True

    def finish(self, outcome: str) -> WriterLeaseVerificationPhases:
        finished = self._monotonic()
        total = max(0.0, finished - self._started)
        slot_wait = (
            max(0.0, self._slot_acquired - self._started)
            if self._slot_acquired is not None
            else 0.0
        )
        # Statement 1 spans [slot acquisition, first response]; statement k
        # spans [response k-1, response k]. Without server-reported timing
        # the sub-millisecond client gap between them is charged to SQL,
        # which keeps the attribution conservative rather than flattering
        # the database. With it, guard_sql is the server's own execution
        # time (clamped to the span it must physically fit inside) and the
        # rest of the span is client resume: transit plus this process's
        # delay in handling an answer PostgreSQL had already given, which
        # is the GIL stall issue #227 could not see.
        if self._last_statement_end is not None and self._slot_acquired is not None:
            statement_span = max(0.0, self._last_statement_end - self._slot_acquired)
        else:
            statement_span = 0.0
        if self._server_reported:
            guard_sql = min(statement_span, self._server_seconds)
            client_resume = statement_span - guard_sql
        else:
            guard_sql = statement_span
            client_resume = 0.0
        return WriterLeaseVerificationPhases(
            mode=self.mode,
            outcome=outcome,
            slot_wait_seconds=slot_wait,
            guard_sql_seconds=guard_sql,
            statement_count=self._statement_count,
            scheduler_delay_seconds=max(0.0, total - slot_wait - guard_sql),
            total_seconds=total,
            client_resume_seconds=client_resume,
            server_reported=self._server_reported,
        )
