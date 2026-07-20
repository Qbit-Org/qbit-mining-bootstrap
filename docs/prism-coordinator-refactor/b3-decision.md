# B3 Finalization Concurrency Decision

Decision: **OMIT** a second landing/finalization lane.

The existing dedicated block-submitter remains the single finalization actor.
B2 adds bounded phase and candidate-interarrival metrics so production evidence
can reopen this decision without adding speculative durability machinery now.

## Evidence

The directly tested phases run in this order:

1. admission;
2. land and durable confirmation;
3. CTV persistence and conditional share credit;
4. evidence construction;
5. ordered audit publication;
6. process-local accounting and stop-or-refresh signaling.

A representative in-memory candidate fixture on 2026-07-20 recorded one
successful finalization followed immediately by another submission:

| Measurement | Observed |
| --- | ---: |
| interval between candidate starts | 0.003005 s |
| land/confirm maximum | 0.001473 s |
| audit publication maximum | 0.001426 s |
| accounting maximum | 0.000013 s |
| admission maximum | 0.000060 s |
| evidence maximum | 0.000004 s |
| CTV/credit maximum | 0.000001 s |

The second same-height candidate was admitted promptly and rejected as stale;
it did not reveal a delayed valid candidate. These values are test-fixture
observations, not production latency promises. The focused finalization,
candidate, payout, audit, share-ledger/writer, and shutdown union passed 369
tests, including parent-persistence and delivery-serialization races. No test
or available runtime evidence demonstrates a valid child candidate or required
tip response waiting long enough to require a separate lane. The live Stratum
targets remain `UNAVAILABLE` because `qbitd` is not installed, so the default
`OMIT` rule applies rather than treating missing live evidence as proof to add
complexity.

## Durability and ordering

- Candidate intent is durable before its bounded in-memory wakeup. The existing
  wakeup queue is capped at 32; the ledger outbox, not that queue, is the replay
  authority after restart or queue coalescing.
- Finalization already runs off the share-ack path on its dedicated submitter.
  A second lane would need another durable post-submit handoff and an exact
  recovery state for the boundary between block landing and confirmation.
- A child payout base depends on its parent's verified and durable transition.
  The existing parent barrier deliberately serializes that dependency. Running
  the later phases concurrently would not remove the required ordering.
- Coordinator shutdown admits finalization as a ledger writer and waits for
  writer quiescence. A second actor would add a new drain/join boundary and a
  new ordering choice between parent completion and lease release.
- Phase metrics retain only count, sum, and maximum per phase plus aggregate
  interarrival count, sum, and minimum. They add no candidate history or
  unbounded backlog.

Revisit this decision only if production metrics show a subsequent valid
candidate or required tip response consistently arriving before the preceding
finalization completes, with material delay attributable to the later phases.
Any future implementation must retain the durable outbox, bound every new
queue, define the post-submit restart state, preserve parent/accounting order,
and extend shutdown quiescence tests.
