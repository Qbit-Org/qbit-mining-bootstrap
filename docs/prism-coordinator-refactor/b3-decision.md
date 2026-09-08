# B3 Finalization Concurrency Decision

Decision: **RETAIN** the single post-offer finalization lane behind the #113
node-first split. Do not add a second landing/finalization actor.

The original version of this decision predates the #113 rework and framed the
question as "one lane or two" for the whole submit path. #113 answered part of
that question upstream: the node offer now runs on a lock/DB-free fast lane
before writer admission, and a separate bounded accounting actor receives the
offer evidence and drives the durable tail. What remains single-lane — and what
this document decides to keep single-lane — is the post-offer finalization
policy itself: one candidate at a time holds the same-hash disposition, lands,
verifies, publishes, and accounts. `BlockFinalizationService`
(`lab/prism/block_finalization.py`) owns that policy and its bounded phase and
candidate-interarrival metrics, so production evidence can reopen this decision
without adding speculative durability machinery now.

## Current shape (post-#113)

```text
B1 dequeue → disposition lease → node offer (fast lane, pre-admission)
→ accounting handoff → B1 writer admission
→ B3 post-offer finalization (this owner)
→ B1 durable outbox terminalization/retry → disposition release
```

- B1 (`lab/prism/block_candidates.py`) owns the node offer, the same-hash
  disposition guard, the accounting queue/thread, replay/quarantine, retry
  pacing, and outbox terminalization. Accounting saturation cannot convoy new
  node offers: the offer happens before the handoff.
- B3 accepts B1's prepared `node_submission` and never opens a second
  transport path. Its only fallback `submitblock` (a candidate that was never
  offered) runs under the append-landing fence and lease verification at the
  RPC boundary, through B1's node-offer seam.
- P1 (`lab/prism/payout_state.py`) owns the payout previews, the balance
  serializer, append/anchor fences, and the durable replay-window proof; B3
  consumes them through coordinator facades and decides only whether a failed
  proof rejects an *unoffered* reconstructed candidate. Already-offered replay
  keeps its as-issued snapshot.

## Ordered phases

The directly tested phases run in this order inside
`_submit_block_candidate_serialized` (the seam B1's accounting actor calls
with the disposition already held; the decorated public
`submit_block_candidate` remains a compatibility entrypoint over the same
runner):

1. admission — post-offer tip/active classification, pool-closure override on
   a provable chain hit, and terminal short-circuits for already-recorded
   acceptances;
2. land and durable confirmation — anchor exposure, the unoffered-replay
   window proof, epoch/lease fences, `submitblock` fallback, audit
   build/verify with the balance serializer released, persist and confirm;
3. CTV persistence and conditional share credit (with the urgent payout
   artifact fold);
4. evidence construction from bounded aggregate counters;
5. ordered audit publication through the PR 75 store (balance-mutation lock,
   then the publication-order guard, with a fresh durable floor read);
6. process-local accounting and stop-or-refresh signaling.

The admission phase also hosts the replay-window *decision context* capture;
the proof itself and the landing execute inside phase 2 so the anchor token's
exposure/retire pair brackets the landing exactly as the pre-extraction
coordinator body did.

## Evidence

The extraction-time fixture evidence from the original decision (interarrival
0.003005 s; land/confirm max 0.001473 s; audit publication max 0.001426 s; all
other phases ≤ 0.000060 s) remains representative: no phase after the node
offer sits on the miner's share-ack path, and the second same-height candidate
was admitted promptly and rejected as stale. The re-cut re-validated the moved
bodies against the full current suite (all accepted-block, replay, lost-ACK,
saturation, lease-deferral, and append-epoch regression tests pass unchanged).
Live `qbitd` validation remains **UNAVAILABLE** in this execution environment
(no installed `qbitd`), so the default rule applies again: missing live
evidence is not proof to add complexity.

## Durability and ordering

- Candidate intent is durable before its bounded in-memory wakeup. The ledger
  outbox, not the wakeup queue, is the replay authority after restart or queue
  coalescing; B3 records the accepted-block result and B1 terminalizes the
  outbox row.
- The #113 accounting actor already gives the offer path its own lane. A
  second *finalization* actor would need another durable post-offer handoff
  and an exact recovery state for the boundary between landing and
  confirmation, duplicating what the outbox and same-hash disposition provide.
- A child payout base depends on its parent's verified and durable transition.
  The parent barrier (`_defer_for_pending_parent_payout_transition`)
  deliberately serializes that dependency; concurrent later phases would not
  remove the required ordering.
- Coordinator shutdown admits finalization as a ledger writer and waits for
  writer quiescence. A second actor would add a new drain/join boundary and a
  new ordering choice between parent completion and lease release.
- Phase metrics retain only count, sum, and maximum per phase plus aggregate
  interarrival count, sum, and minimum. They add no candidate history or
  unbounded backlog.

Revisit this decision only if production metrics show a subsequent valid
candidate or required tip response consistently arriving before the preceding
finalization completes, with material delay attributable to the post-offer
phases (the offer itself is already unconvoyed). Any future implementation
must retain the durable outbox, bound every new queue, define the post-submit
restart state, preserve the offer-before-accounting invariant and
parent/accounting order, and extend shutdown quiescence tests.
