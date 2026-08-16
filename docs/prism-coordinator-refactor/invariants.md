# Refactor Invariants

These are release constraints, not process gates. If a proposed boundary
conflicts with one, change the boundary.

## Architecture

- `prism_coordinator.py` is the construction, signal, startup, and top-level
  shutdown root. Leaf services must not import it.
- Each service owns its mutable state, lock, bounded queue/executor, metrics,
  and worker lifecycle. Do not replace the coordinator with a broad context
  object.
- Cross-service work that can be superseded carries immutable identity:
  connection generation, tip/template generation, payout generation, worker,
  difficulty, or durable candidate identity as applicable.
- No socket, RPC, PostgreSQL, subprocess, filesystem, or blocking queue work
  runs while a shared coordinator/session lock is held.
- When both are required, a client's vardiff lock is acquired before the
  coordinator control lock. Normal share submission takes one immutable
  coordinator control snapshot; deduplication and share accounting use their
  dedicated owner locks rather than reacquiring the coordinator lock.
- Compatibility delegates may remain only while an in-repository caller needs
  them. The implementation body and mutable state remain in one owner.

## Mining work and delivery

- Detected tip, published tip, template artifacts, and payout state remain
  distinct. Older work cannot overwrite newer observation authority.
- Publication-critical latest-tip work preempts or defers routine/initial work
  without allowing concurrent heavy builders.
- `mining.set_difficulty` and its matching `mining.notify` commit as one client
  update after final connection, authorization, tip, template, payout, worker,
  and difficulty validation.
- Slow or failed sends do not hold global locks or block unrelated clients.
- Clean-job retirement, same-tip retention, stale grace, and initial/reconnect
  queue bounds retain their current behavior.
- Vardiff changes become active only with a job stamped at that difficulty;
  every failure or supersession restores speculative state.
- Failed tip refreshes use bounded, jittered owner-held pacing tied to the
  observed tip. A successful refresh or a newer tip clears the holdoff, and a
  same-tip poll may reuse only a still-fresh coherent template snapshot.

## Shares, candidates, and payout

- A normal share is acknowledged only after its durable ledger transaction.
  Accepted counters and vardiff accounting advance at the same boundary.
- A share and required block-candidate intent commit atomically. The durable
  outbox is authoritative; the in-memory queue is only a bounded wakeup.
- Duplicate identity uses the immutable job worker and header. Retry or
  reauthorization cannot bypass it.
- Candidate replay is idempotent, keeps parents ahead of dependent children,
  distinguishes retryable from terminal outcomes, and cannot double-account a
  block or regress published evidence.
- Candidate attempt time is durable before processing. Intentional retry waits
  wake in bounded heartbeat slices, expose pending/unattempted age and backoff
  state, and keep blocked processing phases eligible for watchdog detection.
- A candidate whose network outcome is known but whose durable outbox
  finalization failed resumes finalization with bounded pacing. It does not
  resubmit the block, recount acceptance, rebuild audit evidence, or re-adopt
  the released share-writer floor.
- Payout generations are monotonic and a stale prepared source cannot publish.
  Accepted-block preview, final coinbase, confirmed balances, and delivered
  jobs must agree.
- Post-accept and blockwait paths only notify the single refresh owner; they do
  not enter the heavy refresh lane themselves.

## Audit artifacts

- Owned paths are descriptor-relative, no-follow, strictly parsed, and confined
  to pinned directory identities.
- Canonical bytes are verified before durable publication. Writes and mutable
  pair replacement are atomic and fsync their required parent boundaries.
- Ledger-assigned audit publication sequence, not height or process order,
  decides current evidence across replay, restart, same-height replacement, and
  reorg.
- Retention is post-publication best effort and cannot remove a current,
  reserved, malformed, symlinked, or unowned entry.
- Audit bodies and share segments remain digest-checked and reconstructable in
  memory and PostgreSQL modes.

## Health, HTTP, and metrics

- Health deadlines use monotonic time. Pending age starts at the first
  unresolved change and does not slide on churn.
- First-job starvation and current-tip coverage loss have separate grace
  clocks. Previously delivered clients do not masquerade as first-job
  starvation, and restored coverage resets the coverage-loss clock.
- Publication and successful socket delivery are separate proofs. Cached base
  health never masks a newer progress failure.
- Publication divergence is cleared only by a coherent completed publication,
  including a no-op poll that proves existing work is current.
- Public routes, status codes, schemas, cache headers, metric names/labels, and
  environment defaults remain compatible unless a roadmap item explicitly
  authorizes a bounded behavior change.
- `/healthz` and `/metrics` perform no backend, ledger, artifact, or RPC work
  after cached observability is complete.
- Metrics copy share counters under the dedicated accounting lock and expose
  coordinator-lock contention plus durable candidate pending/backoff state
  through narrow snapshots owned by their source services.
- The coordinator-owned HTTP listener reads complete cached metrics only. The
  externally managed compatibility handler may render metrics synchronously
  while its cache is still uninitialized because it owns no refresher thread.
- An audit HTTP serve-loop exit closes and retires its owned listener before a
  replacement listener can start, including unexpected post-readiness exits.

## Shutdown and liveness

Shutdown remains: close admission and signal stop; cancel refresh work; drain
admitted writers; release or deliberately withhold the exact writer lease;
then drain non-writer threads, sockets, and executors. Publication-progress
watchdog protection remains active even when ordinary heartbeat checking is
disabled.
