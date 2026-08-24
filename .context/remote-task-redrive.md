# Task: in-process targeted ancestor re-drive for stuck accepted-parent payout transitions (fixes SwapLabsInc/qbit-mining-bootstrap#190 Finding 1, addresses Finding 2)

Base branch: `2.x.x` (verify `git rev-parse HEAD` matches the recorded base before touching code; `lab/prism/prism_coordinator.py` must exist and contain `_defer_for_pending_parent_payout_transition`, and `lab/prism/payout_state.py` must contain `_await_pending_parent_payout_preview` — if either assertion fails, report a blocker instead of proceeding).

Commit all work on THIS workspace's task branch (the branch the workspace was created on). Do not push.

## The bug (verified in production 2026-08-24, full detail in issue #190)

When a found block's finalization defers because an ancestor's accepted-parent payout transition is unresolved, the retained-for-retry loop can only RE-CHECK the transition, never resolve it:

- `_defer_for_pending_parent_payout_transition` (lab/prism/prism_coordinator.py:~9084) re-checks `_accepted_block_payout_transition_for_parent` and re-abandons with `ledger-confirmation-failed` each cycle (observed: 37 cycles / 906s).
- Worse, each retry's `preserve_active_candidate_barrier()` re-arms the active candidate's landed barrier — the exact barrier job delivery waits on — re-asserting the wedge every pass.
- The stuck ancestor's preview publication only happens in the ancestor's own finalization (lab/prism/block_finalization.py:~1062/~1229) or in startup durable replay. `_await_pending_parent_payout_preview` (lab/prism/payout_state.py:~3128) raises typed backpressure and schedules only a tip refresh, which itself returns `payout_blocked`.
- Durable enumeration short-circuits while a retry candidate / live queue / replay queue exists (lab/prism/block_candidates.py:~3101-3113) and the submitter skips replay while immediate work succeeds (:~5540-5545), so load starves the resolving path.
- Only the 900s publication-progress watchdog (process exit → docker restart → startup replay) recovers; replay then resolves the same state in ~19 seconds. Meanwhile the pool serves no new-tip work for 15 minutes, which loses rented hashrate (Finding 2).

## The fix to implement

Add a targeted in-process re-drive: when finalization has deferred N times (configurable, sensible default e.g. 3) on the SAME pending ancestor transition, re-drive that ancestor's resolution in-process using the same logic startup durable replay uses — without exiting the process. Requirements:

1. Reuse/extract the existing durable-replay resolution path rather than duplicating logic; the startup replay provably resolves this state, so the in-process re-drive should converge on the same code.
2. The re-drive must be safe under concurrency: respect the existing locks/serializers (payout balance mutation lock, block submitter lock, preview condition) and the invariants documented around `_publish_accepted_block_payout_preview` (a preview must not change during retry; publication crosses the atomic boundary).
3. It must not fight the barrier: the deferral path's `preserve_active_candidate_barrier()` behavior should be reviewed — if the re-drive resolves the ancestor, the retained candidate's next retry should proceed to normal finalization.
4. Bound it: the re-drive itself must not be able to livelock (cap attempts; on exhaustion fall through to today's behavior so the watchdog remains the backstop).
5. Telemetry: add a counter for re-drive attempts/successes (follow the existing `qbit_prism_*` metric conventions in lab/prism/metrics.py) and a log line for each re-drive, so the next stress test can observe it.
6. Do NOT change `coordination_blocked_budget` or the watchdog — they remain the backstop.

## Tests (required)

- A regression test that reproduces the wedge shape: a retained candidate whose parent transition is armed+landed but whose preview never publishes; assert that without the fix the retry loop spins (or the deferral repeats) and with the fix the ancestor is re-driven and finalization completes. Follow the style of existing tests in tests/test_prism_payout_state.py and tests/test_prism_tip_refresh_delivery.py (they already construct these states — see e.g. tests asserting "accepted parent payout preview is not ready yet").
- A test that the re-drive respects the attempt cap and falls back to deferral.
- Run the relevant test files and report results honestly; run any repo lint/format hooks that exist.

## Deliverable

Committed code + tests on the task branch (multiple logical commits fine). In your completion summary: files changed, design decisions (where the re-drive hooks in, lock ordering), test results verbatim, and any risks or open questions. Do not push, do not open a PR.
