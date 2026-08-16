# PRISM Coordinator Refactor

Status: **complete** and organized as a nine-PR review stack. No required
roadmap item remains.

The completed tree is integrated with `origin/1.x.x` at `36267d6` (#120),
carrying all thirty-three upstream commits #87-#120. It preserves
the base branch's public hashrate, refresh/livelock, initial/reconnect delivery,
queue-reclamation, and latest-tip priority fixes in the extracted owners. It
carries the upstream node-first block submission and accounting actor (#113),
tip-refresh epoch waves (#101), incremental payout windows and append/anchor
fencing (#112), the found-block landing livelock fix with landing-class
deadlines and degradable previews (#120), and the lease heartbeat/fencing
work (#111/#114/#116/#118) inside their extracted owners. The former exact-hash, literal-authorization,
mandatory-reviewer, and per-slice full-suite workflow is retired.

## Result

`lab/prism/prism_coordinator.py` is now the construction, startup, signal,
stable-facade, and top-level shutdown root. Domain state machines, background
loops, queues, locks, cached observability, persistence, HTTP, and mining work
live in dedicated owners. See [Structure](structure.md) for the boundary and
the documented size exception.

B3 exists as the post-offer finalization owner (`block_finalization.py`)
behind upstream's #113 node-first split: the block-candidate owner makes the
node offer and schedules durable accounting, and B3 finalizes after the offer
in one lane. There is still no second transport path or duplicate durable
handoff. See the [decision record](b3-decision.md).

The final runnable validation matrix passes, including the Docker-backed
PostgreSQL ledger and A1 publication gates, container compile and image
build, and the Rust workspace. Both live Stratum targets are `UNAVAILABLE`
in the current environment because `qbitd` is absent from the host; that is
missing-environment evidence, not a pass or a product failure. Exact results
are in [Validation](validation.md).

## Reference documents

- [Invariants](invariants.md): release behavior that must remain true.
- [Roadmap](roadmap.md): completed slices and decisions.
- [Structure](structure.md): final ownership map and structural audit.
- [Validation](validation.md): risk-based cadence and final evidence.
- [Stacked PRs](stacked-prs.md): publication order and reconstruction rules.
- [A1 audit artifacts](a1-audit-artifacts.md): durable storage contract.
- [B3 decision](b3-decision.md): finalization concurrency evidence.
