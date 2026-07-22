# PRISM Coordinator Refactor

Status: **complete** and organized as a nine-PR review stack. No required
roadmap item remains.

The completed tree is integrated with `origin/1.x.x` at `b002caa`. It preserves
the base branch's public hashrate, refresh/livelock, initial/reconnect delivery,
queue-reclamation, and latest-tip priority fixes in the extracted owners. It
also ports the latest retry pacing (`#71`) and delivery-health grace (`#82`)
behavior, plus share hot-path lock isolation (`#83`), into the template,
refresh, candidate, submission, vardiff, delivery, ledger, observability, and
metrics owners. The former exact-hash, literal-authorization,
mandatory-reviewer, and per-slice full-suite workflow is retired.

## Result

`lab/prism/prism_coordinator.py` is now the construction, startup, signal,
stable-facade, and top-level shutdown root. Domain state machines, background
loops, queues, locks, cached observability, persistence, HTTP, and mining work
live in dedicated owners. See [Structure](structure.md) for the boundary and
the documented size exception.

B3 is intentionally omitted: the available evidence does not justify a second
finalization lane or its additional durable handoff. See the
[decision record](b3-decision.md).

The final runnable validation matrix passes. Docker-dependent PostgreSQL,
container lint/build, and both live Stratum targets are `UNAVAILABLE` in the
current environment because the OrbStack daemon is stopped; `qbitd` is also
absent. These are missing-environment evidence, not passes or product
failures. Exact results are in [Validation](validation.md).

## Reference documents

- [Invariants](invariants.md): release behavior that must remain true.
- [Roadmap](roadmap.md): completed slices and decisions.
- [Structure](structure.md): final ownership map and structural audit.
- [Validation](validation.md): risk-based cadence and final evidence.
- [Stacked PRs](stacked-prs.md): publication order and reconstruction rules.
- [A1 audit artifacts](a1-audit-artifacts.md): durable storage contract.
- [B3 decision](b3-decision.md): finalization concurrency evidence.
