# Completed Roadmap

There is no active work order. Completed commits are implementation history,
not gates that must be reconstructed.

| Area | Result |
| --- | --- |
| foundations, configuration, RPC, shutdown, executors | extracted |
| progress, background services, CTV | extracted |
| payout, templates, bundles, refresh scheduler | extracted |
| sessions, delivery, share writer/recovery | extracted |
| `origin/1.x.x` through `b002caa` | complete, including retry pacing (`#71`), delivery-health grace (`#82`), and share hot-path lock isolation (`#83`) |
| A1 audit artifact ownership | complete |
| B1 candidate submission | complete |
| S4 share classification | complete |
| D1 bounded duplicate index | complete |
| V1 vardiff and idle retarget | complete |
| B2 accepted-block finalization phases | complete |
| B3 second finalization lane | `OMIT` ([decision](b3-decision.md)) |
| O2 cached health | complete |
| O3 cached metrics | complete |
| H1 audit/public HTTP | complete |
| X1 reorg, metrics, watchdog, and seam cleanup | complete |

X1 removed unused re-exports and drifting state mirrors, moved the remaining
reorg, metrics, and watchdog domain bodies to owners, and replaced magic
attribute forwarding with explicit ports and test hooks. Compatibility aliases
or delegates remain only where in-repository callers demonstrate the stable
facade; they do not own duplicated mutable state.

The upstream retry work is owned by `template_artifacts`, `tip_refresh`, and
`block_candidates`; the upstream health correction is owned by `job_delivery`,
`observability`, and `metrics`. The upstream hot-path correction is owned by
`stratum_session`, `job_delivery`, `share_submission`, `vardiff_service`,
`block_candidates`, `share_ledger`, and `metrics`: client vardiff state has a
per-client lock, share accounting has a dedicated lock, normal submission uses
one coordinator control snapshot, and candidate retry state remains observable
without making intentional waits look healthy. Coordinator code only wires
those ports and forwards stable compatibility calls.

The three cumulative milestones—durability/submission,
concurrency/finalization, and observability/cleanup—are complete. The final
tree satisfies the [invariants](invariants.md), the structural result is
recorded in [Structure](structure.md), and the full evidence is recorded in
[Validation](validation.md).

The implementation is organized into the review stack in
[Stacked PRs](stacked-prs.md). This document intentionally avoids commit IDs so
restacking cannot make the completion record stale.
