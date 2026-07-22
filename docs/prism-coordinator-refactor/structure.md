# Final Structure

## Coordinator boundary

`PrismCoordinator` constructs owners, wires explicit ports, exposes the stable
facade used by existing tests/tools, binds listeners, handles signals, and
orders top-level shutdown. It does not own a background worker loop or a large
domain state machine.

| Concern | Owner modules |
| --- | --- |
| configuration and RPC | `coordinator_config`, `rpc` |
| lifecycle and bounded execution | `background_services`, `bounded_executor`, `coordinator_shutdown` |
| payout, reorg, and CTV | `payout_state`, `reorg_reconciler`, `ctv_runtime` |
| templates, bundles, refresh, delivery | `template_artifacts`, `bundle_compiler`, `job_bundle`, `tip_refresh`, `job_delivery` |
| sessions, shares, candidates, finalization | `stratum_session`, `share_submission`, `share_writer`, `block_candidates`, `block_finalization` |
| vardiff | `vardiff_service` |
| audit, health, metrics, HTTP | `audit_artifacts`, `observability`, `progress_health`, `metrics`, `audit_http` |

Owners hold their own mutable state, locks, bounded queues/executors, counters,
and lifecycle. Cross-owner calls use explicit immutable inputs or narrow ports;
there is no broad replacement context object.

## Structural audit

- The coordinator fell from 19,799 lines on the integrated base to 8,900 lines,
  a 55% reduction.
- No coordinator method at or over 200 lines is a domain state machine. The two
  such methods are the 244-line constructor and 202-line delivery-owner factory;
  other methods at or over 100 lines are configuration, listener startup, port
  assembly, or factory wiring.
- Loop-shaped coordinator facade methods are narrow delegates. The only local
  loop is bounded listener-startup retry, not a background worker.
- No leaf service imports the coordinator. `job_build_benchmark.py` is an
  executable composition-level benchmark and intentionally constructs it.
- No PRISM module defines magic `__getattr__` or `__setattr__` forwarding.
- Docker Ruff reports no unused imports. Remaining compatibility exports and
  descriptors have direct in-repository callers and delegate to one owner.

The earlier approximate 3,000-line aspiration is not a release requirement.
The remaining size is an explicit compatibility/composition exception: 522
mostly narrow facade and wiring methods preserve existing private integration
fixtures and public behavior. Moving those methods to mixins would only hide
the composition surface, while deleting demonstrated callers would be a
breaking API/test migration rather than ownership extraction. Future changes
should remove a facade only when its callers can migrate without weakening the
compatibility contract.

## Optional further decomposition

There is still useful follow-up work, but it is separate from domain ownership:

- About 900 lines of `_Coordinator*` port adapters can move to a
  `coordinator_adapters` module. This is a low-risk file-size improvement, not a
  new ownership boundary.
- The constructor plus 35 lazy `_ensure_*` factories can become a typed
  component builder. This would clarify the dependency graph, but it must
  preserve lazy startup, shutdown order, and partial test construction.
- Remaining cohesive orchestration can move to existing owners: live chain and
  template validation, payout/CTV policy construction, synchronous candidate
  credit, idle-bundle cache validation, and progress eligibility assembly.
- The largest reduction requires migrating the many test/tool construction
  sites and then deleting their private compatibility delegates and
  descriptors. Do this as an explicit compatibility cleanup, not with mixins or
  magic forwarding.

Those changes are optional because current domain state already has one owner.
They should be proposed after the review stack lands so they do not obscure the
behavior-preserving extraction.
