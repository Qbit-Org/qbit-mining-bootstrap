# Documentation Map

This directory contains operator docs, API contracts, and durable supporting
references. For public-facing navigation, prefer the short path below.

## Public Entry Points

- [../README.md](../README.md): repository overview and quick starts.
- [../PRISM.md](../PRISM.md): PRISM overview, runbook, reward model, audit
  model, and CTV fanout explanation.
- [../doc/mining.md](../doc/mining.md): qbit mining operator guide.

## Public PRISM References

- [public-dashboard-api/README.md](public-dashboard-api/README.md): dashboard
  API ownership boundary and endpoint conventions.
- [public-dashboard-api-v1.openapi.yaml](public-dashboard-api-v1.openapi.yaml):
  OpenAPI contract for `/public/v1`.
- [prism-storage-sizing.md](prism-storage-sizing.md): storage, VM sizing,
  artifact retention, and monitoring guidance.
- [prism-rejections.md](prism-rejections.md): stable PRISM rejection reason IDs
  for Stratum errors, metrics, and dashboards.
- [router-integration-notes.md](router-integration-notes.md): router/hash
  aggregator guidance for the qbit ckpool comparison path.

## Supporting Reference

This is useful for reviewers and operators who need implementation detail:

- [prism-ledger-ops.md](prism-ledger-ops.md): formal ledger invariants,
  writer-lease behavior, compaction contract, and readiness probes.

## Public-Site Guidance

If these docs are published outside the repository, use the public entry points
and public PRISM references for top-level navigation. Link supporting references
from detailed sections when readers need the underlying design or test evidence.
