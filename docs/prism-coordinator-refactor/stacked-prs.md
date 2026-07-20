# Stacked PR Review Plan

The refactor is published as nine bottom-up PRs. Every published stack commit
must be GPG-signed. PR 1 targets `1.x.x`; each later PR targets the branch
immediately above it so reviewers see only that slice.

| PR | Branch | Scope |
| --- | --- | --- |
| [#73](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/73) | `prism-test-shards` | mechanical test sharding and shared fixtures; no production behavior |
| [#74](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/74) | `prism-coordinator-core-owners` | configuration/RPC, lifecycle, payout, templates, bundles, refresh, sessions, delivery, share writing, same-tip template reuse, retry pacing, observed coordinator locking, and per-client vardiff lock wiring |
| [#75](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/75) | `prism-audit-artifact-owner` | PostgreSQL publication order, audit filesystem authority, compilation/verification, replay, retention, and migration/process gates |
| [#76](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/76) | `prism-block-candidate-submission` | durable candidate replay, attempt marking, bounded retry heartbeats/backoff state, finalize-only replay pacing, terminalization, and coordinator ports |
| [#77](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/77) | `prism-share-submission` | single-snapshot share classification, bounded duplicate tracking, and dedicated share-accounting synchronization |
| [#78](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/78) | `prism-vardiff-finalization` | per-client vardiff/idle-retarget synchronization, named accepted-block finalization phases, and the documented B3 decision |
| [#79](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/79) | `prism-observability-http` | cached health, separate first-job/coverage-loss grace, complete cached metrics, and audit/public HTTP facade |
| [#80](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/80) | `prism-final-ownership-cleanup` | reorg reconciler, synchronized metrics snapshots including hot-lock/candidate gauges, watchdog owner, compatibility cleanup, and temporary-seam removal |
| [#81](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/81) | `prism-refactor-documentation` | final roadmap, ownership audit, validation evidence, stack guide, and ledger migration operations |

Each PR targets the branch immediately above it; PR 1 targets `1.x.x`. Put the
stack position, base branch, dependency, focused validation, and any operator
impact in every PR body. PR 3 must call out the required existing-database
migration and maintenance-window lock; PR 9 carries the durable operator guide.

Address review feedback from the bottom of the stack upward. Amend each fix
into its owning signed commit, then rebase and sign every descendant so each PR
continues to contain one intentional slice. Push rewritten branches together
with explicit force-with-lease expectations; stop if any remote tip changed.

Validate focused behavior per PR. Run cumulative PRISM discovery after PR 2,
PostgreSQL/Rust after PR 3, the image build after PR 6, and the complete final
matrix on PR 9. Merge bottom-up. If the repository squash-merges a lower PR,
rebase the remaining branches onto the new `1.x.x` tip and force-push only with
lease.
