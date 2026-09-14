# qbit-mining-bootstrap 3.0.0 — unreleased recovery contract

**UNRELEASED. NOT PRODUCTION-READY.** These partial release notes record the
migration decision required by #287. #291 must complete the release notes and
rehearse the cutover and isolated restore on production-sized history, including
restore and `import-audits` timings, before approving a production rollout.
See [development-line status](release-notes-3.x.x-development-line.md).

## Migration and recovery (D5)

Native schema migration is one-way. No down-migrations are provided. Prefer
forward repair or a compatible native image retaining the latest durable state.
Both [migration](../docs/prism-rust-migration.md#recovery-and-rollback) and
[ledger operations](../docs/prism-ledger-ops.md#one-way-migration-and-isolated-restore-reconciliation)
runbooks contain the numbered reconciliation procedure and exact queries.

> Before the first native share is acknowledged, restore the complete
> pre-migration database and artifact backup and restart the pinned old image
> in isolation from the migrated database. After the first native share is
> acknowledged, restoring an older database loses those accepted records.
> Any rollback that discards acknowledged history requires an explicit
> accounting reconciliation and recovery decision; it is not an ordinary
> image rollback. Keep every native frontend stopped while performing an
> isolated restore/recovery operation against its replacement database.

Native `import-audits` is the canonical backfill for recoverable legacy bodies,
including those without a canonical sidecar. It verifies the original audit
digest, trusted ledger key and recorded coinbase. Rehearse it on an isolated
source restore before migrating the source. Production `self-check` fails when
audits lack shared stored bodies or canonical availability; native immutable
snapshots provide canonical reconstruction without a duplicate full byte copy.
