# A1 Audit Artifact Contract

A1 is complete. `lab/prism/audit_artifacts.py` owns audit
filesystem authority; `bundle_compiler.py` owns compiler/subprocess work; the
ledger owns database authorization and accepted-block transactions; the
coordinator only composes them and retains narrow compatibility calls.

## Directory and file authority

- Pin audit root and evidence-parent directory descriptors with no-follow
  semantics. Perform owned reads, creates, replacement, fsync, scans, and
  removals relative to those descriptors.
- A path replacement revokes the store until the original inode is restored or
  `reconfigure()` adopts a new authority pair atomically.
- Parse only exact owned names. Malformed lookalikes, symlinks, nonregular
  entries, traversal, and out-of-root URIs never grant read, pin, repair, or
  deletion authority.
- Track candidate inode identity. Cleanup removes only the exact file created
  by the current attempt, including builder-create-then-raise and swapped-path
  failures.

## Compiler and verifier boundary

J1 receives a duplicated A1 directory descriptor and reserved candidate name,
creates the canonical output relative to it, and transfers the still-open exact
inode to A1. Compatibility builders use canonicalization fallback and are not
reported as exact compiler output.

Verification uses an unlinked read-only descriptor snapshot, a bounded process
group, timeout, and independent stdout/stderr limits. Trust source, writer key,
literal digest/bytes, and normalized report form the verification identity. A
retry never reuses a prior attempt's success.

## Publication and replay

- PostgreSQL assigns a unique durable `audit_publication_sequence` at the
  serialized confirmation boundary. Exact confirmation replay and later
  inactive/reactivation transitions reuse it; reactivation does not create an
  unpublished ordinal.
- Sequence, not height, hash ordering, process order, or mtime, chooses current
  evidence. Hash, height, coinbase, digest, and verification identity remain
  integrity fields.
- Allocation/publication lock order is payout balance mutation then the A1
  publication guard. The guard uses one pinned internal lock inode with an
  in-process reentrant lock plus cross-process `flock`.
- Reload disk evidence while guarded before replay, repair, publish, or prune.
  An already-valid exact pair may replay behind a later floor; damaged state
  repairs only at the fresh durable-row floor.
- Legacy evidence grants no ordering or pin authority after restart until it is
  re-proved against exact confirmed ledger state and adopted at its durable
  sequence.

Mutable publication installs the envelope first and evidence second, fsyncing
file and required parent boundaries. Only then does in-memory current evidence
advance. A failure preserves the previous valid pair. Exact replay is
byte/inode stable except for documented global observational counters.

## Bodies, segments, and retention

Inline and external audit bodies expose the same canonical bytes, digest, and
response metadata in memory and PostgreSQL modes. Share-slot merges are
serialized, lossless for disjoint updates, idempotent for identical overlap,
and preserving on conflict. Audit bodies and share segments are durable and not
reference-blind garbage-collected.

Retention runs after successful publication, is best effort, and revalidates
both directory authorities and current/reserved identities at each removal.
Retention 0/1, tied mtimes, concurrent stores/processes, and prune failure may
not delete or regress current evidence. The internal lock file is never exposed
as an artifact.

## Completion evidence

A1 was reconciled with the integrated owners and validated through its direct
artifact/API/ledger/candidate/metrics suites, PostgreSQL parity/migration/process
helpers, Rust audit CLI tests, Docker compile/lint, and diff/temporary-artifact
hygiene. Exact hashes, literal authorization, and same-reviewer verdicts were
intentionally not part of completion.
