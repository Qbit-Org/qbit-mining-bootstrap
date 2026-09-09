# qbit-mining-bootstrap 2.0.2 Release Notes

Release date: pending

## Candidate persistence and replay

Candidate persistence and recovery use bounded codec and transport work instead
of serializing or decoding a complete payout window in the coordinator. The
candidate identity, timestamp normalization, atomic share/outbox publication and
writer-session fences remain part of the compatibility contract.

Canonical audit finalization uses replayable file-backed views and streamed
artifact publication. Replay membership uses an exact PostgreSQL subset check
fed through bounded COPY writes with native backpressure. Temporary tables,
spools, file indexes and candidate body references have explicit retirement
paths; cyclic GC is not used as a recovery mechanism.

The regression gates cover PostgreSQL and psql, cancellation, deadlines, retries,
writer-session fencing and an actual mined-block crash/restart. Separate large
codec experiments distinguish encoding, decoding, lazy materialization, native
buffering, reference-count destruction and cyclic GC. These experiments explain
possible blocking mechanisms; they do not prove the instruction responsible for
the production exits reported in #255.

## Upgrade and rollback

Follow the schema, capacity, rollout and rollback procedure in
[the candidate storage guide](../docs/prism-candidate-storage.md). Qualify a
compatible reader before enabling the new body representation. Keep the additive
schema during rollback. Once a new-format pending candidate exists, rolling back
to a reader that cannot hydrate it is unsafe; use the compatible reader with
new-format writes disabled, or a separately verified drain/conversion procedure.

Coordinate deployment with #254's retention and self-check work. Production
acceptance requires at least two continuous hours in one process epoch, the
specified risky workloads, stable ownership after drain and unchanged raw lease
failure counters. This release does not increase lease thresholds, suppress
alerts or change mining and payout rules.

The supported qbit mainnet binary pin remains v1.0.0 at
`7ebcddb622d6e639041f005a189b048ec2a221fe`.
