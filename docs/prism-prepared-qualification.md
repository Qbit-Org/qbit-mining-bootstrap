# Prepared-work qualification preparation

This is preparation for #273, not refresh/resume acceptance evidence. At
`2d1baa9e165092a0d37d4bb367c0d3795d1086f1`, the coordinator still persists and
decodes inline `StoredPrepared`. The additive compact storage API is not its
runtime path. No 400k or 500k database run, WAL budget, retention guarantee,
or canonical A-to-B rebuild result is established by this preparation.

## Runnable preparation

```sh
cargo +1.98.1 test --locked -p qbit-prism-server \
  --test prepared_qualification_contract
```

The target reuses [WindowPlan](../crates/qbit-prism-server/tests/support/window_fixture.rs)
from #264. Both 400,000 and 500,000 divide its existing window weight exactly;
the tests check production-shaped serialized rows at sequence-width boundaries,
the midpoint, and the final row without allocating either full window. The
size band applies to the sampled average, matching the fixture's average-byte
contract rather than imposing that band on each row.
They test the failure behavior of the reusable
[qualification assertions](../crates/qbit-prism-server/tests/support/prepared_work_assertions.rs).
Synthetic byte strings and byte counts in these unit tests are guard inputs,
never measured production evidence. There are no ignored or database-gated
acceptance tests here and no new manifest IDs.

## Share rows versus the literal key

#273 literally requires the stored payload to have no `shares` key. The existing
stored format does contain that key: `CompactPrepared.window` embeds
`WindowRef.shares: Option<ShareRange>`. This preparation explicitly adopts a
**no-materialized-share-rows** guard; it does not establish unchanged literal-key
acceptance or close that checkbox. Final qualification and reconciliation of
the literal wording remain open on #273.

`assert_no_materialized_shares` rejects every `shares` key except at the exact
root `window.shares` path. That exception requires the complete, existing
`WindowRef` serialization: integer `anchor_ms`, 64-character lowercase-hex
`prior_balances_digest`, and `shares` as either null (empty window) or an object
with exactly `first_share_seq`, `last_share_seq`, `share_count`, and
`snapshot_sha256`. Sequence bounds must be positive, ordered and fit SQL bigint;
the positive count may not exceed the inclusive span (gaps are valid), and the
snapshot digest must be 64-character lowercase hex. Missing, extra or malformed
metadata fields fail. Arrays at `window.shares`, hidden arrays, and any `shares`
key elsewhere fail, including nulls and metadata-shaped objects.

The guard reuses `WindowRef` deserialization and requires exact serialization
round-trip equality, since the existing deserializer otherwise ignores unknown
fields and defaults a missing optional range. It checks payload structure, not
digest authenticity or the full prepared-record contract. No stored field,
format version, runtime serializer or upstream schema changes here.

The serializer trace is `Ledger::save_compact_prepared` to
[`encode_record`](../crates/qbit-prism-server/src/ledger/jobs/prepared.rs), which
calls `serde_json::to_value(record)` and adds `original_expires_at_ms` without
renaming or removing the range. The pure tests build actual `CompactPrepared`
records from empty and nonempty `Snapshot` references and use that serialization
plus expiry envelope. They exercise both valid shapes and legacy/hidden-array
negative controls without claiming a database write or runtime activation.

## Runtime binding for the integration owner

Reuse [the existing fake node and coordinator configuration](../crates/qbit-prism-server/tests/support/fake_qbitd.rs)
and the two-coordinator pattern in
[candidate_window_qualification.rs](../crates/qbit-prism-server/tests/candidate_window_qualification.rs).
Use a dedicated disposable loopback PostgreSQL primary, serial execution,
and a fresh schema for each size. A fresh schema alone does not isolate
server-wide WAL or database-wide advisory locks. Start no background submit,
refresh, or collector loops during the measurement bracket.

1. Create frontends A and B against the same schema and fake node, with
   distinct instance IDs. Load `WindowPlan::new(n).load(...)` once, before
   measurement, and use `verify_round_trip` on first/middle/last rows.
2. Capture the insert LSN immediately before and after awaiting A's actual
   `Coordinator::refresh_once`. A failed refresh is a failed operation; do
   not seed a substitute job, omit the failure, or report zero WAL. Record
   the published snapshot length and require exactly `n`.
3. Observe every JSONB column written by that refresh using the catalog/write
   attribution approach in [jsonb_ceiling_gate.rs](../crates/qbit-prism-server/tests/jsonb_ceiling_gate.rs).
   Its `Inventory` is file-private: sharing it requires a separate coordinated
   extraction, not copying a second inventory or weakening the all-pipeline
   ratchet. Pass the largest uncompressed value to `assert_refresh_measurements`;
   compressed `pg_column_size` alone does not prove the whole-value bound.
   Inspect the actual persisted prepared payload with `assert_no_materialized_shares`.
   Record its range-metadata exception explicitly; this is not proof of the
   literal no-`shares`-key criterion.
4. Refresh B outside A's WAL bracket so B has its own publication authority.
   Build and persist an issued job on A through `MiningBackend::build_job` and
   `persist_issued_job`, then call `MiningBackend::resume_job` on B with the
   same worker and ID. Require `Some`, not a cache miss. Compare the real
   canonical audit bytes with `assert_canonical_equality`; also compare job
   ID, original worker, target, difficulty, extranonces and version mask.
   Preserve the issued absolute deadline through the existing caller timeout.
   The future body representation must use `canonical_audit_bundle_bytes_from_parts`
   with the reconstructed shares, not native-window or legacy-window digests.
5. Repeat A's unchanged refresh separately and require cached prepared identity
   and unchanged stored payload. Exercise a revision change between snapshot
   construction and save through the existing coordinator test boundary: the
   save must fail without publishing replacement work. A cached run is not a
   substitute for the measured non-cached refresh budget.
   Cached no-write reuse may legitimately observe zero WAL; report it separately
   with unchanged prepared identity/payload and no observed writes. Do not pass
   it to `assert_refresh_measurements`, which requires a logged prepared write
   and a positive WAL delta. Neither missing measurements nor a zero delta
   alongside an observed logged write can qualify the non-cached refresh.
6. Reuse the issued-dependency/compact-storage race tests for retention: both
   GC-before-repair and repair-before-GC must preserve referenced template,
   balance and share data through the original issued deadline. A pending
   candidate remains a reference after its claim lease expires; after its
   terminal transition, a zero-job sweep must reclaim unreferenced blobs.
   Include unsupported-format and malformed-record errors without rewriting
   their expected outcomes. These tests belong to the runtime/repair owners;
   the preparation target does not simulate them.

Activation requires replacing coordinator refresh's inline serialization and
resume's inline decode with the compact storage/reader APIs, preserving final
authority and deadline checks, and integrating issued dependency repair.
The private revision-interleaving and JSONB observation seams also need owner
wiring. Coordinate any newly executable database test ID with the manifest
owner; never reuse an existing ID or register this preparation as acceptance.

## Measurement record

The [#330 WAL helper](../scripts/measure_postgres_wal.py) establishes explicit
operation outcome, server-wide attribution and missing-measurement handling.
It brackets SQL through separate `psql` calls, so it cannot itself bracket a
Rust coordinator refresh. Use the same attribution discipline, with
`pg_current_wal_insert_lsn()` around the awaited Rust operation and
`pg_wal_lsn_diff(after,before)` for integer bytes. Do not attribute fixture
loading, frontend initialization, B's refresh or mining to A's refresh.

Record the exact source/base SHAs, PostgreSQL version, share count, observed
share rows, separate metadata/probe/page query counts, JSONB bytes, WAL bytes,
refresh/resume wall time and outcome. Record `fsync`, `full_page_writes`,
`wal_compression`, checkpoint placement and first-seen versus reused template
and balance keys. Missing measurements remain unavailable, not zero or pass.
Both limits are strict decimal byte bounds: under 1,000,000 JSONB bytes and
under 5,000,000 WAL bytes for the isolated refresh.

Run 400k regression and 500k headroom serially, then a large valid template
case. Include its first insert in WAL; do not hide it behind a warmed key.
Report actual OS-supported RSS separately from unsupported/unavailable RSS.
Replication and socket delivery need their own qualification: the reused
fake-node harness is two in-process coordinators, not physical hosts or a
socket-level miner test.
