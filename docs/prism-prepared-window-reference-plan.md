# Prepared work window reference: implementation checkpoint

This document records the initial dependency checkpoint. The later locally
authorized schema/reader slice and its measured results are recorded in
[the foundation report](prism-window-reference-foundation.md); final runtime
integration still requires A265 and coordinated ownership decisions.

Issue [#273](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/273),
workstream B. This is a proposed implementation and qualification plan, not a
claim that the implementation or its performance gates have passed.

## Verified dependency state

Checked 2026-09-11 against GitHub and child-worktree base
`106a42886ca27baf796469f3f44a09f78043a561`:

| Dependency | Observed state | Consequence |
| --- | --- | --- |
| [PR313](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/313) | Open, head equals this worktree base | Keep the PR313 dependency explicit; do not alter its original worktree. |
| [Issue264](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/264) | Open | Its design-merge acceptance is not complete. |
| [PR297](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/297) | Open, head `6f59a52584a1dea96ab4be5344e117b0a38ca47d` | Use this revision, which is newer than `cc459df`. |
| [Alex's response](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/297#issuecomment-5637165179) | All four B objections answered | Do not treat the earlier B objection comment as evidence that these answers are missing. |
| Borrowing builders and verification | Present in `crates/qbit-prism/src/lib.rs`; `AuditBundleBody`, four borrowed builders, parts verification/canonical serialization, public `prior_balances_digest` | Reuse these APIs. `AuditBundleBody.reward_manifest.shares` remains large and must never enter prepared JSONB. |
| Shared window reader | `ledger/window.rs` exists, but no `WindowRef`, `ShareRange`, `Window`, `WindowError`, or `Ledger::read_window` exists at the base | The latest design assigns these types and reader to B; confirm that assignment with A/C before implementation. |
| Builder version | `AUDIT_BUILDER_VERSION` absent | Latest design assigns its introduction and frozen-vector versioning to A's #265. B must not invent a second constant. |
| Candidate representation | `Candidate.bundle` and submit still require owned `AuditBundle` | Latest design's slim resumed jobs cannot preserve candidate reconstruction without #265 or explicit coordinated integration. |
| Migrations | Base runner applies 002 through 005 using `max(version)`; 006, 007, 008 and 009 absent at this base | Confirm runner ownership/order with C and B276. A newer numbered migration must not silently suppress a later-arriving lower number. |

Read-only inspection of B276 branch `djh58/prism-b4-wrap-safe-sequence` at
`502a95d4bc5f864bd290866e5655e3a3724c63fd` found its runner already checks the
set of applied versions individually. Its `009_wrap_safe_sessions.sql` creates
`qbit_prism_jobs_extranonce1_expiry_idx` on
`(lower(payload->>'extranonce1'), expires_at)` and leaves existing jobs intact.
Coordinate the 008 insertion with that implementation; do not duplicate or
edit 009. This inspection did not integrate or test that branch.

No candidate, landing, import, deployment, metrics, CI, or incremental-window
change is authorized by this plan.

## Proposed schema and storage contract

Migration `008_prepared_window_reference.sql` belongs to B273. It adds:

| Table | Columns / constraints |
| --- | --- |
| `qbit_prism_jobs` | `window_anchor_ms bigint`, `window_prior_balances_sha256 text`, `window_first_share_seq bigint`, `window_last_share_seq bigint`, `window_share_count bigint`, `window_snapshot_sha256 text`, `template_sha256 text` |
| `qbit_prism_templates` | `template_sha256 text PRIMARY KEY`, `template_bytes bytea NOT NULL` |
| `qbit_prism_balance_snapshots` | `prior_balances_digest text PRIMARY KEY`, `balances bytea NOT NULL` |

All digest text is exactly 64 lowercase hex characters. The six window columns
use PR297's explicit `num_nulls`/`num_nonnulls` CHECK, accepting exactly:

1. All six NULL: issued child rows and legacy inline prepared rows.
2. Anchor and balance digest present, four range columns NULL: empty window.
3. All six present: first >= 1, last >= first, and 1 <= count <= last-first+1.

The reference in the payload and the SQL columns must agree exactly. The
template digest is present only on new prepared rows; malformed partial state
is a decode error, not a legacy miss. Index both the job template digest and
balance digest for bounded GC reference checks. Keep B276's issued
`extranonce1` at the same top-level JSON path and preserve its expression index.

Template bytes are the original serialized template, hashed before storage;
their retrieval verifies the digest before decoding. Balance bytes contain the
as-issued vector in the bytewise order used by `prior_balances_digest`
(`order_key`, `recipient_id`, `p2mr_program_hex`). Refresh and reconstruction
use the same ordering. The balance key authenticates the semantic sorted set;
it is not a SHA-256 of those stored bytes. Insertion checks equal existing
bytes after `ON CONFLICT DO NOTHING`; a conflicting immutable row is an error.
Reject updates to either store's digest or content in the database; explicit
GC deletion remains allowed. Validate the insertion digest before publishing
the dependent prepared record, and validate it again on reconstruction.
Bytea keeps both external stores out of the whole-refresh JSONB size limit,
including a large recipient set. These column names and byte encoding are
proposed interfaces for A/C agreement.

`StoredPrepared` contains only the window reference, original `anchor_ms`,
`share_seq` and `payout_revision`, template digest, parent identity,
fingerprint, generation, original coinbase suffix, complete payout/CTV/fee
inputs, builder version, signer public keys, and original audit hashes.
It contains no snapshot, bundle, share array, prior-balance array, or template.
The optional `window.shares` value is a small range object, never an array.

`Prepared` holds immutable repair inputs and the shared local snapshot/body.
Builds borrow `Snapshot.shares`, using the landed parts APIs. The design now
also requires each resumed job to release both `Snapshot.shares` and
`reward_manifest.shares` after constructing its coinbase. This requires the
coordinated candidate representation listed above; silently dropping shares
while the current submit path clones an owned bundle is not a valid adapter.

## Shared window API

Use PR297's proposed signature and types in B-owned `ledger/window.rs`:

```rust
pub async fn read_window(
    &self,
    window: &WindowRef,
    balances: BalanceSource,
) -> Result<Window, WindowError>;
```

`WindowRef` carries `anchor_ms`, `prior_balances_digest: [u8; 32]`, and
`Option<ShareRange>`. The range contains first/last sequence, count, and the
native snapshot digest. Digest serde accepts only lowercase 64-hex strings.
`BalanceSource::{Current, AsIssued}` separates A's current claim economics
from B's original published economics. `Window.payout_revision` is the
current transaction's fence; a resumed snapshot retains its stored original
revision. Candidate admission continues using its distinct fences.

The reader uses one primary `REPEATABLE READ READ ONLY` transaction. Read the
revision and verified balance source first, return directly for an empty
range, probe endpoints, then fetch ascending keyset pages of at most 4096
rows using the existing accepted/sequence/anchor predicate. Check count and
stream SHA-256 of native `AcceptedShare` JSON array bytes. This is the existing
audit snapshot digest; it is different from `PayoutWindow::canonical_digest_hex`
and from the digest of counted-share outputs.

Row mapping, balance decoding, and hashing run in the reader's own
`spawn_blocking` hand-offs. Do not move or change A-owned `audit::read_range`
without agreement. Its current implementation is private and unpaged, so
merely calling it cannot meet this API's paging contract.

Preserve typed `Incomplete`, `PriorBalancesChanged`,
`BalanceSnapshotMissing`, `SnapshotDigestMismatch`, `Database`, and `Decode`
errors. The as-issued source never substitutes current balances and never
reports a corrupt or absent balance blob as a missing job.

## Save, repair, and collection

Prepared save inserts/reuses template and balance blobs and inserts the job
in one transaction under existing `SETTLEMENT_LOCK`, with the current-revision
and configuration-fingerprint fences. Immutable conflict checks cover blobs,
payload, reference columns, parent, and original revision.

Extend `PreparedDependency` and its repair input to carry the external blob
identities and exact immutable bytes. The hot issued-save path reads compact
dependency metadata. If any dependency is missing, it rolls back and lets
the coordinator prepare repair inputs outside transaction locks. The cold
retry restores template, balances, prepared record, and child atomically.
Original prepared economics remain R0 even when admission succeeds under R1.
The child's absolute expiry is computed once and never renewed by repair,
lock waits, cancellation, or reconnect. Preserve PR313's dependency headroom,
post-wait authority checks, and immutable conflicts.

GC takes `SETTLEMENT_LOCK` and deletes at most 4096 expired jobs, retaining
the outer expiry recheck. Capture removed rows' template/balance digests, then
in a separate statement delete only those blobs that no surviving job
references. A modifying CTE with an outer `NOT EXISTS` sees the wrong
statement snapshot for this cleanup. Save and repair use the same lock order;
if GC goes first they restore missing immutable blobs, and if save goes first
GC sees renewed/referenced dependencies. No share-ledger pruning is added.

## Resume admission, capacity, and cancellation

The existing Stratum `initial_job_timeout_seconds` wrapper remains the one
deadline, normally 30 seconds. It includes job/dependency reads, admission,
build/read permits, singleflight waiting, decode, reconstruction, and
per-miner bootstrap. Do not add an inner 25-second timeout or reset a deadline
at a phase boundary.

Rebuild concurrency uses `build_slots`; window reads additionally use
`clamp(database_max_connections - 2, 1, build_workers)` capacity shared with
A's reads. The blocking closure owns its build permit until actual exit, even
if the async waiter is cancelled. Avoid nested acquisition by calling the
borrowed builders directly under the held permit.

A bounded in-flight map keyed by immutable prepared identity shares only the
snapshot/body reconstruction. Each waiter retains its worker, target,
difficulty, extranonce, version mask, absolute expiry, and authority checks.
Empty windows coalesce only the empty snapshot/balances; each miner builds
its own synthetic bootstrap share with stored policy inputs. No completed
entry becomes an unbounded window cache. Define cancellation and error
cleanup so abandoned entries cannot retain windows, waiters, or capacity.

Recheck authority after every admission-relevant async wait. Eligible original
published R0 work can survive the bounded R1 replacement lease; arbitrary older
same-parent work cannot inherit it. Before returning, verify original issued
expiry again. Missing/expired/incompatible work is `Ok(None)`; corruption,
database errors, reconstruction mismatches, and deadline exhaustion retain
the existing truthful backend-error path.

## Qualification matrix

| Area | Required evidence |
| --- | --- |
| Window shape | Empty/range round trips; native and legacy digest distinction; uppercase, malformed, oversized sequence, bad-count, and partial-column rejection; payload/column equality. |
| Reader | Exact ascending range with excluded/rejected/after-anchor rows; incomplete endpoints/interior; digest mismatch; corrupt/missing as-issued balance bytes; database/decode errors; stable RR snapshot; empty range reads no ledger rows. |
| Economics | Signed frozen fixture and A-to-B rebuild equality for normal, bootstrap, CTV, prior-only recipient, and changed-current-balances cases; preserve original R0 audit hashes and candidate fences. |
| Admission | Authority/revision/parent/lease and absolute expiry move while waiting at each permit, shared entry, DB read, blocking build, bootstrap, and final return; arbitrary R0 does not gain another job's lease. |
| Capacity | One build worker cannot deadlock; cancelled blocking builder retains permit until closure exit; shared reader pool leaves two connections; leader cancellation/failure and follower cancellation clean up; waiters cannot exchange identity/economics/expiry. |
| Durability | Both GC/repair lock orders; no dangling children; concurrent identical repair; template/balance/prepared/child immutable conflicts; original absolute expiry after repair; bounded batch and referenced blob retention. |
| Migration | Fresh and pre-008 rows under real runner; exact three-state CHECK; legacy inline prepared miss; new malformed payload error; 008/009 coexistence and restart; missing numbered migration must not be skipped by a higher version. |
| Regression | Existing readiness, Stratum, PR313 issued-dependency and coordinator miner suites, unchanged assertions; workspace all-target compilation on combined base. |
| Scale | Real PostgreSQL 400k refresh then A-to-B resume with exact signed equality; 500k headroom; large valid template; all refresh JSONB values below 1 MB; row-read counts, RSS, wall time, and measured WAL. |

Use the landed `tests/support/window_fixture.rs` production-shaped fixture,
which accepts both 400k and 500k. Add a B-specific refresh/resume test so
unrelated candidate/import JSONB limitations do not masquerade as B results.
Do not weaken the existing all-pipeline ratchet to make B green.

For WAL, load the fixture before measurement, ensure no concurrent writers on
the disposable primary, record `pg_current_wal_insert_lsn()` immediately before
and after the awaited refresh, and calculate `pg_wal_lsn_diff(after,before)`.
Record PostgreSQL version, fsync/full-page-write and WAL-compression settings,
checkpoint placement, first-seen versus reused template/balance keys, and
whether asynchronous standby replay is included (it must not be called
commit latency). Run the decided one-dedicated-asynchronous-standby shape
separately for replication qualification. The target is under 5 MB per refresh;
a first-seen large valid template can itself challenge that budget and must
be reported, never excluded without explanation.

Count actual share rows returned by the reader, separately from metadata,
balance/template reads, endpoint probes, and page query count. Report the
400k and 500k cold read separately from shared followers. Measure process RSS
with an OS-supported sampler; the landed harness's Linux `/proc` RSS path is
not evidence on macOS. Report wall time per refresh and per A-to-B resume,
retained-memory behavior after the final waiter, and timeout outcomes.

## Upgrade and review boundaries

This development line uses a stop-all migration: drain the pre-007 outbox,
stop every old frontend, apply the coordinated ordered migrations, then start
one new frontend before the rest. Legacy inline prepared rows are cache misses
and expire normally. This is not a rolling-upgrade compatibility promise.

This is high-stakes work because it changes payout reconstruction and stored
schema. Later qualification requires the full deep-review battery and accurate
reporting of unavailable lanes. No push or PR is authorized before coordinator
review of the plan and qualified diff; no merge, VERSION, or CHANGELOG change.

## Execution evidence

Implementation is pending the coordinator's A/C ownership and prerequisite
decisions. No B273 acceptance gate has passed yet.

- Default `cargo check --workspace --all-targets --locked`: stopped before
  compilation; Cargo 1.84 cannot parse dependency `base64ct 1.8.3` requiring
  edition 2024. No dependency or lockfile change was made.
- `cargo +1.89.0 check --workspace --all-targets --locked`: passed on the
  unchanged PR313 base.
- PostgreSQL discovered through `pg_config --bindir`:
  `/opt/homebrew/Cellar/postgresql@16/16.14/bin` (PostgreSQL 16.14).
- Pinned qbitd available at `~/bin/qbit-v1.0.0/bin/qbitd`; presence does not
  establish that live-node tests ran.
- Baseline tests on unchanged PR313 code, with Rust 1.89.0:

| Command | Result |
| --- | --- |
| `RUSTUP_TOOLCHAIN=1.89.0 PRISM_TEST_PG_BIN_DIR=/opt/homebrew/Cellar/postgresql@16/16.14/bin bash test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server --test readiness_rpc --test stratum_protocol` | 5 readiness tests and 22 Stratum protocol tests passed; none ignored. The runner created and removed its own disposable PostgreSQL cluster. |
| `RUSTUP_TOOLCHAIN=1.89.0 PRISM_TEST_PG_BIN_DIR=/opt/homebrew/Cellar/postgresql@16/16.14/bin bash test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server --test issued_job_dependency -- --ignored --test-threads=2` | All 7 explicitly gated PostgreSQL dependency tests ran and passed; none skipped. |
| `cargo +1.89.0 test --locked -p qbit-prism-server --lib coordinator::miner_tests` | All 52 selected tests passed; 77 unrelated library tests filtered out. |
| `cargo +1.89.0 test --locked -p qbit-prism --test audit_parts` | All 7 tests passed, including byte-for-byte owning/borrowed equivalence and borrowed-window verification. |

These are prerequisite/baseline results, not B273 implementation acceptance.
No 008 schema, reference reader, or compact prepared representation has been
implemented. The 400k/500k refresh/resume, large-template bound, WAL/RSS/read-row
measurements, asynchronous standby, 008/009 coexistence, combined A/B/C compile,
and live-qbit tests have not run. No deep-review lane has run against an
implementation diff; no remote review or bot result is implied by these tests.
