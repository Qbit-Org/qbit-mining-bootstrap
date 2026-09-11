# Prepared work window reference: implementation checkpoint

This is the current local integration plan, updated after the user's
[contract signoff pinned to bed6ad8](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/297#issuecomment-5638877264)
and [issue273 confirmation](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/273#issuecomment-5638878822).
The B-owned schema/reader implementation and measured results are recorded in
[the foundation report](prism-window-reference-foundation.md). Final runtime
integration still requires A265's actual candidate interfaces.

Issue [#273](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/273),
workstream B. The approved replacement lease bridges a revision change only
while `prior_balances_digest` remains unchanged. Changed balances end that
lease immediately. Broader protection belongs with B8/#289. Approval is for
implementation, not full miner-behavior parity or a cutover waiver: qualification
must measure affected work and verify prompt replacement-job delivery.

## Verified dependency state

Checked 2026-09-11 against GitHub and current `3.x.x` at
`d39cf621ce25fea4d5a1e86b02cf6c3c66949ce5`. This child includes that base
additively through `48a5923` (merged PR313), `c6bce3a` (merged PR297),
and `b480ea1` (merged PR319):

| Dependency | Observed state | Consequence |
| --- | --- | --- |
| [PR313](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/313) | Merged as `0af13ec9b405731ce4eb93fb473352c94972a730`, included | No remaining PR313 dependency; its original worktree is untouched. |
| [Issue264](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/264) | Closed by the design merge | No longer a design-merge blocker. B's exact reader/types are implemented locally. |
| [PR297](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/297) | Head `406b27344f53d9d84b68004adcae05b2f0f46f88` merged as `82a543d`; user signoff pins `bed6ad8` | The unchanged-balances restriction remains. Record later retention/candidate details without inferring a new cutover waiver or choosing an unmeasured retention grace. |
| [Alex's response](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/297#issuecomment-5637165179) | All four B objections answered; the user approved the narrowed contract | Implementation is not waiting for that signoff. |
| Borrowing builders and verification | Present in `crates/qbit-prism/src/lib.rs`; `AuditBundleBody`, four borrowed builders, parts verification/canonical serialization, public `prior_balances_digest` | Reuse these APIs. `AuditBundleBody.reward_manifest.shares` remains large and must never enter prepared JSONB. |
| Shared window reader | `d7526fc` implements the exact types, `read_window` and migration008 under local B authorization | Foundation remains usable; no A-owned `audit::read_range` edit. Local authorization does not imply A/C implementation review. |
| Builder version | `AUDIT_BUILDER_VERSION` absent | Latest design assigns its introduction and frozen-vector versioning to A's #265. B must not invent a second constant. |
| Candidate representation | `Candidate.bundle` and submit still require owned `AuditBundle` | Latest design's slim resumed jobs cannot preserve candidate reconstruction without #265 or explicit coordinated integration. |
| [A265 import PR325](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/325) | Open at `ff74abb0da65f15ddcd25689328a075f7ab2a1b4` | This supplies canonical import/read changes, not the candidate fields, builder constant, witness parser or 007. Import is not a missing B prerequisite. |
| [Source-schema PR321](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/321) | Open at `798860c1a79a05a536a9512208f1960220ff4ef9` | Adds 006/startup capabilities, not 007 or a slim candidate. Coordinate its runner with 008 when integrated; do not copy A-owned code here. |
| Migrations / [PR319](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/319) | Merged as `d39cf621ce25fea4d5a1e86b02cf6c3c66949ce5`; runner applies 002 through 005, 008 and 009 by membership | 009 is included unchanged from current base. No 006 or 007 is copied. Candidate-aware GC needs A's 007 fields and retention predicate. |

The landed B276 runner and migration009 are now included through current
`3.x.x`, without a whole open-branch cherry-pick. The only production runner
addition is 008 membership. Migration009 still creates
`qbit_prism_jobs_extranonce1_expiry_idx` on
`(lower(payload->>'extranonce1'), expires_at)` and leaves existing jobs intact.
The regular B migration test now uses the landed 009 SQL, installs either
predecessor under `MIGRATION_LOCK`, and runs/restarts the real combined runner.
Both migration tests require the exact set `[2, 3, 4, 5, 8, 9]`. Existing job
fields must match exactly, with only 008's seven new null columns added.
Do not duplicate or edit 009. Coordinate the final 006/007 manifest with A/C.

No candidate, landing, import, deployment, metrics, CI, or incremental-window
change is authorized by this plan.

### Post-signoff delta: b3e8ba9

The delta does not change the balance restriction, window API, or GC contract.
It explicitly preserves `JobContext.bootstrap_share: Option<AcceptedShare>`
for local empty-window jobs after replacing the full bundle with a body. The
original synthetic share must reach `Candidate.bootstrap_share` verbatim; a
`CountedShare` lacks required fields. This is an A/B handoff requirement for
the body-only conversion, not authorization to edit A's implementation here.

Locally retained jobs keep each generation's `Prepared`, including its window,
until Stratum releases them. Memory accounting must include all retained local
generations as well as bounded concurrent rebuilds; the single-current-window
claim is invalid. A generation cap, eviction change, and acceptable total RSS
remain qualification/contract decisions, not an inferred approval to alter
retained miner work. The later record also names SQLx's whole-body bind copy on
landing; A265 owns its 400k stall measurement and any follow-up decision.

### Landed follow-ups through 406b273

The empty-window singleflight entry keeps as-issued balances until every
waiter's bootstrap build finishes. The slim resumed job also retains those
balances for leased enqueue repair. A's enqueue re-inserts the digest-checked
balance snapshot inside its `ORDER_LOCK` transaction; that previously open
ordering question is now answered in the record and should not be asked again.
The concrete Rust enqueue input/exports remain unavailable in PR325.

The record now calls for an expiry grace before physical job deletion, and for
expired-but-not-deleted jobs to retain D6's share floor. B must set and qualify
that grace above measured share-path latency without extending the child's
absolute eligibility deadline. The exact grace value is not specified or
implemented here. A still enqueues and alerts on an unexpectedly missing share
prefix so the block reaches the node; B does not change that candidate contract.
Leased dispatch submits first, then lands the audit before any terminal result,
even for an inactive block; null inactive results stay recoverable. These are
A-owned implementation requirements, not missing design answers.

The additional cancellation note on #273 is implemented independently in B's
reader: accumulated state and completed vectors awaiting transaction commit
have a guard that hands destruction to a blocking thread on cancellation/error.
This also covers a completed blocking page whose awaiting future disappears.
Successful return transfers the vectors to the caller, which retains its own
off-thread cleanup obligation. The foundation report records its tests.

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
locally implemented interfaces awaiting A/C integration review.

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

Use PR297's approved signature and types, implemented in B-owned `ledger/window.rs`:

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
`BalanceSource::{Current, AsIssued}` separates ordinary candidate claims from
original published economics (resume and leased-candidate audit).
`Window.payout_revision` is the
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

### Coherent eligibility input (B-owned)

`Ledger::payout_state() -> Result<PayoutState, WindowError>` reads
`{ payout_revision, prior_balances_digest }` from one primary repeatable-read
snapshot. It checks the same fatal/read-only guards as `payout_revision`, reads
only current balance rows, and computes the existing semantic digest off the
runtime. It neither reads share history nor consults the as-issued store.
It sets no private timeout, acquires no build permit, and returns observations,
not an admission decision. This helper is independently implemented and tested;
its existence does not imply that runtime callers use the narrowed policy yet.

At integration, B's WorkLedger/SubmitLedger adapters must supply this coherent
pair, not independently awaited revision and digest lookups. The issued digest
comes from the authenticated `Prepared.window`, computed once at refresh, never
from current balances or a per-submit re-hash of the window. A lease requires
the exact published identity, a live lease and equal balance digests. A known
balance mismatch yields an ineligible resume (`Ok(None)`) or the corresponding
stale-work submit result; a failed state read remains backend-unavailable.
`read_window(AsIssued)` continues to reconstruct original economics even after
balances move: changing it to a current-balance check would break audit recovery.

Recheck the publication/lease after the state read and all later waits, and
revalidate the coherent pair after rebuild/bootstrap before returning work.
Save/repair and append retain their final transactional current-revision
checks; an observation is not authority to commit after that revision changes.
Reuse the original operation deadline across these steps. Do not add a full
window read or unmeasured current-balance scan to every ordinary share: any
revision-keyed reuse of the small pair needs explicit freshness/fence tests.

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

GC acquires `SETTLEMENT_LOCK` **then `ORDER_LOCK`**, before deleting any rows,
and runs three separate statements in that same transaction:

1. Delete at most 4096 jobs beyond the selected post-expiry retention grace,
   keeping the outer expiry/grace recheck, and
   return their template digests.
2. Delete templates among those digests only when no surviving job references
   them.
3. Sweep balance snapshots with no surviving job reference **and no nonterminal
   leased-candidate reference**. This sweep runs even when no jobs were deleted
   and is not restricted to the expired batch's balance digests.

The later sweep is required when a candidate outlives its job, then becomes
terminal: no remaining job can nominate that balance digest for cleanup.
The expired-job batch is bounded; the balance sweep ranges over retained
snapshot metadata, not shares. Measure its cardinality/time rather than claim
that all cleanup is bounded by 4096 rows. Do not add an arbitrary sweep cap
without a progress rule that eventually revisits every orphan.

The settlement lock serializes save/repair with GC; the order lock serializes
leased-candidate enqueue with GC. No path may reverse this order or acquire
settlement after order. If repair goes first, GC observes its references; if GC
goes first, repair atomically restores the original blobs/prepared/child. A
modifying CTE with an outer `NOT EXISTS` sees the wrong statement snapshot, so
it cannot replace the separate delete statements. Claim expiry/retry must not
remove a nonterminal candidate's protection. No share-ledger pruning is added.

Production GC remains gated on A265's typed outbox digest column/index and
agreed authenticated `leased`/nonterminal predicate. The current inline outbox
has no such columns; do not infer a reference from its old bundled payload or
silently collect when a required schema/API is absent. Per landed `406b273`,
A's enqueue re-inserts the digest-checked as-issued balances under `ORDER_LOCK`
before committing its reference, including when GC went first. A probes the
share prefix but still enqueues with an alert if it is missing. The exact Rust
input and immutable-conflict implementation remain A-owned. B cannot promise
preservation based on an unlocked earlier read or invent a different outcome.

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
published R0 work can survive the bounded R1 replacement lease only while
the current and issued balance digests match; arbitrary older same-parent work
cannot inherit it. Broader protection across changed balances remains #289.
Before returning, verify original issued
expiry again. Missing/expired/incompatible work is `Ok(None)`; corruption,
database errors, reconstruction mismatches, and deadline exhaustion retain
the existing truthful backend-error path.

## Qualification matrix

| Area | Required evidence |
| --- | --- |
| Window shape | Empty/range round trips; native and legacy digest distinction; uppercase, malformed, oversized sequence, bad-count, and partial-column rejection; payload/column equality. |
| Reader | Exact ascending range with excluded/rejected/after-anchor rows; incomplete endpoints/interior; digest mismatch; corrupt/missing as-issued balance bytes; database/decode errors; stable RR snapshot; empty range reads no ledger rows. |
| Economics | Signed frozen fixture and A-to-B rebuild equality for normal, bootstrap, CTV, prior-only recipient, and changed-current-balances cases; preserve original R0 audit hashes and candidate fences. |
| Admission | Same-digest revision swap preserves the published lease; changed digest rejects immediately. Coherent revision/digest under concurrent balance changes; authority/revision/parent/lease and absolute expiry move at each permit, shared entry, DB read, blocking build, bootstrap, and final return; arbitrary R0 cannot borrow a lease; failed reads stay errors. |
| Capacity | One build worker cannot deadlock; cancelled blocking builder retains permit until closure exit; shared reader pool leaves two connections; leader cancellation/failure and follower cancellation clean up; waiters cannot exchange identity/economics/expiry. |
| Durability | Both GC/repair orders, both GC/enqueue orders under settlement then order; no dangling children; immutable conflicts; original absolute expiry; expired-job batch bound; pending leased candidate retains balances through claim/retry expiry, then a later zero-job sweep removes its orphan after terminal completion; unrelated live jobs/candidates remain protected. |
| Migration | Fresh and pre-008 rows under real runner; exact three-state CHECK; legacy inline prepared miss; new malformed payload error; 008/009 coexistence and restart; missing numbered migration must not be skipped by a higher version. |
| Regression | Existing readiness, Stratum, PR313 issued-dependency and coordinator miner suites, unchanged assertions; workspace all-target compilation on combined base. |
| Scale | Real PostgreSQL 400k refresh then A-to-B resume with exact signed equality; 500k headroom; large valid template; all refresh JSONB values below 1 MB; row-read counts, RSS including retained local generations, wall time, measured WAL, affected-work count and prompt replacement delivery after balance changes. |

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
schema. The completed foundation may be published as a draft before full
deep-review, with incomplete runtime and qualification items explicit. The
eventual integrated change still requires the full review battery and accurate
reporting of unavailable lanes. Draft visibility is not approval to mark it
ready or merge; no VERSION or CHANGELOG bump is part of this work.

## Initial execution evidence (historical)

The following results predate `d7526fc` and the user's contract signoff. Current
implementation and tests are in the foundation report; full B273 refresh/resume
acceptance and A integration remain incomplete.

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
The later foundation implements 008 and the reader, including reader-only
400k/500k measurements and actual 008/009 coexistence checks. Compact runtime
prepared work, full refresh/resume, large-template/WAL/standby qualification,
combined A/B/C integration, and deep-review remain pending; no remote review or
bot result is implied by these baseline tests.
