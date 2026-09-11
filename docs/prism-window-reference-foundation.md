# B273 window reference foundation

This local slice adds the schema and authenticated reader approved after the
[initial plan](prism-prepared-window-reference-plan.md). It does **not** enable
compact prepared jobs or finish issue273. Authorization covers B-owned code
only and is not A/C owner approval.

This branch includes current `3.x.x` at
`d39cf621ce25fea4d5a1e86b02cf6c3c66949ce5` additively: `48a5923` includes
PR313's merge `0af13ec`, `c6bce3a` includes PR297's merge `82a543d`, and
`b480ea1` includes PR319's merge `d39cf62`.
PR313 and the #264 design dependency are no longer open blockers. The earlier
dependency updates remain in history. The original PR313 worktree is untouched;
this slice adds no release bump.

The API remains compatible with [PR297's approved bed6ad8 contract](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/bed6ad8888d6bd58ec9fc96fed8b5ba409b63cee/docs/prism-coordinator-refactor/window-ref.md).
The [posted user signoff](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/297#issuecomment-5638877264)
authorizes the narrower lease: revision changes are bridged only while the
balance digest is unchanged; changed balances end the lease immediately.
It does not waive affected-work/replacement-delivery qualification or establish
full miner parity. Final resume/submit and leased-candidate wiring remain
unimplemented here, pending the exact A265 interfaces listed below.

## Implemented contract

- Public `WindowRef`, `ShareRange`, `Window`, `BalanceSource` and `WindowError`
  are exported from `ledger`. Digests serialize as exactly 64 lowercase hex
  characters. Invalid sequence bounds and counts fail before database access.
- `Ledger::read_window(&WindowRef, BalanceSource)` uses one primary
  `REPEATABLE READ READ ONLY` transaction. It reads revision and balances,
  probes endpoints, then reads ascending keyset pages of at most 4096 rows.
  Decode and hash work runs in `spawn_blocking`. Native share JSON is streamed
  into SHA-256 without allocating a serialized copy of the entire window.
- A `BlockingDrop` guard transfers accumulated page state and vectors awaiting
  commit to a blocking thread when the reader is cancelled or fails. A page
  task's abandoned result carries the same guard. This implements the later
  [cancellation note on #273](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/273#issuecomment-5639213901)
  without changing `read_window`'s API. After successful return the caller owns
  the window and its off-thread cleanup obligation.
- Current balances are digest checked; as-issued balances are decoded from
  the immutable snapshot identified by their semantic digest. Digest
  validation uses a bytewise `(order_key, recipient_id, p2mr_program_hex)`
  clone, while each path returns the source vector order unchanged so legacy
  persisted snapshots retain their exact balance ordering. The returned
  revision is current, never an implicit replacement for a job's original
  revision.
- Missing endpoints/count mismatches, changed current balances, missing or
  corrupt as-issued balances, native digest mismatches, SQL failures, and decode
  failures keep their specified error variants. An empty reference never
  queries share history. There is no partial-window result or cache-miss mapping.
- Migration008 creates the exact no-window/empty/range column states,
  lowercase digest checks, immutable template/balance bytea stores, and job
  digest indexes. Updates changing blob keys or contents are rejected; deletion
  remains available to the future coordinated GC. Legacy payloads and issued
  top-level `extranonce1` are untouched.
- The migration runner adds 008 to the landed PR319 membership approach.
  It applies 008 even when 009 is already installed, and 009 when only 008
  is installed. No 006, 007, candidate or audit reader implementation was
  imported. Migration009 is unchanged from base. Existing migration assertions
  verify the full applied version set and exact preservation of every original
  job field, with only 008's seven new null columns added.
- The later B-owned `Ledger::payout_state()` returns a `PayoutState` containing
  current revision and current balance digest from one repeatable-read primary
  snapshot. It retains `payout_revision`'s fatal/read-only guards, reads no share
  history or as-issued blob, and decodes/hashes/drops balances in a blocking
  closure. It has no private deadline or implicit eligibility decision.
  `WindowRef`, `Window`, `WindowError` and `read_window` signatures are unchanged.
- B's tests use the shared `qbit_prism_test_gate` introduced by merged PR322.
  All 14 regular B database cases are in `test/prism-gated-tests.txt`; explicit
  scale qualifications use the required-input gate. Test fixture
  modulo expressions follow the current pinned toolchain's Clippy rules.

The bytea column contracts are `qbit_prism_templates(template_sha256,
template_bytes)` and `qbit_prism_balance_snapshots(prior_balances_digest,
balances)`. The latter contains native serialized `CarryForwardBalance` JSON;
its key is `qbit_prism::prior_balances_digest`, **not** the hash of those bytes.
Future writers must authenticate content and compare immutable conflicts in
the same save/repair transaction. Schema immutability alone is not that writer.

## Post-signoff qualification

The coherent-state follow-up was tested on the additive PR313 dependency
`91294e7`, with Rust 1.89.0 and disposable PostgreSQL 16.14 discovered through
`pg_config --bindir`. Workspace all-target compilation, formatting and
`git diff --check` passed. The regular database command below passed 223 tests:

| Suite | Passed |
| --- | --- |
| Server library, including PR313's nine restored D2 coordinator cases | 142 |
| `ledger_postgres` / `migration_rollback` | 29 / 1 |
| `readiness_rpc` / `stratum_protocol` | 5 / 22 |
| `window_read_oracle` | 8 |
| `window_reference` | 16; the two explicit qualification tests remained ignored |

The four new `window_reference::payout_state` database cases passed both alone
and in the regression run:

- Revision-only change preserves the balance identity; changed balances alter
  it, without requiring share history or the as-issued store.
- A test-only database view gates the revision result before the balance
  statement starts. Another transaction commits a new revision and balance set
  during that wait; the reader returns the old coherent pair, and the next read
  sees the new pair. Blocking the balance statement itself is insufficient to
  distinguish transaction isolation from that statement's own snapshot.
- Empty balances remain valid; fatal state, read-only configuration, missing
  current-balance storage and numeric decode overflow remain truthful errors.
- SQLSTATE 57014 and cancellation during the balance query release the only
  connection, which a subsequent state read successfully reuses.

The coherence test was checked with a temporary local mutation replacing
`REPEATABLE READ` with `READ COMMITTED`. It failed with the expected
`payout eligibility mixed two MVCC snapshots` error. The mutation was restored
in a `finally` cleanup; the four focused cases were then rerun on the real code.

The first compile caught the new test fixture's `u64` balance field; it was
corrected to the existing `CarryForwardBalance.balance_sats: i128` contract
before the passing runs. Existing as-issued signed-equality and miner assertions
were not changed. The new tests exercise the helper, not runtime lease
revocation or candidate dispatch. No credit-policy success is inferred from
these regression results.

This follow-up did not rerun the ignored 400k/500k reader, actual external 009
SQL test, or issued-dependency suite; their earlier evidence remains below.
Full refresh/resume, large-template/WAL, async standby, live-qbit, GC/enqueue
races and full deep-review remain unrun for this integration. No production
access, push or public approval was performed.

## Current-base draft checkpoint

At `a6e3482`, the base was `3.x.x` at `82a543d`; Rust is now the repository's
installed pin 1.98.1, with disposable PostgreSQL 16.14 located through
`pg_config --bindir`.
The base merge retained upstream versions of every conflicted file; no
foundation changes existed in those files. No release metadata was changed.

The selected real-database regression run passed 225 tests: 144 library,
29 ledger, 1 migration rollback, 5 readiness, 22 Stratum, 8 window oracle and
16 window reference. Three explicit qualifications were ignored in that run.
All 13 regular window-reference database cases recorded `executed`; the shared
manifest checker passed against the B-only expected/actual subset. That scoped
check is not a claim that the entire CI execution manifest has been qualified.
Workspace all-target compilation and Clippy with `-D warnings` passed. Existing
miner assertions are unchanged. The two new guard unit tests prove cancelled
payloads drop on a different thread and successful handoff preserves ownership.

The full 400k/500k reader and issued-dependency evidence below remains
historical; it was not rerun for this draft checkpoint. The landed 009 SQL is
now covered by the regular combined-base suite below. Runtime
refresh/resume, candidate-aware GC, large-template/WAL, async standby and
cutover qualification remain incomplete. Full deep-review is deferred until
after draft publication; this document does not report missing lanes as green.

### Combined base with landed PR319

After the additive `b480ea1` merge of `3.x.x` at `d39cf62`, the regular suite
passed **236 tests**: 144 library, 37 ledger, 1 migration rollback, 6 readiness,
1 Stratum admission, 22 Stratum protocol, 8 window oracle and 17 window
reference. Three qualifications remained ignored: the admission capacity
measurement and B's two explicit scale measurements. All 14 regular B database
cases recorded execution and passed the scoped manifest check.
Formatting, workspace/all-target Clippy with `-D warnings`, the direct gate
environment-read check (135 Rust files), and `git diff --check` passed after
the combined-base test adjustments. The earlier all-target `cargo check`
passed before this merge; all-target Clippy compiled the final combined base.

The landed 009 SQL now runs in a regular B test in both orders with 008, using
the actual membership runner and restart. The first combined run exposed
PR319's whole-row comparison expecting no new columns; its expected result now
includes exactly 008's seven null columns and still compares every original
field, including payload/extranonce/expiry. Both existing migration tests now
require `[2, 3, 4, 5, 8, 9]`; no assertion was reduced to a maximum version or
subset. Migration009 and the session/candidate implementation are unchanged.

```sh
PRISM_TEST_GATE_MANIFEST=/tmp/window-reference-gates.txt \
  bash test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server \
  --lib --test window_reference --test readiness_rpc --test stratum_protocol \
  --test stratum_admission_postgres --test window_read_oracle \
  --test ledger_postgres --test migration_rollback
```

### Late cancellation at 400k

The explicit `cancellation::cancelling_400k_read_after_96_pages_measures_runtime_stall`
test ran at `a6e3482` on disposable PostgreSQL 16.14 and Rust 1.98.1. It loads the
production-shaped 400,000-row fixture, streams 96 complete pages (393,216
shares), then gates the next page's projected share ID with a transaction-level
advisory lock. A single-thread Tokio runtime aborts and joins the reader while
a 1 ms timer measures its scheduling gaps, then verifies the sole reader
connection can be reused.

For this test-only connection, bitmap/sequence scans and sorting are disabled
and `EXPLAIN` must contain only streaming nodes. The ordered view and this
check prevent a sort from evaluating the gate before the first page has been
returned. An earlier unconstrained sample was rejected for that reason; it is
not evidence of late cancellation. This is a cleanup measurement, not a
production query-plan or read-throughput benchmark.

The validated sample reported 27 microseconds from abort to joined cancellation,
a maximum 5,418 microsecond gap for the 1 ms ticker, and successful connection
recovery. Test wall time was 29.00 s including fixture setup/read/cleanup;
`/usr/bin/time -l` around the command reported 31.97 s and 380,977,152 bytes
maximum RSS. That RSS is command-level (including build/fixture effects), not
per-phase reader residency. No new timing threshold, full 30 s resume result,
WAL result, or 500k cancellation qualification is claimed.

```sh
PRISM_TEST_GATE_MANIFEST=/tmp/window-cancel-gate.txt \
  bash test/prism-native-tests.sh cargo-args --locked -p qbit-prism-server \
  --test window_reference \
  cancellation::cancelling_400k_read_after_96_pages_measures_runtime_stall \
  -- --ignored --exact --nocapture
```

## Initial foundation qualification (d7526fc)

Rust 1.89.0, PostgreSQL 16.14 discovered through `pg_config --bindir`, macOS.
Every database run used the existing disposable-cluster runner; no production
database was accessed. The default Cargo 1.84 remains unsuitable for the
locked dependencies, so no lockfile or global toolchain change was used.

| Check | Result |
| --- | --- |
| Workspace all-target `cargo check --locked` | Passed |
| `cargo fmt --all -- --check`, `git diff --check` | Passed |
| Server library tests | 133 passed, including 4 new reference unit tests |
| `ledger_postgres` | 29 passed |
| `migration_rollback` | 1 passed |
| `readiness_rpc` | 5 passed, assertions unchanged |
| `stratum_protocol` | 22 passed, assertions unchanged |
| `window_read_oracle` | 8 passed |
| `window_reference` | 12 passed: 9 database cases and 3 existing fixture checks; 2 explicit qualifications separately gated |
| Ignored `issued_job_dependency` suite | All 7 ran and passed |
| Actual PR319 migration009, 008 then 009 and 009 then 008 | Explicit test ran and passed |
| Explicit production-shaped reader test | 400k and 500k passed in both debug and optimized builds |

The PostgreSQL cases cover native row equality across page boundaries and
filtered sequence gaps; signed audit equality with original balances after a
revision change; empty-range reads without a share table; malformed stored
balances and numeric decode overflow; missing history, count and digest errors;
SQLSTATE 57014 timeout and cancellation cleanup with one connection; concurrent
revision/balance/history changes across a blocked read; all 64 partial-null
column states; immutable blobs; old-schema upgrade and repeated application.

The first broader run found the existing migration test's expected maximum
version 5. After replacing it with the exact applied set `[2, 3, 4, 5, 8]`,
the full selected regression run passed. No miner assertion was relaxed.

Reproduce the regular database run with:

```sh
RUSTUP_TOOLCHAIN=1.89.0 bash test/prism-native-tests.sh cargo-args \
  --locked -p qbit-prism-server --lib --test window_reference \
  --test readiness_rpc --test stratum_protocol --test window_read_oracle \
  --test ledger_postgres --test migration_rollback
```

The external 009 fixture was read from PR319 commit
`3a7ba469c9318791d5fb366359ca406397f3065b`. It was not copied into the repository.
The historical reproduction at `d7526fc` set `PRISM_TEST_MIGRATION_009`
to that reviewed SQL file and ran (current HEAD uses the landed regular test):

```sh
RUSTUP_TOOLCHAIN=1.89.0 bash test/prism-native-tests.sh cargo-args \
  --locked -p qbit-prism-server --test window_reference \
  reviewed_009_sql_coexists_with_008_in_both_orders -- --ignored --exact
```

This proves SQL coexistence under the migration lock and the real 008 runner,
not compilation or runtime integration of the entire PR319 branch.

### Large reader measurement

The separate `production_shaped_reader_at_400k_and_500k` test uses the landed
production-shaped fixture and checks every reconstructed native row. It times
only the awaited reader after fixture load and expected-digest construction.
Returned-share counts exclude revision/balance queries and endpoint probes;
page query counts are not separately instrumented.

| Build | Shares / returned share rows | Reader wall time |
| --- | --- | --- |
| Debug | 400,000 / 400,000 | 32.692 s |
| Debug | 500,000 / 500,000 | 21.078 s |
| Optimized (`--release`) | 400,000 / 400,000 | 16.144 s |
| Optimized (`--release`) | 500,000 / 500,000 | 24.453 s |

These are single local samples, not latency percentiles or an end-to-end
deadline result. Debug 400k exceeded 30 seconds; even the optimized results
leave material read/build/permit budgeting to validate in integration. No
latency scaling conclusion follows from the non-monotonic debug measurements.
The native digests matched between builds:

- 400k: `d9a8ea1fd49b977193b218eb6ce8f7dac36c046df4145e54cb066d43b6322e75`
- 500k: `91b976807eb19516ac6d723153297d2b42cd871b0ed047501f0a13fd74cc8791`

The debug command was wrapped in macOS `/usr/bin/time -l`: 194.26 seconds
total wall time and 635,486,208 bytes maximum resident set size. That is a
command-level measurement including fixture creation, the expected vectors,
and reconstructed vectors, not per-phase reader RSS or retained production
memory. The optimized test took 128.29 seconds excluding its 78-second build;
optimized RSS was not measured. Neither run measured WAL or standby replay.

```sh
RUSTUP_TOOLCHAIN=1.89.0 bash test/prism-native-tests.sh cargo-args \
  --locked --release -p qbit-prism-server --test window_reference \
  production_shaped_reader_at_400k_and_500k -- --ignored --exact --nocapture
```

## Remaining integration hooks

1. Obtain A265's concrete interfaces below and A/C integration review. User
   signoff on the narrowed contract has already been given; it is not a blocker.
   A's `audit::read_range` is unchanged. No placeholder builder version or
   candidate adapter exists here.
2. Switch `StoredPrepared`/`Prepared`, WorkLedger and refresh to references and
   stored policy/signer/hash inputs. Authenticate fetched template bytes before
   decoding, compare payload/reference columns, and preserve exact original
   signed hashes. Wire coherent `payout_state` through WorkLedger/SubmitLedger
   admission, comparing against the issued digest with publication/lease/expiry
   rechecks after waits and the unchanged transaction revision fence. State-read
   failure remains an error; known mismatch is ineligible. Define the legacy
   prepared-row miss at that runtime cutover.
3. Insert/reuse template, balance snapshot and prepared job atomically; extend
   PR313 cold repair to restore all dependencies and the child with immutable
   conflict checks and the child's original absolute expiry. Add transactional,
   job/blob GC under `SETTLEMENT_LOCK` then `ORDER_LOCK`: expire at most 4096
   jobs beyond a qualified post-expiry grace, prune their unreferenced templates, then sweep balances unreferenced by
   any job or nonterminal leased candidate. The balance sweep also runs when
   zero jobs expire, so candidate-retained orphans disappear after terminal
   completion. See the plan's GC/repair/enqueue race matrix. This foundation
   does not change `save_job` or runtime GC.
4. Add shared reader capacity, build permits, bounded singleflight, original
   deadline, and per-waiter authority/expiry checks in callers. The reader has
   no permits or private timeout by design. Release full rebuilt windows inside
   the blocking closure before resumed jobs enter sessions, using A265's
   candidate integration. Preserve the original synthetic bootstrap share.
5. Qualify real 400k refresh plus frontend-A-to-B resume, 500k headroom, a large
   valid template, all refresh JSONB values below 1 MB, and measured refresh WAL
   against the 5 MB target. Large reader tests do not establish those gates.
   Record dedicated asynchronous standby results, per-phase RSS/read counts,
   retained local generations, affected work, and replacement delivery latency.

### Exact A265 dependencies at this checkpoint

Inspection of `3.x.x` at `d39cf62` and A265's open
[PR325 at ff74abb](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/325)
(`ff74abb0da65f15ddcd25689328a075f7ab2a1b4`) still finds
`Candidate.bundle: AuditBundle`, an optional suffix, no candidate window
columns, no `AUDIT_BUILDER_VERSION`, and no
`codec::witness_merkle_leaves_from_block`. PR325 supplies canonical import/read
work and explicitly leaves enqueue for A's next PR; it is not a missing B
import prerequisite. #264 is closed and PR297 is merged, so neither is a design
blocker. The following remain #265 implementation handoffs, not unanswered
design objections or a request for another user signoff:

| A-owned interface | Needed by B |
| --- | --- |
| `qbit_prism::AUDIT_BUILDER_VERSION: u16` and versioned frozen vectors | Persist/check the actual builder version before reconstructing prepared work; no B-local substitute constant. |
| Slim candidate fields: `window`, `found_block`, `payout_policy`, nested optional `ctv { direct_floor_sats, settlement_config, fanout_fee_policy }`, `audit_builder_version`, `signer_keys { manifest_key_hex, ledger_key_hex }`, `bootstrap_share`, `leased`, required `coinbase_suffix_hex`, original `payout_revision`, `job_id`, `deferred_share`, block identity/digest and bytes | Agree concrete shared Rust types/exports and construction API. B must retain these original inputs in the resumed job after releasing its full window/body; the old owned-bundle submit constructor cannot do that. |
| Migration007's six outbox window columns, especially indexed `window_prior_balances_sha256`; authenticated `leased` flag and nonterminal-state predicate | GC must join actual typed references. Current states are `pending`, `submitted`, `abandoned`; A must confirm the retention predicate with 007, including claimed/retrying pending rows. Do not invent a SQL `leased` column when the record only specifies a JSON flag. |
| Enqueue under `ORDER_LOCK`, receiving as-issued balances for digest-checked re-insertion and probing the share prefix | `406b273` answers the GC-first ordering case: reinsert balances before committing the reference, and enqueue/alert even if the share prefix is unexpectedly absent. A's concrete Rust method/input and immutable-conflict behavior are still needed; B does not invent them. |
| Leased candidate dispatch: submit stored bytes first; rebuild with `AsIssued` and land before any terminal result, whether active or inactive; null inactive results and changed-balance landing failures remain recoverable | Required by the landed follow-up record while preserving issued revision. B's current independent candidate fences remain unchanged until A integration. |
| Parts-based claim/landing and `codec::witness_merkle_leaves_from_block(&[u8]) -> Result<Vec<String>>` | Candidate reconstructs from immutable original inputs without forcing B to keep an owned `AuditBundle` or a full resumed window. No edits to A's audit/read-range path here. |

B's `qbit_prism_templates(template_sha256, template_bytes)` and
`qbit_prism_balance_snapshots(prior_balances_digest, balances)` shapes already
exist in 008. A/C must review their encoding/conflict and retention integration;
that review does not require B to invent candidate types or copy migration007.

The open source-schema [PR321](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/321)
at `798860c1a79a05a536a9512208f1960220ff4ef9` supplies migration006 and startup
capabilities, not the missing 007/candidate shape. Its runner will need the
minimal 008 membership integration when combined; it is not copied here.
Its latest delta strengthens source-schema sequence/partial-001 detection and
adds gate records; it introduces none of the missing candidate interfaces.

### b3e8ba9 delta and decisions still open

The newer head preserves the exact synthetic `JobContext.bootstrap_share` for
empty-window local jobs when converting to body-only storage. It also corrects
memory accounting: each retained local generation keeps its full window, even
after another generation is published. Both are recorded integration facts;
the current full-bundle representation is unchanged. A retention cap, eviction
policy, and acceptable total RSS need separate qualification/review rather than
being inferred from the bed6ad8 signoff. A265 owns the additional measurement of
SQLx's contiguous whole-body bind copy on the runtime during first landing.
The landed `406b273` follow-up also retains as-issued balances in the slim
resumed job for enqueue repair and in an empty-window singleflight entry for
each bootstrap build. Physical deletion waits a B273-selected expiry grace;
that grace must be qualified above share-path latency and cannot extend job
eligibility. No grace value or retention/eviction policy is selected here.

At runtime cutover, drain old outbox work and stop all old frontends before the
coordinated migration/start procedure. This development policy is not a rolling
compatibility guarantee.

Full deep-review remains pending against the eventual integrated change:
medium code-review, thermo, Fable and Codex adversarial lanes, and GitHub/CI/bot
review have not run for this slice. Local implementation review and tests do
not establish those lanes. The foundation is intended for draft publication
before that review, with all incomplete runtime/qualification items explicit.
Draft visibility does not authorize ready-for-review, merge-to-base or deployment.
