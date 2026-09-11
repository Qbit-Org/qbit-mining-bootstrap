# B273 window reference foundation

This local slice adds the schema and authenticated reader approved after the
[initial plan](prism-prepared-window-reference-plan.md). It does **not** enable
compact prepared jobs or finish issue273. Authorization covers B-owned code
only and is not A/C owner approval.

The dependency is open PR313 at `ec888ecfe131f4f9deebbafda9926b299392cac0`,
added to this child branch through merge `91294e7`, after the earlier additive
update `bae584e`. The new dependency commit restores D2 coordinator test
membership. Its original worktree is untouched; this slice adds no release bump.

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
- Current balances are digest checked; as-issued balances are decoded from
  the immutable snapshot identified by their semantic digest. Both paths sort
  bytewise by `(order_key, recipient_id, p2mr_program_hex)`, as does the shared
  snapshot balance helper. The returned revision is current, never an implicit
  replacement for a job's original revision.
- Missing endpoints/count mismatches, changed current balances, missing or
  corrupt as-issued balances, native digest mismatches, SQL failures, and decode
  failures keep their specified error variants. An empty reference never
  queries share history. There is no partial-window result or cache-miss mapping.
- Migration008 creates the exact no-window/empty/range column states,
  lowercase digest checks, immutable template/balance bytea stores, and job
  digest indexes. Updates changing blob keys or contents are rejected; deletion
  remains available to the future coordinated GC. Legacy payloads and issued
  top-level `extranonce1` are untouched.
- The migration runner checks version membership, matching PR319's minimal
  approach. It applies 008 even when a higher version is already recorded.
  No 006, 007, 009, candidate, audit reader, or deployment implementation was
  imported. The existing legacy migration test now verifies the entire applied
  version set instead of a hard-coded maximum of 5.
- The later B-owned `Ledger::payout_state()` returns a `PayoutState` containing
  current revision and current balance digest from one repeatable-read primary
  snapshot. It retains `payout_revision`'s fatal/read-only guards, reads no share
  history or as-issued blob, and decodes/hashes/drops balances in a blocking
  closure. It has no private deadline or implicit eligibility decision.
  `WindowRef`, `Window`, `WindowError` and `read_window` signatures are unchanged.

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
Set `PRISM_TEST_MIGRATION_009` to that reviewed SQL file and run:

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
   jobs, prune their unreferenced templates, then sweep balances unreferenced by
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

Inspection of this combined base still finds `Candidate.bundle: AuditBundle`,
an optional suffix, no candidate window columns, no `AUDIT_BUILDER_VERSION`,
and no `codec::witness_merkle_leaves_from_block`. No open A265 implementation PR
was visible in the repository's open-PR list at this check. The needed handoffs
are concrete; B is not waiting for a second user signoff:

| A-owned interface | Needed by B |
| --- | --- |
| `qbit_prism::AUDIT_BUILDER_VERSION: u16` and versioned frozen vectors | Persist/check the actual builder version before reconstructing prepared work; no B-local substitute constant. |
| Slim candidate fields: `window`, `found_block`, `payout_policy`, nested optional `ctv { direct_floor_sats, settlement_config, fanout_fee_policy }`, `audit_builder_version`, `signer_keys { manifest_key_hex, ledger_key_hex }`, `bootstrap_share`, `leased`, required `coinbase_suffix_hex`, original `payout_revision`, `job_id`, `deferred_share`, block identity/digest and bytes | Agree concrete shared Rust types/exports and construction API. B must retain these original inputs in the resumed job after releasing its full window/body; the old owned-bundle submit constructor cannot do that. |
| Migration007's six outbox window columns, especially indexed `window_prior_balances_sha256`; authenticated `leased` flag and nonterminal-state predicate | GC must join actual typed references. Current states are `pending`, `submitted`, `abandoned`; A must confirm the retention predicate with 007, including claimed/retrying pending rows. Do not invent a SQL `leased` column when the record only specifies a JSON flag. |
| Enqueue under `ORDER_LOCK`, with balance-reference existence/recovery contract | A must define the outcome if GC deletes an unreferenced balance row before enqueue acquires its lock. No dangling outbox reference may commit; B does not implement A's retry/repair API. |
| Leased candidate dispatch: submit stored bytes before terminal supersession/rebuild; active audit uses `AsIssued`; changed-balance landing failure remains recoverable | Required to implement the approved candidate behavior while preserving issued revision. B's current independent candidate fences remain unchanged until A integration. |
| Parts-based claim/landing and `codec::witness_merkle_leaves_from_block(&[u8]) -> Result<Vec<String>>` | Candidate reconstructs from immutable original inputs without forcing B to keep an owned `AuditBundle` or a full resumed window. No edits to A's audit/read-range path here. |

B's `qbit_prism_templates(template_sha256, template_bytes)` and
`qbit_prism_balance_snapshots(prior_balances_digest, balances)` shapes already
exist in 008. A/C must review their encoding/conflict and retention integration;
that review does not require B to invent candidate types or copy migration007.

### b3e8ba9 delta and decisions still open

The newer head preserves the exact synthetic `JobContext.bootstrap_share` for
empty-window local jobs when converting to body-only storage. It also corrects
memory accounting: each retained local generation keeps its full window, even
after another generation is published. Both are recorded integration facts;
the current full-bundle representation is unchanged. A retention cap, eviction
policy, and acceptable total RSS need separate qualification/review rather than
being inferred from the bed6ad8 signoff. A265 owns the additional measurement of
SQLx's contiguous whole-body bind copy on the runtime during first landing.

At runtime cutover, drain old outbox work and stop all old frontends before the
coordinated migration/start procedure. This development policy is not a rolling
compatibility guarantee.

Full deep-review remains pending against the eventual integrated change:
medium code-review, thermo, Fable and Codex adversarial lanes, and GitHub/CI/bot
review have not run for this slice. Local implementation review and tests do
not establish those lanes. No push, PR, merge-to-base, deployment, or paid
review capacity was used.
