# B273 window reference foundation

This local slice adds the schema and authenticated reader approved after the
[initial plan](prism-prepared-window-reference-plan.md). It does **not** enable
compact prepared jobs or finish issue273. Authorization covers B-owned code
only and is not A/C owner approval.

The dependency is open PR313 at `4e366c12518d8ecf795fbe1aa7adfb2ecb48e0b8`,
added to this child branch through merge `bae584e`. Upstream release metadata
and D2 tests arrived with that dependency; this slice adds no release bump.
The API follows [design PR297 at 652438d](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/652438d00376f5eb7edc668e8d41b542bea97eff/docs/prism-coordinator-refactor/window-ref.md).
That newer design's leased-candidate changes remain outside this slice.

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

The bytea column contracts are `qbit_prism_templates(template_sha256,
template_bytes)` and `qbit_prism_balance_snapshots(prior_balances_digest,
balances)`. The latter contains native serialized `CarryForwardBalance` JSON;
its key is `qbit_prism::prior_balances_digest`, **not** the hash of those bytes.
Future writers must authenticate content and compare immutable conflicts in
the same save/repair transaction. Schema immutability alone is not that writer.

## Local qualification

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

1. Obtain A/C agreement and A265's `AUDIT_BUILDER_VERSION`, stored candidate
   inputs, and slim candidate representation. A's `audit::read_range` is
   unchanged. No placeholder builder version or candidate adapter exists here.
2. Switch `StoredPrepared`/`Prepared`, WorkLedger and refresh to references and
   stored policy/signer/hash inputs. Authenticate fetched template bytes before
   decoding, compare payload/reference columns, and preserve exact original
   signed hashes. Define the legacy prepared-row miss at that runtime cutover.
3. Insert/reuse template, balance snapshot and prepared job atomically; extend
   PR313 cold repair to restore all dependencies and the child with immutable
   conflict checks and the child's original absolute expiry. Add transactional,
   bounded job/blob GC under the existing settlement lock and both lock-order
   race tests. This foundation does not change `save_job` or GC.
4. Add shared reader capacity, build permits, bounded singleflight, original
   deadline, and per-waiter authority/expiry checks in callers. The reader has
   no permits or private timeout by design. Release full rebuilt windows before
   resumed jobs enter sessions, using A265's candidate integration.
5. Qualify real 400k refresh plus frontend-A-to-B resume, 500k headroom, a large
   valid template, all refresh JSONB values below 1 MB, and measured refresh WAL
   against the 5 MB target. Large reader tests do not establish those gates.
   Record dedicated asynchronous standby results and per-phase RSS/read counts.

At runtime cutover, drain old outbox work and stop all old frontends before the
coordinated migration/start procedure. This development policy is not a rolling
compatibility guarantee.

Full deep-review remains pending against the eventual integrated change:
medium code-review, thermo, Fable and Codex adversarial lanes, and GitHub/CI/bot
review have not run for this slice. Local implementation review and tests do
not establish those lanes. No push, PR, merge-to-base, deployment, or paid
review capacity was used.
