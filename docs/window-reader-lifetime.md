# Window reader admission lifetime

Refs #273, reader cancellation and error-classification gates from
[the activation review](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/273#issuecomment-5671702658).

## Completion boundary

`read_window_with_permit` puts the caller's permit in one shared
`ReadCompletion`. The read future, each blocking mapping, and every guarded
payload retain that same admission. A blocking mapping transfers ownership
to its output before returning; a cleanup owns the payload before the
completion reference in field drop order. The final reference releases the
permit only after all of those owners finish.

Cancelling the future stops further paging and drops its transaction.
An already submitted blocking page can still finish. Its accumulated state,
the decoded balances, and any vectors waiting for commit retain admission
through destruction on blocking workers. Cleanup tasks can run concurrently
and finish in either order without admitting another read early. Errors can
return before cleanup finishes; the permit still bounds that outstanding work.

On success, the authenticated vectors transfer to the caller and the read
permit is released before the caller's rebuild. The caller then owns those
vectors and their cleanup obligation. There is no additional semaphore
acquisition, retry, background paging loop, or timeout in the reader.

## Caller and compatibility audit

The signatures of `read_window`, `read_window_with_permit`,
`read_range_paged`, and `payout_state` are unchanged, as are stored bytes,
window digests, ordering, and error variants.

| Path | Contract retained |
| --- | --- |
| `Coordinator::rebuild_claim_parts` → coordinator reader → `Ledger::read_window_with_permit` | Build admission precedes read admission; the existing outer rebuild deadline includes read admission and I/O. The returned window moves immediately into the blocking builder. |
| `hydrate_compact_inputs` → `WorkLedger` → `Ledger::read_window_with_permit` | As-issued balances, existing outer deadline, original issued revision and absolute expiry remain caller-owned. The adapter's publication and eligibility checks remain in place. |
| Direct `Ledger::read_window` users | Same snapshot, bounds, count, digest, and corruption checks; no implicit admission requirement is added. |
| Public `read_range_paged` consumers | The same generic state/consumer API and result remain available; row mapping and accumulated-state cleanup still run off the runtime. |
| Candidate landing after reconstruction | `land_candidate_checked` authenticates rebuilt parts, verifies durable rows through its separate `verify_durable_range` reader, then applies its existing claim and revision fences. Its implementation is unchanged. |
| `WorkLedger::payout_state` | Revision and balance digest still come from one primary repeatable-read snapshot. A blocking task failure is `TaskFailed`; malformed balances are `Decode`. |

The read result does not itself authorize publication or credit. Existing
caller checks and transactional revision fences retain that responsibility;
the current observed revision never replaces the issued operation identity.

## Regression evidence

The focused `ledger::window` unit suite covers:

- cancellation while mapping is held, followed by a separately held output
  destructor; a second admission remains pending through both;
- two simultaneous payload destructors completing in different order, with
  admission retained until both finish;
- mapping failure and panic retaining admission until input cleanup ends;
- successful transfer releasing admission without destroying the result;
- exactly-once payload destruction and permit recovery;
- runtime timer progress and spare blocking-worker progress while cleanup or
  mapping is held;
- payout decoder panic as `TaskFailed`, corruption as `Decode`, and valid
  empty balances as a successful digest.

The existing database cancellation regression in `candidate_window_migration`
also exercises the real reader after one accumulated page while the next
database page is gated. Its single-blocking-worker setup proves queued
cleanup ownership; the controlled multi-worker unit cases prove completion
ordering independently of blocking-pool scheduling.

Run the focused unit suite:

```sh
cargo test --locked -p qbit-prism-server --lib ledger::window::
```

Run the caller and storage regressions on a disposable local database:

```sh
env -u PRISM_TEST_DATABASE_URL test/prism-native-tests.sh cargo-args --locked \
  -p qbit-prism-server --lib --test window_reference --test window_read_oracle \
  --test window_balance_order --test candidate_window_migration \
  --test candidate_window_switch --test candidate_window_qualification \
  --test ledger_deadline_atomicity -- --nocapture --test-threads=1
```

Qualification on 2026-09-14: the focused unit suite passed 15 tests and Clippy
passed for all server targets with warnings denied. The regression selection
passed 267 library tests and 62 integration tests, with 6 explicit ignores.
The first parallel run hit a PostgreSQL lock timeout in
`ascending_pages_match_native_rows_and_filter_gaps`; the complete
`window_reference` suite then passed serially (19 tests, 2 ignores). The
serial invocation above avoids concurrent fixture migration lock contention;
no production timeout or connection configuration was changed.

These changes satisfy the reader ownership and error-classification gates.
They do not activate compact runtime work or establish the separate
replacement-lease and first-build readiness gates.
