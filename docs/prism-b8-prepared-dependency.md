# Issued-work prepared dependency lifetime

A miner job may be issued while published tip A remains authoritative during a
failed replacement build. Its shared prepared record must remain readable until
that job's original reconnect deadline. An initial TTL calculated when A was
built cannot guarantee this: same-tip reuse, delayed publication, late divergence
and a tip returning before departing again can all make valid issuance occur
later. With default intervals, prepared A could expire at t165 while a job issued
at t170 remained valid until t200; immediate reconnect incorrectly returned an
unknown job.

`Ledger::save_issued_job` atomically guards the prepared dependency and saves the
compact issued record under the existing settlement lock and current payout
revision fence. The dependency identifies its original storage key, parent and
payout revision separately from that current revision. The child's JSON reference
and deadline must match the typed arguments. Its absolute expiry is fixed before
persistence; waiting, repair and exact duplicate retries cannot extend it.
Mismatching immutable data and elapsed or unrepresentable deadlines fail.

The common path reads only dependency metadata with `FOR KEY SHARE`, which blocks
concurrent deletion while permitting a non-key update. It avoids loading or
serializing the prepared payload. A dependency already outliving the issued job
receives no expiry update. Otherwise its expiry advances monotonically to the
child deadline plus 60 seconds, avoiding an expiry update for every miner.
Row locking itself can still cause disk writes; this is not a zero-WAL claim.
[PostgreSQL 16 locking documentation](https://www.postgresql.org/docs/16/explicit-locking.html)
describes these row-lock semantics.

A missing dependency returns `PreparedMissing` after releasing the transaction,
without saving a child. Coordinator serializes the exact original typed record
only on this cold path. An `Arc` retains that record and shares its snapshot and
bundle with live prepared work; there is no eager serialized JSON cache. Resume
also keeps the original record, including bootstrap `bundle: null` and its
original coinbase suffix, even if reconstructing miner work requires a bundle.
A per-prepared mutex coalesces concurrent repairs, and the existing build-slot
semaphore bounds serialization. Both owned guards live through the actual
blocking work when its asynchronous waiter is canceled. A follower retries
compact persistence before doing heavy work. Repair rechecks issuance admission,
retains the original child deadline, and verifies any competing existing prepared
row exactly; it never replaces a conflicting payload. The repair and child save
commit together.

Garbage collection retains its 4096-row batch limit and now rechecks expiry in the
outer `DELETE` predicate. If a candidate ID was selected before a saver renewed
its row, PostgreSQL re-evaluates that predicate against the updated row after
waiting for the lock. If deletion wins first, issuance observes a missing record
and repairs it before saving the child. This relies on the production default
Read Committed isolation. [PostgreSQL 16 transaction isolation documentation](https://www.postgresql.org/docs/16/transaction-iso.html)
describes the recheck behavior.

Persisted JSON and schema remain compatible with existing records. This is a
behavior restoration, not the upcoming compact-window representation in #273.
All native frontends sharing this jobs table must use the corrected GC query;
an older frontend's unconditional outer deletion could still remove a renewed
row. Once issuance stops, existing retention and the bounded renewal headroom
allow the prepared record to expire; issued leases are never renewed.

## Focused verification

`coordinator/miner_tests/prepared_expiry.rs` contains ten ungated tests using real
Coordinator refresh, build, persistence, resume and submit decisions with loopback
RPC and controlled storage/serialization boundaries. The memory ledger now
filters reads by expiry and models the atomic dependency save. The initial
expiry/reconnect regression failed against the old production persistence path
and passed after this change.

Coverage includes expired and physically deleted prepared rows, reuse and delayed
publication, returning/redeparting tips, original/current revision separation,
unchanged target/worker/version mask, independent candidate fencing, bootstrap
identity after resume, fixed child expiry, competing repairs, duration overflow,
coalesced repair, and cancellation retaining capacity through blocking completion.
The full native library run passed 107 tests, with no ignored or skipped tests:

```sh
CARGO_TARGET_DIR=/tmp/prism-b8-expiry-target \
  cargo +1.89.0 test --locked -j2 -p qbit-prism-server --lib -- --test-threads=2
```

Real PostgreSQL transaction and GC concurrency qualification is recorded separately
by `tests/issued_job_dependency.rs`; the ungated memory fixture does not establish
PostgreSQL lock semantics. Full database/node/physical-failover qualification is
required again after integration and the final base update.
