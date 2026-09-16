# Template and payout-window refresh

The refresh loop retains one immutable, anchored `Snapshot` and its native
`WindowRef`. A transaction or fee-policy change builds a new template,
coinbase, and compact prepared record using those same window inputs. It does
not select the accepted-share window again or recompute its native digest.

The window is invalidated by a changed accepted-share cutoff, payout revision,
prior-balance digest, network difficulty, or the existing
`PRISM_PAYOUT_ARTIFACT_REANCHOR_SECONDS` interval. The interval starts before the
snapshot read; rebuilding templates does not renew it. The accepted-cutoff
probe uses the existing partial sequence index. Production appends serialize
under the ordering barrier, assign the acceptance timestamp from the ledger
clock, and reject future job timestamps, so a newly committed accepted row is
eligible at the next snapshot barrier. Rejected rows do not invalidate it.

`Prepared`, issued jobs, persisted records, and resumed jobs still contain
compact references rather than accepted-share arrays. The cache is owned only
by the serialized refresh loop. Its steady retained memory is one snapshot:
O(window shares + prior-balance recipients), including each share's owned
strings. Builders still need their existing transient counted-share and
artifact allocations. Old cache rows are released on the blocking executor
under build admission before reading a replacement; keeping old issued jobs
does not keep those rows alive. No idle build permit is retained.

The unpublished snapshot keeps its build admission through reservation and
publication, including cancellation cleanup. Publication still uses the existing
reservation and publication authority
boundary, with fresh tip, readiness, revision, and balance checks. The cache
becomes reusable only after successful publication. Original issued balances,
references, and absolute expirations are unchanged. Native share-array hashes
and canonical audit hashes continue to use their existing serializers; the
Python-compatible `PayoutWindow` digest is not substituted for either.

## Issue #274 scope and evidence

This implements the September 10 refresh split. It removes repeated full-window
SQL reads during template churn; it does not introduce delta SQL, the
incremental `PayoutWindow` engine, or a new hash recipe. Existing borrowing
builders still fold and hash the captured inputs for each changed template.
Those remaining costs need the existing 400k/500k harness and the agreed refresh
budget to determine whether the conditional incremental-engine work is needed.

`refresh_window_split` exercises the real Coordinator and PostgreSQL through the
existing execution proxy: three transaction changes two seconds apart return
zero additional accepted-share rows; new shares, revisions, interval expiry,
and difficulty changes trigger fresh reads. It also verifies as-issued resume,
compact storage and canonical audit reconstruction, rejection of a delayed
refresh after a newer tip observation, and cancellation during a completed
COMMIT response wait. Unit regressions cover same-revision
balance changes, the empty-to-nonempty transition, and release of retired rows
while old prepared work remains alive.

The unchanged frozen money/window corpus and compact runtime tests remain the
compatibility gates. No sub-second one-share-delta result, production timing,
24-hour memory soak, or measured per-frontend RSS is claimed by this change.
The old incremental/differential and 24-hour criteria remain conditional work;
the bounded cache ownership above is a structural property, not a soak result.
