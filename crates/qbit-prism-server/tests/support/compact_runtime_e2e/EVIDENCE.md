# Compact runtime end-to-end evidence

## Independent baseline, 2026-09-15

Base: `cff94df5` (the inline runtime, after #377). The test slice touches only
`tests/compact_runtime_e2e.rs` and its new dedicated support directory.
Cargo automatically discovers the target. Gate-manifest registration remains
with the activation owner.

The run used a newly initialized disposable PostgreSQL 16.14 instance with
`fsync=on`, `full_page_writes=on`, and `synchronous_commit=on`, checked by the
fixture. Each case creates and drops a unique schema; cases run serially because
the ledger advisory locks are database-wide. The nonempty window contains 16
accepted rows loaded by the existing WindowPlan. All prepared and issued data
comes from real public Coordinator calls against the existing FakeNode.

```sh
PRISM_TEST_REQUIRE_INTEGRATION=1 \
PRISM_TEST_DATABASE_URL=postgresql://USER@127.0.0.1:PORT/postgres \
cargo test --locked -p qbit-prism-server --test compact_runtime_e2e \
  -- --nocapture --test-threads=1
```

Compilation passed. The baseline test result is intentionally **red**:
7 passed, 6 failed, 0 ignored (4 passing runtime cases plus 3 inherited
WindowPlan unit tests). All six failures report exactly:
`runtime refresh still wrote inline prepared storage: no compact format_version`.

| Runtime test | Inline baseline result |
| --- | --- |
| `refresh_issue_resume_preserves_original_work_and_compact_storage` | A/B independent publication, exact wire/bundle/input equality, unchanged refresh and wrong-worker rejection pass; compact shape fails |
| `bootstrap_resume_keeps_each_workers_original_payout` | Two distinct worker payouts and exact cross-frontend resume pass; compact shape fails |
| `real_socket_reconnect_resumes_original_entropy_mask_and_submits_once` | Real socket subscribe/configure/authorize/notify, reconnect on B with different session entropy, original-mask submit, duplicate refusal, one accepted row and original header hash pass; compact shape fails |
| `resume_expiry_does_not_slide_and_expired_work_is_a_miss` | Pass |
| `malformed_issued_inputs_are_errors_and_missing_work_is_a_miss` | Pass: zero/oversized/invalid targets, nonpositive/string difficulty, bad entropy; missing job is a miss |
| `compact_reference_corruption_is_an_error_and_blobs_survive_collection` | Blocked at compact prerequisite; the corruption/collection assertions have not executed on this base |
| `missing_prepared_dependency_repairs_original_record_without_renewing_identity` | Blocked at compact prerequisite; original-record repair and retention assertions await activation |
| `resumed_compact_work_authenticates_retained_share_rows` | Blocked at compact prerequisite; middle-share corruption rejection awaits activation |
| `cancelled_issued_save_releases_sql_resources_without_publishing` | Pass: a real SQL advisory-lock waiter is observed before cancellation, a subsequent save succeeds, cancelled job stays absent |
| `unknown_issued_commit_is_observed_and_reconciled_without_reissuing` | Pass: existing PostgreSQL wire proxy withholds COMMIT acknowledgement, original durable job resumes through B, exactly one INSERT and unchanged expiry |

The compact assertions use `Ledger::compact_prepared`, the public validating
reader, and canonical audit/manifest serializers. They compare the original
window, watermark, payout revision, template, balance digest, builder policy,
keys and audit hashes, and reject recursively embedded share arrays. A fixture
AFTER ROW trigger records actual committed job-row writes and their maximum
uncompressed JSONB size; this is scoped to job rows, not a global write inventory
or a WAL measurement. Missing measurements fail rather than becoming zero.

## Remaining qualification

- Core qualification owns internal admission/read/rebuild/cleanup deadline
  proofs and the 400k/500k memory/WAL runs. This 16-share fixture makes no scale
  claims and does not qualify replacement-lease authority by cross-frontend
  substitution.
- Full deep-review lanes require the final qualified activation diff and are
  launched by the coordinator. This test branch is intended for integration
  into that activation PR; no standalone PR or overall merge readiness is claimed.

## Approved-helper follow-up

After preserving the independent baseline commit, the coordinator approved
integrating support-only `c52a7572f31096f589848682ebe5aa306f354d52` for its
FakeNode controls and PostgreSQL returned-row observer. No shared helper was
edited by this test slice.

`delayed_old_refresh_cannot_replace_new_tip_publication_or_resume_retired_work`
passes: A's old template response is held at the approved HTTP barrier, B
publishes an advanced node tip, A's delayed refresh fails, and both frontends
refuse the old issued job after A publishes its own new-tip replacement.
The A/B resume test now also requires exactly 16 actual returned share rows,
using the shared proxy's DataRow/CommandComplete accounting and excluding
endpoint probes and metadata. This assertion follows the compact-storage
prerequisite and therefore remains unqualified on the inline base.

The complete helper-backed inline run reports **8 passed, 6 expected failures,
0 ignored**, including the three inherited WindowPlan unit tests. All six
failures remain at the compact-format prerequisite. Clippy passes with
`cargo clippy --locked -p qbit-prism-server --test compact_runtime_e2e -- -D warnings`.

## Integrated functional run, 2026-09-15

Activation base: `a154eb5969e1fe64b180914c099dc257b701cf50`, plus approved
COMMIT-pause helper `487827d`. Only the new E2E
files were replayed onto this exact committed head; production, shared support,
and gate-manifest files remain the activation owner's versions. The original
inline-red commits and evidence remain preserved on their separate branch.

The completed suite reports **18 passed, 0 failed, 0 ignored** in the same durable
PostgreSQL 16.14 fixture: 15 runtime cases and 3 inherited WindowPlan unit tests.
Strict Clippy also passes. The previously blocked corruption, reference-row
authentication, blob retention, and original-dependency repair cases now execute
and pass. Observed prepared-row maxima are 1,518 uncompressed JSONB bytes for
the 16-share window and 1,146 bytes for the empty bootstrap window.

`PreparedBundle` is now treated solely as submission metadata. On an independent
blocking test task, the oracle reads original `StoredCompactPrepared` inputs
and actual `Ledger::read_window(..., BalanceSource::AsIssued)` rows, then invokes
the public borrowing native-audit builders. It verifies BOTH original stored
audit/coinbase-manifest hashes and the issued coinbase against the rebuilt
artifact. Builder options come from the stored record, and the fixture signing
seeds must match its recorded public keys. Setup also reads the actual referenced
window outside measurement brackets; it never assumes retained snapshot arrays.

Bootstrap workers prove distinct actual coinbases and synthetic payout shares,
and each coinbase is independently reconstructed. The real-socket replacement
listener advertises a harder target and negotiates no rolling mask; the old
job still submits under its original target, entropy, and mask exactly once.
That strengthened socket case is included in the final full integrated run.
The 16-row fixture does not claim enabled-CTV or large-window qualification.

Two additional cases exercise original expiry across real PostgreSQL waits.
Issued persistence waits behind SETTLEMENT past its one-second expiry, then
refuses publication after the lock is released without changing the prepared
reservation. Resume reaches an observed AccessShareLock wait on the actual
share table and returns an expired-job miss while the blocking lock is still
held. Both cases subsequently issue/resume fresh work successfully, proving
recovery after the queued SQL operations are released. These are public runtime
tests, not claims about internal build-permit or blocking-owner cancellation.

The final two tests use the owner's `ExecutionProxy::pause_after_commit` helper
to hold an actual completed COMMIT reply while the database row is already
visible and transaction locks are free. An unchanged cached refresh completes
during that wait and preserves the original publication, payload, and expiry;
the original job is then delivered and resumed successfully. A genuinely newer
tip publication during the same wait causes the old persistence to refuse
delivery after the reply is released, and both frontends reject the retired
job while accepting fresh work. The committed old row stays unchanged: database
commit alone does not authorize miner delivery after revocation.

## Gate IDs for activation-owner registration

```text
qbit-prism-server::compact_runtime_e2e::bootstrap_resume_keeps_each_workers_original_payout
qbit-prism-server::compact_runtime_e2e::cancelled_issued_save_releases_sql_resources_without_publishing
qbit-prism-server::compact_runtime_e2e::compact_reference_corruption_is_an_error_and_blobs_survive_collection
qbit-prism-server::compact_runtime_e2e::delayed_old_refresh_cannot_replace_new_tip_publication_or_resume_retired_work
qbit-prism-server::compact_runtime_e2e::issued_expiry_during_sql_wait_does_not_publish_or_renew_original_identity
qbit-prism-server::compact_runtime_e2e::malformed_issued_inputs_are_errors_and_missing_work_is_a_miss
qbit-prism-server::compact_runtime_e2e::missing_prepared_dependency_repairs_original_record_without_renewing_identity
qbit-prism-server::compact_runtime_e2e::real_socket_reconnect_resumes_original_entropy_mask_and_submits_once
qbit-prism-server::compact_runtime_e2e::refresh_issue_resume_preserves_original_work_and_compact_storage
qbit-prism-server::compact_runtime_e2e::resume_expiry_does_not_slide_and_expired_work_is_a_miss
qbit-prism-server::compact_runtime_e2e::resume_expiry_includes_blocked_share_read_and_releases_resources
qbit-prism-server::compact_runtime_e2e::resumed_compact_work_authenticates_retained_share_rows
qbit-prism-server::compact_runtime_e2e::superseding_publication_during_completed_commit_wait_refuses_old_delivery
qbit-prism-server::compact_runtime_e2e::unchanged_refresh_during_completed_commit_wait_preserves_original_authority
qbit-prism-server::compact_runtime_e2e::unknown_issued_commit_is_observed_and_reconciled_without_reissuing
```
