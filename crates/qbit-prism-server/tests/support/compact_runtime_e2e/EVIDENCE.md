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

- Run on the qualified integrated activation head, including compact corruption
  and retained-blob checks that are unreachable on the inline baseline.
- Add the cached unchanged-refresh/blocked-persistence race when the shared
  proxy can pause delivery of a completed COMMIT acknowledgement. Holding an
  advisory or prepared-row lock cannot reproduce that boundary: persistence
  holds SETTLEMENT while waiting, so refresh must also wait. This gap remains
  explicit; the existing cancellation test does not claim that coverage.
- Execute the cold dependency repair and retained range/balance integrity
  assertions that are blocked at the inline baseline's compact prerequisite.
- Core qualification owns internal admission/read/rebuild/cleanup deadline
  proofs and the 400k/500k memory/WAL runs. This 16-share fixture makes no scale
  claims and does not qualify replacement-lease authority by cross-frontend
  substitution.
- Full deep-review lanes require the final qualified activation diff and are
  launched by the coordinator. No standalone red PR is ready for review or merge.

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

## Gate IDs for activation-owner registration

```text
qbit-prism-server::compact_runtime_e2e::bootstrap_resume_keeps_each_workers_original_payout
qbit-prism-server::compact_runtime_e2e::cancelled_issued_save_releases_sql_resources_without_publishing
qbit-prism-server::compact_runtime_e2e::compact_reference_corruption_is_an_error_and_blobs_survive_collection
qbit-prism-server::compact_runtime_e2e::delayed_old_refresh_cannot_replace_new_tip_publication_or_resume_retired_work
qbit-prism-server::compact_runtime_e2e::malformed_issued_inputs_are_errors_and_missing_work_is_a_miss
qbit-prism-server::compact_runtime_e2e::missing_prepared_dependency_repairs_original_record_without_renewing_identity
qbit-prism-server::compact_runtime_e2e::real_socket_reconnect_resumes_original_entropy_mask_and_submits_once
qbit-prism-server::compact_runtime_e2e::refresh_issue_resume_preserves_original_work_and_compact_storage
qbit-prism-server::compact_runtime_e2e::resume_expiry_does_not_slide_and_expired_work_is_a_miss
qbit-prism-server::compact_runtime_e2e::resumed_compact_work_authenticates_retained_share_rows
qbit-prism-server::compact_runtime_e2e::unknown_issued_commit_is_observed_and_reconciled_without_reissuing
```
