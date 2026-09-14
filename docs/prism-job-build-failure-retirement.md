# Shared job-build failure retirement (#340)

Local implementation and verification on 2026-09-14. This addresses the
confirmed shared job-build boundary in [#340](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/340).
It does not establish production failure frequency or explain the whole
memory/lease incident in #254/#332.

## Source identity and integration status

- Isolated branch: `fix-job-build-failure-leaks`.
- Starting revision, after `git fetch origin`:
  **`ede86019aa20b8f2dce0fd110ba6e8a26d5c582b`**, then-current `origin/2.x.x`.
  The existing clean Conductor worktree was moved to that base before editing;
  no implementation is based on `main` or another agent's workspace.
- No repository `AGENTS.md` was present; the session's supplied working
  agreements apply. Read #340 (including its reproductions), #254, #332,
  #249/#253 and PR #335, and searched repository issues/PRs for overlap.
  #249/#253 address cancellation; #335 addresses fanout, prefetch and oracle
  isolation; #339/#341 retain their separate scopes.
- PR #335 was still **open/unmerged** at the final verification check. Its initial head was
  `f6a9fbcd31d56721fd5d2d663f660d308cd843c7`; the final separately tested head was
  **`8ff3fb55443c2080bb689c8b6f2cb8c1385801fb`**. The intervening commit changes
  failed-helper timing and its tests, not the job-build boundary.
  At PR preparation, #335 remained open but had advanced to
  `02f4b1ee0d371c3c0e7bbb13fd2eabb26726b487`; that newer revision has not been
  integration-tested here.
- The implementation patch applied without conflict to both disposable,
  detached #335 checkouts. No #335 implementation files were copied into the
  deliverable. Its spool/oracle, failure and ownership facilities are exercised
  in the integration checkout; the existing job-build helpers are extended
  locally because those facilities are absent from the actual base.
- SHA-256 of the final base `lab/prism/job_bundle.py`:
  `ec7a6fa460602b8904099259a43298e93269ca962f0bcdbde256b2e871b3be4d`.
- SHA-256 of the implementation-only `git diff HEAD -- lab/prism/job_bundle.py`:
  `a38703c11e813d2708556f4d53e7b16bff47ecaa835f6e8ea08f4dc49d957330`.
- SHA-256 of `lab/prism/job_bundle.py` after applying that patch to final #335:
  `f0cea2eb8e9fa480dfa3920cbb58e1ba539aa239984aa1fe499e4056f151744e`.

All Python 3.14.7 runs used cached Linux ARM64 image
`python@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6`,
with networking disabled and source mounted read-only. This is **historical
production-version reproduction**, based on the version recorded in #254;
current production runtime/image/source identities were not inspected.
Local supplemental tests used macOS Python 3.14.6. No packages were installed.

## Ownership change and compatibility

The old ownership chain was:

`shared promise -> stored ordinary exception -> producer/waiter traceback -> request/promise/window`

Clearing scheduler slots and joining workers leaves this cycle intact.
Producer-only cleanup is insufficient because `Future.result()` adds each
waiter's request-owning frame to the same stored exception. Consumer-only
copying leaves the original producer frame cycle intact.

The fix releases completed invocation-owned frame locals for every failed
build before publishing its promise outcome. It checks actual frame ancestry,
including the Python 3.14 executor context frame holding the task tuple, and
walks causes, contexts and exception-group members. It excludes the executing
executor boundary and foreign invocations. No original traceback link is
changed. Orphan recovery uses the same outcome path as normal completion.

Every shared-promise observer receives private exception objects, note lists,
and traceback links. Producer code locations, line numbers, error type, args,
standard native fields, subclass slots, diagnostic attributes, cause/context
and suppression survive. Custom constructors and copy hooks are not rerun.
Cyclic diagnostic chains keep a separate unraised root so they cannot acquire
a waiter frame. Such diagnostic-only cycles can still require GC, without
retaining the obsolete request, promise or window.

Compatibility changes are intentional: callers can no longer compare an
observed failure to the stored exception by identity, share modifications to
its traceback/notes with other consumers, or inspect retired producer locals.
Cause/context instances are also private. Group child exceptions are copied,
so their identities change too. Diagnostic attributes retain shallow-copy
semantics: an explicitly attached payload remains an intentional owner, and
foreign traceback frames remain untouched. Holding a consumer exception can
legitimately hold that consumer's own frame until the exception is released.
Raw external `Future.result()` remains the standard Future API; scheduler
waiters use `_await_job_build_promise`.

Scheduler admission, bookkeeping, cancellation, publication/fencing and retry
logic are unchanged. Ordinary failures do not automatically retry; a subsequent
request can start fresh. The existing worker-`TimeoutError` policy remains:
the shared waiter polls until its deadline, then schedules a retry and reports
`JobBuildCancelled`. A pending-Future wait expiry and queued Future cancellation
also retain their existing behavior.

Security implications: no new logging of frame locals, source payloads or
credentials; no authority or environment changes to the isolated helper;
no payout, signing, writer-lease or publication fence changes.

## Before/after reproduction

Ran the issue's weak-reference probe on the actual starting revision **before
editing**. GC was disabled only inside this isolated reproduction. The real
coordinator facade, service methods, executor and scheduler ran; workers were
joined, slots empty, and the fixture/coordinator remained alive. The legacy
ledger converts a 1,024-row snapshot and fails at row 300.

| Route / waiters | Before fix, retained before GC | After fix, retained before GC | After explicit GC |
| --- | ---: | ---: | ---: |
| Base legacy / 1 | 4/4 | 0/4 | 0/4 |
| Base legacy / 3 | 8/8 | 0/8 | 0/8 |
| #335 oracle / 1 | 3/3 | 0/3 | 0/3 |
| #335 oracle / 3 | 7/7 | 0/7 | 0/7 |

Legacy references are requests, promises, snapshot and first converted row.
Oracle references are requests, promises and the canonical sequence. The
initial #335 head supplied the unmodified oracle baseline; the final head plus
this patch supplied the final after-probe. Both use the real isolated child
with a streaming test ledger and an injected failure at `build_audit_bundle`.
The unparsed **336,646-byte** canonical buffer remained owned before GC on the
unmodified integration; final ownership gauges return to baseline before GC.
The input sink and closed source spool already retired in the baseline.

The replacement regressions assert weak-reference release **before collection**.
Controls cover cached healthy rows surviving until their legitimate owner
retires, cancellation, in-frame supersession retry, ordinary failure followed
by a healthy request, one/multiple/late waiters, ledger read/conversion and
compiler errors, queued cancellation, shutdown, orphan/completion races,
nested/cyclic/grouped diagnostics and foreign frames. Integration tests add
spool read failures, actual helper rejection and compiler failures with both
unparsed and deliberately parsed canonical windows.

## Executed verification

| Source/runtime | Result |
| --- | --- |
| Final base patch, Linux Python 3.14.7 | 698 tests run: 694 passed, four explicit oracle integration skips |
| Final patch on #335 `8ff3fb5`, Linux Python 3.14.7 | 721 tests passed, no skips |
| Final focused retention/ownership tests, macOS Python 3.14.6 | 25 passed |
| Changed Python sources compiled; `git diff --check` | Passed |

The four base skips are the new oracle integration gates: #335 has not landed.
They all ran successfully in the separate integration checkout. The 16 base
modules were:

```text
tests.test_prism_job_build_exception_retention
tests.test_prism_job_build_failure_ownership
tests.test_prism_job_build_oracle_failure_retention
tests.test_prism_job_build_retention
tests.test_prism_singleflight_exception_retention
tests.test_prism_bounded_executor
tests.test_prism_job_build_promises
tests.test_prism_job_builder
tests.test_prism_job_delivery
tests.test_prism_tip_refresh_delivery
tests.test_prism_initial_job_delivery
tests.test_prism_admission_lock
tests.test_prism_payout_state
tests.test_prism_incremental_payout_window
tests.test_prism_payout_window_daemon_recenter
tests.test_prism_share_ledger
```

Executed with `python -m unittest` and those module names. Integration also ran
`tests.test_prism_async_failure_ownership` and `tests.test_prism_window_oracle`,
including #335's final failed-helper timing regressions. Earlier checks caught
and corrected test-fixture retention of healthy compiler kwargs and native
OSError formatting/native exception layouts; the table reports final verified runs.

## Repeated production-sized failure/drain workload

On the final #335 integration plus the exact patch above:

```sh
python -m tests.perf.job_build_failure_retirement --records 228397 400000 --cycles 6
```

Docker used `--network none --memory 4g --pids-limit 128`, a read-only source
mount, and the pinned Python image. Each six-cycle run keeps **one coordinator,
scheduler and executor alive**, changes the canonical input each cycle, and
alternates one/three shared waiters. The existing #335 production-shaped row
generator feeds its real isolated oracle. The injected compiler failure occurs
after canonical output is returned. A subsequent executor task proves the
previous completion callback drained before asserting release.

Ordinary GC stays enabled throughout; neither test setup/teardown collection
nor a forced collection runs in this workload. Each drain asserts every
tracked obsolete reference is gone, all scheduler slots/scan anchors are
empty, failure/start counts match cycles, and the weak ownership snapshot
exactly equals its initial baseline (including no dropped observations).

| Rows | Cycles | Canonical bytes per failure | Obsolete refs / canonical bytes / parsed rows / page rows after every drain | Current RSS after drain | Duration |
| --- | ---: | ---: | --- | ---: | ---: |
| 228,397 | 6 | 143,322,210–143,322,235 | 0 / 0 / 0 / 0 | 68,669,440–68,718,592 bytes | 17.78 s |
| 400,000 | 6 | 251,088,894–251,088,919 | 0 / 0 / 0 / 0 | 69,324,800–69,451,776 bytes | 31.11 s |

The asserted post-drain ownership plateau is **zero obsolete windows/bytes**
above baseline. At the failure gate, exactly one distinct canonical buffer
is owned and it remains unparsed. The smaller integration regression separately
forces all 1,024 rows to parse and asserts their retirement as well.

Current parent RSS at the failure gate was about 212 MB / 321 MB. Parent
`ru_maxrss` was 206,424 / 312,360 KiB, respectively: an allocator/process
high-water statistic, not retained-buffer accounting. RSS is reported rather
than used as the ownership assertion. These observations do not bound helper
peak RSS, legitimate production job history, arbitrary diagnostic attributes,
or the full coordinator heap, and are not a lease-monitor latency guarantee.

## Remaining integration and deployment disposition

The focused fix is locally verified on the required base. Production spool/
helper coverage depends on still-unmerged #335 and was executed separately,
not silently assumed present. Re-run the integration gate if that PR or the
release base changes. The ledger is a streaming test double, and compiler
failure injection is local; this work did not inject a real database/Rust
failure or inspect production occurrence frequency.

An additional isolated Python 3.14.7 probe found a **closed-generator
self-cycle**: a generator that stores its error in a local before raising can
retain another generator-local payload until GC. All eight targeted
request/promise/snapshot/converted-row references still retired before GC in
that probe. A separate frame-origin diagnostic confirmed the closed generator
has `f_back=None`; invocation ancestry cannot prove it belongs to this build.
It is deliberately not cleared by this patch. This is a concrete limitation
on generalizing the cleanup to arbitrary generator/async producer locals,
recorded for the #341 audit, not a claim of production occurrence. The local
probe is `generator-self-cycle.py` with output `generator-self-cycle-3147.log`.
No sibling issue or implementation was changed.

The user subsequently authorized publishing this patch as a non-draft PR
against `2.x.x`, with commit author `Anatolie <anatolie@swaplabs.xyz>` and
existing commit signing preserved. No merge, deployment, restart, migration,
production change, global install or incident closure was performed.
Production runtime/source verification, review/integration and a separately
authorized deployment/soak remain outstanding. #254/#332 stay open;
#339 daemon/fallback attribution and #341 other async boundaries are untouched.

Local raw issue snapshots, before/after probes, test logs, patch and workload
JSON are in the gitignored `.context/issue-340/` directory. The disposable
integration checkouts are retained there for review; they are not part of the
release branch.
