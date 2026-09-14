# Remaining asynchronous ownership audit (#341)

## Source and scope

Starting point: freshly fetched `origin/2.x.x`,
`ede86019aa20b8f2dce0fd110ba6e8a26d5c582b`, on isolated branch
`audit-341-async-retention`. No other agent's workspace is used.
The repository and its ancestor directories contain no on-disk `AGENTS.md`;
the session's supplied working agreements apply.

Read #341, #254 (including its September 9 follow-up), #332, #340,
#247/#249/#251 and their landing references (#248/#250/#252/#253).
GitHub issue/PR searches for retention, ownership and heavy state found no
additional focused implementation for these four boundaries. #335 remains
open at `f6a9fbcd31d56721fd5d2d663f660d308cd843c7` at initial inspection.
Its helper, detached-failure and weak ownership telemetry are dependencies
to validate in a separate disposable checkout, not this branch's base.
#340's generic shared-build error producer/waiter repair and #339's daemon
state-loss attribution are excluded. No production access is needed.

## Inventory recorded before edits to application code

| Boundary | Producer and stored outcome | Consumers/callbacks | Legitimate roots and retirement | Existing defenses | Evidence gap to test |
| --- | --- | --- | --- | --- | --- |
| Payout preparation | `_schedule_payout_ledger_artifact_preparation` submits `_payout_artifact_preparation_loop`; one executor Future, one latest `(generation, difficulty)` slot and bypass bit; task result is `None` | Loop dequeues/coalesces requests; `_prepare_payout_ledger_artifact` builds and installs; no external Future-result consumer | Executing snapshot/materialization; incremental window, installed artifact, published balances, job/cache/history aliases; retire replaced artifacts/windows after work joins; shutdown clears requested work | Ordinary speculative snapshot/materialization exceptions return no artifact; generation/append/anchor install fences; debounce/backoff; successful loop clears Future slot | Normal caught errors versus failures beyond catch (serialization/install/phase flush), slot retirement, pending latest request after failure, queued cancellation/shutdown |
| Deferred capacity | `_defer_job_build_locked` attaches one wake closure to blocker promises; returns a Future storing fresh `JobBuildSuperseded` | Async request path returns it; synchronous bundle loop and idle/initial shared-build consumers use `_await_job_build_promise`; blocker completion wakes once | Active/retiring/pending critical builds and their promises; deferred Future while callers await/retry; downstream callback owners until completion | Lock serializes multiple blockers; done check avoids duplicate resolution; cancellation errors copied per waiter without stored-error rethrow | Retention after all blockers/consumers retire, late consumers, already-complete blocker, cancellation, actual wait expiry versus worker outcomes |
| Initial job | `schedule_initial_job`/`_submit_initial_job_request` submits real `_run_initial_job`; `PendingInitialJob.future` stores delivery Future | Releasing done callback calls `_initial_job_future_finished`; cancellation, `note_initial_job_delivered`, timeout sweep, disconnect and predecessor handoff consume admission state | Pending map, running/queued request, connected client, active jobs/graveyard, replacement's predecessor; retirement after delivery/disconnect/cancellation and retained-job policy expiry | Releasing callbacks; ordinary completion errors inspected without rethrow; replacement installed before predecessor cancellation; preparation catches/retries errors, socket errors disconnect | Actual escaped delivery error producer graph (as distinct from #340); request/client/window release; completion before registration; queued replacement ordering and cancelled/deadline cases |
| Vardiff idle | `_enqueue_idle` submits `_run_idle_task` with request and optional cached bundle; local Future result `None` | `finish_task` captures `(client, connection_id)` and clears pending/timing via `_finish_idle_task`; task calls real skip/build/retarget paths | Pending key, executor arguments, client active job, cache/history; retire pending entry, task, disconnected client and expired job owners | Ordinary supersession and generic errors caught; socket disconnect only after delivery starts; queue/inflight counters; current-client/window/job fences | Whether closure has any back-reference; healthy/caught failures, worker timeout, connection replacement, queued cancellation/shutdown, inline completion |

The closure or stored exception alone is not a finding. Weak references must
outlive joined workers and completed callbacks, with the service still alive.
Tests explicitly retire only fixture cache/history/client roots at their
declared retirement boundary. They check before any explicit collection.
GC is disabled only in isolated lifetime tests; repeated size measurements
keep normal GC enabled. No heap walk or referrer inspection is used.

## Findings and focused corrections

| Boundary | Executed result | Correction/disposition |
| --- | --- | --- |
| Payout preparation | Ordinary caught materialization/partial-conversion failures release. An exception after that catch, at install or phase flush, leaves `_payout_artifact_future` pointing at the failed Future. Its traceback retains the failed artifact/window even after workers join and fixture artifact/cache owners retire. A newer queued request is stranded. | Catch and print ordinary escaped preparation errors around the whole iteration, including phase flush; proceed to the latest queued request, without retrying the failed request itself. Clear the Future slot on fatal exit, preserving propagation, and after shutdown, including a task cancelled before it entered the loop. |
| Initial completion | An escaped delivery `ValueError` retains **5/5** tracked request, Future, client, bundle and canonical sequence, including 32 parsed rows, after callback, disconnect and history retirement. The same failure occurs when work completes before callback registration. Success and caught socket error/timeout controls release. | Under the admission lock, drop the completed request's back-reference to its Future. The callback argument still carries the outcome and predecessor identity. Keep the releasing callback, ordinary `future.exception()` inspection, original diagnostic traceback/cause/notes and replacement-before-cancellation ordering. No exception/frame clearing or error copying is introduced. |
| Deferred capacity | All tested blocker outcomes, multiple/late consumers, already-completed blockers, wait expiry and cancellation release after the declared owners retire. The stored wake exception has no consumer traceback; each waiter receives its private cancellation signal. | **No residual retention demonstrated in these cases. No production change.** The first completed blocker wakes once; another pending blocker may legitimately retain the lightweight deferred signal until it completes. |
| Vardiff idle | Success, caught failures, supersession, socket/preparation errors, reconnect, fatal worker exit, cancelled queued work and completion before callback registration all release after task, pending entry, client and history retirement. | **No residual retention demonstrated in these cases. No production change.** Its callback captures the client key but no cycle back to the Future is established. |

The initial defect is in the successful-build → delivery → completion path.
The build is already complete before failure injection, so this is independent
of #340's shared-build producer/waiter error. The fix changes an internal
completed request's `future` field to `None`; public admission results and
exception identity are unchanged. Payout background failures that previously
silently stranded a slot now print their original chain and permit subsequent
preparation. The normal speculative `None` result/backoff behavior is unchanged.

On the unchanged starting source, the final **37-test** audit selection has
**seven failures**: escaped payout install diagnostics/drain, escaped install
lifetime, phase-flush retirement, fatal payout exit/readmission, queued payout
shutdown retirement, and the two initial delivery-error completion orderings.
The remaining 30 pass. These are seven regressions covering **two ownership
defects plus payout slot cleanup**, not seven independently attributed leaks.
With the focused patch all 37 pass before teardown collection.

## Boundary-to-test matrix

Every name below is executable with `python -m unittest MODULE.CLASS.TEST`.
`prism_async_ownership_support.LifetimeCase` disables GC only for these
isolated test lifetimes and checks weak references before its teardown
collection. Executor shutdown joins workers; events/barriers select races.
`RecordingExecutor` uses the actual bounded or standard executor and releases
its observer Futures after join. Services stay strongly owned during checks.

### Payout preparation

Module: [`tests.test_prism_payout_preparation_ownership`](../tests/test_prism_payout_preparation_ownership.py),
class `PayoutPreparationOwnershipTests`.

| Test suffix (all begin `test_`) | Boundary and behavior proved alongside retirement |
| --- | --- |
| `healthy_artifacts_release` | Real build/prepare/install/loop; installed artifact is legitimate until fixture retirement; Future/request slots clear. |
| `generation_supersession_discards_built_window` | Generation changes after build; install discards the obsolete window, keeps artifact unarmed and doubles backoff. |
| `caught_materialization_failure_releases_and_backs_off` | Caught generic failure releases parsed canonical payload; Future succeeds with `None`, anchor exposure retires and backoff doubles. |
| `real_compatibility_conversion_failure_releases_rows` | Actual legacy full-snapshot conversion of 1,024 rows fails at row 300; weakly tracked input record and converted first row release, while ledger/service remain alive. |
| `escaped_install_failure_releases_and_drains_latest_request` | Held install fails with chained diagnostic; intermediate generation 41 is coalesced away, latest generation 0 runs and installs. Original error type/message/cause and producer location appear in output. |
| `escaped_install_failure_lifetime_after_join` | Actual built artifact is given a canonical parsed share view, then install fails. Drop fixture/cache/history/observer roots, retaining the service's own slot for the assertion: artifact, Future and view must all die. |
| `exceptional_phase_flush_retires_future` | Failure in the loop's phase finalizer releases its payload and slot. |
| `worker_timeout_is_caught_but_wait_expiry_keeps_worker_owned` | An event-held materializer remains owned after a true observer wait expiry; its later worker `TimeoutError` takes normal caught-build/backoff behavior and releases. |
| `queued_shutdown_retires_future_and_latest_slot` | Real worker occupied, preparation cancelled in queue; shutdown clears both slots and bypass bit and rejects later admission. |
| `fatal_worker_exit_releases_slot_and_can_readmit` | Custom `BaseException` remains the Future outcome; slot releases, a later schedule succeeds, old fatal producer payload retires. |

### Deferred capacity

Module: [`tests.test_prism_deferred_capacity_ownership`](../tests/test_prism_deferred_capacity_ownership.py),
class `DeferredCapacityOwnershipTests`.

| Test suffix | Boundary and behavior proved alongside retirement |
| --- | --- |
| `multiple_and_late_consumers_release_after_all_blockers` | Two blockers, three concurrent consumers and a late consumer run the real deferral and waiter methods. One blocker succeeds with a window, the other fails; one wake signal, correct private exception args and no stored consumer traceback. |
| `already_complete_blockers_and_all_outcomes` | Inline callback registration after success, generic error, worker timeout or cancellation; all wake as capacity supersession, never propagate the blocker's unrelated error. |
| `true_join_expiry_and_cancelled_deferred` | Wait expiry leaves both Futures pending; cancelling the deferred Future then completing its blocker preserves cancellation and releases all owners. |
| `shutdown_cancels_real_queued_blocker_and_wakes_waiter` | Standard executor shutdown cancels an actual queued blocker; callback wakes the waiter and releases after running worker join. |

All four call `_defer_job_build_locked` and `_await_job_build_promise`.
Scheduler/admission routing is additionally covered by the existing
`test_publication_critical_build_cannot_be_displaced_by_initial_work` in
`test_prism_job_builder` and
`test_resolved_priority_corpse_does_not_defer_routine_requesters` in
`test_prism_job_build_promises`. Source review covers every deferral return
in `_request_job_build`. The other priority subscription consumers only call
`result()` after a successful `exception()` check; they do not rethrow a
deferred failure. Generic shared-build error behavior remains #340's scope.

### Initial job

Module: [`tests.test_prism_initial_job_ownership`](../tests/test_prism_initial_job_ownership.py),
class `InitialJobOwnershipTests`.

| Test suffix | Boundary and behavior proved alongside retirement |
| --- | --- |
| `success_releases_after_job_history_retires` | Real schedule/run/stamp/send/completion produces notify and preserves the window as active-job state until retirement. |
| `delivery_failure_releases_producer_request` | Generic error from the send seam after successful build; real delivery unwinds into bounded executor and completion; failure counted once, diagnostics preserved, pending slot removed. |
| `completion_before_registration` | Same real failure finishes before releasing callback registration; identical retirement and diagnostics. |
| `socket_disconnect_releases` | Real outer `OSError` handler disconnects without turning transport loss into a build failure. |
| `worker_timeout_releases_without_becoming_join_expiry` | `TimeoutError` from socket delivery is an `OSError` and follows the same existing disconnect behavior. |
| `queued_replacement_reclaims_admission_before_submit` | Saturated one-worker/one-queued-slot executor; superseded task physically removed, replacement owns client slot before callback, replacement delivers current authorization. |
| `queued_cancellation` | Explicit cancellation reclaims physical queue capacity and admission ownership. |
| `deadline_disconnects_and_retires_queued_work` | Timeout sweep commits closing, cancels queued task and removes admission. |
| `shutdown` | Stop/shutdown cancels request, joins work and empties pending state. |
| `running_replacement_waits_for_predecessor` | Event-held real preparation; replacement holds only predecessor until completion, then exactly one replacement task delivers. |
| `running_cancellation` | A running request remains owned through a true observer wait expiry; explicit cancellation suppresses delivery and releases after join. |
| `disconnect_reconnect_during_preparation` | Old client disconnects mid-build; new connection receives current work, obsolete client/request retire. |
| `caught_preparation_failure_retries` | Ordinary preparation error retries on the same request and then delivers; failure counted once, no request/window residue. |

### Vardiff idle

Module: [`tests.test_prism_vardiff_idle_ownership`](../tests/test_prism_vardiff_idle_ownership.py),
class `VardiffIdleOwnershipTests`.

| Test suffix | Boundary and behavior proved alongside retirement |
| --- | --- |
| `success` | Real sweep/enqueue/task/build/retarget/completion sends adjacent difficulty/notify and changes difficulty 16 → 4. |
| `caught_failure` | Generic preparation failure with a built bundle retains the live client/difficulty, counts once, then releases retired task/bundle/view. |
| `worker_timeout` | Worker timeout follows the preparation `OSError` handler; client remains connected, no job sent. |
| `superseded` | Superseded preparation is a skip; no delivery or stale pending ownership. |
| `preparation_oserror_preserves_client` | Backend preparation I/O failure keeps the client and its still-submittable work. |
| `send_disconnect` | Transport failure after delivery starts disconnects and retires after history expiry. |
| `reconnect_during_preparation` | Held old-client preparation loses membership; replacement survives and receives no obsolete delivery. |
| `completion_before_callback_registration` | Real task finishes before `finish_task` registration; pending and queue/inflight counters still reach zero. |
| `fatal_exit_completes_and_releases` | `BaseException` escapes task into its Future, preserves type/args and still completes pending bookkeeping; observers release outcome after join. |
| `queued_cancellation_on_shutdown` | Actual queued task is cancelled before starting; pending key and queue count retire. |

### Applicability and limits

- Payout preparation has no connection owner, completion callback or published
  Future consumer. Disconnect/reconnect and multiple/late result consumers
  are therefore inapplicable there; generation replacement is its supersession.
- Deferred capacity produces no bundle and performs no timed work itself.
  Blocker failure/timeout outcomes all mean capacity became available. Client
  disconnect/reconnect is exercised in its initial/vardiff consumers; there
  is no client state in the deferral helper.
- Initial/vardiff admission returns a boolean/status, not a shared Future.
  Each submitted task has one completion callback. Repeated scheduling
  coalesces; completion before registration and cancelled completion are
  covered. Multiple/late *shared build* consumers are #249/#340, with
  multiple/late *deferred signal* consumers tested here.
- Initial observer wait expiry is distinguished from its request deadline
  and from a worker socket timeout. Vardiff has no timed Future join; its
  shutdown waits for true worker completion. Payout's observer timeout does
  not cancel its running materialization.
- Supported no-retention findings are limited to the injected outcomes and
  explicit owners tested. Arbitrary error attributes that deliberately own
  payloads, interpreter-aborting initial callback failures, and foreign
  exception graphs are not given a universal no-retention guarantee.

## Repeated failure/drain measurements

[`async_ownership_retirement.py`](../tests/perf/async_ownership_retirement.py)
runs six cycles **per boundary**, 24 failure/drain operations per invocation.
It never calls lifetime-test setup/teardown, `gc.disable`, `gc.collect`, a
heap census, or allocator trim. The service for payout/deferred stays alive
across cycles; all initial/vardiff services remain alive through the final
measurement (13 services total). Worker/callback and history retirement are
the same real methods as the lifetime tests.

Synthetic canonical rows are 623 bytes including separators. The payload is
attached to a real built payout artifact, initial bundle or vardiff bundle;
deferred consumers hold a payload while consuming the real wake signal. The
database/RPC/compiler seams use repository fixtures; this measures lifetime,
not full ledger/helper/native-builder throughput or payout computation at size.
Two explicit historical sequences and their aliases remain live throughout
each run. Every drain returns to exactly those two windows, with no live
tracked task, request, client, artifact, bundle or Future left over.

| Replay | Bytes per window | Maximum target buffers / bytes | Every drain: target buffers / parsed rows | After history retirement |
| --- | ---: | ---: | ---: | --- |
| Base patch, 228,397 rows, unparsed | 142,291,330 | 3 / 426,873,990 | 2 / 0 | 0 buffers, 0 bytes, 0 parsed rows |
| Base patch, 400,000 rows, unparsed | 249,199,999 | 3 / 747,599,997 | 2 / 0 | 0 buffers, 0 bytes, 0 parsed rows |
| Base patch, 400,000 rows, parsed | 249,199,999 | 3 / 747,599,997 | 2 / 800,000 | 0 buffers, 0 bytes, 0 parsed rows |
| #335 `8ff3fb5` integration, 400,000 rows, parsed | 249,199,999 | 3 / 747,599,997 | 2 / 800,000 | Both target instrumentation and #335 registry: all zero; zero dropped observations |

The parsed runs peak at 1,200,000 target parsed rows, of which 800,000 belong
to the two declared historical owners. The bound **for this serial replay**
is `history + one active payload` (3 buffers), returning to 2 after drain and
0 after history expiry. This is not the deployment's total concurrent-job,
active/stale-grace/graveyard/serialization ownership bound.

RSS is a separate measurement. Unparsed base drains range from 384,446,464
to 386,310,144 bytes (228k) and 628,441,088 to 630,308,864 bytes (400k).
After historical buffers retire, RSS is respectively 101,720,064 and
131,907,584 bytes; glibc reports 50,882,720 and 81,086,624 bytes free in
its arenas. The parsed #335 integration ends with **1,201,393,664 bytes RSS**
despite zero tracked windows, while glibc reports **1,068,738,496 free arena
bytes**, 11,311,168 in-use arena bytes and 401,408 directly mmapped bytes.
This is concrete allocator-retention evidence in this replay, not evidence
of a still-reachable canonical window or an attribution of production RSS.
No trim or forced GC was used to obtain a lower RSS number.

The base lacks #335's process-wide weak registry. Test instrumentation counts
distinct byte identities through known weak sequence owners, parsed rows
through those sequences, and explicit weak request/artifact/client/Future
references. It does not count standalone raw-byte aliases or row objects
that escape those owners, unrelated Python/native allocations, daemon memory,
or all production roots. The partial-conversion test separately tracks an
input and converted row. #335's registry adds canonical/page/parsed accounting,
but remains capped at 16,384 observations and does not cover arbitrary raw
aliases either; dropped observations would invalidate complete accounting.
The integrated replay records zero drops and zero final page/parsed records.
Ownership and allocator data are sampled separately without a heap traversal.

## Runtime, executed validation and reproduction

Date: 2026-09-14. Historical reproduction runtime:
**CPython 3.14.7**, `main, Aug 31 2026, 23:42:40`, GCC 14.2.0,
Linux `7.0.12-linuxkit`, aarch64, glibc 2.41. Cached Docker image:
`python@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6`.
All containers use `--network none --read-only --tmpfs /tmp` and a read-only
source mount. Native secondary verification uses macOS CPython 3.14.6.
Neither is claimed to be Union's current runtime; no production identity
was read or production operation performed.

Exact selected modules, counts/times, log hashes, per-file source SHA-256s,
and every retained-object/allocator measurement from the final size runs are
in [`prism-async-ownership-results.json`](prism-async-ownership-results.json).
The JSON records source identity independently for each integration/replay.
Raw local stdout/stderr and disposable checkouts are under
`.context/audit-341/` (gitignored).

Final checks include the new lifetime matrix, landed #247 stall-probe,
#250 callback/idle-worker, #249 cancellation and #251 singleflight tests;
payout-state, initial-job, vardiff and resume, bounded executor,
admission/reconnect, retained-job, incremental-window/recenter, job-builder
and scheduler-promise suites. The payout-state and retained-job suites
exercise byte/digest equality, anchor/append/generation fences, payout
correctness, live-job preservation and stale-grace submissions. No still-live
job is evicted by the production fix. Compilation and `git diff --check` pass.

| Final selection | Runtime | Result |
| --- | --- | --- |
| Unchanged starting SHA plus new tests | Linux Python 3.14.7 | 37 tests, 7 expected failures, 0.322 s |
| Focused patch, all relevant suites listed above | Linux Python 3.14.7 | 557 tests pass, 11.412 s |
| New lifetime matrix, native secondary check | macOS Python 3.14.6 | 37 tests pass, 0.236 s |
| Latest #335 integration including new matrix/helper/metrics | Linux Python 3.14.7 | 329 tests pass, 6.694 s |

To run only the new matrix (37 tests) from the repository root:

```sh
docker run --rm --network none --read-only --tmpfs /tmp \
  -e PYTHONDONTWRITEBYTECODE=1 -v "$PWD:/src:ro" -w /src \
  python@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6 \
  python -m unittest \
  tests.test_prism_payout_preparation_ownership \
  tests.test_prism_initial_job_ownership \
  tests.test_prism_deferred_capacity_ownership \
  tests.test_prism_vardiff_idle_ownership -v
```

Using the same container prefix, execute
`python tests/perf/async_ownership_retirement.py --rows 400000 --cycles 6`
and repeat with `--parsed`. The default two historical windows are explicit;
`--history 0` removes that control. The harness bounds rows at 400,000,
cycles at 20 and historical windows at two.

## Dependencies, gates and security

#335 advanced during the audit from `f6a9fbcd31d56721fd5d2d663f660d308cd843c7`
to **`8ff3fb55443c2080bb689c8b6f2cb8c1385801fb`**, still open at the last
check. The new commit adds failed-helper scan timing and tests. A new
disposable checkout at that exact SHA accepts this patch without conflicts;
**329 relevant integration tests pass**, including helper/failure/metrics
coverage, plus the parsed production-sized replay above. Earlier integration
runs at `f6a9fbc` passed 321 and then 327 tests as the matrix expanded.
No #335 source is copied into this branch. These fixes do not require its
helper or failure-detachment implementation; only registry measurements do.

#340 remains open, with no linked/open focused implementation available at
the last GitHub check. Its generic shared-build regression is rerun in its
existing form, but that test's collection-before-release expectation is not
treated as proof of a fixed generic failure. Interaction with a future #340
patch remains an integration requirement; this audit does not implement or
claim to resolve that path. #339 remains excluded.

These demonstrated payout and initial-delivery defects must be included in
qualification before claiming a complete asynchronous-retention fix. This
does not prevent merging #335's independent improvements. #254/#332 remain
open pending broader ownership bounds, fresh runtime/source and exit-cause
attribution, complete production-shaped ledger/helper/native paths and the
separately approved minimum two-hour soak. No monitor-lateness, production
occurrence-frequency or incident-causality claim follows from these tests.

Security implications: the changes release completed internal references and
restore local background admission. They preserve payout publication fences,
stale-grace policies, signing, lease timing and access controls. Test payloads
are synthetic; containers have no network or credentials. No push, external
message, merge, deployment, service restart, production migration, package
installation or issue closure is part of this work.
