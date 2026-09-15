# PR #402 review (READ-ONLY, medium lane) — Qbit-Org/qbit-mining-bootstrap

Head 3d4775cacc8c6f8376df0ce028d1083bda738746 vs base 1b0d409344c1b99f5f4bd04970890b9b034a9eeb (5 files, +821, tests/docs only).
Reviewer: Claude Fable 5.1 worker. No files edited, no PR changes, no compilation, no cargo test, no clones; diff read via `git show` from the existing local object store. Tests were NOT independently run; author-reported smoke numbers are cited with attribution only. Runtime behaviour claims below were verified by reading base-commit sources (paths under crates/qbit-prism-server/src, line numbers at base).

Lane: installed `code-review` skill invoked at medium; it executes as one background fork (its mechanism, not an extra nested worker spawned by me). Its output had not returned when this report was written, so the findings below are from direct medium review; nothing here depends on it.

## Ranked findings

### 1. Medium — retried/failed job deliveries are reported as slow successes (EP-OBSERVABILITY)
- Where: `crates/qbit-prism-server/tests/support/b275_persistence_measure/mod.rs:262-301`; `.../socket.rs:27-43`.
- Runtime fact (base): `deliver_job` sets `session.retry_job = true` on build error, persist error, or the 30 s `initial_job_timeout_seconds` (stratum.rs 886-919); the session loop retries on the next 1 s timer tick (stratum.rs 1461); `DeliveryObservation::drop` counts each failed attempt in `StratumStats.job_delivery_failures` (stratum.rs 404-415). `persist_issued_job` maps every ledger error to the same protocol error (coordinator.rs 2030-2041).
- Harness gap: `Listener::start` builds `StratumConfig::default()` (fresh `stats`) and discards it; the report only records the client's first decoded new-parent notify. A session whose persist timed out at 30 s and succeeded on retry is indistinguishable from one that took 31 s.
- Concrete scenario: 2 000 sessions on one frontend, debug profile, every `save_issued_job` serialised on the database-wide `SETTLEMENT_LOCK` (jobs.rs 80-83). Sessions beyond the 30 s window fail, retry, and eventually deliver; JSON prints `complete:true`, `delivery_errors:[]`, `max_observed_delivery_seconds` ≈ 31–60, and nothing says persistence failed 1 000+ times. The `qbit_prism_database_advisory_lock_wait_seconds{result="failure"}` delta will not catch it either: a cancelled wait is a dropped future, not a failed statement.
- Fix: keep `Arc<StratumStats>` (or the `StratumConfig`) in `Listener`, snapshot before/after the bracket, report the `job_delivery_failures` and `job_delivery_successes` deltas per frontend in `B275_MEASUREMENT`, and make `complete` require zero failures (or at least print them so the acceptance reader can see them).

### 2. Medium — fanout admission limit is undisclosed and differs between the two topologies
- Where: `socket.rs:27-35`; report keys at `mod.rs:287`; `tests/perf/b275_persistence_measure.md:13-19, 32-34`.
- Runtime fact (base): every job delivery, including refresh fanout, first acquires `config.initial_job_limit` (default `Semaphore::new(128)`, stratum.rs 207, 863-868) and the whole build+persist is bounded by `initial_job_timeout_seconds = 30` and `write_timeout_seconds = 20`. The semaphore lives in the per-listener `StratumConfig`.
- Effect: 1×2 000 runs with 128 concurrent builds; 2×1 000 runs with 256. The JSON lists runtime threads, build workers, pool connections and the 2 000 admission ceiling but not this, so the "second frontend non-regression" comparison the md sets up has an undisclosed structural advantage for the two-frontend topology, on top of the two extra build workers and four extra pool connections it already gets (those are disclosed).
- Fix: set `initial_job_limit`, `initial_job_timeout_seconds`, and `write_timeout_seconds` explicitly in `Listener::start` and emit them in the JSON/md; if a per-process bound is wanted, share one semaphore across listeners.

### 3. Low — the 240 s case budget is not carried into the bracket, so an overrun loses the whole measurement (EP-ERRORS)
- Where: `mod.rs:189-196` (outer `timeout(240 s, body)`), `mod.rs:254-255` (bracket deadline `start + 120 s`), `mod.rs:300` (report printed only after the bracket).
- Scenario: 2 000 debug-profile logins at 32 concurrency, each initial delivery also serialised on `SETTLEMENT_LOCK`; login takes > 120 s, the bracket starts, and the outer timeout cancels the body first. Nothing is printed; cleanup runs; the test fails with "exceeded 240 seconds" and no `received`/`delivery_errors` evidence survives. Partial evidence is exactly what the scale slot is being scheduled to collect.
- Fix: take one case deadline before login and use `min(start + 120 s, case_deadline)` for both refresh and client reads, or budget login separately and print the report from a `Drop`/`finally`-style path.

### 4. Low — runtime-path fence assertion accepts any error (false-positive shape)
- Where: `mod.rs:445-451` (`persist_issued_job(...).is_err()`), `mod.rs:458` (tautology: `job` is an immutable local and `original` was copied from it at 410).
- Because `persist_issued_job` erases the cause (coordinator.rs 2030-2041), a pool failure or connection reset would pass this assertion. The durability claim still holds because `mod.rs:452-457` proves no row for either ID, so this is a strength issue, not a correctness bug. Suggest asserting the settlement-lock `result="success"` count delta of +1 for that call (proves it waited and reached the revision check) and deleting line 458.

### 5. Low, unverified boundary — `options` appended to a URL that already carries one (EP-VALIDATION)
- Where: `mod.rs:63-65`. `PRISM_TEST_DATABASE_URL` is taken as-is and a second `options=-csearch_path=…` pair is appended. If a shared CI URL already has an `options` pair (statement_timeout, search_path), which pair sqlx honours was not verified here. Other suites in this crate take the same URL, so this is likely a non-issue for the current environments; flagged only because the scale test is explicitly meant to run on a separately coordinated database.

## Checked and found correct (no finding)
- Clock bracket: `start` is taken after logins and metric snapshot and before the first `refresh_once` poll (mod.rs 250-266); refresh return and each client's decode use the same monotonic `start`; both use the same absolute `deadline`; receiving another frame never extends it (socket.rs 147-155). Attribution in the md matches.
- Notify ordering: authorize reply is written inside `request` before `deliver_job` runs in the same loop iteration (stratum.rs 1455-1462), so `response()`'s "notify preceded authorization reply" check cannot misfire; `mining.set_difficulty` precedes notify and is skipped correctly. Notify params index 1 = prevhash, 8 = clean_jobs (codec.rs 462-465); single-byte repeated parents are swap-invariant.
- Advisory lock identity: `SETTLEMENT_LOCK` matches ledger `0x505249534d000003`; ledger uses single-bigint `pg_advisory_xact_lock` (connect.rs 454), so `classid = key>>32`, `objid = key & 0xffffffff`, `objsubid = 1` in `pg_locks` is the right mapping; both halves fit an `oid`.
- Metric deltas: only two `qbit_prism_database_*` families exist and both are histograms (registry.rs 69-70); `_bucket` filtered, `_count`/`_sum` kept; no gauge can produce a negative "counter reset". Overlapping-sum caveat is stated in JSON and md. `commit_seconds: null` is kept distinct from zero; `p50`/`max` are `null` when nothing was received; `received` and error lists are always emitted before `ensure!(complete)`.
- Config claims: `database_connections: 4`, `build_workers: 2` match `fake_qbitd::coordinator_config_at`; `ConnectionLimit::new(2_000)` is a plain semaphore with no per-IP limit; `Coordinator::new` spawns no poll loop (poll loop is a separate `run`), so the 600 s intervals only disable in-process timers as the comment says. `work_ledger` is the same `Ledger` (coordinator.rs 652), so closing `ledger.pool` closes every runtime pool before `DROP SCHEMA`.
- Durable row identity: query at mod.rs 347 checks job_id ∈ delivered IDs, `parent_hash` (unswapped template hash), `payout_revision`, `payload->>'prepared_key'` and liveness; prepared record readback compared byte-for-byte. Duplicate-ID check across all sessions is correct given per-session job IDs (author's 8-session pass corroborates).
- Cleanup/concurrency: setup failure path closes listeners, pools and drops the schema; `run` catches panics, always runs `close()`, then resumes the panic; `close()` attempts every step and aggregates errors; `Drop for Listener` aborts the task as a last resort. `SERIAL` correctly serialises the three gated cases in-binary (advisory locks are database-wide); the ignored scale test is a separate process and the md correctly asks for an idle database.
- Lock baseline: rollback always happens at `start + 3 s` even when no waiter was observed, and the waiter error is surfaced afterwards; `settlement_waiter_observed:true` is only printed after that check passed. The ledger has no lock/statement timeout under 3 s, consistent with the author's reported 3.0036 s.
- Gate: `gate::required_database_url` never skips; the three ordinary tests use the skipping `database_url`; manifest lines are sorted and in `<package>::<binary>::<test>` form. `usize::is_multiple_of` is fine on the pinned 1.98.1 toolchain.

## Evidence limitations
- Not compiled, not clippy'd, no test executed by this reviewer. Author-reported: 6 passed / 1 ignored small suite, 8-session max 0.030/0.032 s, lock baseline 3.0036 s, fmt/clippy clean.
- The 2 000-session case has not been run by anyone; findings 1–3 describe what its output would and would not show, not observed results.
- The code-review skill fork's own findings were not available at write time.
