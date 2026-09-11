# Integration test gate

Every Rust test whose execution depends on an environment input takes that
input through one gate, the `qbit-prism-test-gate` crate in
`crates/qbit-prism-test-gate`. The gate applies one decision table, records
what it decided in an execution manifest, and prints a skip line with a fixed
prefix when it lets a test return early. This page is the contract: the
switch, the manifest, the expected list, and how to add a gated test. Issue
#286 introduced it after 59 native tests were found to report `passed`
without running.

## Inputs

| variable | what it carries | tests that need it |
| --- | --- | --- |
| `PRISM_TEST_DATABASE_URL` | a disposable PostgreSQL the test may write to (each test creates and drops its own schema) | ledger, API, rollups, readiness, authorization, migration, JSONB ceiling, window oracle, candidate lease, the D2 payout rules, the explicit `#[ignore]` runs, and the live suite |
| `PRISM_TEST_PG_BIN_DIR` | a directory holding `initdb`, `pg_ctl` and `pg_basebackup` | the physical failover and public read replica tests, which run their own clusters |
| `QBITD_BIN` | a `qbitd` executable | the live regtest suite (`live_regtest`, with its high-difficulty and CTV/CPFP modules) |

An empty or whitespace-only value counts as unset. A value that is not valid
Unicode is an error, never "absent": treating it as unset would let a
deliberately configured run skip a test without a word.

## The decision table

| input set and non-empty | switch | result |
| --- | --- | --- |
| yes | any | run |
| no | `PRISM_TEST_REQUIRE_INTEGRATION=1` | fail, naming the missing variable and the test |
| no | `GITHUB_JOB=prism-native-postgres` | fail, same |
| no | neither | print one skip line and return |

`PRISM_TEST_REQUIRE_INTEGRATION` accepts `1` (require), `0` or unset (do not
require); any other value is an error, so a typo cannot pass silently. The
switch is consulted before the job id, so a run that sets both is reported as
demanded by the switch.

`GITHUB_JOB` is set by GitHub to the running job's id. Matching it means a
database outage in the `prism-native-postgres` job surfaces as a failure even
if the job's own environment were edited. Keying on `CI` would be wrong:
GitHub sets `CI=true` in every job, including `rust-tests`, which runs the
whole workspace with no database.

Explicit `#[ignore]` runs are always required. Four gated tests are selected
explicitly, with `#[ignore]` and `--ignored`, because they are long or
destructive: the 10,000-connection admission test in
`stratum_admission_postgres`, the collector test in `observability_database`,
and the full-size ratchet and baseline sweep in `jsonb_ceiling_gate`. They use
the gate's `required_*` entry points, which never skip: a missing input fails
them whatever the switch says, since a vacuous pass is exactly what selecting
them explicitly tried to avoid. The first two are in the expected list because
the native job selects them; the two JSONB measurement runs are not, because
CI never selects them.

The table is a pure function, `qbit_prism_test_gate::decide`, over injected
values, with unit tests for every row in the crate itself.

## The skip line

A skipped test prints exactly one line on the process's real standard error,
past libtest's capture, so it is visible with or without `--nocapture`:

```text
[prism-test-gate] skipped <package>::<binary>::<test path>: PRISM_TEST_DATABASE_URL is unset or empty; set PRISM_TEST_DATABASE_URL to run it, or PRISM_TEST_REQUIRE_INTEGRATION=1 to fail instead
```

The prefix `[prism-test-gate] skipped` is fixed. `scripts/check_gate_manifest.py`
fails the native job if it appears in the job's log, and also if any of the
per-file skip messages the gate replaced reappears (`skipping ... set
PRISM_TEST_DATABASE_URL`, `PRISM_TEST_DATABASE_URL not set`, `set
PRISM_TEST_DATABASE_URL for ...`, `SKIPPED: jsonb_ceiling_gate`).

## Test identity

A gated test is identified as `<package>::<binary>::<test path>`:

- `<package>` is the Cargo package, for example `qbit-prism-server`;
- `<binary>` is the test binary, the file stem of an integration test
  (`ledger_postgres`) or the crate name for `#[cfg(test)]` modules in the
  library (`qbit_prism_server`);
- `<test path>` is the test's module path and name inside that binary, as
  `cargo test` prints it (`two_x::legacy_2x_upgrade_repairs_partial_carry_seed_and_preserves_shared_state`).

The first two come from the `gate::site!()` macro at the call site. The third
is the name libtest gives the thread it runs each test on, which is why the
gate must be called from the test's own thread: at the top of the test body,
or from a fixture helper the test body calls before spawning anything. A
call from an unnamed or runtime worker thread is an error that names the
thread.

## The manifest

When `PRISM_TEST_GATE_MANIFEST=<path>` is set, every decision appends one
line to that file:

```text
executed qbit-prism-server::ledger_postgres::candidate_renewal_requires_a_live_pending_token
skipped  qbit-prism-server::live_regtest::real_two_server_mining_failover_audit_and_reorg
failed   qbit-prism-server::postgres_failover::acknowledged_shares_survive_synchronous_primary_loss_and_pool_reconnect
```

Each line is one `O_APPEND` write, so parallel test threads and the several
test binaries of one `cargo test` run share the file safely. A test that
asks the gate more than once (a fixture opened in a loop) writes more than
one line; the checker counts it once. An unwritable manifest fails the test.

## The expected list and the proof

`test/prism-gated-tests.txt` lists every gated test the `prism-native-postgres`
job must execute, one id per line, sorted; 114 today. Its length is the minimum
count.
After the job's three `cargo test` invocations (the whole workspace with
`--nocapture`, then the two explicit `--ignored` runs), it runs

```sh
python3 scripts/check_gate_manifest.py \
  --manifest "$PRISM_TEST_GATE_MANIFEST" \
  --expected test/prism-gated-tests.txt \
  --log "$PRISM_TEST_LOG"
```

which fails when any expected test has no `executed` line, any `skipped` or
`failed` line appears, fewer distinct tests executed than the list holds, a
test executed that is not in the list, or the log shows a skip line. It
prints the manifest to the log and to the job summary, and the job uploads
the manifest as the `prism-gate-manifest` artifact. The job sets
`PRISM_TEST_REQUIRE_INTEGRATION=1`, so an input that goes missing fails the
tests themselves before the checker runs.

`rust-tests` runs the same workspace with no inputs, where every gated test
prints its skip line and passes; nothing there asserts execution, which is
why the native job is in the required `checks` list and `rust-tests` is not
the proof.

## Running locally

`test/prism-native-tests.sh` provisions a private PostgreSQL cluster (or uses
`PRISM_TEST_DATABASE_URL`), resolves `PRISM_TEST_PG_BIN_DIR` and `QBITD_BIN`,
and exports the switch once every input a mode needs is present, so a local
run is as non-vacuous as CI:

| invocation | what runs | required mode |
| --- | --- | --- |
| `make test-prism-postgres` (`test/prism-native-tests.sh`) | the whole workspace, the two explicit `--ignored` runs, then the manifest check | when `qbitd` and the PostgreSQL server tools are found; otherwise the manifest is printed and the run says which input is missing |
| `make test-prism-regtest` (`... live`) | `live_regtest` | always (the mode refuses to start without `qbitd`) |
| `make test-prism-public-read-replica` (`... replica`) | `postgres_failover` | always |
| `test/prism-native-tests.sh cargo-args <args>` | the `cargo test` you name | always |

Without the script, a bare `cargo test --workspace --all-targets` prints one
skip line per gated test and passes. Set `PRISM_TEST_REQUIRE_INTEGRATION=1`
to turn those skips into failures, for example to prove that a test really
depends on its input.

## Adding a gated test

1. Take the input through the gate at the top of the test body, or in the
   fixture helper the body calls first:

   ```rust
   use qbit_prism_test_gate as gate;

   #[tokio::test]
   async fn my_ledger_contract() -> anyhow::Result<()> {
       let Some(url) = gate::database_url(gate::site!())? else {
           return Ok(());
       };
       // ...
   }
   ```

   `gate::pg_bin_dir`, `gate::qbitd_and_database_url` and the general
   `gate::inputs(site, &[Input::...])` cover the other inputs;
   `gate::required_database_url` is for explicitly selected `#[ignore]` tests.
   Tests that return `()` call `.expect("integration gate")` instead of `?`.
2. Never read the variables directly. `scripts/check_gate_env_reads.py`, run
   in the lint job, fails on any Rust source line outside the gate crate that
   holds one of the variable names as a string literal.
3. Add the test's id to `test/prism-gated-tests.txt`, keeping the file
   sorted. The id is what the gate writes; the easiest way to get it is to
   run the test with `PRISM_TEST_GATE_MANIFEST` set and copy the line. A
   gated test that runs without being listed fails the native job, so the
   list cannot silently drift.
4. If the test lives under `crates/qbit-prism`, nothing else changes: the
   native job runs the whole workspace, and the crate already has the gate
   as a dev-dependency.
5. Renaming or removing a gated test means changing the list too.

## Related

- [prism-deleted-test-map.md](prism-deleted-test-map.md): every 2.x.x test
  file absent on 3.x.x, with its native replacement or the issue that closes
  the gap.
- `crates/qbit-prism-test-gate/src/lib.rs`: the gate, its decision table and
  its unit tests; `crates/qbit-prism-test-gate/tests/process.rs` drives the
  runtime path through a child process.
