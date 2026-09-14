# Optional PRISM Capacity Qualification

This document defines an optional operator load-test record for a future public
promotion decision. A capacity artifact is not required for PRISM startup,
restart, mainnet readiness, or CI. Compose, `make doctor`, `prism-self-check`,
and the coordinator do not consume an artifact or any `PRISM_CAPACITY_*`
environment variables.

The bootstrap repository ships the native `qbit-prism-server
capacity-evidence` command for the strict `qbit-prism-capacity-evidence/v3`
format, but it does not ship a production
qualification runner. An operator may use the format after capturing the
complete miner-facing path: valid Stratum submission, share validation, ACK,
and durable Postgres commit. Process health and a schema-only database benchmark
are not capacity evidence. Paid rented hash is not required; owned miners or a
controlled load generator may be used if they exercise the real path.

## Qualification Policy

Choose the forecast peak, ACK limit, maximum evidence age, coordinator revision
and image digest, exact Postgres version, and database-profile digest outside the
artifact. Pass them to the standalone validator so the artifact cannot choose
its own passing threshold.

The database-profile SHA-256 is the digest of the reviewed database profile used
for the run. Keep the source profile with qualification records. It should
identify the storage class, CPU and memory allocation, Postgres configuration,
connection path, replica policy, and resource limits that can change commit
latency. A changed profile digest requires a new run.

The artifact separately records and requires these live Postgres settings:

- `fsync=on`
- `full_page_writes=on`
- `synchronous_commit=on`

Do not improve benchmark numbers by weakening durability.

## Bound PRISM Configuration

Schema `qbit-prism-capacity-evidence/v3` binds only settings the native server
reads at startup, so the artifact names the values the measured binary ran
with. Record each value from the measured coordinator's environment:

- `PRISM_STRATUM_SHARE_DIFF` and `PRISM_STRATUM_VARDIFF` (`0` or `1`)
- vardiff minimum, start, maximum, target, retarget, step up, step down, EWMA,
  and tolerance
- `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` and `PRISM_STRATUM_SEND_TIMEOUT_SECONDS`
- `PRISM_RUNTIME_WORKERS`: Tokio worker threads, `1..=1024`
- `PRISM_DATABASE_MAX_CONNECTIONS`: the PostgreSQL pool that share commits
  draw from, `4..=1024`
- `PRISM_STRATUM_MAX_CONNECTIONS`: accepted miner connections, from `1` to the
  platform's Tokio semaphore permit limit
- `PRISM_JOB_BUILD_EXECUTOR_WORKERS`: blocking workers that build job bundles,
  `1..=PRISM_RUNTIME_WORKERS + 8`

The validator requires exactly this set, enforces `minimum <= start <= maximum`
and the native startup ranges above, and rejects the exact `1e-9` local-lab
difficulty. Changing a bound value requires a new qualification run. The set
does not describe frontend/resource topology or prove multi-instance scaling;
retain the complete measured environment and topology beside the image and
revision, database profile, raw miner output, and reconciliation results.

Version 1 and 2 artifacts are rejected with an error naming the retired schema,
and a pre-rewrite artifact does not qualify the native binary. Any other
`qbit-prism-capacity-evidence/` version is rejected as unsupported. Version 2
bound Python-runtime knobs the native server never read. Evidence or an
`--expect` flag that names one is rejected with the key named:

- `PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS` <!-- retired-setting: PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS -->
- `PRISM_SHARE_COMMIT_BATCH_SIZE` <!-- retired-setting: PRISM_SHARE_COMMIT_BATCH_SIZE -->
- `PRISM_SHARE_COMMIT_LINGER_MILLISECONDS` <!-- retired-setting: PRISM_SHARE_COMMIT_LINGER_MILLISECONDS -->

## Load-Run Contract

A qualification run must satisfy all of the following:

1. Use a non-zero run UUID and finish within the configured evidence age. A
   timestamp more than five minutes in the future is rejected.
2. Exercise steady-state, miner-reconnect, and slow-database phases. Every phase
   lasts at least 60 seconds, and phase durations reconcile with total duration.
3. Sustain at least twice the externally reviewed forecast peak in aggregate and
   independently during every phase.
4. Keep aggregate and per-phase ACK p99 within the externally reviewed limit.
5. Acknowledge every offered valid share and reject none of them.
6. Reconcile ACK identifiers against unique Postgres ledger identifiers with no
   missing or unexpected rows. Counts and canonical identifier-set SHA-256
   digests must agree in aggregate and for every phase.
7. Record at least ten reconnect events and at least 10 milliseconds of injected
   database delay, so the fault phases cannot be satisfied by token events.

Use a run identifier in every load-generator correlation ID and in the ledger
query predicate. Canonically sort the unique correlation IDs before hashing so
the ACK and Postgres digests are comparable. Query Postgres only after the
writer has drained. Background pool traffic must not enter either set.

The evidence file is an operator-controlled attestation, not a trust boundary.
Generate it from the load runner and reconciliation query rather than editing
measurements by hand. Preserve the raw load-run output, database query output,
and database-profile source beside the release record.

## Example Artifact

`tests/fixtures/prism-capacity-evidence.json` documents the complete JSON shape.
It is marked `artifact_kind=example`, contains synthetic values, and is rejected
by normal standalone validation. The CLI test-only override exists solely so automated
tests can validate the example's structure:

```bash
qbit-prism-server capacity-evidence \
  tests/fixtures/prism-capacity-evidence.json \
  --allow-example-evidence-for-tests
```

Never use that override in a deployment command or runtime environment.

## Validate Qualification Evidence

Pass the independently configured policy and subject together with every bound
value. The `CAPACITY_*`, subject, and profile shell names are local validator
inputs; each `--expect` value is the measured coordinator's native setting:

```bash
qbit-prism-server capacity-evidence /path/to/capacity-evidence.json \
  --forecast-peak-shares-per-second "$CAPACITY_FORECAST_SHARES_PER_SECOND" \
  --ack-p99-limit-milliseconds "$CAPACITY_ACK_P99_LIMIT_MILLISECONDS" \
  --max-age-seconds "$CAPACITY_EVIDENCE_MAX_AGE_SECONDS" \
  --expect-coordinator-revision "$COORDINATOR_REVISION" \
  --expect-coordinator-image-digest "$COORDINATOR_IMAGE_DIGEST" \
  --expect-postgres-server-version "$POSTGRES_SERVER_VERSION" \
  --expect-database-profile-sha256 "$DATABASE_PROFILE_SHA256" \
  --expect PRISM_STRATUM_SHARE_DIFF="$PRISM_STRATUM_SHARE_DIFF" \
  --expect PRISM_STRATUM_VARDIFF="$PRISM_STRATUM_VARDIFF" \
  --expect PRISM_STRATUM_VARDIFF_TARGET_SECONDS="$PRISM_STRATUM_VARDIFF_TARGET_SECONDS" \
  --expect PRISM_STRATUM_VARDIFF_MIN_DIFF="$PRISM_STRATUM_VARDIFF_MIN_DIFF" \
  --expect PRISM_STRATUM_VARDIFF_START_DIFF="$PRISM_STRATUM_VARDIFF_START_DIFF" \
  --expect PRISM_STRATUM_VARDIFF_MAX_DIFF="$PRISM_STRATUM_VARDIFF_MAX_DIFF" \
  --expect PRISM_STRATUM_VARDIFF_RETARGET_SECONDS="$PRISM_STRATUM_VARDIFF_RETARGET_SECONDS" \
  --expect PRISM_STRATUM_VARDIFF_MAX_STEP_UP="$PRISM_STRATUM_VARDIFF_MAX_STEP_UP" \
  --expect PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN="$PRISM_STRATUM_VARDIFF_MAX_STEP_DOWN" \
  --expect PRISM_STRATUM_VARDIFF_EWMA_ALPHA="$PRISM_STRATUM_VARDIFF_EWMA_ALPHA" \
  --expect PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE="$PRISM_STRATUM_VARDIFF_RETARGET_TOLERANCE" \
  --expect PRISM_SHARE_COMMIT_TIMEOUT_SECONDS="$PRISM_SHARE_COMMIT_TIMEOUT_SECONDS" \
  --expect PRISM_STRATUM_SEND_TIMEOUT_SECONDS="$PRISM_STRATUM_SEND_TIMEOUT_SECONDS" \
  --expect PRISM_RUNTIME_WORKERS="$PRISM_RUNTIME_WORKERS" \
  --expect PRISM_DATABASE_MAX_CONNECTIONS="$PRISM_DATABASE_MAX_CONNECTIONS" \
  --expect PRISM_STRATUM_MAX_CONNECTIONS="$PRISM_STRATUM_MAX_CONNECTIONS" \
  --expect PRISM_JOB_BUILD_EXECUTOR_WORKERS="$PRISM_JOB_BUILD_EXECUTOR_WORKERS"
```

No startup or deployment path runs this validator automatically. If an operator
later adopts capacity qualification as public-routing policy, deployment
orchestration should call it explicitly before opening public routes and archive
the artifact and raw results. Private canaries and routine restarts do not
require it. Re-run only when the qualified image, bound load configuration,
database/hardware profile, forecast, or ACK objective changes.

## Retired Python Memory Instrumentation

The heap census, allocator telemetry, `malloc_trim` controls, and resident-set
soak tooling from issue #226 instrumented the Python coordinator. They were
retired with that runtime and have no native equivalent settings; a capacity
run on the native server does not configure or record them.

## Payout-window JSONB storage ceiling

Independent of any load-test record, the payout window has a hard storage
ceiling. PostgreSQL refuses a JSONB container whose elements exceed 268,435,455
bytes, and three native writes still embed the whole window, so each grows
linearly with the share count. `cargo test -p qbit-prism-server --test
jsonb_ceiling_gate` measures them against 25% of that limit (67,108,863 bytes)
and fails when the set of crossing writes changes in either direction.

| Path | Column written | Window copies | At 400,000 shares | Removed by |
| --- | --- | --- | --- | --- |
| refresh | `qbit_prism_jobs.payload` | 3 | refused by PostgreSQL (measured) | #273 |
| enqueue | `qbit_block_candidate_outbox.candidate` | 2 | refused by PostgreSQL (measured) | #265 |
| landing | `qbit_pool_audit_bundles.audit_bundle` | 1 | 235 MB, 3.5x the gate threshold (measured) | #267 |

The legacy audit import (`import-audits`) was a fourth such write, refused by
PostgreSQL at 400,000 shares. #265 removed it. The import now stores only
`canonical_audit_bytes`, which is `bytea`, so PostgreSQL's 1 GiB value limit
bounds it rather than the JSONB ceiling. It writes no JSONB value.

The host, gate commit, PostgreSQL version (16.15) and build mode (debug) behind
these measurements are recorded under "Baseline, measured at the base commit
plus the gate's own test files" and "At 400,000 shares" in
`docs/prism-payout-artifact-measurement.md`.

Sizes are uncompressed JSONB containers. A refused write is reported as refused,
never with a size. After the enqueue write is refused, the gate writes a
window-free substitute outbox row itself so the later phases stay measurable;
the report labels that row `SUBSTITUTE` and never counts it as the enqueue write.
See "JSONB ceiling gate and 400k-share
baselines" in `docs/prism-payout-artifact-measurement.md` for the 50,000-,
100,000-, 200,000- and 400,000-share measurements and how to reproduce them. A
capacity qualification run should record the payout window size it exercised,
because a window near 400,000 shares reaches this ceiling before it reaches any
throughput limit.
