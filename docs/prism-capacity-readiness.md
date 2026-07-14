# PRISM Production Capacity Qualification

PRISM production readiness requires recent evidence from the complete
miner-facing path: valid Stratum submission, share validation, ACK, and durable
Postgres commit. Process health and a schema-only database benchmark are not
capacity evidence.

## Deployment Policy

Set these values independently of the qualification artifact:

```dotenv
PRISM_CAPACITY_FORECAST_PEAK_SHARES_PER_SECOND=<reviewed peak>
PRISM_CAPACITY_ACK_P99_LIMIT_MILLISECONDS=<reviewed ACK limit>
PRISM_CAPACITY_EVIDENCE_MAX_AGE_SECONDS=86400
PRISM_CAPACITY_COORDINATOR_REVISION=<40 lowercase hex characters>
PRISM_CAPACITY_COORDINATOR_IMAGE_DIGEST=sha256:<64 lowercase hex characters>
PRISM_CAPACITY_POSTGRES_SERVER_VERSION=<exact server version>
PRISM_CAPACITY_DATABASE_PROFILE_SHA256=<64 lowercase hex characters>
PRISM_CAPACITY_EVIDENCE_FILE=/absolute/host/path/capacity-evidence.json
```

The validator requires the artifact's forecast, ACK limit, runtime identity,
database identity, and every load-affecting configuration value to match these
deployment inputs. The artifact cannot choose its own passing threshold.

`PRISM_CAPACITY_DATABASE_PROFILE_SHA256` is the SHA-256 digest of the reviewed
database profile used for the run. Keep the source profile with deployment
records. It should identify the storage class, CPU and memory allocation,
Postgres configuration, connection path, replica policy, and any resource limits
that can change commit latency. A changed profile digest requires a new run.

The artifact separately records and requires these live Postgres settings:

- `fsync=on`
- `full_page_writes=on`
- `synchronous_commit=on`

Do not improve benchmark numbers by weakening durability.

## Bound PRISM Configuration

Qualification schema `qbit-prism-capacity-evidence/v2` binds:

- share difficulty and vardiff enablement
- vardiff minimum, start, maximum, target, retarget, step, EWMA, tolerance, and
  idle-sweep values
- share commit batch size, linger, and timeout
- Stratum send timeout

The validator enforces `minimum <= start <= maximum`. Changing any bound value
requires a new qualification run. The exact `1e-9` local-lab difficulty is
rejected.

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
by production validation. The CLI test-only override exists solely so automated
tests can validate the example's structure:

```bash
python3 scripts/prism_capacity_evidence.py \
  tests/fixtures/prism-capacity-evidence.json \
  --allow-example-evidence-for-tests
```

Never use that override in a deployment command or runtime environment.

## Validate Qualification Evidence

Pass the independently configured policy and subject together with every bound
runtime value. For example:

```bash
python3 scripts/prism_capacity_evidence.py /path/to/capacity-evidence.json \
  --forecast-peak-shares-per-second "$PRISM_CAPACITY_FORECAST_PEAK_SHARES_PER_SECOND" \
  --ack-p99-limit-milliseconds "$PRISM_CAPACITY_ACK_P99_LIMIT_MILLISECONDS" \
  --max-age-seconds "$PRISM_CAPACITY_EVIDENCE_MAX_AGE_SECONDS" \
  --expect-coordinator-revision "$PRISM_CAPACITY_COORDINATOR_REVISION" \
  --expect-coordinator-image-digest "$PRISM_CAPACITY_COORDINATOR_IMAGE_DIGEST" \
  --expect-postgres-server-version "$PRISM_CAPACITY_POSTGRES_SERVER_VERSION" \
  --expect-database-profile-sha256 "$PRISM_CAPACITY_DATABASE_PROFILE_SHA256" \
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
  --expect PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS="$PRISM_STRATUM_VARDIFF_IDLE_SWEEP_SECONDS" \
  --expect PRISM_SHARE_COMMIT_BATCH_SIZE="$PRISM_SHARE_COMMIT_BATCH_SIZE" \
  --expect PRISM_SHARE_COMMIT_LINGER_MILLISECONDS="$PRISM_SHARE_COMMIT_LINGER_MILLISECONDS" \
  --expect PRISM_SHARE_COMMIT_TIMEOUT_SECONDS="$PRISM_SHARE_COMMIT_TIMEOUT_SECONDS" \
  --expect PRISM_STRATUM_SEND_TIMEOUT_SECONDS="$PRISM_STRATUM_SEND_TIMEOUT_SECONDS"
```

Compose mounts the host evidence path read-only at
`/run/qbit-prism/capacity-evidence.json`; the coordinator validates that fixed
container path before opening Stratum in production. Runtime validation checks
the immutable subject, policy, configuration, reconciliation, and durability
contract with freshness enforcement disabled, so a routine restart does not
become a time bomb after the deployment window closes. The host pre-deployment
gate always enforces freshness. A missing bind source is not created
automatically.
