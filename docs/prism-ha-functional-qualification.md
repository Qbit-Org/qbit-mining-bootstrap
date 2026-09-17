# Bounded HA functional preparation for #281

This procedure reuses the existing HA overlay, readiness harness, managed
PostgreSQL driver and Coordinator/Stratum tests. It prepares the three remaining
[#281](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/281) live gates.
**A local pass does not close any of those gates**, establish D1 capacity, or
replace the owner-coordinated [#291](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/291)
rehearsal. Do not run a competing full rehearsal or change acceptance thresholds.

## Inventory and exact remaining acceptance

| Existing capability | What it actually provides | Remaining live evidence |
| --- | --- | --- |
| [#304](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/304), `compose.prism-ha.yaml` | Two frontend services, distinct IDs/ports, per-frontend RPC wiring, one writer DSN; four-stack CI renders and self-check instance census. | Start the **actual overlay** with two healthy frontends; retain work issued by A, reconnect through the operator endpoint to B and accept that original share exactly once. A new job after reconnect alone is insufficient. |
| [#331](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/331), `prism_ha_readiness_probe.py` | Deterministic health classification and rise/fall state machine, with no network I/O or routing. | Observe the **operator TCP endpoint** preserve ordinary tip/payout rebuilds, eject sustained unavailability within its stated bound, reject new admission when all backends are down, and require recovery successes. |
| [#449](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/449), HA reference | Approved D3: one primary and **one dedicated asynchronous failover standby**; public reads use a separate replica. Documents fencing, stable writer authority, reconciliation and primary-side alerts. | Run #291's overlay failover drill under mining load: positively fence all old writer connections, promote the eligible standby, move the stable endpoint, reconcile ACK/accounting and record replication gap/loss, recovery time and alert delivery. |
| `compact_runtime_e2e` | Real Coordinator methods, Stratum sockets, small PG-backed windows, controlled local RPC node, original issued-work authority/expiry and unknown-commit coverage. | Its passing socket resume test is functional evidence; it neither launches Compose nor uses an operator LB or a real qbitd. |
| `qbit-prism-load::cluster::ManagedPostgres` | Disposable PG primary and named async/sync standby, explicit durability and physical slot. | The existing load driver has no operator frontend LB or managed promotion phase. This preparation does not replace it with a new mining simulator. |
| `postgres_failover.rs` | Existing synchronous ledger-loss/reconnect fixture. | Its zero-loss assertion belongs to that synchronous fixture and cannot be applied to approved asynchronous D3. |

The authority for the deployment settings and unchanged live acceptance is the
[HA reference](prism-ha-reference-architecture.md#281-acceptance-evidence).
The bundled public-read replica is **not** the dedicated failover candidate.

## Resource contract

Agree on a slot with concurrent performance work before any build, container or
PG run. Use at most two build jobs. The functional command below creates two
loopback-only PostgreSQL 16 processes, 32 connections per server, seven selected
small existing tests, two ledger pools, one local writer proxy and eleven initial
ledger shares. The existing tests use controlled RPC fixtures on loopback.
No containers, external database, real node, public-read workload or production
access is involved. There is no existing-database URL argument.

The wrapper creates a private, short `/tmp/b281-*` parent and passes it as the
child's `TMPDIR`. Every managed cluster lives below that exact new parent.
It never stops a process/container by name, scans for another user's databases,
or runs global Docker cleanup. Its resource census records the owned parent,
child PID, child exit status and exact PG paths. PG shutdown must be verified
with `pg_ctl status` and disappearance of `postmaster.pid` before deleting data.
Unknown cleanup retains the owned directory and fails the command. A killed
wrapper or machine loss requires inspecting that recorded parent; absence of
an artifact is not proof of cleanup.

## Run the reusable preparation

The first two commands are lightweight and start no services. Compose output is
captured privately, validated using explicit fixture values and reduced to an
allowlist; never print resolved Compose configuration containing real secrets.
The render exercises nondefault IDs, separate RPC URLs, health/high-difficulty listener ports,
loopback host bindings and a shared writer DSN through the real Compose merge.
It does not claim those values reached running frontend containers.

```sh
python3 -m unittest tests.test_prism_ha_qualification tests.test_prism_ha_readiness_probe
python3 scripts/prism_ha_qualification.py render
```

After the resource slot is available, build the example and the **existing**
test executables. Keep Cargo's JSON receipts so the selected binaries are exact;
do not select a possibly stale executable with a wildcard. These are debug
functional builds, not performance measurements.

```sh
B281_BUILD_DIR=$(mktemp -d /tmp/b281-build-XXXXXXXX)
CARGO_BUILD_JOBS=2 cargo build --locked -p qbit-prism-load --example ha_qualification
CARGO_BUILD_JOBS=2 cargo test --locked -p qbit-prism-server \
  --test compact_runtime_e2e --no-run --message-format=json > "$B281_BUILD_DIR/runtime-build.jsonl"
CARGO_BUILD_JOBS=2 cargo test --locked -p qbit-prism-server \
  --lib --no-run --message-format=json > "$B281_BUILD_DIR/ack-build.jsonl"
B281_RUNTIME_TESTS=$(python3 -c 'import json,sys; print(next(r["executable"] for r in map(json.loads,open(sys.argv[1])) if r.get("reason")=="compiler-artifact" and r.get("executable") and r["target"]["name"]=="compact_runtime_e2e"))' "$B281_BUILD_DIR/runtime-build.jsonl")
B281_ACK_TESTS=$(python3 -c 'import json,sys; print(next(r["executable"] for r in map(json.loads,open(sys.argv[1])) if r.get("reason")=="compiler-artifact" and r.get("executable") and r["target"]["name"]=="qbit_prism_server"))' "$B281_BUILD_DIR/ack-build.jsonl")
python3 scripts/prism_ha_qualification.py run \
  --pg-bin-dir "$(pg_config --bindir)" \
  --example-bin target/debug/examples/ha_qualification \
  --runtime-tests "$B281_RUNTIME_TESTS" --ack-tests "$B281_ACK_TESTS" \
  --out "$B281_BUILD_DIR/evidence"
```

Both test executable arguments are required by the wrapper and the example;
omitting either fails argument parsing before a PostgreSQL process can start.
Select a PG16 `--pg-bin-dir` explicitly if `pg_config` names another version.
If `CARGO_TARGET_DIR` is customized, select the example from that build's target
directory too. Use the wrapper so startup failures also receive an owned-resource
census and cleanup; invoking the example alone omits that outer cleanup guard.

`functional/ha-functional.json`, individual test logs under `functional/`,
`functional-process.log` and `resource-census.json` are local artifacts. Preserve them together with the
checkout SHA, build receipts and binary hashes when handing off evidence.
The wrapper has a 480-second execution ceiling plus a bounded termination/cleanup
period; the example bounds its exercise to 180 seconds. Neither extends a share
or issued-job deadline inside the runtime to force success.

## What the functional phase proves

The four selected `compact_runtime_e2e` tests exercise original-work socket
resume with one credited share, stale-publication fencing, expiry during a
blocked read, and reconciliation of an unknown issued-job commit. Three existing
`coordinator::commit_reconcile_tests` exercise a queued append refused at its
original share deadline, a confirmed in-flight commit accepted within grace,
and an unknown answer at deadline plus grace whose commit lands later. The
selected test names and logs are recorded. Missing inputs or selecting zero
tests fails the run; a skipped test is not evidence.

The async ledger phase uses the real `Ledger` and managed PG driver:

1. Validate PG16, `fsync=on`, `full_page_writes=on`, writer
   `synchronous_commit=on`, and exactly one named streaming async standby on
   the **primary**. Both differently named ledger pools use one unchanged DSN
   through a loopback TCP endpoint.
2. Append eight shares and wait for the standby to replay the captured primary
   flush LSN. Stop only that owned standby, then append three more shares.
   Record the successful `Ledger::append` returns and the unreplicated WAL byte
   gap. These returns are **not miner wire ACK measurements**. Time-based replay
   lag while disconnected stays null/unknown, never zero.
3. Stop the exact owned primary and confirm both an already-open writer socket
   fails and a new direct connection is refused. Verify the primary is stopped
   **before** restarting/promoting the dedicated standby. No restart supervisor
   exists for the test primary. This is a process fence for a local fixture;
   production requires the operator's partition-safe fencing authority.
4. Verify recovery before promotion and writer role afterward; only then
   retarget the stable endpoint. The two original pools reconnect without a
   DSN change. Reconcile all eleven acknowledged IDs against the promoted DB:
   eight survive and the deliberately unreplicated three are reported missing.
   Do not retry those missing IDs and relabel them as recovered history.
5. Retry a **surviving** ID and verify no second credit; append one fresh share
   on the new primary. Record fence-to-promotion time and the interval between
   observed successful appends. Mining-endpoint downtime remains unmeasured.
   Capture replication status from the promoted primary, where the lack of a
   replacement standby is explicit. Stop all owned PG processes, close the
   proxy and remove only verified-stopped owned directories.

The injected three-share loss is a positive control showing that reconciliation
detects asynchronous loss. It is neither a production loss forecast nor an
acceptance budget, and must not weaken the full rehearsal's accounting criteria.

## Operator components still required for the live gates

Do not convert this example's writer proxy into a production failover service.
Before an actual overlay run, the rehearsal owner must supply a disposable
isolated Compose network, exact project/container/volume ownership, separate
node wiring, a stable writer endpoint with connection draining, fencing and
promotion authority, the dedicated standby, and the actual operator TCP LB.
Provide any public reader as a separately named member; never route reads to
the dedicated standby. Confirm the render's nondefault settings in each live
`check-config`, health `instance_id`, RPC target and self-check instance census.
Keep container IDs and volumes in the resource manifest before perturbations;
stop/remove only those IDs and verify the final census matches the baseline.

For the TCP gate, retain the real operator configuration and timestamped probe,
routing, connection and submit events. Use the existing
[readiness state machine](prism-ha-readiness-probe-harness.md) as an expected
trace: HTTP 200 **and** JSON `ok: true`, two-second start cadence, one-second
total timeout, fall six and rise two. Exercise new tips and payout revisions,
then prolonged work loss, all-backends-down and recovery. Preserve established
connections during ordinary rebuilds. The current sustained-observation bound
is 13 seconds (28 seconds including the default stale-publication guard); record
actual scheduling and publication settings. A replayed simulator trace cannot
certify real routing. Longer rebuilds require reviewed policy, not a hidden
threshold increase.

For failover, capture offered, miner-ACKed, rejected, unknown/no-response and
committed IDs before and after the authority change, including duplicate retries.
Record pre-loss primary flush and standby receive/replay LSNs, the acknowledged
unreplicated gap, measured downtime and any unmeasurable interval. Preserve the
original ACK deadline across dependent work and keep late durable/unknown
outcomes distinct. Observe the **dedicated standby on the current primary** via
the operator's [PostgreSQL exporter and D3 rules](prism-alert-migration.md#d3-deployment-provided-primarystandby-rules):
healthy streaming, lag, disconnection, stale/missing exporter data and post-promotion
no-standby state. Native public-replica metrics describe a different target and
cannot substitute for this signal. This preparation samples primary SQL only;
it does not deploy or certify exporter refresh, scrape cadence or alert delivery.

If the operator LB, fencing/endpoint component or rehearsal host is unavailable,
retain the reusable preparation and record that component as missing. Leave all
three live criteria open. Report every acceptance row as executed, failed or
unexecuted, with its exact command/artifact and the reason for any gap.
