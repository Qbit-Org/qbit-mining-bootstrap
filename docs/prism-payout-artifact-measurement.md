# Measure native Prism performance

Measure the Rust deployment with the intended payout window, miner population,
database, and frontend count. The removed Python incremental-window scheduler,
subprocess builder, and its event names are not measurement targets for the
native server. A faster synthetic builder result alone does not demonstrate
miner-facing capacity or HA durability.

## Native builder benchmark

The built-in benchmark builds and verifies actual signed audit bundles in
process using synthetic shares:

```sh
cargo run --locked --release -p qbit-prism-server -- benchmark \
  --shares 100000 --miners 100 --iterations 20 \
  --output-json /tmp/prism-native-builder.json
```

Record the commit, binary/image digest, compiler/build mode, CPU allocation,
memory limit, and exact dimensions. Output schema
`qbit.prism.native-builder-benchmark.v1` reports:

- `build_and_verify_p50_ms` and `build_and_verify_p99_ms`
- `canonical_audit_bytes`
- `shares`, `miners`, `iterations`, and `engine`

The timings include native construction and verification. They exclude Stratum
networking, PostgreSQL commits, node RPC, multi-instance contention, and durable
CTV broadcasting. The benchmark uses direct settlement with synthetic data;
measure the configured CTV path in a real integration run as well. A percentile
from few iterations is descriptive, not a well-sampled latency tail.

## Miner-facing load measurement

Use controlled miners or a load generator that submits valid work to the actual
Stratum listeners. Keep load bounded and run against isolated test infrastructure
before a production canary. Measure at least:

| Measurement | Why it matters |
| --- | --- |
| Valid submissions, successful ACKs, unique committed proofs | Establishes throughput and accounting reconciliation |
| ACK p50/p95/p99 and maximum | Captures database, target validation, and routing latency |
| Subscribe/authorize to initial difficulty/job | Detects reconnect admission or delivery stalls |
| Tip observation to fresh job delivery | Measures usable mining work under refresh load |
| RSS and CPU per frontend, peak concurrent builders | Shows actual resource use and window memory cost |
| Database lock waits, pool utilization, statement latency | Reveals the shared database bottleneck |
| WAL bytes and table/index growth | Quantifies durability/storage cost |
| Candidate submit-to-active and active-to-accounted latency | Separates node acceptance from durable settlement |
| Reclaimed candidate/CTV work after interruption | Exercises recovery rather than process uptime alone |

Capture steady state, reconnect bursts, slow database service, a frontend
restart, and actual HA database failover if zero acknowledged-share loss is a
requirement. Reconcile unique ACKed proofs against database identifiers after
in-flight transactions have resolved. Do not count exact duplicate resubmissions
as additional accepted work.

Record the same workload against one and several frontends, with total miner
load and total CPU allocation stated separately. More frontends provide routing
and process redundancy, but every share still crosses one PostgreSQL commit and
ordering boundary. Report measured scaling; do not infer it from thread count.

The retained [optional v2 qualification validator](prism-capacity-readiness.md)
checks a strict evidence record. The repository's synthetic builder benchmark
does not generate a qualification artifact or replace the complete load runner.

## Compare database query costs

If `pg_stat_statements` is enabled in the reviewed database profile, capture
cumulative counters at the start and end of matching observation intervals.
Do not reset shared production statistics. A reset invalidates the subtraction:

```sql
SELECT now() AT TIME ZONE 'UTC' AS captured_at_utc, stats_reset
FROM pg_stat_statements_info;

SELECT queryid, calls, total_exec_time, rows, shared_blks_hit,
       shared_blks_read, temp_blks_read, temp_blks_written, query
FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY total_exec_time DESC;
```

Capture every query row or explicitly retain all relevant ledger, snapshot,
carry-forward, audit, and outbox queries at both boundaries. Absence from a top-N
list does not mean a zero counter. For matching query IDs, compute differences
in calls, execution time, reads, and temporary I/O; interval mean execution time
is `total_exec_time_delta / calls_delta`.

Use the actual native query with representative arguments when inspecting a
plan. `EXPLAIN ANALYZE` executes the query, so use an isolated restored database
for expensive window/audit probes. Keep a statement timeout and read-only
transaction. Native reward windows use eight times network difficulty; the old
Python prefetch's sixteen-times scan is not the current reward policy.

## Storage and audit response measurement

Record `pg_total_relation_size` deltas for the ledger, proof registry, audit
snapshots, non-share audit bodies, job records, and payout/CTV tables. Count
native snapshot references separately from imported legacy inline bundles.
Measure full public audit serialization time and response bytes for large
windows; normalization reduces persistent duplication but the compatible public
response still contains the logical share array.

Verify sampled reconstructed bundles against their canonical SHA, the trusted
ledger public key, and recorded coinbase. A reduced storage footprint is useful
only when old and new audits remain independently reproducible.

## Comparison record

Retain raw measurements and a concise record containing revision/image,
configuration, database profile and durability settings, workload, observation
start/end, frontend/resource counts, latency distributions, unique-commit
reconciliation, storage/WAL growth, and recovery outcomes. State omissions and
sample sizes. Compare matched workloads and label predictions separately from
observed results.
