# PRISM Ledger Operations

This is the operating contract for the native Rust Prism server. The database
retains the original accounting tables and functions while additive migrations
provide active-active application instances. For the upgrade procedure, see
[Rust migration](prism-rust-migration.md).

## Share commit and ordering

All instances insert accepted shares into `qbit_share_ledger` in one PostgreSQL
database. A short transaction-scoped advisory lock orders insertion and snapshot
creation. PostgreSQL assigns the canonical `share_seq`; sequence gaps are
permitted, and sequence order is authoritative. Local arrival timestamps,
frontend counters, and per-worker summaries do not determine reward order.

The submission path validates identity, job, header, and target before durable
admission. A normal successful Stratum response follows the database commit,
with `synchronous_commit=on`. A share meeting the network target also persists
its complete candidate intent atomically with the share. Connection loss after
commit can lose the reply without losing the share.

An exact replay returns the existing share without another reward credit.
Conflicting reuse of a share identifier fails. The native global proof-hash
registry prevents the same newly submitted proof receiving separate credits
under different usernames or on different servers. Historical rows are retained
unchanged during migration, including any duplicates accepted by older code.

There is no Python batch queue or process-wide ledger writer lease. All healthy
instances may append; transaction locks protect a shared ordering boundary.
Database connection and statement/lock limits bound resource use. Session
extranonce allocation also uses the shared database sequence and never cycles.

## Snapshot and payout boundary

`qbit_prism_window(anchor_job_issued_at, window_weight)` selects eligible shares
newest first by `share_seq`, counting a partial oldest share when the requested
weight is reached. Both `job_issued_at` and `accepted_at` must be no later than
the anchor. The audit wrapper fixes the reward weight to eight times network
difficulty.

Native snapshots record a database-coordinated monotonic anchor, share range,
and payout revision. Share acceptance and snapshot creation use the same
ordering lock, so equal wall-clock timestamps or host clock differences cannot
let a later share enter an earlier snapshot. Published jobs bind their snapshot;
later accepted work cannot change their committed coinbase.

Payout-changing operations serialize under the settlement transaction lock.
They advance the shared payout revision, and job publication checks that its
revision remains current. A cluster fingerprint binds genesis, ledger and
manifest public keys, reward multiplier, payout policy, and CTV policy. A
mismatched instance fails startup. Coordinators can use different local resource
limits and synchronized qbit nodes on the same chain.

A pool with no historical shares issues solver-paid bootstrap work. There is no
three-miner gate. A network-valid candidate below its assigned share target is
stored without ordinary share credit. Active-chain confirmation inserts its
deferred share at network difficulty; a losing candidate receives no credit.

## Durable block candidates

`qbit_block_candidate_outbox` stores complete candidate evidence before a node
submission can be lost to a process crash. Workers claim pending rows with
expiring, token-fenced database claims. Multiple instances may process different
candidates; a stale claimant cannot overwrite a successor's accounting result.

The worker submits the preserved block bytes, observes the active chain, and
persists verified block/audit/payout state transactionally. Transient failures
leave durable retry work. A lost RPC reply is resolved by querying the chain or
re-offering the same block; an accepted duplicate does not create another payout.
Terminal candidates retain the evidence needed for replay identity while large
pending payloads can be released. Deferred below-share-target credit is tied to
the same durable candidate lifecycle.

PostgreSQL and qbitd do not share a transaction. Duplicate node offers are
possible after an interrupted attempt; accounting effects are idempotent and
claim-fenced. Do not infer active-chain acceptance from a socket write or a
missing RPC reply.

## Blocks, balances, and reorgs

Core durable tables remain:

| Table | Purpose |
| --- | --- |
| `qbit_share_ledger` | Canonical accepted share history |
| `qbit_pool_blocks` | Pool blocks and active-chain/maturity state |
| `qbit_pool_payout_entries` | Per-recipient payout records |
| `qbit_payout_carry_forward` | Auditable balance deltas |
| `qbit_pool_audit_bundles` | Canonical audit metadata and stored body |
| `qbit_ctv_fanout_sets` | Committed fanout sets |
| `qbit_ctv_fanout_artifacts` | Transactions, maturity, and broadcast state |

Current balances replay active confirmed carry-forward deltas. Zero net balances
need no current row; negative balances remain visible debt offsetting future
rewards. `qbit_carry_forward_integrity_report()` checks stored prior, candidate,
and carry values against replay and publishes `audit_head_sha256`. Preserve that
head with independent release/recovery records.

Coinbase maturity is 1,000 blocks: a height-H payout becomes mature only at tip
height H+1,000 or later. An immature disconnected block is marked inactive, so
its balances stop contributing; it can reactivate. Terminal reversal preserves
audit history and marks payout/carry rows reversed. A mature disconnect sets a
shared fatal state and stops ordinary accounting until investigated. Other
instances must not continue with a different interpretation of that event.

## Audit storage and retention

Native accepted-block audits store the non-share bundle fields and a
`share_snapshot_sha256` reference to `qbit_prism_audit_snapshots`. A snapshot
records the canonical share interval, anchor, count, and digest. Its shares are
reconstructed from the immutable ledger. Bootstrap snapshots may retain their
synthetic share inline. The reader verifies the reconstructed share digest and
canonical bundle SHA before returning a logical v1/v1.1 bundle.

Share UPDATE, DELETE, and TRUNCATE are prohibited. Removing a share could break
both future accounting and already published audit hashes. No supported pruning
or share-compaction command exists. Keep the canonical share history and all
referenced snapshot rows. A future archive design must preserve exact range
reconstruction and verification before relaxing this invariant.

Imported historical external audits retain their verified inline bundle body;
they are not silently rewritten into references to potentially incomplete
legacy share history. Import preserves their published canonical SHA. Legacy
body-ref and v2 segment formats remain supported by the Rust offline loaders.
Back up external bodies and their segments until import and restore validation
are complete, and retain the original backup under the migration retention plan.

## CTV recovery and broadcasting

Committed fanout artifacts are durable database records. Broadcasting requires
a mature active parent and a current claim. Workers record transaction/package
outcomes and retry state so another instance can resume after a crash. Failed,
reorged, or completed artifacts remain visible through the audit/public API.

To reconstruct missing artifact sets from verified database audits:

```sh
qbit-prism-server backfill-ctv
```

Run `import-audits` first for historical file-backed bodies. Backfill verifies
the trusted ledger key, canonical digest, and recorded coinbase before repairing
rows; matching existing artifacts are idempotent. It processes stored audits,
replacing the former Python tool's individual-path and block-filter CLI.

To process a single batch using the normal broadcaster policy:

```sh
qbit-prism-server broadcast-ctv
```

The integrated periodic worker uses `PRISM_CTV_BROADCASTER_ENABLED=1`. An optional
CPFP wallet and fee configuration must be consistent with the intended operating
policy. Durable claims coordinate work across instances; node RPCs may still
receive an identical transaction more than once after a lost reply.

Confirmed fanouts are observed every five seconds until 1,000 confirmations;
afterward the latest deep checkpoint is checked every 60 seconds. A shallow
fanout disconnect returns the transaction to broadcast work. Disconnection of a
deep checkpoint halts the shared cluster for explicit reconciliation.

Without transaction indexing, the broadcaster uses a durable block-scan cursor
and chain anchor. `PRISM_CTV_SPEND_SCAN_BLOCKS` bounds each pass (default 32,
range 1–256); a reorg resets the cursor. The node must retain the historical
blocks needed by that scan. A pruned/unavailable block range cannot be treated
as proof that a fanout is unspent.

For positive CPFP sponsorship, use a dedicated wallet. Broadcasters that create
or recover unsigned packages must reach the same sponsorship wallet RPC service;
unrelated wallets with the same name cannot sign each other's reserved inputs.
Other nodes can replay an already signed package without opening that wallet.
The database reserves
funding before wallet locking and preserves the exact signed child before
submission. A replacement process recovers the reservation and replays that
same package rather than selecting fresh funding after a lost reply. Funding
is not unlocked until the node observes it spent, including a mempool spend.
Automatic replacement fee bumps and abandoned-reservation release are not
implemented; retain and reconcile the durable reservation when handling those
cases manually.

## HA database and shutdown

Point every instance at the same writable primary endpoint. Do not route ledger
queries to a lagging read replica or distribute writes among independent
PostgreSQL primaries. Sum `PRISM_DATABASE_MAX_CONNECTIONS` across frontends and
reserve capacity for migrations, monitoring, backup, and failover administration.

The cluster records its highest observed cumulative chain work. A node that is
still synchronizing, follows a lower-work tip, or disagrees at equal work cannot
advance accounting. Share commits are fenced by the current revision as well.
This prevents a lagging node from reversing another frontend's accepted blocks.
For manual regtest invalidation/reconsideration, extend the intended branch past
the previous work record before expecting the pool to resume; do not clear the
record to accommodate a lagging production node.

Keep PostgreSQL `fsync=on`, `full_page_writes=on`, and
`synchronous_commit=on`. A local durable commit protects against a coordinator
crash. Zero acknowledged-share loss on database-primary failure additionally
requires synchronous standby flush and a promotion policy restricted to a
standby containing acknowledged commits. An HA endpoint does not establish this
by itself. Test primary failure and client reconnection using the actual
replication, proxy, and storage configuration.

SIGTERM closes listener admission and asks tasks to drain before the database
pool closes. The native server bounds shutdown drain to 30 seconds; unfinished
candidate/CTV intents remain in PostgreSQL and become reclaimable after their
claims expire. There is no legacy writer-lease release barrier. Observe each
frontend's health before restoring traffic after a restart.

Backups require the database, signing-key recovery material, and any unimported
external audit bodies/segments. Use independent base backups plus WAL archives
for point-in-time recovery; replication is not a replacement for backups.
Restore into isolation and verify share order, audit hashes, carry-forward
integrity, CTV state, and API reads before declaring recovery complete.

## Health, diagnostics, and validation

`/healthz` returns 200 only when the process has fresh work for its observed tip
and current payout revision and job delivery can progress; otherwise it returns
503. The HTTP handler reads a published snapshot and fails closed when that
snapshot becomes stale. Database or node outages therefore cannot keep an old
green response indefinitely.

`/metrics` exports native process health, accepted/rejected share and block
counters, runtime workers, connections, pending builds, current-work delivery
coverage, and delivery outcomes. Scrape every instance with its own label;
process counters reset after restart. Dashboard accounting is read
from PostgreSQL across instances. Detailed Python queue, writer lease, watchdog,
and incremental-refresh metrics no longer describe this runtime.

The coordinator (`run`) serves the last complete metrics publication and adds
three gauges at scrape time:

| Gauge | Meaning |
| --- | --- |
| `qbit_prism_metrics_snapshot_available` | `0` before the first publication; `1` afterward, including when stale |
| `qbit_prism_metrics_snapshot_stale` | `1` when missing or older than the freshness budget; otherwise `0` |
| `qbit_prism_metrics_snapshot_age_seconds` | Monotonic age in seconds; `-1` before the first publication |

The coordinator uses the same budget as `/healthz`:
`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15)` seconds. The setting defaults to
2 seconds, giving a 15-second budget. Once the age exceeds that budget, a scrape
sets `qbit_prism_health_state` to `0` while retaining the other cached samples.
Scraping neither renews the publication age nor queries the database.

Both `run` and `public-api` return HTTP 200 for `/metrics`, including missing
and stale observations. Inspect the freshness signals and `/healthz` rather
than treating a successful scrape as readiness. GET and HEAD responses carry:

- `Cache-Control: no-store`.
- `X-Prism-Metrics-State: fresh`, `stale`, or `unavailable`.
- `Age`: elapsed whole seconds, rounded down, including `0`; omitted when unknown.
- `Warning: 110 qbit-prism "metrics snapshot is stale; serving last complete payload"`
  only when the state is `stale`.

The public role derives these headers from its existing readiness probe age,
also exported as `qbit_prism_public_ledger_probe_age_seconds`. Its budget is
`max(3 * PRISM_PUBLIC_READINESS_PROBE_INTERVAL_SECONDS, 15)` seconds; the probe
interval defaults to 5 seconds. Before the first completed probe the state is
`unavailable`. A recent failed probe is still `fresh`, with
`qbit_prism_public_ledger_ready 0`; freshness does not mean the database is ready.
The public role keeps its existing metrics body, without the coordinator's
three snapshot gauges.

Useful checks:

```sh
qbit-prism-server check-config
qbit-prism-server healthcheck --url http://127.0.0.1:3341/healthz
curl --silent --show-error --include --max-time 5 http://127.0.0.1:3341/metrics
qbit-prism-server self-check
bash test/prism-native-tests.sh
QBITD_BIN=/path/to/qbitd bash test/prism-native-tests.sh live
```

The database test wrapper starts a private local cluster unless
`PRISM_TEST_DATABASE_URL` is supplied. Live tests add actual qbitd regtest and
bounded CPU mining. Use an isolated database for tests. The native builder
benchmark measures CPU build/verify work, not end-to-end accepted-share capacity;
see [measurement](prism-payout-artifact-measurement.md) and
[optional qualification](prism-capacity-readiness.md).

The physical failover test uses disposable PostgreSQL primary/synchronous
standby processes and two ledger clients through a stable TCP endpoint. It
checks survival of acknowledged IDs, deduplication, and resumed writes after
immediate primary loss and promotion:

```sh
PRISM_TEST_PG_BIN_DIR=/usr/lib/postgresql/16/bin \
  cargo test --locked -p qbit-prism-server --test postgres_failover -- --nocapture
```

The test skips unless that server-tool directory is provided. Its test proxy
and promotion sequence do not replace validation of a production HA manager.
