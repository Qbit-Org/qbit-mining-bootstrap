# Migrate Prism to Rust and multiple instances

The Rust server replaces all Python Prism runtime and operator scripts from the
`2.x.x` branch. Stratum, public/audit HTTP routes, signed audit formats, payout rules, and existing
PostgreSQL accounting history remain compatible. The runtime uses native threads
and allows multiple frontends to write to one shared PostgreSQL database.

This is a coordinated cutover. **Do not run Python and Rust coordinators against
the same database at the same time.** The additive native migration checks the
legacy writer lease and prevents a Python process from acquiring it afterward.
Stopping the Python services and retaining a recoverable backup are required
before migration.

## Prepare the deployment

1. Pin the source revision and native image digest. Build and test the native
   binary and validate the reviewed environment with `qbit-prism-server
   check-config`. Keep the same manifest signing seed, ledger attestation seed,
   trusted ledger public key, chain, payout policy, pool fee, and CTV policy.
2. Choose the shared PostgreSQL writer endpoint. All frontends need access to
   that same database and a synchronized qbitd on the same chain. Configure TLS,
   credentials, connection limits, and replication through the database's
   existing operating procedures.
3. Give each frontend a unique `PRISM_INSTANCE_ID` or use generated UUIDs.
   Configure `PRISM_RUNTIME_WORKERS`, `PRISM_JOB_BUILD_EXECUTOR_WORKERS`, and
   `PRISM_DATABASE_MAX_CONNECTIONS` per host. Budget total database connections
   across all frontends and administrative clients.
4. Record a baseline: latest accepted `share_seq`, accepted share count, pool
   blocks and pending candidates, carry-forward integrity report and
   `audit_head_sha256`, CTV states, and representative audit SHA values. Save
   canonical audit downloads and independently verify them against the trusted
   ledger key and on-chain coinbase.
5. Back up external audit bodies **and every referenced segment**, including
   legacy compact body refs and v2 proof-body segments. A live evidence envelope
   alone is not an audit backup. Preserve recorded file paths or plan a mount at
   the original absolute path for import. Include the `2.x.x` canonical
   `prism-audit-bundle-canonical-*.json.gz` sidecars.

## Drain and migrate

Remove the Python frontends from new connection routing, stop the Python
coordinators, public read services, and standalone CTV broadcaster daemons, and wait for admitted
writes and candidate accounting to finish. Inspect pending candidate state and
resolve it while the old runtime is still available. Confirm every old process
has stopped and its writer lease has been released or expired. A paused old
process is not a completed shutdown.

Take the final consistent database backup after draining, along with the audit
filesystem backup and securely retained key/configuration recovery material.
Retain the old pinned image for recovery. Rehearse restoring this backup into an
isolated database before the production change.

Run the following with the reviewed operator environment exported, using the
new executable and the existing database:

```sh
qbit-prism-server check-config
qbit-prism-server migrate
qbit-prism-server import-audits --root /var/lib/qbit-prism/audit
qbit-prism-server backfill-ctv
```

`migrate` applies the existing schema and additive native migration under a
transaction lock. It preserves share sequence/history, balances, audit metadata,
and settlement rows. New tables hold shared configuration/revision, instance
heartbeats, expiring jobs, immutable audit snapshots, and durable claim state.
Migration 3 retains `2.x.x` publication ordinals, retained worker difficulty,
hashrate rollups, and their watermark. The base schema and native migrations
apply in one transaction, including carry-forward summary repair.
The migration is idempotent; a refusal due to an old active writer or unresolved
legacy work must be resolved before admitting native traffic.

Specifically, a pending legacy candidate without the native bundle/hash/revision
fields cannot be replayed by Rust. Migration refuses it transactionally without
changing the schema. Drain it through the pinned legacy submitter first, then
repeat the backup and migration boundary. Do not delete pending rows merely to
bypass this check.

`import-audits` processes database rows whose audit body is external. It resolves
full v1/v1.1 bodies, legacy body refs, and v2 proof bodies, verifies segment
references, the trusted ledger signature, recorded coinbase, and canonical
bundle hash, then stores the logical body in PostgreSQL. Original files and
published hashes are retained. Canonical gzip sidecars are verified against
their uncompressed hash and stored byte-for-byte in the database. Content-addressed
HTTP responses preserve these exact bytes. This makes history available on every frontend
without a shared filesystem.

`--root` resolves relative body paths and restricts imports to that directory;
it does **not** rewrite an absolute URI to a different root. Mount old absolute
paths at their recorded locations. Keep referenced segment paths readable as
well. Import failure is a reason to repair the missing/corrupt artifact before
continuing; a matching hash without recoverable bytes is insufficient.

`backfill-ctv` scans verified stored bundles and restores missing fanout records.
Run import first. It is idempotent for matching existing records and replaces the
old Python repair command's individual-path/block selection interface.

## Bring up native instances

Start one instance first and verify readiness before admitting controlled miners:

```sh
qbit-prism-server run
# In a separate shell with the same environment:
qbit-prism-server healthcheck --url http://127.0.0.1:3341/healthz
qbit-prism-server self-check
```

For the supplied external database Compose overlay:

```sh
# Public reads may use the writer, or a separate standby endpoint with mode=require.
export PRISM_PUBLIC_DATABASE_URL="$PRISM_DATABASE_URL"
export PRISM_PUBLIC_REPLICA_MODE=off
docker compose -f compose.yaml -f compose.prism-external-db.yaml \
  --profile prism up -d qbitd prism-coordinator prism-public-api
```

Set `PRISM_DATABASE_URL` to the external writer endpoint on every host. The
overlay removes dependencies on the local primary and replica containers.
Set `PRISM_PUBLIC_STRATUM_URL` to the advertised mining endpoint. The public
service uses its own read pool and does not require signing keys or an audit
filesystem mount. To read a standby, set `PRISM_PUBLIC_DATABASE_URL` to it and
`PRISM_PUBLIC_REPLICA_MODE=require`; health then requires a recovering server
and a fresh replication stream. Replay lag is reported separately.
Use an up-to-date Docker Compose release supporting `!override`. Production
operators should combine their pinned-image/storage production configuration
with this overlay **last**, and render `docker compose ... config` before start.

`QBIT_RPC_HOST` is overridable. With an already managed external qbitd, set its
RPC connection and start only the coordinator with `up -d --no-deps
prism-coordinator`. The local qbitd dependency then does not launch another node.
The `make up-prism-pool` helper manages the local node/database/public-service stack; use the
explicit Compose overlay commands for the external database topology.

Add the remaining instances with identical signing and payout configuration.
Keep TCP connection routing stable while a miner is connected; reconnects may
land on another instance. Existing TCP sessions do not move between processes.
The shared extranonce allocator, durable job records, proof deduplication, and
canonical ledger provide cross-instance accounting continuity.

Check these before restoring ordinary traffic:

- Every frontend is healthy and follows the intended qbit chain/tip.
- Controlled miners subscribe, authorize, receive difficulty/jobs, submit shares,
  and recover after reconnect or a frontend restart.
- Database share order and counts reconcile with unique committed proofs;
  duplicate submission on another instance gives no second credit.
- Baseline audit downloads have the same canonical hashes; carry-forward
  integrity and CTV state agree with the pre-migration records.
- Public dashboard and audit routes return the expected data on every instance.
- Candidate and CTV claim recovery work after a process interruption.

A subsequent rollout between compatible **Rust** versions may drain and replace
one frontend at a time. Review each version's schema compatibility separately.

## HA durability and failover

Multiple frontends remove dependence on one Prism process. The database still
has one authoritative primary/writer endpoint. All accounting writes and reads
must reach that endpoint, rather than independent primaries or lagging read
replicas. The separately served public dashboard may use a checked read replica;
it never makes accounting or settlement decisions.

Keep `fsync=on`, `full_page_writes=on`, and `synchronous_commit=on`. The last
setting makes a local commit durable but does not create replication. If the
requirement is no loss of acknowledged shares after primary storage loss,
configure synchronous replication and promote only a standby holding all those
durable commits. An asynchronous failover can lose already acknowledged work.
Avoid silently degrading the required synchronous policy during an outage.
The bundled public-read replica uses asynchronous replication by default; its
presence alone does not provide this lossless accounting failover guarantee.

Exercise the actual database failover endpoint under controlled mining load.
Reconcile ACKed proof IDs with the promoted database and inspect retry/dedupe,
payout, and candidate outcomes. Use independent backups and WAL archives even
when replication is synchronous; replicas can reproduce operator mistakes.

The repository also includes a disposable physical PostgreSQL failover test:

```sh
PRISM_TEST_PG_BIN_DIR=/usr/lib/postgresql/16/bin \
  cargo test --locked -p qbit-prism-server --test postgres_failover -- --nocapture
```

It checks two ledger clients' acknowledged IDs through a stable TCP proxy,
synchronous standby promotion after immediate primary loss, duplicate replay,
and continued writes. Run it with installed PostgreSQL server tools as an
unprivileged user. This test complements production-specific failover drills.

## Configuration and observable changes

Retained interfaces include miner username syntax, BIP310 version rolling,
vardiff and high-difficulty listeners, the payout/CTV policy variables, public
`/public/v1` routes, audit route aliases, canonical bundle hashes, and legacy
`*_SATS` monetary aliases. Keep explicit mainnet share/vardiff bounds. As on
`2.x.x`, mainnet permits bounded one-parent stale-share grace (default 3 seconds).
The grace interval starts when that client receives replacement work; stale
blocks are never submitted. Retained vardiff hints are shared by listener and
exact username, with accepted-work evidence controlling their expiry.

Mainnet `check-config` and `run` require `QBIT_EXPECTED_GENESIS_HASH` to contain
the trusted 64-hex genesis hash. Startup compares it to the connected node;
production flags reject regtest. Public-chain readiness also requires completed
initial block download, matching block/header heights, and at least
`PRISM_MIN_PEERS` connected peers (default 1). Templates must satisfy
`PRISM_TEMPLATE_MAX_AGE_SECONDS` (default 120). A failed readiness observation
closes mining readiness until a fresh valid poll succeeds.

General node and wallet RPCs use `PRISM_RPC_TIMEOUT_SECONDS` (15 seconds).
`PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS` (1 second) bounds only `submitblock`;
ambiguous submission results retain the durable candidate for reconciliation.
Wallet selection preserves any configured `QBIT_RPC_URL` proxy path prefix.
The independent public service retains its separate public read deadline.

Canonical payout changes retire prior jobs even when the parent tip is
unchanged. Miners receive replacement work with `clean_jobs=true`; ordinary
same-tip refreshes keep valid retained jobs, and previous-parent share grace
remains bounded by each connection's notification time.

CTV fee policies, including explicit rates, are checked against the node's live
`minrelaytxfee` and `mempoolminfee` before building payout artifacts. A configured
rate or discounted premium below these floors fails the build; correct the fee
policy before admitting mining work.

Removed implementation settings include Python writer IDs/epochs/session leases,
share batch/linger queues, psql/native-client selection, subprocess builders,
incremental window schedulers, refresh rollout gates, and their watchdog knobs.
Remove them from the operator environment; they do not tune the Rust runtime.
The supported settings are documented in [.env.example](../.env.example) and the
[native server README](../crates/qbit-prism-server/README.md).

The combined audit/operator HTTP port remains 3341; the separate native
`public-api` role preserves public port 3342. `/healthz` retains its readiness role;
process metrics describe native work rather than the old Python scheduler.
Update alerts that depended on removed internal queue/lease/refresh series.
Dashboard totals continue to derive from the shared database. The native
`self-check` emits structured JSON and fails nonzero on an error instead of
printing the Python checker's old PASS/WARN/FAIL table.

The newer block-marker, dual-rate chart, network-hashrate, and reorg-view
contracts are retained. Native block intents are persisted before acceptance,
so the default active block view, chart markers, and found-block totals include
confirmed blocks only. Use `chain_state=all` to inspect every candidate with its
explicit state. Previously landed blocks that disconnect appear as `reversed`
with their disconnection time, while remaining eligible for native reactivation.
Earnings and payout histories remain confirmed-only.

Hashrate history uses the existing three rollup grains and raw tail. Any
frontend may advance the shared watermark; an atomic comparison prevents
concurrent passes from double-counting. `PRISM_HASHRATE_ROLLUP_ENABLED`,
`PRISM_HASHRATE_ROLLUP_INTERVAL_SECONDS` (15), and
`PRISM_HASHRATE_ROLLUP_BATCH_SHARES` (50000, maximum 100000) retain their roles.

Bootstrap now pays the solver only when there are no historical shares; there
is no three-miner readiness gate. A network-valid proof below its assigned share
target is credited at network difficulty only after its block is confirmed on
the active chain. These deliberate accounting simplifications should be
included in operator/miner rollout notes.

## Recovery and rollback

Before native traffic is admitted, a failed cutover can be recovered by restoring
the complete pre-migration database and artifact backup and restarting the
pinned old image in isolation from the migrated database. Do not drop native
migration guards and point Python at the changed live schema.

After native shares have been acknowledged, restoring an older database loses
those accepted records. Prefer repair or a compatible native binary while
preserving the latest durable state. Any rollback that discards acknowledged
history requires an explicit accounting reconciliation and recovery decision;
it is not an ordinary image rollback. Keep every native frontend stopped while
performing an isolated restore/recovery operation against its replacement DB.
