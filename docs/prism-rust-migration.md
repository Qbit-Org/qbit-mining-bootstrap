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
hashrate rollups, and their watermark. Migration 6 pins the accepted `2.x.x`
source schema, records what was migrated, and declares the schema capability
every later start checks. The base schema and native migrations apply in one
transaction, including carry-forward summary repair. The migration is
idempotent; a refusal due to an old active writer, an unsupported source
schema, or unresolved legacy work must be resolved before admitting native
traffic.

### Supported 2.x.x source schemas

The minimum supported source release is **v2.0.1** (`95ffe06`). A v2.0.0
(`f6854a0`) database has the identical schema and migrates as the same source
state, but drain it with the v2.0.1 or later image, because the offline
recovery command below first shipped in v2.0.1. The newest supported source is
**v2.0.2** (`504846c`, #258). The SQL those releases applied is frozen
byte-for-byte under `crates/qbit-prism-server/tests/fixtures/schema_2x/`, and
the migrator classifies the database before it runs any DDL:

| Source state | Evidence | Verdict |
| --- | --- | --- |
| pre-#258 (v2.0.0, v2.0.1) | `001_share_ledger.sql` only: no `qbit_prism_schema_capabilities`, no `002_candidate_bodies.sql` object | accept after the drain check |
| #258 applied (v2.0.2) | `candidate_storage_version = 2` and every `002_candidate_bodies.sql` object present | accept after the drain check |
| partial 002 | some 002 objects or the capability row, but not all (v2.0.2 applies 001 and 002 as two script calls, and a restart between them leaves this) | refuse, naming the missing object; finish 002 with the v2.0.2 release (`PRISM_POSTGRES_INIT_SCHEMA=1`) or restore the backup |
| newer | `candidate_storage_version > 2`, or a capability this release does not know | refuse before any DDL; a newer PRISM release wrote the database |

`3.x.x` never applies `002_candidate_bodies.sql` and does not import #258's
chunked candidate bodies. On a #258 source it keeps the 002 tables, triggers
and the capability row untouched; native writers store version 1 JSONB
candidates, which 002's dual-format rule accepts.

**#258's rollback floor.** Once a `storage_version = 2` outbox row exists, a
writer that predates v2.0.2 must not run against the database (see the header
of `002_candidate_bodies.sql`). The same floor applies to this cutover from the
other side: `3.x.x` cannot replay a v2 row either, so every v2 row must be
drained by the v2.0.2 coordinator before migration. Never roll a #258 database
back below v2.0.2 to drain it.

**The drain requirement covers v1 and v2 rows.** A pending v1 candidate
(JSONB body without the native `payout_revision`, `bundle` and `block_hash`
fields) and a pending v2 candidate (`candidate` NULL, body in the chunk
tables) both cannot be replayed by Rust. `migrate` refuses them
transactionally, without changing the schema, and names the blocking rows:

```
legacy Python block outbox is not drained: 2 pending 2.x.x candidate row(s) cannot be
replayed natively (block_hash=... storage_version=1 ...; block_hash=... storage_version=2 ...).
Drain them with the pinned 2.x.x release before migrating: ...
```

The check reads outbox rows, not the capability row: 002 declares
`candidate_storage_version = 2` whatever the writer stored, so the row proves
002 ran, not that v2 work is pending. Drain with the pinned `2.x.x` image:

1. Start the `2.x.x` coordinator (v2.0.2 for a #258 database, v2.0.1 or later
   otherwise) and let its block submitter finish every pending candidate; it
   replays every durable pending row on start.
2. For a block that is already accepted on the active chain but cannot complete
   through normal replay, run the `2.x.x` offline recovery command from the
   `2.x.x` image, with the managed coordinator stopped:

   ```sh
   python3 -m lab.prism.recover_pending_blocks --block-hash "$HASH"          # plan
   python3 -m lab.prism.recover_pending_blocks --block-hash "$HASH" --apply  # drain
   ```

   That command was removed from `3.x.x` with the Python runtime; it only
   exists in the `2.x.x` image.
3. Confirm `qbit_block_candidate_outbox` has no `pending` rows, then repeat the
   backup and migration boundary. Do not delete pending rows merely to bypass
   this check.

A v2 row that reaches the native claim lane anyway (one written after the
drain, for instance) is parked, not retried: the lane records why in
`last_error`, releases the claim, and sets `next_attempt_at` to `infinity`, so
no lease expiry offers it again. Find parked rows with
`SELECT block_hash, storage_version, last_error FROM qbit_block_candidate_outbox
WHERE state = 'pending' AND next_attempt_at = 'infinity'` and drain them with
the `2.x.x` image as above; resetting `next_attempt_at` re-offers a row.

**What was migrated.** After a successful migration
`qbit_prism_migration_source` holds one row: the accepted source state
(`pre_258`, `258_applied`, `fresh`, or `native` for a database that was
already on the Rust schema), the `2.x.x` release and commit that source
corresponds to, the capability value the source declared, the schema version
before this migration, and which instance migrated it. `migrate` prints it,
every start logs it, and a repeated `migrate` never rewrites it.

**Startup gate.** Every start reads `qbit_prism_schema_migrations` and
`qbit_prism_schema_capabilities`, with or without
`PRISM_POSTGRES_INIT_SCHEMA`. A database below schema version 6, above it, or
declaring a capability or `candidate_storage_version` this release does not
understand is refused at connect, naming the required version, before any
accounting statement runs. With the native default
`PRISM_POSTGRES_INIT_SCHEMA=0` that means a newer binary refuses to start until
`migrate` has run, instead of failing later in the claim path.

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
one frontend at a time. Review each version's schema compatibility separately:
a release that adds a migration requires its schema version exactly, so run its
`migrate` with every frontend stopped and start the new binaries afterwards.

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

Native submit classification also preserves the published-tip authority used by
`2.x.x` at `95ffe063846d51f83999a66cc654da5f7476fdef`. A detected tip immediately
fences block candidates. Miner share credit continues against the last published
work while replacement is prepared, including when stale grace is zero. Its
ordinary freshness budget is `PRISM_SUBMIT_TIP_MAX_AGE_SECONDS` (default 10);
zero forces a live tip RPC for each share. A detected replacement may extend
published authority through `PRISM_TEMPLATE_REFRESH_FAILURE_EXIT_SECONDS`
(default 120), measured from the first departure. Repeated detections and failed
refresh attempts do not restart that deadline. Once both budgets have expired,
submit falls back to the node; RPC failures remain backend-unavailable.

Successful work publication opens one-parent stale grace after the startup
baseline. The deadline starts separately for each connection's first delivery
of that tip, and same-tip refreshes do not slide it. An undelivered replacement
keeps eligible retained work in a bounded per-connection graveyard;
absolute reconnect-job expiry is never extended. Share credit retains the
original issued worker, target, network difficulty and policy. Both ordinary
published-work credit and stale-grace credit still commit under the current
transactional payout revision. These are restorations of miner behavior, with
ungated real-coordinator decision and socket/session regression tests; they do
not relax current-chain candidate submission checks.
See the [miner decision parity reference](prism-b8-miner-parity.md) for retained
work bounds, the regression coverage map and disposable qualification commands.

`PRISM_USERNAME_FALLBACK_ADDRESS` applies when validation explicitly identifies
an invalid address or a recognized address type that Prism cannot pay. RPC
failures and malformed validation responses reject authorization without
substituting another payout recipient.

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
Candidate claims renew throughout processing, including waits for build workers.
Losing the lease cancels the attempt; the durable row remains recoverable.
Fresh candidates take priority over retries, with periodic oldest-due selection
to keep older recovery work moving.
Wallet selection preserves any configured `QBIT_RPC_URL` proxy path prefix.
The independent public service retains its separate public read deadline.

Canonical payout changes retire prior jobs even when the parent tip is
unchanged. Miners receive replacement work with `clean_jobs=true`; ordinary
same-tip refreshes keep valid retained jobs, and previous-parent share grace
remains bounded by each connection's notification time.
Reauthorizing a connection preserves the original payout identity of retained
work. When `PRISM_STRATUM_MAX_CONNECTIONS_PER_USERNAME` is enabled, that work
also keeps its original username's capacity slot until it expires or is discarded.

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
Migrate deployed alerts using the [native inventory](prism-native-metrics.md)
and [complete alert migration and deployment diff](prism-alert-migration.md).
Queue-pressure intent moves to `qbit_prism_share_ack_seconds` and
`qbit_prism_rejections_total{reason_id}`; Python lease wake delay moves to
`qbit_prism_runtime_lag_seconds`, retained `qbit_prism_runtime_poll_lag_seconds`
and `qbit_prism_runtime_task_stalled`. Refresh impact uses
`qbit_prism_stratum_oldest_pending_initial_job_seconds` and
`qbit_prism_stratum_current_tip_coverage_gap_seconds`; pending-candidate age and
count retain their names. Guard body-based rules with #277's
`qbit_prism_metrics_snapshot_available` / `qbit_prism_metrics_snapshot_stale`
and database/RSS rules with `qbit_prism_collector_available` so unknown -1 is
never healthy zero. First-offer and advisory-lock histograms are declared with
rules deferred to A/#266 and #283. D3's dedicated standby alerts require the
primary's deployment-provided PostgreSQL exporter, not public read replica data.
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
the active chain. These deliberate accounting simplifications were approved
under decision D2 in #260, and belong in the operator release notes. Their
payout effect is recorded in
[Payout differences from 2.x.x (decision D2)](#payout-differences-from-2xx).

`2.x.x` also read `PRISM_STRATUM_SHARE_WEIGHT` (default 1) and
`PRISM_STRATUM_SHARE_WEIGHTS_JSON` (default empty), a JSON object keyed by miner
username or payout address. A worker with an entry in that object was credited
`max(1, override)` instead of its share target's difficulty; the default weight
only filled the job's delivered `share_weight` field. `3.x.x` does not support
these per-worker credited-difficulty overrides. With the `2.x.x` defaults, which
set no overrides, both versions credit a share at its share target's difficulty,
so crediting is unchanged. A deployment that set either variable must remove it
before cutover, because `3.x.x` ignores it.

<a id="payout-differences-from-2xx"></a>

## Payout differences from 2.x.x (decision D2)

`crates/qbit-prism/fixtures/vectors/` holds money-path vectors exported from
`2.x.x` at `504846cc0b72e8f86ed17f896d4ccbbe196a31dc`. They were computed by the `2.x.x` engine
and rule code, not by hand. `crates/qbit-prism/tests/money_path_vectors.rs`
replays every case through this engine. Window clipping, carry-only recipients,
pool fees, dust recompute, remainder ties, and CTV chunking match `2.x.x`
exactly. Every other difference is one of the entries below, under decision D2 of issue #260. Each
differing case stores both the `2.x.x` and `3.x.x` payout and names its entry
by anchor. The test fails if the anchors the vectors use differ from the
entries below, and it pins every vector file's sha256, so a vector changes only
through a re-export in a reviewed commit that updates the pin. All amounts are
in sats.
All vectors use the day-one floor of 14720 sats.

<a id="d2a-bootstrap-pooling"></a>

### D2a: bootstrap pooling

- **Vector:** `bootstrap_transition.json`, case
  `below-gate-with-other-miners-shares`.
  - miner-a has a 30-difficulty share and miner-b a 20-difficulty share in the ledger.
  - miner-b solves a 500000000-sat block.
- **2.x.x:** miner-b is paid 500000000.
  - Two distinct miners is below the `PRISM_MIN_READY_MINERS` gate (default 3).
  - So the job is a collection job: one synthetic solver share, and the solver
    is paid the whole coinbase.
- **3.x.x:** miner-a is paid 300000000 and miner-b 200000000.
  - The window is proportional as soon as any share exists.
- **Reason:** `crates/qbit-prism-server/src/coordinator.rs` (bundle selection
  after the ledger snapshot) builds the solver-only bundle only when
  `snapshot.shares` is empty. There is no readiness gate.
- **Matching cases:** `at-readiness-gate` (three miners, prior balances kept on
  both) and `empty-ledger` (the solver is paid everything on both) match `2.x.x`.
- **D2 item:** bootstrap pooling.

<a id="d2b-below-target-credit"></a>

### D2b: below-target block credit

- **Vector:** `below_target_credit.json`, cases `block-only-proof-accepted`,
  `block-only-proof-confirmed-after-reconciliation`, and
  `block-only-proof-reorged-after-acceptance`.
  - The listener floor holds the share target at difficulty 4000000.
  - Network difficulty is 1000000.
  - miner-a submits a proof that meets the network target but not the share
    target.
- **Credited amount:**
  - **2.x.x:** the assigned share difficulty, 4000000.
  - **3.x.x:** network difficulty, 1000000.
- **Next block's payout** (8000000 window; miner-b has 2000000 + 2000000, miner-c 1000000):
  - **2.x.x:** miner-a 250000000, miner-b 187500000, miner-c 62500000.
    - The 9000000 of credited work overfills the window.
    - miner-b's oldest share counts only 1000000.
  - **3.x.x:** miner-a 83333334, miner-b 333333333, miner-c 83333333.
    - The window holds 6000000.
- **Timing:**
  - **2.x.x:** credits at node acceptance of the block.
  - **3.x.x:** credits inside the transaction that marks the block confirmed on the
    active chain.
    - That is either after acceptance or during reconciliation.
    - The Stratum acknowledgement waits for that credit row.
    - A block that is abandoned instead fails the submission with "block-only
      proof was not accepted on the active chain".
  - On both versions a credited share survives a later reorg.
  - **Modeling assumption:** the `3.x.x` next-block payout assumes the
    deferred credit lands before the next block's anchor, because the credit
    row is stamped at confirmation, not at submission.
- **Reason:**
  - `crates/qbit-prism-server/src/coordinator/miner_submit.rs` credits `network` difficulty
    when `share_pass` is false, and holds the share as the candidate's
    `deferred_share` until the credit row exists.
  - `crates/qbit-prism-server/src/ledger/blocks.rs` (`credit_deferred_share`)
    appends it only on confirmation.
- **Matching cases:** `block-only-proof-rejected` (no credit on either) and
  `share-and-block-proof-control` (a share-passing proof is credited at the
  assigned difficulty on both) match `2.x.x`.
- **D2 item:** below-target credit.

<a id="d2c-prior-balances-during-bootstrap"></a>

### D2c: prior balances during bootstrap

**Status:** approved on 2026-09-11 under decision D2 in #260. This entry is
recorded separately from D2a because it was approved on its own.

- **Vector:** `bootstrap_transition.json`, case
  `bootstrap-carry-only-account-at-or-above-floor`.
  - The share window is empty.
  - miner-old has a 20000-sat carry and no share.
  - miner-b solves a 500000000-sat block.
- **2.x.x:** miner-b is paid 500000000.
  - miner-old is not in the payout manifest and is not paid in this block.
  - Its 20000-sat balance waits for a later block.
- **3.x.x:** miner-old is paid 19999, and carries 1.
  - miner-b is paid 499980001, and carries 19999.
  - The engine allocates the coinbase over both accounts' candidate balances.
- **Reason:**
  - The `2.x.x` collection bundle passes `prior_balances=[]`
    (`lab/prism/job_bundle.py` `build_collection_bundle`, which is `2.x.x`
    code at `504846cc0b72e8f86ed17f896d4ccbbe196a31dc` and does not exist on
    `3.x.x`).
  - `crates/qbit-prism-server/src/coordinator.rs` `build_bundle` passes
    `snapshot.prior_balances` in bootstrap too.
  - So a carry-only account at or above the floor can be paid in a bootstrap
    block.
- **D2 item:** D2c, prior balances during bootstrap.

## Retained mining work

Same-connection active eviction preserves the original worker, target and
version mask, including after reauthorization. The configured per-connection
retention count N bounds both the existing active set and the same-tip graveyard.
A previous parent can temporarily own its former active set plus graveyard;
the graveyard has a conservative total cap of 3N (4N including active work).
Same-tip graveyard TTL starts at eviction. Previous-parent entries require a
published transition and delivery-based grace; unrelated parents and absolute
resume expiries are removed. Original username admission permits remain held
while this retained work can credit, and are released on actual expiry, capacity
eviction, payout replacement or disconnect. Reauthorization reuses the same
connection's permit. Cross-connection resume still requires the exact original
worker, the current published template's parent and payout revision, and an
unexpired absolute job deadline. The durable current-revision fence remains
separate from those original published payout inputs.

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
