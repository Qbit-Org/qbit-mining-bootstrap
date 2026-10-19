# qbit-mining-bootstrap 3.1.0-rc.1 Release Notes

Release date: 2026-10-19 (release candidate; tag `v3.1.0-rc.1`, published as
a GitHub pre-release).

This is the first release candidate of 3.1.0. It is 3.0.0-rc.6 plus three
things:
- the fixes that would have shipped as 3.0.0-rc.7, a candidate that was never
  tagged: #770's fix for #765, in which `import-audits` held `SETTLEMENT_LOCK`
  while it sent and stored each legacy audit body, and #760's fix for #759,
  in which the node RPC client reused an idle connection past qbitd's own
  idle close;
- the 3.1 dual writer, five pull requests that let two nodes each write their
  own PostgreSQL and serve miners at once. **It is off by default:** with
  `PRISM_DUAL_WRITER` unset, a frontend behaves as 3.0's does;
- three schema migrations, 027, 028 and 031, which every start of this
  candidate requires, whether or not the dual writer is turned on.

It is not a production release.

Everything in the
[3.0.0-rc.1](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/doc/release-notes-3.0.0-rc.1.md),
[3.0.0-rc.2](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/doc/release-notes-3.0.0-rc.2.md),
[3.0.0-rc.3](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/doc/release-notes-3.0.0-rc.3.md),
[3.0.0-rc.4](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/doc/release-notes-3.0.0-rc.4.md),
[3.0.0-rc.5](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/doc/release-notes-3.0.0-rc.5.md) and
[3.0.0-rc.6](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/doc/release-notes-3.0.0-rc.6.md)
notes applies, with the changes below.

## Changes since 3.0.0-rc.6

### Fixes

- #770 (#765): `import-audits` writes each legacy audit before it takes
  `SETTLEMENT_LOCK`, holds the lock only to check the write guard and
  commit, and bounds every phase of a row's transaction.
  - **Before:** for each row, rc.6 took `SETTLEMENT_LOCK`, checked the write
    guard, and only then encoded the row's UPDATE and sent it, with the
    canonical body as `bytea`. Nothing bounded the send: the session's
    `statement_timeout` starts only once the whole statement has arrived. In
    #765, a legacy audit with a ~112 MB canonical body held the lock for
    about 90 s, until it was killed. Every other `SETTLEMENT_LOCK` user
    waited behind it, so template refresh was deferred. `docker stop` didn't
    stop it either: in a container the command runs as PID 1, which ignores
    a SIGTERM it has no handler for.
  - **Now, per row:** the body is loaded, verified and canonicalized, and
    the UPDATE's parameters are encoded, before any transaction opens. The
    UPDATE is sent and run holding only that row's lock. Only then does the
    importer poll for `SETTLEMENT_LOCK` with `pg_try_advisory_xact_lock`,
    never queued, check the write guard again under it, and commit. A lock
    holder that waits on the row, directly or through another session, goes
    first: the row is rolled back and the command fails naming it.
  - **Two new settings, read by `import-audits` only:**
    `PRISM_IMPORT_AUDITS_WRITE_TIMEOUT_MS` (60000; 1000 to 600000) bounds the
    write, and `PRISM_IMPORT_AUDITS_LOCK_TIMEOUT_MS` (5000; 100 to 600000)
    bounds the wait for `SETTLEMENT_LOCK` and, once it is held, any statement
    or client pause under it.
  - A row that exceeds a bound is rolled back, and the command exits 1
    naming the row; rows imported before it stay imported, and a re-run
    resumes from it. SIGTERM or SIGINT stops the command before the next
    row, or rolls back a row that has not been granted the lock. Each row
    prints one line on stderr with its sizes and timings.
  - Measured for #770 on a synthetic legacy row with a 112.2 MB canonical body, over
    TLS through a TCP proxy: rc.6's order held `SETTLEMENT_LOCK` for 3.145 s,
    this candidate's for 13 ms.
  - Why #765's send took so long is not proven; the likely cause is #771
    (below). This candidate's `import-audits` doesn't depend on it.
- 3.x.x: drop an idle node connection after 20 s, below qbitd's 30 s idle
  close (#759) (#760), merged as `12f1ae8e`: every node RPC client keeps an
  idle connection for at most 20 s.
  - **Before:** the frontends' qbitd client kept an idle connection for 60 s,
    and the public API's for 90 s, while qbitd closes one after its
    `-rpcservertimeout`, 30 s by default. A request written into a
    connection as the node closes it fails with the plain `transport failed`
    error, which nothing retries; on `submitblock` that is an unknown
    outcome, and the block is never offered again.
  - **Now** the frontends', the public API's and the load harness's node
    clients all drop an idle connection after 20 s, before the node can.
  - **Operator rule:** keep qbitd's `-rpcservertimeout`, and the idle timeout
    of any proxy between a PRISM process and its node, above 20 s. qbitd's
    default, 30 s, is.

### The 3.1 dual writer

In 3.0, the pair is one writer: both nodes' frontends write node A's
PostgreSQL, and node B holds its streaming standby. In the 3.1 dual writer,
each node's frontend writes only its own PostgreSQL and pulls the rows the
other node originated, so either node keeps mining when the other dies. The
design is
[#761](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/761). It came in
five pull requests:

- **#766, node identity and peer sync** (`719ae638`).
  `qbit-prism-server node-identity set --index N` personalises a database as
  node A (`0`) or B (`1`): every copied row carries its `origin_node`, keys
  two nodes allocate never collide (A even, B odd), and each node hands out
  its own half of the extranonce1 space. One peer sync task per dual-writer
  frontend pulls the peer's shares and header mappings, carry-owner journal,
  landed blocks and prepared jobs, over a read-only role
  (`prism_peer_sync`). A conflict is recorded and alerted, never written
  over. Until a node holds every row it originated that the peer holds (the
  own-log latch), it admits no miners; a node restored from a backup pulls
  its own rows back first. `node-identity repersonalise --index N` turns a
  promoted physical copy of the peer into this node.
- **#768, per-node window cuts** (`1a750c93`). A peer's share can reach a
  node after that node's own, higher-numbered shares, so every dual-writer
  payout window records a per-node cut: the highest `share_seq` of each
  node's rows it may include. The cut is stored in the window reference, in
  migration 028's columns on the audit snapshot, and in the audit bundle's
  reward manifest, which the coinbase commits. A late peer row never makes
  a built window, an offered block or a landing unverifiable; it joins a
  later window. A 3.0 verifier refuses a bundle with a cut rather than
  mis-verifying it.
- **#767, carry owner and peer settlement** (`44fd3e4c`). Only one node, the
  carry owner (`PRISM_CARRY_OWNER`), builds work that pays down carried
  balances; the other node's work is carry-free. A guard checks the
  database's identity, the setting, this node's journal
  (`qbit_prism_node_roles`) and a live read of the peer's journal, and builds
  carry-free work with an alert whenever they disagree.
  `qbit-prism-server carry-owner status|release|transfer` moves ownership.
  Peer blocks land `prepared` and count once this node's own reconciler
  confirms them. Either node broadcasts a mature fanout; the finder's node
  sponsors a zero-fee one, and the other node takes it over once it is
  overdue and the finder silent. A block found by a node that died is
  adopted by its peer once it is 60 deep (S8).
- **#769, readiness and Stratum gating** (`92559d71`). In dual-writer mode,
  readiness also needs the own-log latch and a local writer (`/healthz`
  gains a `dual_writer` object), and the Stratum listeners accept
  connections only while the frontend admits miners. An optional
  token-protected endpoint, `GET /readyz`, answers the balancer's checks. In
  dual-writer mode `qbit-prism-server healthcheck` checks liveness, not
  readiness.
- **#763, the dual-writer E2E simulation and CI** (`f11b8176`).
  `crates/qbit-prism-dual-sim` runs the whole pair on one runner (two
  PostgreSQL clusters, regtest `qbitd`s, the real frontends, a balancer
  stand-in and relays a fault can cut or slow) and holds it to the 3.1
  payout invariants. CI's required `prism-dual-writer` job runs its fast
  lane; `dual-writer-nightly` runs its whole matrix, 24 tests.

After the five, #773, 3.1 pre-migrate: rc.6 serves while 3.1's migrate
applies 027, 028 and 031, and p0 is never rewritten (`4b1aefee`), adds two
gated tests of the [upgrade from rc.6](#upgrading), in the fast lane and the
matrix, which they bring to 26 tests; it changes no runtime code.
- **R1:** the 3.0 pair serves on 3.0.0-rc.6 (`81c592c3`, its database made by
  rc.6's own `migrate`) under load while this candidate's `migrate` runs once
  as a one-off command with A's frontend environment, and a reader's snapshot
  holds 031's `CREATE INDEX CONCURRENTLY` part-way. 027 and 028 commit with
  every share sent meanwhile answered and none waiting 5 s, appends continue
  during the build, and blocks land before, during and after it. Both rc.6
  frontends restart and serve, A while 31 is unrecorded and the index
  invalid, B once 31 is recorded and the index valid on the writer; the
  standby then has the valid index by replication. No frontend statement is
  cancelled, and the checker's invariants hold.
- **p0:** on a populated ledger that rc.6's `migrate` made (300,000 rows in
  CI, no frontend), with a test appender under the
  frontends' 5 s lock timeout, the migrate leaves the share ledger's data
  partition `p0` and its TOAST table unrewritten, unfrozen and unvacuumed:
  their `relfilenode`, `relfrozenxid` and `relminmxid` and their vacuum and
  analyze counters are unchanged, the cluster logs no `VACUUM`, `ANALYZE`,
  `CLUSTER` or `REINDEX`, every index build on `p0` is
  `CREATE INDEX CONCURRENTLY`, and every append succeeds.

**With `PRISM_DUAL_WRITER` off and `PRISM_READINESS_PORT` unset, behaviour is
3.0's.** No new task starts, the statements a single writer runs have 3.0's
results, money-path vectors and existing goldens verify byte for byte, and of the
new settings a frontend reads only `PRISM_DUAL_WRITER` itself,
`PRISM_DUAL_WRITER_DOWNGRADE`, `PRISM_READINESS_GRACE_SECONDS` (validated, 0
to 120) and `PRISM_READINESS_PORT`. A single writer may turn on the readiness
endpoint with that port, and then reads its token and bind settings too. Two things do
change for a single writer:
- every start requires migrations 027, 028 and 031 ([Migrations](#migrations));
- a single writer refuses a database that has run as a dual writer (its
  carry-owner journal holds rows), unless `PRISM_DUAL_WRITER_DOWNGRADE=1`
  marks the deliberate rollback to one writer (D-12).

### New settings

Each setting below is accepted by 3.1.0-rc.1 binaries only (see
[Upgrading](#upgrading)). The configuration reference's
[dual writer](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-configuration.md#dual-writer-31)
and
[readiness endpoint](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-configuration.md#readiness-endpoint-and-admission-31)
sections document them; the two `import-audits` settings are in the
[migration guide](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-rust-migration.md).

| Setting | Default | Read |
|---|---|---|
| `PRISM_DUAL_WRITER` | `0` | always; turns the dual writer on |
| `PRISM_NODE_INDEX` | required in dual mode | `0` on node A, `1` on node B |
| `PRISM_CARRY_OWNER` | required in dual mode | `1` on the one node that pays down carried balances |
| `PRISM_PEER_DATABASE_URL` | required in dual mode | the peer's PostgreSQL, as its read-only sync role |
| `PRISM_PEER_DATABASE_URL_FALLBACK` | unset | a second path to the same database |
| `PRISM_PEER_SYNC_INTERVAL_MS` | `250` | wait between pulls that found nothing; 10 to 60000 |
| `PRISM_PEER_SYNC_BATCH_ROWS` | `5000` | most rows of one stream per pull; 1 to 20000 |
| `PRISM_PEER_INGEST_WAIT_MS` | `250` | before a found block's `submitblock`, the longest wait for the peer to hold what adopting it needs; 0 (off) to 10000 |
| `PRISM_DUAL_WRITER_DOWNGRADE` | `0` | by a single writer: start on a database that has run as a dual writer |
| `PRISM_READINESS_PORT` | `0` (off) | the readiness endpoint's port |
| `PRISM_READINESS_BIND` | `127.0.0.1` | its bind address |
| `PRISM_READINESS_TOKEN` or `PRISM_READINESS_TOKEN_FILE` | required with the port | the token a check sends in `X-Qbit-Healthcheck-Token` |
| `PRISM_READINESS_GRACE_SECONDS` | `10` | how long readiness may stay false before the frontend stops admitting; 0 to 120 |
| `PRISM_IMPORT_AUDITS_WRITE_TIMEOUT_MS` | `60000` | by `import-audits` only (#770) |
| `PRISM_IMPORT_AUDITS_LOCK_TIMEOUT_MS` | `5000` | by `import-audits` only (#770) |

A dual-writer node has no failover standby, so
`PRISM_OFFER_STANDBY_APPLICATION_NAME` and `PRISM_OFFER_STANDBY_FLUSH_WAIT_MS`
stay unset there; `PRISM_PEER_INGEST_WAIT_MS` takes the standby wait's place.
A dual writer also refuses to start without a `PRISM_COINBASE_TAG` of at least
4 bytes, and both nodes must issue work with the same `PRISM_COINBASE_TAG`
and `PRISM_STRATUM_EXTRANONCE2_SIZE`.

### Migrations

Every start of this candidate requires native migrations 2 to 28 and 31, each
checked on its own. 029 and 030 are reserved for the rest of the 3.1 dual
writer and are not part of this candidate; 31 needs neither. None of the three
declares a capability or needs a shutdown proof, so a 3.0.0-rc.6 binary
accepts a database that has them, logging that a later release applied
migrations it does not know.

- **027**, the dual writer's identity
  ([node identity](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-ledger-ops.md#dual-writer-node-identity-027)):
  `origin_node smallint NOT NULL DEFAULT 0` on the 13 tables the peer sync
  copies, the share ledger and its partitions included; `sync_seq` on landed
  blocks and jobs; the carry-owner journal; the node identity and lineage;
  and the peer sync's cursors, with the safe peer mark
  `qbit_prism_peer_share_mark()`, and conflicts. Every new column has a
  constant default or none, so no table is rewritten and no constraint is
  validated by a scan. It runs in the migration transaction, which alters
  the share ledger and its header mappings last, so serving frontends wait
  on them only from that step to the commit.
- **028**, the window cuts
  ([window cuts](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-ledger-ops.md#dual-writer-window-cuts-028)):
  `cut_seq_0` and `cut_seq_1` on `qbit_prism_audit_snapshots`, both `NULL`
  (no cut, 3.0's meaning) or both set, with a `CHECK`. It runs in the
  migration transaction with 027.
- **031**, the share ledger's `(origin_node, share_seq)` index,
  `qbit_share_ledger_origin_seq_idx`, **built online**
  ([migration 031](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-rust-migration.md#migration-031-the-share-ledgers-origin-index-applied-online)).
  PostgreSQL cannot build an index of a partitioned table concurrently, so
  after the migration transaction `migrate` builds one leaf per partition
  with `CREATE INDEX CONCURRENTLY` while appends continue, then creates the
  index `ON ONLY` the parent and attaches each leaf, those two steps under a
  2 s lock timeout with retries. 31 is recorded once PostgreSQL marks the
  index valid; until then every start of this candidate refuses the
  database, and an interrupted build resumes. The build holds the share
  archive's lifecycle lock throughout. It reads each partition twice and
  sorts a 16-byte key per row, and the index adds about 30 bytes a row: run
  `migrate` from a shell, not through a starting frontend. A fresh deployment, or
  a 2.x.x source whose share ledger is empty, applies 031 in the migration
  transaction, and
  `migrate --offline-indexes` builds the leaves with a plain `CREATE INDEX`.

## Upgrading

- **From 2.0.x, or from an earlier 3.0.0 candidate:** as in the
  [rc.6 notes](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/doc/release-notes-3.0.0-rc.6.md#upgrading),
  with this candidate in rc.6's place: its `migrate` applies 027, 028 and 031
  with the rest, and the cutover's `migrate --offline-indexes` builds 031
  offline too.
- **From 3.0.0-rc.6, as a single writer:**
  1. **Migrate first, while rc.6 serves.** Run this candidate's
     `qbit-prism-server migrate` once, as a one-off command with a
     frontend's database settings. 027 and 028 commit in one short
     transaction, and 031 then builds online beside the serving rc.6
     frontends, which keep accepting and landing. rc.6 frontends serve, and
     restart, at every step, warning of the migrations they don't know.
     #773's R1 test runs this step beside rc.6 frontends under load, restarting
     A during 031's build and B once 31 is recorded, and its p0 test shows
     the step never rewrites the ledger's data partition
     ([above](#the-31-dual-writer)).
     A later `migrate`, such as a cutover's, then finds every migration
     recorded and changes nothing.
  2. **Then replace the binary** on every frontend and tool, and on the
     `public-api` service. A frontend or tool of this candidate refuses the
     database until 31 is recorded. Nothing refuses a mix of rc.6 and this
     candidate's single-writer frontends on one database (no capability or
     fingerprint changes), so they can be replaced one at a time.
  3. Pass the new settings only to this candidate's binaries. rc.6's `run`
     and `check-config` refuse a `PRISM_*` name they don't know in
     production mode (`QBIT_CHAIN` main or mainnet, `QBIT_PRODUCTION` or
     `QBIT_TOOLS_PRODUCTION`), and warn about it otherwise.
  4. **#760's node rule:** check that qbitd's `-rpcservertimeout`, and the
     idle timeout of any proxy between a PRISM process and its node, are
     above 20 s.
  5. `import-audits` beside serving frontends no longer needs #765's guard
     (a watchdog that kills it with SIGKILL once a settlement waiter has waited a few seconds, since as a container's PID 1 it ignores SIGTERM): its hold is the guard read and the commit, and `docker stop` stops it.

  Don't overlap the migrate with heavy disk jobs on the database host:
  fsync starvation can push the serving frontends past their 5 s lock
  timeouts.
- **From one writer to the dual writer:** the cutover itself is the pair's
  runbook. On this candidate's side, each database is personalised with
  `node-identity set --index N` before its first dual-writer start, each
  grants the peer's pulls the `prism_peer_sync` role with exactly the grants
  in the
  [peer sync chapter](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-ledger-ops.md#the-peer-sync),
  and the frontends get the dual-writer settings above. A node rebuilt from
  a physical copy of its peer is re-personalised with
  `node-identity repersonalise --index N`
  ([rebuilding a node](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-ledger-ops.md#rebuilding-a-node-from-its-peer)).
  The balancer routes on `/readyz`
  ([dual-writer readiness](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-dual-writer-readiness.md)).

### Rolling back

- **From the dual writer to one writer:** stay on this candidate. Start the
  single writer with `PRISM_DUAL_WRITER` off and
  `PRISM_DUAL_WRITER_DOWNGRADE=1`; without it the start refuses a database
  whose carry-owner journal holds rows, because a second payer of carried
  balances would pay them twice. Only the deliberate rollback sets it. The
  database stays personalised, so declare the peer's tail lost
  (`qbit_prism_node_lineage.peer_tail_lost_at`) or `share-archive plan`
  waits for a peer that no longer syncs
  ([the peer sync](https://github.com/Qbit-Org/qbit-mining-bootstrap/blob/v3.1.0-rc.1/docs/prism-ledger-ops.md#the-peer-sync)).
- **To 3.0.0-rc.6, from a database that was never personalised**
  (`node-identity show` prints no identity): first
  remove the new settings from every environment an rc.6 binary will read,
  then replace the binary. rc.6 serves on a database at 31, as above;
  nothing needs undoing. With rc.6, #765 and #759 return: run its
  `import-audits` beside serving frontends only under #765's guard, a watchdog that kills it with SIGKILL once a settlement waiter has waited a few seconds, since as a container's PID 1 it ignores SIGTERM.
- **Never start a 3.0.0-rc.6 binary on a personalised database,** whether
  or not it has run as a dual writer. rc.6 doesn't know the carry-owner journal or the node identity,
  so it doesn't refuse such a database as this candidate's single writer
  does. Its `share-archive` would archive the peer's rows without their
  `origin_node`, and a restore by it would label every restored row with
  this database's own node.
- **To a candidate before 3.0.0-rc.6:** untested, so don't, once this candidate's
  `migrate` has run. The earlier notes' rollbacks, such as rc.4's at fence 2,
  apply only before it; from a migrated database, roll back to 3.0.0-rc.6.

## Verification

The changes between rc.6 and this candidate, and their evidence, are:
- #770, merged as `4c134913`: `import-audits` only, with
  `tests/import_audits_settlement_lock.rs` (13 gated tests: a session taking
  `SETTLEMENT_LOCK` every 20 ms beside the throttled import of a 107 MB body
  waits under 3 s while the write takes longer; stalls, a busy lock, a waiting
  holder and every stop point each behave as above).
- #760, merged as `12f1ae8e`: the node clients' idle bound, with
  `rpc_idle_close` against a loopback node that drops a request arriving
  after its idle limit.
- The dual writer, #766, #768, #767, #769 and #763. With `PRISM_DUAL_WRITER`
  off, the existing suites pass unchanged apart from version pins,
  whole-row digests and metric counts that the new migrations and metric
  families change, and `money_path_vectors` is unchanged. Each pull request
  adds its own suites: gated PostgreSQL suites (`dual_writer*`,
  `carry_owner*`, `dual_writer_readiness`, `readiness_frontend` and others),
  listed in `test/prism-gated-tests.txt`, and plain ones such as
  `stratum_admission_gate` and `window_cut_bundles`. The required
  `prism-dual-writer` CI lane runs the E2E fast lane on every pull request,
  and all 26 tests of the matrix run on this version-bump pull request
  (`dual-writer-nightly`, by its `run-dual-writer-e2e` label).
- #773, merged as `4b1aefee`: tests only. R1 runs 3.0.0-rc.6 frontends
  beside this candidate's `migrate`, and p0 runs that `migrate` on an
  rc.6-migrated ledger under appends, both in the fast lane and the matrix.

The serving path's single-writer statements keep 3.0's results, so the
serving evidence the 3.0.0-rc.3 notes cite still applies to a single writer: the D1
peak-second gate, the 750/s ceiling and the failover drill under load, run 1,
all measured on #719's build. None of it covers the dual writer, whose
evidence is the E2E matrix.

Otherwise the evidence is the same set as rc.6's (#557):
- CI on the version-bump pull request, and dispatched on the tag, including
  the required `prism-dual-writer` lane;
- the L3 production-window matrix on the version-bump pull request, which
  the tag promotes on a tree match (#608);
- the targeted nightly batch: `short-plan-fake-node`, `short-plan-real-node`,
  `soak-smoke`, `faults-short-real-node` and `faults-failover-fake-node`,
  plus `dual-writer-nightly`;
- the live regtest weekly scenarios.

3.0.0-rc.1's full nightly set and 5.5 h soak carry over for the single
writer. The
evidence bundle on the `v3.1.0-rc.1` GitHub pre-release lists every run.

## Known issues

As in the rc.6 notes, with these changes:

- **Fixed since rc.6:**
  - **#765, by #770:** rc.6's `import-audits` held `SETTLEMENT_LOCK` while
    it sent and stored each body, with nothing bounding the send, and ignored
    `docker stop` (above).
  - **#759, by #760:** the node clients kept an idle connection past qbitd's
    idle close, so a call could fail in the close (above).
- **Open since rc.6, not fixed in this candidate:**
  - **#771:** sqlx 0.8.6's rustls stream can lose the wakeup of a large write.
    A large statement sent over TLS can then stall until something closes the
    connection. It is the likely cause of #765's hold. This candidate's
    `import-audits` works around it, but other large statements from the
    serving binary don't. The fix is upstream, launchbadge/sqlx#4251, which
    no 0.8.x release carries.
  - **#762:** a `submitblock` whose deadline,
    `PRISM_BLOCK_SUBMIT_RPC_TIMEOUT_SECONDS` (1 s by default), passes while
    its connection is still opening is recorded as an unknown offer, not an
    unsent one, so that block is never offered again. It needs no free
    pooled connection when the call starts and a connect slower than the
    deadline, which the 2 s refresh calls make unlikely.
  - **#757:** five alert-rule UIDs in `docs/prism-native-alert-rules.json`
    exceed Grafana's 40-character limit, so provisioning the rule set as
    published stops Grafana from starting. Map the five to short forms when
    rendering the rules; #757 lists them.
  - **#758:** the two accepted-block revision-work warnings,
    `PrismAcceptedRevisionWorkPending` and
    `PrismAcceptedRevisionWorkTrackingUnknown`, fire after every pool block
    on a frontend that has no miners and doesn't commit the settlements,
    and the second latches until that frontend restarts. Silence the two on
    idle frontends.
- **The dual writer's known limits,** from its pull requests' follow-ups:
  - **Share archives:** archive and restore a dual-writer database's
    partitions only with a 3.1 binary; an earlier one drops `origin_node`
    when archiving, and labels restored rows as this node's.
    `share-archive restore --attach` doesn't compare imported peer rows with
    the peer sync's cursors, so never import the peer's rows above the safe
    peer mark. A partition detached before 027 lacks `origin_node`:
    reattaching it by hand needs
    `ALTER TABLE <partition> ADD COLUMN origin_node smallint NOT NULL DEFAULT 0`
    first.
  - **Peer URLs:** the found-block wait trusts whichever peer path answers,
    so a peer URL misrouted to another PRISM database could confirm a wait
    wrongly. Every block is still offered; only the wait's metric and log
    line would be wrong. The peer sync itself checks the identity,
    fingerprint and columns on every path and alerts on a mismatch.
  - **Fanout takeover:** a finder that is alive but cut off from the other
    node for 30 minutes looks dead to it, and both can then sponsor a
    zero-fee fanout; one funding coin stays locked until an operator
    releases it. Fee-bearing fanouts need no sponsor.
  - **Adoption:** a block that cannot be adopted is retried and alerted
    every 10 minutes in the logs, with a final `ALERT` when it leaves the
    1,440-block lookback, from a running loop only; there is no durable
    record. The solving share of an adopted block isn't credited.
  - **Layout:** while the two nodes' coinbase tag or extranonce2 width
    differ, both build carry-free work under the alerting state
    `layout_mismatch`; a mismatch seen only through peer records is
    forgotten if they expire while the peer's database can't be read.
  - **Carry-owner release:** a carry-paying offer rechecks the carry gate
    right before `submitblock` but doesn't hold a release out across the
    call. A block paying carry after a release needs a process stall of the
    release depth at that point plus a reorganization at least as deep.
  - **Readiness endpoint:** publish it so client source addresses are
    preserved, or its per-source connection cap counts every checker as one
    source.
  - **Stratum addresses:** two dual-writer frontends misconfigured with one
    Stratum address both start, since a gated listener binds without
    listening, and the second one admitted fails to listen and exits.
  - **Solver columns:** a block captured on a superseded payout revision
    (#478) can differ between the nodes in its four informational
    `solver_*` columns, NULL on its origin and filled on the peer by
    migration 016's trigger. No payout reads them.
- **Still open from rc.6,** as the rc.6 notes describe them: #752, #738,
  #739, #724's other answer, #737's open part, #741, #731, #734, #716's open
  part, #711 and #712.
