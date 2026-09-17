# PRISM HA Compose and database reference architecture

This is the deployment contract for [#281](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/281),
after the 2026-09-10 scope trim and the [approved D3 decision](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/260#issuecomment-5635989715),
its [addendum](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/260#issuecomment-5636035280)
and the [#281 policy confirmation](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/281#issuecomment-5635994877).
The selected policy is **one primary plus one dedicated asynchronous failover
standby**, separate from the public-read replica. Writer sessions use
`synchronous_commit=on` and the primary uses `synchronous_standby_names=''`:
positive share ACKs require local WAL durability and do not wait for the standby.
Primary loss can lose acknowledged shares in the replication gap; this is
**not lossless failover and has no guaranteed lag bound**.

This separate document owns the complete frontend-to-database architecture.
[prism-postgres-replica.md](prism-postgres-replica.md) remains the detailed guide
to provisioning the existing public read replica and managing its slot.
The supplied overlay is a local two-frontend deployment, not a production HA
certification: its default node and database still share a Compose host.
Compact cross-frontend resume landed in [#397](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/397),
and [#273 is closed with runtime qualification evidence](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/273#issuecomment-5704427645).
That does not establish real-overlay reconnect/resume, promotion under load or
operator load-balancer behavior. Those live #281/#291 gates remain open in the
[acceptance evidence matrix](#281-acceptance-evidence).

## Seven deployment requirements

| # | Requirement | Exact wiring/settings | Delivery |
| --- | --- | --- | --- |
| 1 | Two active mining frontends | `prism-coordinator` and `prism-coordinator-2`; `PRISM_INSTANCE_ID=prism-frontend-1` / `prism-frontend-2`; identical `PRISM_DATABASE_URL`, payout configuration and signing keys; shared database job/extranonce authority. | **Provided here:** runtime, compact resume and two-service overlay. **Operator-supplied:** placement in independent failure domains and matching secret distribution. Real-overlay resume qualification remains open. |
| 2 | Highly available Stratum TCP endpoint | Operator endpoint sends TCP to frontend ports `3340` / `3343` on the Compose host by default; probe each frontend's own `/healthz`, with the hysteresis below. Publish that endpoint in `PRISM_PUBLIC_STRATUM_URL`. | **Operator-supplied:** TCP load balancer, endpoint redundancy, routing and firewall. No LB service is shipped. |
| 3 | A separate node per frontend | Set `PRISM_HA_RPC_HOST_1=node-1` and `PRISM_HA_RPC_HOST_2=node-2`, or set each `PRISM_HA_RPC_URL_1/2` to its node's complete HTTP(S) URL. Same chain/genesis and consensus policy on both. | **Provided here:** independent per-service RPC wiring. **Operator-supplied:** both independent production nodes, authentication and placement. Both lab defaults use bundled `qbitd`. |
| 4 | PostgreSQL primary plus one dedicated asynchronous failover standby, with separate public reads | Primary: `fsync=on`, `full_page_writes=on`, `wal_level=replica`, `max_wal_senders=10`, `max_replication_slots=10`, `synchronous_standby_names=''`; writer sessions: `synchronous_commit=on`. HA standby: `hot_standby=on`, its own physical slot and `application_name=prism_standby_1`. `prism-public-api` uses the separate public-read DSN in `PRISM_PUBLIC_DATABASE_URL` with `PRISM_PUBLIC_REPLICA_MODE=require`. | **Provided here:** lab primary, asynchronous **public-read** replica, slot bootstrap and checked public API. **Operator-supplied:** dedicated HA standby, D3 configuration/monitoring, independent storage/failure domains, backups and capacity. |
| 5 | Promotion | After fencing and checking the eligible standby, `SELECT pg_promote(wait => true, wait_seconds => 60)`; verify `pg_is_in_recovery() = false` and the required history before moving the writer endpoint. | **Operator-supplied:** decision authority, failure detection, promotion execution and rehearsal. Procedure below; no automatic promotion service. |
| 6 | Fence the old primary | Power/storage fencing or enforced network isolation that stops **all existing and new** writer connections, including both frontends and settlement workers; disable old-primary restart automation. Keep the fence until rejoin as a standby. | **Operator-supplied:** fencing mechanism and positive confirmation. Changing DNS or stopping one frontend is insufficient. |
| 7 | Stable authoritative writer endpoint | Both frontends use the same `PRISM_DATABASE_URL=postgresql://prism_writer:<secret>@prism-writer.internal:5432/qbit?sslmode=verify-full`; the endpoint routes only to the unfenced primary. | **Provided here:** shared DSN pass-through and external-DB overlay. **Operator-supplied:** endpoint ownership, TLS, failover routing, connection draining and fencing. The bundled `prism-postgres` name is not a failover endpoint. |

D3's primary and dedicated failover standby are two HA members. A separate
public-read replica is an additional database member, not a second approved
failover candidate. Keep public queries off the dedicated standby so they cannot
delay its replay. The bundled lab's primary/public-replica pair does not supply
this complete production topology, and the HA overlay does not provision it.

## Compose stacks and configuration path

Use Docker Compose **2.24.4 or newer** for
[`!override` support](https://docs.docker.com/reference/compose-file/merge/#replace-value).
Apply HA last:

```sh
# Bundled lab database, one bundled node, two frontends:
docker compose -f compose.yaml -f compose.prism-ha.yaml --profile prism config --quiet
docker compose -f compose.yaml -f compose.prism-ha.yaml --profile prism up -d

# Production image/storage restrictions, bundled database:
docker compose -f compose.yaml -f compose.production.yaml \
  -f compose.prism-ha.yaml --profile prism config --quiet

# Operator database endpoint:
docker compose -f compose.yaml -f compose.prism-external-db.yaml \
  -f compose.prism-ha.yaml --profile prism config --quiet

# Production restrictions and operator database endpoint:
docker compose -f compose.yaml -f compose.production.yaml \
  -f compose.prism-external-db.yaml -f compose.prism-ha.yaml --profile prism config --quiet
```

All four stacks are supported and validated in CI on pushes and PRs. Production
requires the existing explicit image and storage variables; its invalid-image
and missing-bind-source defaults remain effective on **both** frontends.
External DB stacks require an explicit writer DSN, and should explicitly set the
public read DSN and `PRISM_PUBLIC_REPLICA_MODE=require`: the external-DB overlay's
existing fallback otherwise uses the writer with replica checks `off`.
Do not activate `local-database` when using external DBs.

The second service shares the base coordinator template through a YAML anchor.
Its `prism-ha-overlay-required` profile is an implementation detail: do not enable
it directly. The HA overlay enables it with the ordinary `prism` profile and
replaces host port mappings. Production and external-DB overrides share their
coordinator settings through anchors too. A cross-file `extends` would resolve
before overlays merge and silently lose some effective production settings.
Without HA, the existing `prism` deployment starts only one coordinator.

Run `check-config` in **each** service before serving traffic. Shared keys,
chain/genesis, fee and payout policy must match; the existing cluster fingerprint
enforces that agreement. IDs must be distinct and stable across restarts; each
passes the existing `Config::from_env` validation (at most 128 characters, no
control characters or reserved `prepared:` prefix). Duplicate IDs overwrite the
same heartbeat row and cannot establish HA. Do not scale either named service
with `--scale`, which would duplicate its configured ID and published ports.

### Every HA setting reaches its consumer

Compose uses shell variables ahead of `--env-file`/`.env`; later env files win
over earlier files, and later Compose overlays win on the same service keys.
These are interpolation knobs, not new application environment readers:

| Operator input | Default | Container/effective consumer |
| --- | --- | --- |
| `PRISM_HA_INSTANCE_ID_1`, `PRISM_HA_INSTANCE_ID_2` | `prism-frontend-1`, `prism-frontend-2` | `PRISM_INSTANCE_ID` → existing `Config::from_env().instance_id` → `Coordinator` / `Ledger` / health and heartbeat. HA supersedes the global `PRISM_INSTANCE_ID`. |
| `PRISM_HA_RPC_HOST_1`, `PRISM_HA_RPC_HOST_2` | global `QBIT_RPC_HOST`, or `qbitd` | `QBIT_RPC_HOST` → existing `rpc_connection_from_env()` → `Config.rpc_url` → coordinator RPC client. A nonempty per-frontend setting wins; otherwise the global setting is preserved. |
| `PRISM_HA_RPC_URL_1`, `PRISM_HA_RPC_URL_2` | global `QBIT_RPC_URL`, or empty | `QBIT_RPC_URL` → same existing reader. A nonempty URL wins over host **and** `QBIT_RPC_PORT`; blank uses `http://<host>:<QBIT_RPC_PORT>/`. A nonempty per-frontend setting wins; otherwise the global URL is preserved. To use a per-frontend host when a global URL exists, provide a complete per-frontend URL; a blank override falls back to the global URL. `QBIT_RPC_USER/PASSWORD` and the fallback port remain shared; different node credentials require a service environment override. |
| `PRISM_DATABASE_URL` (existing) | bundled `prism-postgres:5432`, default DB/user `qbit` | Same full DSN on both services → `Config.database_url` validation → `Ledger::connect`; no HA-specific DSN or second environment reader. Database credentials are inherited from the base. |
| `PRISM_HA_STRATUM_PORT_HOST_1`, `PRISM_HA_STRATUM_PORT_HOST_2` | existing `PRISM_STRATUM_PORT_HOST` or `3340`; `3343` | Docker publishes to the common `PRISM_STRATUM_PORT` (default `3340`), read by existing `StratumConfig`. Host mappings do not alter the listener. |
| `PRISM_HA_HIGHDIFF_PORT_HOST_1`, `PRISM_HA_HIGHDIFF_PORT_HOST_2` | existing `PRISM_STRATUM_HIGHDIFF_PORT_HOST` or `127.0.0.1:0`; `127.0.0.1:0` | Docker allocates separate loopback host ports; target is `PRISM_STRATUM_HIGHDIFF_PORT` or `4334`. The actual listener remains disabled unless its existing runtime setting enables it. Choose distinct explicit host ports when enabling it. |
| `PRISM_HA_HEALTH_PORT_HOST_1`, `PRISM_HA_HEALTH_PORT_HOST_2` | `127.0.0.1:3341`, `127.0.0.1:3344` | Docker publishes the common `PRISM_HA_AUDIT_PORT` target; the effective `PRISM_AUDIT_PORT` is read by existing API configuration and `healthcheck`. |
| `PRISM_HA_AUDIT_BIND` | `0.0.0.0` in HA | `PRISM_AUDIT_BIND` → existing API listener reader; this HA knob supersedes the base `PRISM_AUDIT_BIND`, including the loopback value in `.env.example`, so published probes work. An explicit loopback-only bind defeats host forwarding. Use a reachable bind for HTTP probes. |
| `PRISM_HA_AUDIT_PORT` | `3341` | Supersedes base `PRISM_AUDIT_PORT` (including `0`) on both services and their published targets → existing `Config.audit_port` / API listener and `healthcheck`. Must be nonzero. |

The HTTP health listener is required by this overlay's external LB contract.
Audit-disabled deployments are unsupported with this overlay; they need an
explicit operator override of both environment and port mappings, plus their own
readiness contract. Set the HA port knob when migrating a custom audit port.

Only health ports default to host loopback; Stratum defaults publish on all host
interfaces. Permit the health ports only from the operator's management/LB
network. The audit listener exposes more than health: do not publish it as a
public endpoint. For a remote operator TCP load balancer bind the health mapping to the intended
private host address, and enforce the corresponding network ACL.

Changing RPC targets does not remove the inherited `depends_on: qbitd`; the
bundled node still starts in these stacks. Operators deploying independently
managed nodes can override dependencies in their placement configuration.

Safe configuration verification (use fixture values, never log real secrets).
Both fixture URLs are explicit so inherited credentialed RPC URLs cannot appear
in the projection:

```sh
PRISM_HA_INSTANCE_ID_1=verify-east PRISM_HA_INSTANCE_ID_2=verify-west \
PRISM_HA_RPC_HOST_1=node-east.internal PRISM_HA_RPC_HOST_2=node-west.internal \
PRISM_HA_RPC_URL_1=http://node-east.internal:19452/ \
PRISM_HA_RPC_URL_2=http://node-west.internal:19452/ \
docker compose -f compose.yaml -f compose.prism-ha.yaml --profile prism config \
  --format json | jq '.services | with_entries(select(.key | startswith("prism-coordinator"))) |
    map_values({ports, environment: (.environment |
      {PRISM_INSTANCE_ID, QBIT_RPC_HOST, QBIT_RPC_URL, PRISM_AUDIT_BIND, PRISM_AUDIT_PORT})})'

docker compose -f compose.yaml -f compose.prism-ha.yaml --profile prism \
  exec prism-coordinator qbit-prism-server check-config
docker compose -f compose.yaml -f compose.prism-ha.yaml --profile prism \
  exec prism-coordinator-2 qbit-prism-server check-config
```

For image qualification also render with `config --resolve-image-digests --quiet` using
available registry images, or start the built images and inspect health IDs,
effective RPC targets and port mappings through the real startup path. A plain
render alone does not demonstrate that a node answers RPC or that a coordinator
is healthy. Review credentials privately if a complete RPC URL contains any.

## D3: approved asynchronous replication and ACK policy

The following describes PostgreSQL commit semantics with `fsync=on` and
`full_page_writes=on`. It assumes the selected replication wait finishes
normally. **Cancellation is a separate qualification concern below.**

| `synchronous_commit` | ACK guarantee with `synchronous_standby_names='FIRST 1 (prism_standby_1)'` | Standby down | Current PRISM writer session |
| --- | --- | --- | --- |
| `off` | No local WAL-flush guarantee; acknowledged shares can disappear after a primary crash. | Does not wait. | Replaced with `on`. |
| `local` | Local WAL is durable; no replica durability guarantee. | Does not wait. | Replaced with `on`. |
| `remote_write` | Local WAL durable, standby OS has written WAL; standby OS/power failure can lose it. | Waits for the named standby. | Replaced with `on`. |
| `on` | WAL flushed on primary and named standby; survives primary loss if the standby's storage survives. | Waits for the named standby. | Preserved. |
| `remote_apply` | As `on`, plus the commit is replayed and visible to standby queries. | Waits for standby replay. | Preserved if set when the connection opens. |

With `synchronous_standby_names=''`, every non-`off` level only waits for local
flush, and standby loss does not block replication-related ACK progress.
[PostgreSQL 16 commit semantics](https://www.postgresql.org/docs/16/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT).

The runtime pool's existing `after_connect` explicitly keeps `remote_apply` or
sets `on`, and refuses `fsync=off` / `full_page_writes=off`. A role default of
`local`, `off`, or `remote_write` therefore does **not** select that mode for PRISM.
`self-check.durability` queries `pg_settings` through this same configured ledger
pool, so it observes the session value actually used, not just a server default.
For `remote_apply`, set the writer role/database default before reconnecting all
writer pools; existing connections do not retroactively receive a role default.

### Selected policy and optional synchronous qualification

**Asynchronous availability is selected.** Keep `synchronous_standby_names=''`
and `synchronous_commit=on`. Standby loss does not add a replication wait to
positive ACKs. During standby downtime, and after promotion until a replacement
is caught up, locally durable writes have no failover-copy guarantee. The loss
exposure is the unreplicated WAL gap, which can grow throughout standby downtime;
five-second alerting does not cap it. D3's rough expected local-link lag was an
estimate, not a measurement or service guarantee. #291 must record real lag,
ACK/accounting reconciliation, promotion time and frontend recovery time.

Strict synchronous operation remains deferred behind D3's two preconditions:
(1) close and qualify the synchronous-wait cancellation/ACK gap across the actual
runtime paths, including the remaining limits below; (2) provide either a second
standby or an approved manual degrade procedure, so standby maintenance does not
stall mining. The landed share-commit guard addresses part of (1), not both
preconditions. No strict or automatic-degrade policy is approved here.

Provision a stable `application_name=prism_standby_1` from the start. The D3
addendum permits an optional **qualification** flip on the primary, without a
restart, once that intended standby is streaming. For a server whose settings
are managed through `ALTER SYSTEM`, execute these as separate statements outside
a transaction; an operator-managed configuration must use its owning mechanism:

```sql
-- Optional #291 experiment, with synchronous ACK caveats below:
ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (prism_standby_1)';
SELECT pg_reload_conf();
SHOW synchronous_standby_names;

-- Restore the approved asynchronous policy after the experiment:
ALTER SYSTEM SET synchronous_standby_names = '';
SELECT pg_reload_conf();
SHOW synchronous_standby_names;
```

Record the effective settings, replication state, ACK latency and throughput
under load before/during/after the flip, including standby-down behavior and
return to async. A reload request is not evidence of effective configuration;
verify the `SHOW` results and standby identity. Existing D3 alerts intentionally
flag a non-async topology, so record that expected observation during the drill.
This experiment does not establish strict no-unreplicated-ACK durability or
change the approved production policy. PostgreSQL documents the
[reloadable standby-name setting](https://www.postgresql.org/docs/16/runtime-config-replication.html#GUC-SYNCHRONOUS-STANDBY-NAMES).

### Current timeout/cancellation behavior and remaining limits

The ledger sets `statement_timeout=15000` ms by default
(`PRISM_DATABASE_STATEMENT_TIMEOUT_MS`, valid `1..600000`) and
`lock_timeout=5000` ms (`PRISM_DATABASE_LOCK_TIMEOUT_MS`). PostgreSQL can cancel
a synchronous-replication wait **after local commit**, emit a warning and finish
the commit without replica confirmation; disconnects can also leave an unknown
commit outcome. A timeout therefore is not proof of rollback or a reliable
"no positive ACK" policy. See PostgreSQL's
[synchronous-wait cancellation handling](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/replication/syncrep.c).

[#333](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/333) landed #324's
commit reconciliation. The current [share-ACK path](../crates/qbit-prism-server/src/coordinator/miner_submit.rs)
still passes a one-shot commit gate immediately before COMMIT. For a share-pass
append without a block candidate, at `PRISM_SHARE_COMMIT_TIMEOUT_SECONDS` the ACK
path closes the gate if COMMIT has not been sent: the append rolls back
and the miner gets `ledger-confirmation-failed`. If COMMIT is already in
flight, the ACK path waits a further `share_commit_grace` (5 s) for its reply.
Only a severity-ERROR reply counts as a rollback; any other failure, or no
reply by then, is answered `ledger-outcome-unknown` and logged with the share
ID. A sync-rep guard also answers `ledger-outcome-unknown`, not accepted, when
COMMIT took at least the ledger sessions' effective `statement_timeout`,
because that may be a synchronous-replication wait cancelled after local
commit. This is a duration-based guard, not a direct replica-confirmation check.
Share-pass appends carrying a block candidate are never refused by the ACK deadline;
they wait up to `block_only_ack_timeout` and use the same result classifier.
Block-only proofs instead wait for their credit/candidate disposition up to that
bound. See the [commit reconciliation tests](../crates/qbit-prism-server/src/coordinator/miner_tests/commit_reconcile.rs)
and [writer session setup](../crates/qbit-prism-server/src/ledger/connect.rs).

**Remaining limits:** a block-only ACK can follow a locally visible credit whose
sync-rep wait was cancelled; its poll does not observe the crediting transaction's
replication wait. A share-pass COMMIT whose wait is cancelled before
`statement_timeout` (for example by `pg_cancel_backend`) can also evade the
duration guard. PostgreSQL may return a warning and local success in that case.
These paths prevent a blanket no-unreplicated-ACK claim for optional sync mode;
they do not change the approved async loss model.

#291 must exercise the actual PRISM share-ACK path with the standby stopped,
through and beyond these timeouts, including lost client responses. Its
standby-down run must cover block-only proofs as well. **Strict standby-down
ACK behavior is not certified by selecting `on` alone.** Resolve any required
runtime changes with the ledger owner before adopting strict synchronous
operation. Increasing a finite timeout only postpones the question; do not claim
an infinite wait.

### Exact database provisioning choices

Provision the dedicated HA standby with a replication role, scoped HBA
rule and base-backup procedure adapted from [the public-replica runbook](prism-postgres-replica.md).
Use separate primary/standby storage and a separate HA slot. From initial async
provisioning, assign a unique `application_name=prism_standby_1` in the standby's existing
`primary_conninfo` (retain its host, credentials and TLS settings), and set:

```conf
# Primary and dedicated HA standby, so the latter can later be the writer:
fsync = on
full_page_writes = on
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on

# Dedicated HA standby only, alongside standby.signal and its primary_conninfo:
primary_slot_name = 'prism_ha_standby'  # example: operator must provision this slot
wal_receiver_status_interval = 1s     # required by the alert observation contract

# Primary: approved D3 asynchronous policy:
synchronous_standby_names = ''
```

Set the writer role's default to the approved level:
`ALTER ROLE prism_writer IN DATABASE qbit SET synchronous_commit = 'on';`.
Reconnect writer pools after changing role defaults. Reload server configuration
with `SELECT pg_reload_conf()` where applicable; restart for start-only settings.
Do not enable the synchronous name until the standby is streaming and capable
of satisfying the policy, or even bootstrap/writer startup commits can wait.

`application_name`, not the slot name or container name, is what
`synchronous_standby_names` matches. The shipped bootstrap does not assign that
explicit name; operators must configure and verify it on the dedicated HA standby.
Use a different application name and slot for the public-read replica.
Never use a wildcard that could count an unintended replication client.
[PostgreSQL replication configuration](https://www.postgresql.org/docs/16/runtime-config-replication.html).

The bundled **public-read** replica's physical slot is `prism_public_replica`, selected by
`PRISM_POSTGRES_REPLICATION_SLOT` in the base Compose environment and consumed
by `config/prism-postgres/replica-entrypoint.sh` (`pg_basebackup --slot`, then
`primary_slot_name`). It is not the dedicated HA standby's slot. Provision a separate
physical slot for that standby (the template uses `prism_ha_standby`); each slot
retains WAL independently and does not make replication synchronous. Monitor
`active`, `restart_lsn`, retained bytes, WAL disk space and `wal_status`.
The shipped `max_slot_wal_keep_size=-1` default does not cap retention. Operators
must size and approve a cap (for example `max_slot_wal_keep_size='16GB'` only
after measuring WAL rate and outage budget); exceeding it may require a fresh
base backup. Do not drop a live standby's slot to clear disk pressure.
[PostgreSQL slot retention settings](https://www.postgresql.org/docs/16/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE).

Verify from an authorized operator connection to the primary:

```sql
SHOW synchronous_standby_names;
SELECT application_name, state, sync_state, sent_lsn, write_lsn, flush_lsn, replay_lsn
FROM pg_stat_replication;
SELECT slot_name, active, restart_lsn, wal_status,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
FROM pg_replication_slots;
```

For approved D3 expect empty `synchronous_standby_names`, exactly one matching
`prism_standby_1` row in `state='streaming'` / `sync_state='async'`, and its expected
active slot. The separately named public replica can have its own streaming row.
During the optional synchronous experiment, the HA row should become `sync`.
Also inspect the writer session's effective durability through self-check,
not only an administrator's
`SHOW synchronous_commit`, whose role/session defaults may differ.

### Dedicated HA standby alerting

D3 requires alerts for replay lag above **5 seconds** and standby disconnection.
Use the current [D3 alert contract](prism-alert-migration.md#d3-deployment-provided-primarystandby-rules),
[PostgreSQL rule source](prism-postgres-alert-rules.json) and
[primary query extension](prism-postgres-exporter-queries.yaml). These are
**operator-provided primary PostgreSQL exporter/custom-query metrics**, not
native PRISM metrics or a provisioned monitoring deployment. Never substitute
`qbit_prism_public_replica_*`: those describe the separate public-read target.

The rules use a one-minute dwell and one-second primary scrapes/standby status
updates. The lag rule compares the HA standby's replay position with the
primary's durable WAL position sampled five seconds earlier; it does not treat
PostgreSQL's last reported `replay_lag` as a current backlog-age bound. An idle
caught-up standby may report NULL lag. Missing/stale/failed observations,
ambiguous identity and invalid topology remain alertable, not healthy. Verify
permissions, query refresh/cost, scrape cadence and exporter failure behavior in
#291 using the linked contract. An alert threshold is not an enforced loss bound.
[PostgreSQL replication statistics semantics](https://www.postgresql.org/docs/16/monitoring-stats.html#MONITORING-PG-STAT-REPLICATION-VIEW).

## Promotion, fencing and the stable writer endpoint

This is an operator procedure, not automatic failover. Follow it under the
approved asynchronous D3 policy, with an authoritative decision maker outside the
primary/failover pair; the pair alone does not supply a partition-safe election.

1. Record the incident and pause new mining admission if required by the ACK
   policy. Capture acknowledged-share evidence and available WAL positions.
2. **Fence the old primary first.** Confirm power/storage isolation or network
   isolation covering existing connections and every writer. Disable its restart
   automation. If positive fencing cannot be established, do not promote.
3. Verify the single eligible standby: correct cluster/history, recovery active,
   received/replayed WAL positions and storage health. In a planned switch, quiesce
   writes and wait for replay through the captured primary flush LSN. After an
   unplanned loss, only claim the approved replication guarantee; an asynchronous
   or degraded interval requires explicit accounting-loss reconciliation.
4. Promote on that standby with `SELECT pg_promote(wait => true, wait_seconds => 60)`
   and confirm `pg_is_in_recovery() = false`. A timeout needs inspection, not a
   second competing promotion. Apply the approved no-standby ACK policy.
5. Move `prism-writer.internal`/the managed writer endpoint to this sole primary.
   Flush or drain existing database proxy connections and reconnect both PRISM
   pools as needed; DNS changes do not retarget established sockets. Verify both
   frontends use the same writer, matching keys/fingerprint and preserved share
   history, then resume admission under the approved policy.
6. Rejoin the fenced former primary only after a verified rewind or fresh base
   backup of the new primary. Create/reconcile the physical slot on the new
   primary; PostgreSQL 16 physical slots are not automatically transferred by
   this Compose setup. Re-establish the single named standby and verify its
   async streaming/replay status and monitoring. Keep `synchronous_standby_names=''`
   under approved D3; record the interval without a caught-up failover copy.
7. Reconfigure or rebuild the **separate public-read replica** to follow the new
   primary, and verify its read endpoint and freshness. Do not direct public reads
   to the dedicated failover standby. With `PRISM_PUBLIC_REPLICA_MODE=require`,
   the public process refuses a promoted writer. Deliberately choosing `off` to
   read the writer is a separate recorded operator action.

The bundled replica's healthcheck requires recovery and will mark a promoted
instance unhealthy. Its bootstrap entrypoint also refuses to restart a complete
cluster lacking `standby.signal`. For a lab drill, reconfigure the promoted
service to use the stock PostgreSQL entrypoint and a primary healthcheck before
restart; never empty or re-bootstrap the promoted data directory. Production
database role transitions belong to the operator's HA system.
[PostgreSQL promotion and fencing guidance](https://www.postgresql.org/docs/16/warm-standby-failover.html).

## Operator TCP load-balancer readiness contract

Check each **mining frontend**, not the public API, PostgreSQL port, aggregate
pool endpoint or a TCP-connect-only check. No authentication or signing key is
needed for these probes:

- Preferred: `GET http://<frontend-management-address>:<health-host-port>/healthz`.
  Success requires HTTP **200** and JSON boolean **`ok: true`**. Do not use just
  HTTP status or `ready`: the stale-snapshot guard can clear `ok` independently.
  The body includes `schema=qbit.prism.audit-health.v1`, `instance_id`, `ready`,
  `status` and `snapshot_age_seconds`.
- Local equivalent: `qbit-prism-server healthcheck` exits 0 only for success.
  `--url http://.../healthz` selects a remote HTTP target; its internal HTTP timeout
  is 3 seconds. Do not pass `--public-api` for mining routing.
- When the audit listener is disabled (`PRISM_AUDIT_PORT=0`), the subcommand uses
  Stratum instead: send `{"id":1,"method":"mining.get_health","params":[]}\n` to
  that frontend's enabled listener, without subscribe/authorize. Require one
  newline-terminated JSON frame at most 4096 bytes, matching `id=1`, null `error`
  and `result.ready=true`, within 3 seconds. HTTP is preferred for the shipped
  overlay because it also checks the published-health freshness guard.

`ready` is an instantaneous ability to provide current work: prepared work must
match the observed node tip and database payout revision, the tip poll must be
younger than `PRISM_HEALTH_TIP_POLL_MAX_AGE_SECONDS` (default 15), and an enabled
CTV policy must satisfy the current fee floor. The server can also clear readiness
for stalled job delivery. A new tip **or payout-revision change** invalidates
prepared work until rebuilding and publishing its replacement; this normal
transition clears readiness even when the process and TCP listener are alive.

Recommended operator policy, subject to #291's measured rebuild durations:

| Parameter | Value / behavior |
| --- | --- |
| Probe start interval | Every **2 seconds**, per frontend, using a monotonic scheduler. No overlapping probes; do not add a full interval after completion. |
| HTTP timeout | **1 second** total; timeouts, malformed JSON, non-200 or `ok != true` count as failures. A local command adapter must enforce the same outer deadline. |
| Fall threshold | **6 consecutive failures**. A successful probe resets the failure count. Until this threshold, preserve the prior routing state. |
| Rise threshold | **2 consecutive successes** before a down/starting backend accepts new sessions. A failure resets the success count. |
| Routing action | Mark down for **new TCP sessions**; do not kill established sessions solely because of an ordinary rebuild. If all are down, do not silently route to known-unready backends. |

Six starts span ten seconds, so an observed rebuild outage **shorter than ten
seconds** does not by itself eject a previously up backend. A continuously
failing endpoint is ejected within **13 seconds** (at most 2 seconds to the
first start, five further intervals and a 1-second final timeout). This is an
endpoint-observation bound, not a guarantee that every 500k-share rebuild meets
that budget. Measure the latter before sign-off; a longer ordinary rebuild must
lead to an explicitly reviewed threshold and revised bound.

HTTP health is published every `PRISM_HEALTH_REFRESH_SECONDS` (2 seconds by
default in the binary, 5 in Compose). If publication stalls, the
HTTP handler rejects a snapshot older than `max(15, 3 * PRISM_HEALTH_REFRESH_SECONDS)`
seconds (default 15); `ready` alone need not reflect that rejection. With these
defaults, stale-publication detection plus LB hysteresis has a conservative
**28-second** bound. If that environment setting is customized outside Compose,
recompute the bound. Probe schedulers must honor the stated cadence, deadlines
and scheduling budget for either bound to hold. The Compose container check
remains its existing 5-second/3-retry diagnostic; it is not the operator TCP load balancer's
routing policy and Docker does not implement this TCP endpoint for the operator.

Qualification: generate new tips and payout-revision changes under load, record
readiness transitions and routing decisions, confirm ordinary rebuilds stay
within budget, then hold work unavailable and confirm ejection by the stated
bound and re-entry after two successes. This is the #186 carry-over to #291.

## Self-check live instances

`qbit-prism-server self-check` adds `live_instances` to its existing JSON report.
It snapshots `qbit_prism_instances` through a **read-only connection using the
same resolved writer DSN**, before the existing self-check initializes its own
coordinator. That coordinator writes no heartbeat: the diagnostic is not a
frontend, so it never appears in its own sample, leaves no row behind when it
exits, and does not touch the row of a running frontend that shares its
`PRISM_INSTANCE_ID`. No new configuration reader, migration or HA election is
introduced.

The server attempts to publish a heartbeat every **2 seconds**. `observed_at` is
one PostgreSQL `clock_timestamp()` sample through `PRISM_DATABASE_URL`; counts
describe that instant, before the rest of self-check runs. Ages and the fixed
**15-second inclusive** freshness window use that database clock. A fresh
`qbit.prism.audit-health.v1` row with boolean `ready` counts as live even when
`ready=false` during a rebuild. This is process liveness, not mining readiness
or a proof of distinct failure domains. Startup/stopped rows are listed as
inactive; they do not manufacture redundancy. Old rows remain listed as stale.
After a frontend has been stopped and its ID permanently retired, an operator
may run `DELETE FROM qbit_prism_instances WHERE instance_id = '<retired-id>';`
against the writer. Retirement is manual, never automatic; do not remove a
running frontend's row to hide a liveness problem.

| Observation | `status` / `count` | Meaning |
| --- | --- | --- |
| Empty table | `empty` / `0` | Successful read, no instance observations. |
| Only old rows | `stale` / `0` | Heartbeats exist but exceed the window. |
| Only fresh startup/stopped rows | `inactive` / `0` | No observed running server heartbeat. |
| One fresh server row | `observed` / `1` | `single_instance=true` and an HA warning. A single instance must not present itself as HA. |
| Two fresh server rows | `observed` / `2` | IDs and rows listed; inspect readiness separately. |
| Future-dated or unrecognized fresh rows | `unknown` / `null` | Count is not asserted; inspect `unknown_instances`. |
| Failed query/connect, or 5-second read deadline exceeded | `failed` / `null` | No sample timestamp or instance list; self-check emits `ok=false` and exits unsuccessfully, without logging a credentialed DSN. |

The report always uses the same `qbit.prism.self-check.v2` shape. A heartbeat
read failure does not skip the remaining local checks: the command completes
its report with `ok=false`, null unavailable fields, and a nonzero exit. Local
check failures also produce that shape and a nonzero exit; successful local
checks cannot mask a failed heartbeat read. `single_instance` is null when no
live rows are known (or the observation is unknown/failed), true for exactly one,
and false for two or more.

Successful observations include `instance_ids`, `instances`, `stale_instances`,
`inactive_instances` and `unknown_instances`. Fewer than two known live rows
sets `ha_warning`. The existing top-level successful `ok` covers the command's
local checks; it is **not** an HA certification. [#412](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/412)
removed operator-command heartbeat registration, including the old transient
`starting` write. Self-check no longer manufactures or overwrites a frontend row.

The `ledger::instances::live_instance_tests` unit tests exercise
empty/stale/fresh/boundary/future/startup rows and a failed connection. The SQL
test `ledger::instances::live_instance_tests::heartbeat_sql_observes_empty_stale_and_missing_table`
creates a connection-local temporary table and verifies an empty result, an old
heartbeat and a missing-table error without modifying deployment rows. Export
`PRISM_TEST_DATABASE_URL` for a **disposable** database, then run:

```sh
(
  set -eu
  name=ledger::instances::live_instance_tests::heartbeat_sql_observes_empty_stale_and_missing_table
  cargo test --locked -p qbit-prism-server --lib -- --list --exact "${name}" |
    grep -Fqx "${name}: test"
  out="$(PRISM_TEST_REQUIRE_INTEGRATION=1 cargo test --locked -p qbit-prism-server \
    --lib -- --exact "${name}" --nocapture 2>&1)" || { printf '%s\n' "${out}"; exit 1; }
  printf '%s\n' "${out}"
  printf '%s\n' "${out}" | grep -Fq 'test result: ok. 1 passed; 0 failed;'
)
```

Record the step as passed only when the subshell exits zero. It fails when the
name does not enumerate exactly that test, when the run does not report one
pass (a `0 passed` result is a failure), and, because
`PRISM_TEST_REQUIRE_INTEGRATION=1` turns the gate's skip into a failure, when
`PRISM_TEST_DATABASE_URL` is unset, empty or unreachable.

For a full-command SQL failure probe, use a **disposable** database
and a DSN whose `options=-csearch_path=missing_ha_probe` hides the heartbeat
table; `self-check` must return `live_instances.status=failed`, `count=null`,
`observed_at=null` and a nonzero exit, not an empty or healthy cluster. The SQL
test samples directly through the reader; the operator command also leaves
frontend heartbeat registration untouched.

## #281 acceptance evidence

This matrix maps the six #281 acceptance items to landed evidence as of
`3.x.x` commit `31d20f464be9e465254b878253f56d9b69f92f66`. Test references describe
existing coverage and linked historical execution, not tests rerun by this
reconciliation. No live gate is completed by documentation or issue closure alone.

| #281 acceptance item | Landed evidence | Remaining qualification / status |
| --- | --- | --- |
| Two healthy overlay coordinators; reconnect to either and accept the resumed share | [#304](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/304) (`984b3b44`) supplies the overlay; [#397](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/397) (`0cb9c233`) activates compact resume. [#273 closing evidence](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/273#issuecomment-5704427645) records PostgreSQL/runtime qualification; [`compact_runtime_e2e.rs`](../crates/qbit-prism-server/tests/compact_runtime_e2e.rs) includes `real_socket_reconnect_resumes_original_entropy_mask_and_submits_once`. | **Open:** bring up this overlay, route/reconnect a miner to each frontend and record accepted original work. Runtime/test evidence is not this deployed exercise. |
| Compose config validated in CI | #304; [CI's `Validate PRISM HA Compose stacks`](../.github/workflows/ci.yml) renders all four combinations and asserts distinct IDs/ports, one writer DSN and reachable health bindings. | **Landed.** Rendering does not establish healthy containers, RPC reachability or production placement. |
| Seven requirements, exact settings and provided/operator markings | #304 and the [requirements table](#seven-deployment-requirements), reconciled here with the approved D3 comments. | **Landed documentation.** Production provisioning and effective configuration still require verification. |
| Self-check lists live instance count and IDs | #304; [#362](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/362) consolidates the reader/heartbeat contract; #412 removes tool heartbeats. [`live_instance_tests`](../crates/qbit-prism-server/src/ledger/instances.rs), including `heartbeat_sql_observes_empty_stale_and_missing_table`; [#408](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/408) hardens the qualification command. | **Landed.** Capture two distinct fresh frontend IDs in the actual overlay drill; count alone is not proof of separate failure domains. |
| #291 failover drill on this overlay under D3 | Procedure above; [#333](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/333) (`e13051cf`) supplies commit reconciliation/guard coverage. [`postgres_failover.rs`](../crates/qbit-prism-server/tests/postgres_failover.rs) has `acknowledged_shares_survive_synchronous_primary_loss_and_pool_reconnect`, a disposable **synchronous** fixture. | **Open:** actual async promotion/fencing/writer-endpoint drill under mining load, measured replication gap/loss and promotion/recovery time, accounting reconciliation, dedicated-standby alert checks, and the qualified sync flip/back experiment. The synchronous fixture cannot prove D3's async loss behavior or the real overlay. |
| LB preserves ordinary rebuilds and ejects prolonged unavailability within the stated bound | #304 documents readiness/hysteresis; [#331](https://github.com/Qbit-Org/qbit-mining-bootstrap/pull/331) (`2914a629`) supplies the [deterministic simulator](prism-ha-readiness-probe-harness.md) and [`test_prism_ha_readiness_probe.py`](../tests/test_prism_ha_readiness_probe.py). | **Open:** real operator TCP load balancer under tip/revision churn and prolonged unavailability; record routing, ejection and recovery timings. The simulator neither deploys nor qualifies that load balancer. |

#291 must report observed acknowledged-share loss (including zero if observed),
the unreplicated interval and reconciliation under **asynchronous D3**. Historical
zero-loss wording is not a guarantee supplied by this policy. Runtime capability,
synthetic tests and rendered Compose do not establish zero outage or cutover readiness.

## Cutover checklist

- [ ] Verify and record the approved async D3 settings, distinct HA/public replica
  identities/slots, standby-down ACK behavior and the interval after promotion
  before a replacement is caught up; qualify dedicated-standby alert observations.
- [ ] Deploy independent nodes, frontends and database storage/failure domains;
  validate both effective configurations and the stable writer/public endpoints.
- [ ] Confirm the operator TCP load balancer implements the per-frontend probe contract and
  measured hysteresis; verify two distinct fresh heartbeat IDs.
- [ ] Run the addendum's optional sync flip/back qualification with ACK latency and
  throughput evidence; retain both strict-mode preconditions and exercise timeout,
  non-timeout cancellation, block-only and ambiguous-commit cases.
- [ ] **Exercise database promotion under mining load before production traffic**:
  fence the old primary, promote, move the writer endpoint, verify acknowledged
  share accounting and both frontend recovery, then rejoin the one standby.
- [ ] Complete #291 using D1's separate dimensions: 500k-share headroom (400k
  regression), 2,000 shares/s for a minute, 500/s for five minutes, 2,000 sessions
  and dense block cadence; public read load still needs measurement. Do not infer
  one dimension from another.
- [ ] Verify real-overlay cross-frontend job resume using the landed #273 runtime;
  record the actual failover drill results and async loss/reconciliation evidence.
