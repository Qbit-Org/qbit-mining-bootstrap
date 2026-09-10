# PRISM HA Compose and database reference architecture

This is the deployment contract for [#281](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/281),
after the 2026-09-10 scope trim. [D3 fixes the topology at one primary and one
standby](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/260#issuecomment-5624821505).
The replication mode and share-ACK policy during standby loss remain **undecided**.
The options below are for Dan's decision, not an approved production policy.

This separate document owns the complete frontend-to-database architecture.
[prism-postgres-replica.md](prism-postgres-replica.md) remains the detailed guide
to provisioning the existing public read replica and managing its slot.
The supplied overlay is a local two-frontend deployment, not a production HA
certification: its default node and database still share a Compose host.
Cross-frontend job resume is **pending #273**; this change does not claim that
acceptance criterion or the promotion-under-load qualification in #291.

## Seven deployment requirements

| # | Requirement | Exact wiring/settings | Delivery |
| --- | --- | --- | --- |
| 1 | Two active mining frontends | `prism-coordinator` and `prism-coordinator-2`; `PRISM_INSTANCE_ID=prism-frontend-1` / `prism-frontend-2`; identical `PRISM_DATABASE_URL`, payout configuration and signing keys; shared database job/extranonce authority. | **Provided here:** runtime and two-service overlay. **Operator-supplied:** placement in independent failure domains and matching secret distribution. Resume remains pending #273. |
| 2 | Highly available Stratum TCP endpoint | Operator endpoint sends TCP to frontend ports `3340` / `3343` on the Compose host by default; probe each frontend's own `/healthz`, with the hysteresis below. Publish that endpoint in `PRISM_PUBLIC_STRATUM_URL`. | **Operator-supplied:** hashrouter/load balancer, endpoint redundancy, routing and firewall. No LB service is shipped. |
| 3 | A separate node per frontend | Set `PRISM_HA_RPC_HOST_1=node-1` and `PRISM_HA_RPC_HOST_2=node-2`, or set each `PRISM_HA_RPC_URL_1/2` to its node's complete HTTP(S) URL. Same chain/genesis and consensus policy on both. | **Provided here:** independent per-service RPC wiring. **Operator-supplied:** both independent production nodes, authentication and placement. Both lab defaults use bundled `qbitd`. |
| 4 | PostgreSQL primary plus one standby, with the chosen ACK durability and checked public reads | Primary: `fsync=on`, `full_page_writes=on`, `wal_level=replica`, `max_wal_senders=10`, `max_replication_slots=10`. Standby: `hot_standby=on`, a physical slot, and an explicit replication `application_name`; D3 selects `synchronous_standby_names` / `synchronous_commit` below. `prism-public-api` uses `PRISM_PUBLIC_DATABASE_URL` and `PRISM_PUBLIC_REPLICA_MODE=require`. | **Provided here:** bundled primary, one **asynchronous** standby, slot bootstrap and checked public API. **Operator-supplied:** synchronous policy if selected, separate storage/failure domains, backups and capacity. |
| 5 | Promotion | After fencing and checking the eligible standby, `SELECT pg_promote(wait => true, wait_seconds => 60)`; verify `pg_is_in_recovery() = false` and the required history before moving the writer endpoint. | **Operator-supplied:** decision authority, failure detection, promotion execution and rehearsal. Procedure below; no automatic promotion service. |
| 6 | Fence the old primary | Power/storage fencing or enforced network isolation that stops **all existing and new** writer connections, including both frontends and settlement workers; disable old-primary restart automation. Keep the fence until rejoin as a standby. | **Operator-supplied:** fencing mechanism and positive confirmation. Changing DNS or stopping one frontend is insufficient. |
| 7 | Stable authoritative writer endpoint | Both frontends use the same `PRISM_DATABASE_URL=postgresql://prism_writer:<secret>@prism-writer.internal:5432/qbit?sslmode=verify-full`; the endpoint routes only to the unfenced primary. | **Provided here:** shared DSN pass-through and external-DB overlay. **Operator-supplied:** endpoint ownership, TLS, failover routing, connection draining and fencing. The bundled `prism-postgres` name is not a failover endpoint. |

The primary and standby are the **only two database members**. Serving the
public API from that standby does not create a second standby candidate.

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
public endpoint. For a remote hashrouter bind the health mapping to the intended
private host address, and enforce the corresponding network ACL.

Changing RPC targets does not remove the inherited `depends_on: qbitd`; the
bundled node still starts in these stacks. Operators deploying independently
managed nodes can override dependencies in their placement configuration.

Safe configuration verification (use fixture values, never log real secrets):

```sh
PRISM_HA_INSTANCE_ID_1=verify-east PRISM_HA_INSTANCE_ID_2=verify-west \
PRISM_HA_RPC_HOST_1=node-east.internal \
PRISM_HA_RPC_URL_2=http://node-west.internal:19452/ \
docker compose -f compose.yaml -f compose.prism-ha.yaml --profile prism config \
  --format json | jq '.services | with_entries(select(.key | startswith("prism-coordinator"))) |
    map_values({ports, environment: (.environment |
      {PRISM_INSTANCE_ID, PRISM_AUDIT_BIND, PRISM_AUDIT_PORT})})'

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

## D3: replication and ACK policy still needs a decision

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

| Decision for Dan | Exact policy | Where to set it | Standby-loss ACK contract | Recommendation / remaining gate |
| --- | --- | --- | --- | --- |
| Strict replicated durability | `synchronous_commit=on`; `synchronous_standby_names='FIRST 1 (prism_standby_1)'` | Set names in primary `postgresql.conf`, then `SELECT pg_reload_conf();`; PRISM's pool `after_connect` sets `on` unless the role selects `remote_apply`. | Intended policy: stop positive ACKs until a standby can durably confirm. One standby means losing it consumes all redundancy. | **Recommended**, if preserving acknowledged shares after primary loss outranks continued ACK availability. Must qualify cancellation and timeout handling before claiming strict enforcement. |
| Strict durability plus immediate read visibility | `remote_apply` with the same named standby | Set names in primary `postgresql.conf` and reload; run `ALTER ROLE prism_writer IN DATABASE qbit SET synchronous_commit = 'remote_apply';` and reconnect **all** writer pools. The pool preserves this value. | Intended policy: stop ACKs until replay confirmation. | Choose only if ACK-time standby visibility is required; extra replay latency couples public reads to mining. Same qualification gate. |
| Asynchronous availability | `on`; `synchronous_standby_names=''` | Clear names in primary `postgresql.conf` and reload; PRISM's pool selects `on` (or preserves `remote_apply`, which also only waits locally with no names). | Continue locally durable ACKs while the standby is absent; primary loss can lose the replication gap. | Explicit loss-risk choice; do not call it lossless accounting failover. This is the existing bundled **lab** default, not an approval of D3. |
| Synchronous normally, explicitly degraded during outage | `on` + named standby normally; an authorized operator changes names to `''` and reloads during the outage | Change names in primary `postgresql.conf` and run `SELECT pg_reload_conf();` at each policy transition; PRISM keeps `on` in its writer sessions. | ACKs continue locally after degradation; pending waits can be released. New ACKs have no replica guarantee until synchronization is restored. | Requires Dan to approve who may degrade, for how long, and how the risk interval is recorded. No automatic fallback is supplied or recommended by this change. |

No timeout, missing heartbeat, load-balancer state or Compose restart authorizes
changing that policy. After promotion there is temporarily **no standby** until
the old member is safely rebuilt/rejoined: the same D3 decision governs ACKs in
that interval. An approved strict policy must wait for replacement redundancy;
promotion alone cannot restore both two-copy durability and ACK availability.

### Existing timeout/cancellation limitation

The ledger sets `statement_timeout=15000` ms by default
(`PRISM_DATABASE_STATEMENT_TIMEOUT_MS`, valid `1..600000`) and
`lock_timeout=5000` ms (`PRISM_DATABASE_LOCK_TIMEOUT_MS`). PostgreSQL can cancel
a synchronous-replication wait **after local commit**, emit a warning and finish
the commit without replica confirmation; disconnects can also leave an unknown
commit outcome. A timeout therefore is not proof of rollback or a reliable
"no positive ACK" policy. See PostgreSQL's
[synchronous-wait cancellation handling](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/replication/syncrep.c).

This overlay does not change the ledger's timeout/notice handling. #291 must
exercise the actual PRISM share-ACK path with the standby stopped, through and
beyond these timeouts, including lost client responses. **Strict standby-down
ACK behavior is not certified by selecting `on` alone.** Resolve any required
runtime changes with the ledger owner before adopting that D3 option. Increasing
a finite timeout only postpones the question; do not claim an infinite wait.

### Exact database provisioning choices

Provision the single standby with the dedicated replication role, scoped HBA
rule and base backup in [the replica runbook](prism-postgres-replica.md).
Use separate primary/standby storage. Before selecting synchronous replication,
assign a unique `application_name=prism_standby_1` in the standby's existing
`primary_conninfo` (retain its host, credentials and TLS settings), and set:

```conf
# Both database members, so the standby can later be the writer:
fsync = on
full_page_writes = on
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on

# Standby only, alongside standby.signal and its primary_conninfo:
primary_slot_name = 'prism_public_replica'

# Primary: choose exactly one D3 branch after approval:
# synchronous_standby_names = 'FIRST 1 (prism_standby_1)'  # synchronous
# synchronous_standby_names = ''                         # asynchronous
```

Set the writer role's default to the approved supported level, for example
`ALTER ROLE prism_writer IN DATABASE qbit SET synchronous_commit = 'on';`
or the same statement with `remote_apply`. Reload server configuration with
`SELECT pg_reload_conf()` where applicable; restart for start-only settings.
Do not enable the synchronous name until the standby is streaming and capable
of satisfying the policy, or even bootstrap/writer startup commits can wait.

`application_name`, not the slot name or container name, is what
`synchronous_standby_names` matches. The shipped bootstrap does not assign that
explicit name; operators must configure and verify it before using this template.
Never use a wildcard that could count an unintended replication client.
[PostgreSQL replication configuration](https://www.postgresql.org/docs/16/runtime-config-replication.html).

The existing physical slot is `prism_public_replica`, selected by
`PRISM_POSTGRES_REPLICATION_SLOT` in the base Compose environment and consumed
by `config/prism-postgres/replica-entrypoint.sh` (`pg_basebackup --slot`, then
`primary_slot_name`). It retains WAL; it does not make replication synchronous.
Monitor `active`, `restart_lsn`, retained bytes, WAL disk space and `wal_status`.
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

For strict mode expect exactly the intended `prism_standby_1` streaming row,
`sync_state='sync'`, and the expected active slot. Also inspect the writer
session's effective durability through self-check, not only an administrator's
`SHOW synchronous_commit`, whose role/session defaults may differ.

## Promotion, fencing and the stable writer endpoint

This is an operator procedure, not automatic failover. Follow it under the
chosen D3 policy, with an authoritative decision maker outside the two database
members; two members alone do not supply a partition-safe election.

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
   sync status before lifting a degraded interval or resuming strict ACKs.
7. Repoint the public read endpoint to that standby. With
   `PRISM_PUBLIC_REPLICA_MODE=require`, the public process refuses the promoted
   writer. Deliberately choosing `off` to read the writer is a separate operator
   action and must be recorded.

The bundled replica's healthcheck requires recovery and will mark a promoted
instance unhealthy. Its bootstrap entrypoint also refuses to restart a complete
cluster lacking `standby.signal`. For a lab drill, reconfigure the promoted
service to use the stock PostgreSQL entrypoint and a primary healthcheck before
restart; never empty or re-bootstrap the promoted data directory. Production
database role transitions belong to the operator's HA system.
[PostgreSQL promotion and fencing guidance](https://www.postgresql.org/docs/16/warm-standby-failover.html).

## External hashrouter / TCP load-balancer readiness contract

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

HTTP health is normally published every 2 seconds. If publication stalls, the
HTTP handler rejects a snapshot older than `max(15, 3 * PRISM_HEALTH_REFRESH_SECONDS)`
seconds (default 15); `ready` alone need not reflect that rejection. With these
defaults, stale-publication detection plus LB hysteresis has a conservative
**28-second** bound. If that environment setting is customized outside Compose,
recompute the bound. Probe schedulers must honor the stated cadence, deadlines
and scheduling budget for either bound to hold. The Compose container check
remains its existing 5-second/3-retry diagnostic; it is not the hashrouter's
routing policy and Docker does not implement this TCP endpoint for the operator.

Qualification: generate new tips and payout-revision changes under load, record
readiness transitions and routing decisions, confirm ordinary rebuilds stay
within budget, then hold work unavailable and confirm ejection by the stated
bound and re-entry after two successes. This is the #186 carry-over to #291.

## Self-check live instances

`qbit-prism-server self-check` adds `live_instances` to its existing JSON report.
It snapshots `qbit_prism_instances` through a **read-only connection using the
same resolved writer DSN**, before the existing self-check initializes its own
coordinator (which writes a `starting` heartbeat). No new configuration reader,
ledger method, migration or HA election is introduced.

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
local checks; it is **not** an HA certification. Run the check in an existing
frontend with its configured ID; its legacy initialization still briefly writes
`starting` for that ID after the snapshot. Concurrent diagnostics may therefore
temporarily undercount; wait for the next server heartbeat before rechecking.

The tools unit tests exercise empty/stale/fresh/boundary/future/startup rows and
a failed connection. This SQL test creates a connection-local temporary table
and verifies an empty result, an old heartbeat and a missing-table error without
modifying deployment rows:

```sh
PRISM_TEST_DATABASE_URL='postgresql://fixture:fixture@127.0.0.1:5432/fixture' \
  cargo test -p qbit-prism-server tools::live_instance_tests -- --nocapture
```

For a full-command SQL failure probe, use a **disposable** database
and a DSN whose `options=-csearch_path=missing_ha_probe` hides the heartbeat
table; `self-check` must return `live_instances.status=failed`, `count=null`,
`observed_at=null` and a nonzero exit, not an empty or healthy cluster. The SQL
test samples before any coordinator startup, so the command's legacy heartbeat
write cannot turn its empty/stale fixtures into a new live frontend.

## Cutover checklist

- [ ] Record Dan's D3 replication level, named-standby choice and standby-down
  ACK policy, including the interval after promotion before rejoin.
- [ ] Deploy independent nodes, frontends and database storage/failure domains;
  validate both effective configurations and the stable writer/public endpoints.
- [ ] Confirm the hashrouter implements the per-frontend probe contract and
  measured hysteresis; verify two distinct fresh heartbeat IDs.
- [ ] Resolve the synchronous-wait cancellation/ACK qualification gap if strict
  durability is selected; include timeout and ambiguous-commit cases.
- [ ] **Exercise database promotion under mining load before production traffic**:
  fence the old primary, promote, move the writer endpoint, verify acknowledged
  share accounting and both frontend recovery, then rejoin the one standby.
- [ ] Complete #291 using D1's separate dimensions: 500k-share headroom (400k
  regression), 2,000 shares/s for a minute, 500/s for five minutes, 2,000 sessions
  and dense block cadence; public read load still needs measurement. Do not infer
  one dimension from another.
- [ ] Verify cross-frontend job resume after **#273** lands; record the actual
  failover drill results and approved loss/reconciliation policy.
