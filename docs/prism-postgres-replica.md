# PRISM Postgres read replica

For two mining frontends, the operator load-balancer contract and the primary
plus **one** standby promotion/ACK policy choices, see the
[HA reference architecture](prism-ha-reference-architecture.md). Replication and
standby-down ACK policy remain open under D3; the bundled replica is asynchronous.

`prism-public-api` runs `qbit-prism-server public-api` and serves public reads
from a hot standby in the default Compose topology. The native process uses a
separate SQL pool, requires no signing keys, and serves canonical audit bytes
from PostgreSQL without an artifact filesystem mount. This document covers how that standby is provisioned in
compose, what a production deployment has to do by hand instead, and how to
operate the replication slot it depends on.

Scope note: everything below describes what is in this repository. The
production topology beyond the compose files (placement, storage classes,
backup schedules, monitoring stack) is not modelled here and is not guessed at.

## What the compose wiring does

`compose.yaml` defines two Postgres services under the `prism` profile:

- `prism-postgres` — the primary the coordinator writes to.
- `prism-postgres-replica` — a streaming standby, read-only, that
  `prism-public-api` reads through `PRISM_PUBLIC_DATABASE_URL`.

The primary runs `postgres -c hba_file=/etc/postgresql/pg_hba.conf` with
`config/prism-postgres/pg_hba.conf` bind-mounted read-only. That file
reproduces the stock image's authentication rules and adds the two
`host replication ...` rules the standby connects through. Postgres 16 already
defaults to `wal_level=replica`, `max_wal_senders=10` and
`max_replication_slots=10`, which is enough for one standby, so no other server
settings are overridden.

The standby's data directory is never `initdb`'d.
`config/prism-postgres/replica-entrypoint.sh` runs first and, when the
directory does not already hold a completed standby, streams one from the
primary:

```
pg_basebackup --host=prism-postgres --port=5432 --username="$PRISM_POSTGRES_USER" \
  --pgdata=/var/lib/postgresql/data --format=plain --wal-method=stream \
  --progress --write-recovery-conf --slot=prism_public_replica --create-slot
```

`--create-slot` creates the physical slot on the primary;
`--write-recovery-conf` writes `standby.signal` and a `primary_conninfo`
(including `primary_slot_name`) into `postgresql.auto.conf`. The script then
`exec`s the stock image entrypoint, which sees a populated data directory,
skips initialization, and starts the server in standby mode.

Operational details worth knowing:

- **Bootstrap retries.** The base backup is retried on a bounded loop (60
  attempts, 5s apart, tunable through
  `PRISM_POSTGRES_REPLICA_BASEBACKUP_ATTEMPTS` and
  `PRISM_POSTGRES_REPLICA_BASEBACKUP_RETRY_SECONDS`) so a cold
  `docker compose up` succeeds even before the primary is accepting
  connections. Exhausting the budget exits the container non-zero rather than
  looping forever.
- **Slot reuse.** The primary keeps the slot while the standby is gone, so
  `--create-slot` would fail on a rebuild. The script checks
  `pg_replication_slots` first and reuses an existing slot instead of
  recreating it.
- **The replica volume is disposable.** `standby.signal`, not `PG_VERSION`, is
  the completion marker: `pg_basebackup` writes `PG_VERSION` early in the
  stream but `standby.signal` only at the end, so an interrupted bootstrap is
  distinguishable from a usable standby. Anything in the data directory without
  `standby.signal` is treated as debris from an interrupted bootstrap and is
  cleared before retrying. Never point this service at a data directory holding
  a cluster you care about.
- **Authentication rules on the standby.** The base backup copies the
  *primary's data directory* `pg_hba.conf` — the one `initdb` wrote, which
  trusts loopback — not the mounted `hba_file`. That is what the standby then
  runs with, and it is why the standby's healthcheck names the role explicitly:
  an unauthenticated `pg_isready` would reach the role lookup on a trusted
  connection and log `FATAL: role "root" does not exist` on every interval.
- **Healthcheck.** TCP listener, then a real `SELECT 1`, then
  `pg_is_in_recovery()` returning `t`. The last clause means an out-of-band
  promotion makes the service report unhealthy instead of quietly leaving the
  read tier pointed at a second writable cluster.

### Non-default superuser names

`config/prism-postgres/pg_hba.conf` is a plain file; compose does not
interpolate it. The replication rules name the default superuser `qbit`
literally:

```
host    replication     qbit            0.0.0.0/0               scram-sha-256
host    replication     qbit            ::/0                    scram-sha-256
```

A deployment that overrides `PRISM_POSTGRES_USER` **must edit those two lines**
to match, or the base backup fails with `no pg_hba.conf entry for replication
connection from host ...`.

### Settings

| Variable | Default | Effect |
| --- | --- | --- |
| `PRISM_POSTGRES_REPLICATION_SLOT` | `prism_public_replica` | Physical slot the standby creates and streams through. |
| `PRISM_POSTGRES_REPLICA_DATA_SOURCE` | `prism-postgres-replica-data` | Volume or host path backing the standby's data directory. |
| `PRISM_PUBLIC_DATABASE_URL` | `postgresql://qbit:change-this@prism-postgres-replica:5432/qbit` | Read-only DSN `prism-public-api` uses. Compose passes it into the container as `PRISM_DATABASE_URL`; it is a separate operator knob because `PRISM_DATABASE_URL` names the primary, which the coordinator needs it to. |
| `PRISM_PUBLIC_POSTGRES_PASSWORD` | Empty | Optional dedicated public-reader password, passed by Compose as `PGPASSWORD`. Used when the public DSN omits its password; never defaults to `PRISM_POSTGRES_PASSWORD`. |
| `PRISM_PUBLIC_REPLICA_MODE` | `require` in compose, `off` in code | `require` refuses replica-backed routes unless the backing server is in recovery with a live replication stream. `off` serves whatever the DSN names — the behaviour before a standby existed. |
| `PRISM_PUBLIC_REPLICA_MAX_LAG_SECONDS` | `60` | How long the standby's replication stream may be silent before its answers stop counting as current. |

`PRISM_PUBLIC_REPLICA_MODE` defaults to `off` in the code and `require` in the
shipped compose. That split is deliberate: merging the replica work must not
503 a deployment that has not provisioned a standby yet, but anyone who brings
the stack up from this repository's compose gets the enforced contract without
having to opt in. Cut over by provisioning the standby, pointing
`PRISM_PUBLIC_DATABASE_URL` at it, and then setting `require`.

### Public-reader credentials

For a dedicated reader, set `PRISM_PUBLIC_DATABASE_URL` to that role's complete
DSN, including its own password (percent-encode reserved URL characters):

```dotenv
PRISM_PUBLIC_DATABASE_URL=postgresql://prism_reader:<encoded-reader-password>@standby.internal:5432/qbit?sslmode=verify-full
```

Alternatively, omit the password from the DSN and set
`PRISM_PUBLIC_POSTGRES_PASSWORD` to the reader's raw password. In `.env` files,
single-quote that value so Compose preserves dollar signs and comment characters
literally; do not percent-encode this separate carrier. For example, these
synthetic values preserve the password `reader$literal #:@/383`:

```dotenv
PRISM_PUBLIC_DATABASE_URL=postgresql://prism_reader@standby.internal:5432/qbit?sslmode=verify-full
PRISM_PUBLIC_POSTGRES_PASSWORD='reader$literal #:@/383'
```

If the password contains a single quote, use the percent-encoded DSN form above
so the `.env` file also remains valid for the shell-based preflight scripts.
Set the passwordless reader DSN as well: the carrier alone does not select a
reader role, and a default DSN with an embedded bootstrap password still takes
precedence.
Compose maps these two settings to `PRISM_DATABASE_URL` and `PGPASSWORD` inside
`prism-public-api`.
The native SQLx driver loads PostgreSQL environment defaults before applying
the DSN; a DSN password takes precedence over `PGPASSWORD`. The public process
receives no extra bootstrap password, even when production, external-database
or HA overlays are applied. Keep reader and bootstrap passwords distinct.

Create the reader on the primary so the role and grants reach its physical
standby. Grant `CONNECT` on the database, `USAGE` on the application schema and
`SELECT` on the tables used by the public read models. It must not own the
database or tables, inherit a writer role, or have superuser, replication,
create-role or create-database privileges. Reconcile read grants when migrations
add public tables. In `require` mode it also needs `pg_read_all_stats` so the
readiness probe can see `pg_stat_wal_receiver.last_msg_receipt_time`; this
monitoring grant does not permit replication or application writes. Role
provisioning remains an operator task.

With password authentication required by PostgreSQL, an omitted, empty or
incorrect reader password leaves `/healthz` unready with an authentication
error; database-backed public reads return 503 and
`qbit-prism-server healthcheck --public-api` exits unsuccessfully. The listener
stays up to report the failure and retry its readiness probe. It does not borrow
`PRISM_POSTGRES_PASSWORD`. The HTTP healthcheck needs no database password of
its own. Non-database public metadata can still be served while unready.

Compatibility: an unset or empty `PRISM_PUBLIC_DATABASE_URL` retains the base
Compose lab DSN with the bootstrap password embedded; the external-database
overlay instead retains its fallback to `PRISM_DATABASE_URL` and replica mode
`off`. These defaults do not provide a dedicated-reader boundary. For an
external fallback DSN without a password, explicitly supply the intended
public credential with `PRISM_PUBLIC_POSTGRES_PASSWORD`. The former implicit
bootstrap `PGPASSWORD` is no longer supplied. A direct binary or `docker run`
invocation still uses `PRISM_DATABASE_URL` and SQLx's native PostgreSQL settings;
the `PRISM_PUBLIC_*` credential names above are Compose inputs only. Supply only
reader credentials to that process, including any `PGPASSWORD` or password file.
The shipped Compose service sets `PGPASSWORD` to empty when its reader carrier
is unset, so it does not select a password file as an implicit replacement.

#### Preflight and production policy

`public-api` and `check-public-database-config` use the same SQLx options parser
and production policy. The existing `DatabaseConfig` contract rejects the
`change-this` marker anywhere in a production DSN. Public-reader validation also
rejects that marker in the **effective decoded password**, including a URI or
query password, `PGPASSWORD`, or a native SQLx password-file entry. An unused
carrier does not override or invalidate an explicit non-default DSN password.
The `qbit` role name is permitted; the lab password remains permitted outside
production. This is a default-credential check, not a password-strength or
database-role privilege check.

Production is selected by the public process's `QBIT_PRODUCTION`,
`QBIT_TOOLS_PRODUCTION`, or a `QBIT_CHAIN` of `main`/`mainnet`, following
`DatabaseConfig`. A missing, empty, malformed, unsupported-scheme, or invalid
SQLx-options **effective DSN** fails before connecting or opening the listener,
with a value-free configuration error. Missing, empty, or incorrect **passwords**
are not rejected solely for their absence: PostgreSQL may use another supported
authentication mechanism. Password authentication failures remain readiness
failures as described above. Validation neither connects to PostgreSQL nor
proves authentication, grants, schema availability, or replication health.

`scripts/check-env.sh` now runs this native validator for the Prism lane through
the selected Compose public service, using the image and effective environment
that deployment will use. Prepare the selected PRISM image before running
preflight. The check starts no service dependencies and publishes no ports;
its one-shot container is removed on exit. A missing/old image or failed
launcher is a failed preflight, never a passing or skipped credential check.
Pass the same additional overlays in deployment order, for example:

```sh
DEPLOY_ENV_FILE=/absolute/path/deployment.env bash scripts/check-env.sh \
  --compose-file compose.production.yaml \
  --compose-file compose.prism-external-db.yaml \
  --compose-file compose.prism-ha.yaml
```

`--public-reader-only` runs just this credential configuration preflight; it
does not replace the other deployment checks. The preflight preserves Compose's
environment precedence, explicit empty overrides, and base/external DSN
fallbacks, without sourcing shell example defaults into the container. It
uses only the public service's reader carrier and does not add a bootstrap or
writer password. For direct binary execution, export the reader's
`PRISM_DATABASE_URL` and, if needed, its `PGPASSWORD` or `PGPASSFILE`, then run
`qbit-prism-server check-public-database-config` before `public-api`.

### What the bound actually bounds

The enforced number is the **walreceiver heartbeat age**, not replay lag.
Heartbeats flow every `wal_receiver_status_interval` (10s by default) whether
or not the primary has anything to send; replay lag grows with wall clock on an
idle primary. Bounding replay lag would therefore 503 a perfectly healthy
standby every time the pool went quiet — exactly when the dashboard is least
likely to be wrong.

Replay lag and apply backlog are still measured and published, because they are
the numbers an operator wants:

- `X-Prism-Replica-Lag-Seconds` on every replica-backed response;
- the `replica` block in `prism-public-api`'s `/healthz`;
- `qbit_prism_public_replica_*` gauges on its `/metrics`, including
  `qbit_prism_public_replica_refusals_total`.

Freshness ages locally: the bound is checked against the heartbeat age the last
successful probe reported **plus how long ago that probe ran**. A probe that
starts failing ages its own last good answer out of the bound rather than
pinning it there, so a standby that stops answering entirely ends in a 503
rather than an indefinite 200.

Routes that read no replica state are not gated — today that is
`/public/v1/mining-configuration`, assembled from environment, and the
content-addressed `/public/v1/artifacts/{sha256}`, whose body either hashes to
the requested digest or does not. The native route classification preserves these two freshness-independent
responses; artifact bytes still undergo content-hash verification.
| `PRISM_POSTGRES_REPLICA_BASEBACKUP_ATTEMPTS` | `60` | Bootstrap retry budget. |
| `PRISM_POSTGRES_REPLICA_BASEBACKUP_RETRY_SECONDS` | `5` | Delay between bootstrap attempts. |

## Production provisioning checklist

The compose wiring is a lab convenience: it bootstraps as the superuser over a
network it trusts. A production standby should be provisioned deliberately.
This is a checklist, not automation — nothing in this repository performs these
steps.

1. **Create a dedicated replication role on the primary.** Do not replicate as
   the superuser.

   ```sql
   CREATE ROLE prism_replication REPLICATION LOGIN PASSWORD '<generated>';
   ```

2. **Authorize it in `pg_hba.conf`**, scoped to the standby's address rather
   than the whole internet:

   ```
   host    replication     prism_replication     <standby-cidr>     scram-sha-256
   ```

   Reload the primary (`SELECT pg_reload_conf()`) and confirm the rule is live
   in `pg_hba_file_rules`.

3. **Take the base backup with an explicit slot**, from the standby host:

   ```
   pg_basebackup --host=<primary> --port=5432 --username=prism_replication \
     --pgdata=<standby-data-dir> --format=plain --wal-method=stream \
     --progress --write-recovery-conf --create-slot --slot=prism_public_replica
   ```

4. **Put the standby's data and WAL on storage separate from the primary's.** A
   standby that shares a device with its primary protects against neither a
   full disk nor a device failure, and its read traffic competes with the
   primary's writes.

5. **Verify before pointing traffic at it**: `pg_is_in_recovery()` returns `t`,
   the slot shows `active = true` on the primary, and a row written on the
   primary is visible on the standby.

## Replication slot management

The physical slot is what makes the standby reliable and what makes it
dangerous to forget. While the slot exists, the primary refuses to recycle WAL
the standby has not consumed — **including while the standby is down**. An
abandoned slot fills the primary's WAL volume and eventually stops the primary.

Monitor it on the primary:

```sql
SELECT slot_name, active, restart_lsn, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
  FROM pg_replication_slots;
```

- `active` — `false` for anything longer than a deliberate standby restart
  means WAL is piling up for a consumer that is not reading it.
- `restart_lsn` — the oldest LSN the primary must keep. A `restart_lsn` that
  stops advancing is the leading indicator of a stuck standby.
- `wal_status` — `reserved` is normal, `extended` means retention has grown
  past `max_wal_size` and is the point to act on. `unreserved` means the
  required WAL is at risk of removal, and `lost` means it is gone and the
  standby can no longer catch up. **Alert when `wal_status` leaves
  `reserved`/`extended`.**

Setting `max_slot_wal_keep_size` on the primary caps that retention: the slot
is invalidated (`wal_status = 'lost'`) instead of the primary filling its disk.
That trades an unusable standby for a healthy primary, which is normally the
right trade for a read replica. Nothing in this repository sets it.

**Decommissioning.** When the replica is retired, drop the slot on the primary
in the same change — the primary keeps retaining WAL for it otherwise:

```sql
SELECT pg_drop_replication_slot('prism_public_replica');
```

Dropping the slot cannot be undone: the standby cannot resume streaming, and
bringing one back requires a **fresh base backup**. That is also the recovery
path for a slot invalidated with `wal_status = 'lost'`.

## Failover semantics

The supplied public replica provides read scaling. Its default asynchronous
replication does not guarantee preservation of acknowledged shares after loss
of the primary. No cluster manager or automatic promotion is configured by
these Compose files.

Native mining frontends can recover through a stable HA writer endpoint after
promotion of a suitable synchronous standby. Configure and validate that
replication and promotion policy using the [HA migration guide](prism-rust-migration.md).
The database retains the same accounting history and signing configuration;
there is no application writer lease to elect.

A public process configured with `PRISM_PUBLIC_REPLICA_MODE=require` refuses a
server after it is promoted: it is now a writer rather than the required
standby. Repoint the public read endpoint at the replacement standby, or
explicitly use `off` to serve bounded reads from the new primary. Keep mining
and settlement connections on the authoritative writer endpoint.
