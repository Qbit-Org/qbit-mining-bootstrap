# Four-byte session allocation (migration 009)

The first valid `mining.subscribe` reserves one four-byte `extranonce1`.
Connect-only traffic consumes no sequence values, and repeated subscriptions on
the same connection retain their original value. The wire response and stored
job payloads retain their previous format.

The sequence cycles through 1–4,294,967,295. Each allocation attempt atomically
inserts a reservation keyed by its numeric value, or replaces a reservation
whose owner explicitly reports `{"state":"stopped"}`. Before committing, it
also checks for unexpired job rows with that hexadecimal `extranonce1`, including
pre-009 rows. A collision rolls back that attempt and advances to the next
candidate. There is no global allocator lock and no transaction spans multiple
attempts. After 1,024 occupied candidates, the client receives Stratum error 20
with reason `session-allocation-exhausted` and can retry subscription; no partial
subscription is published.

Each Ledger process incarnation has a random owner token, recorded in its
reservations and heartbeat status. A stopped marker must match that token as
well as the instance ID, so another process using the same instance ID cannot
make live reservations reclaimable. Reporting stopped atomically closes local
allocation admission and requires zero pending or active session guards; a
shutdown that has not finished cancelling sessions cannot publish the marker.
All clones of the Ledger share that admission state.

The session owns a non-cloneable guard. Disconnect and task cancellation schedule
deletion of only that guard's reservation token; a late cleanup cannot delete a
replacement reservation. Unexpired persisted jobs independently block reuse
after the guard is released. Reservations do **not** expire based on elapsed
time: a connected miner's current in-memory job can outlive its database row on
an unchanged tip. Neither a stale heartbeat nor an absent instance row proves
that a session has ended.

## Upgrade and compatibility

Stop and drain **all pre-009 frontends before applying migration 009**. The old
binary calls unchecked `nextval`, so an old and a new frontend must not run
together after cycling is enabled. This is a coordinated stop/drain upgrade,
not a mixed-version rolling upgrade. No job row is rewritten and the old binary
can still read the job schema and four-byte wire format, but must not allocate
sessions against the migrated sequence.

Start one or more new frontends with the existing migration runner enabled.
Its transaction advisory lock serializes concurrent migrations, and 009 is
recorded after the 002–006 migrations. Numbers 007 and 008 remain reserved
for other workstreams; the runner checks applied versions individually, so 009
does not conceal a lower-numbered gap when those migrations are integrated,
and a database at 2, 3, 4, 5 and 9 still receives 006 on its next migrate.
Every start also requires each of those migrations (the startup gate in
`docs/prism-rust-migration.md`), so a frontend with initialization disabled
refuses a database missing 009. Before starting one, verify on the writer:

```sql
SELECT version FROM qbit_prism_schema_migrations WHERE version = 9;
```

The result must contain 9. Migration DDL and its version record commit atomically;
the job lookup index can briefly block writes during creation. A lock or statement
timeout rolls the migration back and can be retried. Two new frontends can
migrate/start concurrently under the existing migration lock.

## Retained reservations and recovery

Normal disconnects release reservations. Normal server shutdown drains/aborts
sessions before publishing `stopped`, which also makes any missed cleanup
eligible for reuse at allocation. A crash, a lost commit response, a failed
cleanup, or runtime termination can retain reservations conservatively.

For a crashed owner, first prove that its instance ID is not running **anywhere**
and prevent that process from restarting during the operation. On the writer,
mark that owner stopped (substitute the verified instance ID):

```sql
UPDATE qbit_prism_instances
SET status = jsonb_set(status, '{state}', '"stopped"'::jsonb),
    heartbeat_at = clock_timestamp()
WHERE instance_id = 'verified-stopped-instance';
```

Preserve `session_owner_token` in that status: the allocator will reclaim only
that process incarnation's reservations as candidates are encountered, while
retaining protection from unexpired jobs. If the heartbeat row is absent or
there are older incarnations, their reservations stay protected. To remove all
reservations for the verified-stopped instance ID immediately, under the same
precondition that **none** of its processes can still run or restart:

```sql
DELETE FROM qbit_prism_session_reservations AS r
WHERE r.instance_id = 'verified-stopped-instance'
  AND NOT EXISTS (
      SELECT 1 FROM qbit_prism_jobs AS j
      WHERE lower(j.payload->>'extranonce1') = lpad(to_hex(r.extranonce1), 8, '0')
        AND j.expires_at > clock_timestamp()
  );
```

Retention consumes IDs, not every successful connection over the process's
lifetime: ordinary disconnect cleanup frees them. With the default 384-connection
cap, losing every active reservation in 100 crashes per day would retain 38,400
IDs per day; exhausting 4,294,967,295 values at that rate takes about 306 years.
That illustration excludes additional failed disconnect cleanups, which can
accumulate during outages and should be recovered using the procedure above.
If automatic crash reclamation becomes necessary, use renewable leases with
session and job-write fencing; increasing a heartbeat-age threshold is not a
safe substitute.
