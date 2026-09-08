# Prism public and operator endpoints

The Rust runtime keeps the `2.x.x` public read tier as a separate process:

```sh
qbit-prism-server public-api
```

Its default port is 3342. Configure `PRISM_DATABASE_URL` for that process's
read pool and `PRISM_PUBLIC_STRATUM_URL` for the advertised mining endpoint.
Compose supplies the read DSN through `PRISM_PUBLIC_DATABASE_URL`, independently
of the mining writer DSN. This process requires no signing keys and does not
migrate the schema, allocate mining sessions, or claim accounting work.

The public role serves `/public/v1/*`, `/healthz`, and `/metrics`. Operator
aliases under `/audit`, legacy `/miners` and `/payouts`, and private status
routes remain on the combined coordinator listener (default 3341). Keep that
listener bound to a trusted interface.

Read concurrency, SQL statement deadlines, cache capacity, and stale response
budgets are bounded independently of mining. Replica mode additionally checks
recovery state and WAL receiver heartbeat age; promotion or a stale stream
closes replica-dependent routes. Content-addressed canonical bytes remain
verified against their requested digest. All shared evidence is in PostgreSQL;
the public role needs no audit filesystem mount.

See the [public API contract](public-dashboard-api/README.md),
[replica operations](prism-postgres-replica.md), and
[coordinated Rust migration](prism-rust-migration.md) for parameters, readiness
headers, database privileges, and deployment examples.
