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

The independent public listener's unauthenticated `/healthz` reports readiness,
not database driver diagnostics. Database failures use these fixed `error`
strings; they never include raw SQLx messages, DSNs, credentials, role names,
database names, hostnames, addresses, or server-provided SQL identifiers:

| Failure category | Public `error` | Operator checks |
| --- | --- | --- |
| Authentication | `database authentication failed` | Public-reader credentials and database authentication rules |
| Access | `database access denied` | Public-reader schema, table, and monitoring grants |
| Connection | `database connection failed` | Database availability, network, TLS, database name, connection limits |
| Configuration | `database connection configuration is invalid` | Public-reader connection configuration |
| Schema | `native public read schema is incomplete` | Schema migrations and public-reader `search_path` |
| Timeout | `database probe timed out` | Database load, pool availability, probe query latency |
| Cancellation | `database readiness query was canceled` | Database statement deadlines and operator query cancellations |
| Other query/driver failure | `database readiness query failed` | Database service logs and readiness query compatibility |

The public process emits a `public readiness probe failed` warning for failed
database probes, with fixed `category`, `phase` (`schema`, `replica`, or `probe`
for the whole-probe timeout), and `action` fields. Enable warnings for
`qbit_prism_server::api::public_service` in `RUST_LOG` to collect these operator
diagnostics. Classification uses SQLx variants and recognized SQLSTATE values;
unknown errors remain failures without guessing their cause. PostgreSQL reports
both an expired `statement_timeout` and an operator cancellation as SQLSTATE
`57014`; both use the cancellation category with deadline/cancellation guidance.
If the whole-probe timer wins instead, the category is timeout. These labels
describe the observed failure, not a guess based on localized driver text.
The warning does not format the raw error,
its source chain, or even an unrecognized SQLSTATE. Consult access-controlled
database logs and deployment configuration for detailed investigation; do not
publish those sources or enable verbose driver logging on a public diagnostics
surface. This boundary applies to readiness diagnostics, not every runtime log.

Compatibility: this intentionally reduces database error detail in the existing
string-valued `error` field. The `qbit.prism.public-read-health.v1` schema, all
response fields/types, and successful response shape are unchanged. Failures
still return HTTP 503 with `ok=false`; database probe failures also retain
`database_ready=false`. Starting, stale, replica refusal, GET/HEAD, and HTTP
healthcheck behavior are unchanged, as is the five-second probe deadline.
Existing replica refusal messages still take precedence in the public response;
the warning records any underlying database probe failure. No client should
depend on the former raw driver text. The combined coordinator listener and
its authentication policy are outside this diagnostic change.

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
