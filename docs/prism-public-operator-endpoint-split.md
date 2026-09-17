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
| Timeout | `database probe timed out` | Database availability, network, connection limits, pool availability, load, query latency |
| Cancellation | `database readiness query was canceled` | Database statement deadlines and operator query cancellations |
| Other query/driver failure | `database readiness query failed` | Database service logs and readiness query compatibility |

The public process emits bounded `public readiness probe failed` warnings for
failed database probes, with fixed `category`, `phase` (`schema`, `replica`, or
`probe` for the whole-probe timeout), and `action` fields. The event policy is:

- The first failed probe emits one warning immediately.
- A change of failure category emits one warning immediately, even within the
  reminder interval. A phase change alone does not count as a category change.
- An unchanged failure emits at most one reminder per 60 seconds, measured with
  monotonic time from the last warning decision. The first completed failed
  probe at or after that boundary emits the reminder; missed intervals do not
  produce a burst of catch-up events.
- After an unhealthy episode, the first healthy probe emits one
  `public readiness probe recovered` event at INFO. Further healthy probes and
  a healthy startup are silent. A new failure after recovery warns immediately;
  flapping therefore preserves every observed failure/recovery transition.

An episode begins with a database probe failure from the categories above.
Recovery requires a fresh successful probe that also passes the existing
replica readiness policy. A successful query while replica policy still refuses
health does not announce recovery or reset the warning budget. Replica-only
refusals without a preceding database probe failure remain visible through the
existing health and metrics surfaces and do not start a diagnostic episode.
These events describe completed probes; they do not create a second observer
for startup or snapshot staleness between probes.

The readiness snapshot and diagnostic state are published together and unlocked
before calling the synchronous logging sink. A blocked sink can delay the probe
task, but health readers can still see its freshly published result; the existing
staleness policy continues to apply if subsequent probes cannot complete. There
is no additional background task, event queue, or configuration setting.

Set `RUST_LOG=qbit_prism_server::api::public_service=info` to collect warnings
and recovery events, or use `=warn` to collect only warnings. Filters that exclude
these levels hide the events; filtered events still advance the policy state,
and enabling a filter does not replay them. The process continues to use its
existing `RUST_LOG` reader and default filter.

Classification uses SQLx variants and recognized SQLSTATE values;
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

Categories describe the error that reaches the probe, not necessarily the first
underlying cause. SQLx retries refused connections and PostgreSQL connection
errors `53300` (connection limit) and `57P03` (cannot connect now). The existing
five-second probe deadline expires before the pool's acquisition deadline, so
these outages report timeout, as does an exhausted pool. Check reachability and
connection limits as well as query latency for timeout warnings. Non-retried
connection errors, such as a missing database, can report connection directly.
No retry or deadline policy is changed by this diagnostic categorization.

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

This page classifies HTTP endpoints. Operator commands are not endpoints and
are not listed here: `migrate`, `import-audits`, `backfill-ctv`,
`policy-transition`, `share-archive` and the rest run from a shell against the
primary and reach neither listener. `share-archive`, the share ledger retention
path, is documented in
[ledger operations](prism-ledger-ops.md#share-ledger-partitions-and-retention).

One operator route's answer depends on retention. `/audit/share-window?anchor=`
reads the online share ledger, so an anchor inside a range whose partition has
been archived returns no rows rather than an error. Those shares are in the
archive and in every sealed artifact whose window covers them; an
anchor-to-archive index is deferred. Public artifact and audit bundle routes
are unaffected, because a block whose shares have been archived is served from
its stored canonical bytes under the same digest.

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
