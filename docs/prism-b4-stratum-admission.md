# Stratum subscription admission: B4 first slice

This first slice of [#276](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/276)
allocates the existing four-byte extranonce only on the first valid
`mining.subscribe`. Connect-only sockets, health probes, configuration and
authorization before subscription consume no session sequence value. Repeated
or pipelined subscriptions return the same eight-character lowercase hex value.
The backend allocator and wire width are unchanged.

A failed allocation returns `backend-rpc-unavailable` and leaves the connection
unsubscribed. An immediate failure can be retried on that connection. A timeout
returns the same category with `session allocation timed out`; a sequence value
already consumed before the timeout is not published or reused.

The initial-job lifetime now begins during connection setup, before the later
subscription allocation. The base runtime began that lifetime after eager
allocation completed.
Allocation time therefore counts toward the connection's initial-job lifetime.
The configured duration and timer cutoff are retained, with no reset or extension
on subscribe, failure or retry. A full allocation timeout can exhaust that
lifetime and close the connection after its error response; recovery reconnects
and obtains a fresh value. Delivery deadlines, retained-work expiry, original
worker permits, share grace and candidate/revision checks retain their existing
behavior.

Connection failures now emit structured tracing events with an `error` field.
No new request-rate or IP policy, configuration setting, allocator migration or
wire change is part of this slice.

## Evidence

The socket tests use the production listener and request loop. Five regressions
cover no allocation before subscribe; malformed and repeated subscriptions;
immediate allocator failure and retry; timeout after a consumed allocation; and
structured tracing for an actual TCP reset. On `3.x.x` base `f316eb9`, the
independent focused suite passed 64 tests: 36 library, five readiness, five new
socket and 18 existing protocol cases.
All passed; none were ignored in that invocation.

An explicit PostgreSQL test admits and closes 10,000 real sockets without
subscription, synchronizing each session's entry and exit. It delegates allocation
to the real Ledger implementation in a unique disposable schema. Before this
change, `(last_value, is_called)` advanced from `(1, false)` to `(10000, true)`.
After the change it stays `(1, false)`. A subsequent subscription through the
same listener returns `00000001` and changes the sequence to `(1, true)`.

The PostgreSQL test is ignored in ordinary runs and must be invoked explicitly
in database CI. Explicit invocation without the required DSN fails; it does not
silently pass or skip. Use a disposable database:

```sh
LC_ALL=C PRISM_TEST_DATABASE_URL="$DISPOSABLE_POSTGRES_URL" \
  cargo +1.89.0 test --locked -j2 -p qbit-prism-server \
    --test stratum_admission_postgres -- --ignored --exact \
    ten_thousand_unsubscribed_connections_do_not_advance_postgres_sequence --nocapture
```

The focused validation was:

```sh
LC_ALL=C PRISM_TEST_DATABASE_URL="$DISPOSABLE_POSTGRES_URL" \
  cargo +1.89.0 test --locked -j2 -p qbit-prism-server \
    --lib --test stratum_protocol --test stratum_admission --test readiness_rpc \
    -- --test-threads=2 --nocapture
```

These results qualify this slice; they do not claim a fresh all-targets run.

## Remaining work and scope

Wrap-safe allocation remains pending. The current sequence is still `NO CYCLE`;
subscribe floods can still consume it. Migration009 must cover active reservations,
unexpired issued jobs, resumed jobs and concurrent frontends before reuse is safe.
A wider wire format requires an explicit compatibility decision.

Per-IP admission, trusted proxy or edge address handling, request/malformed-frame
budgets, bounded unknown-job lookups/negative caching and username-validation
budgets are P2 follow-ups in [#262](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/262),
outside the trimmed #276 scope. NAT and a TCP proxy can place many legitimate
miners behind one observed peer address; a lower cap needs an explicit source
identity and deployment policy. **This slice does not complete #276.**
