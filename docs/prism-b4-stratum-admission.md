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

Trusted proxy and edge address handling remain P2 follow-ups in
[#262](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/262); load
balancer policy itself belongs to
[#281](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/281). The
per-source cap, malformed-frame budget, bounded unknown-job lookups and the
authorization budget are the second slice, below.

# Second slice: per-source cap and per-session budgets

This slice adds one admission cap keyed on the observed peer address and three
per-session rate budgets. Every one of them defaults to disabled, so a default
deployment behaves exactly as the first slice did. Valid `mining.submit`
traffic is never charged against any budget, and no share acceptance, landing
or refresh behavior changes.

## What each limit does

`PRISM_STRATUM_MAX_CONNECTIONS_PER_IP` bounds concurrent connections from one
observed peer address. The decision is taken in the accept loop, from the
address the accepted socket reports, before the global permit is taken and
before any session task is spawned: a refused source costs one hash lookup and
no database round trip. The refused socket is dropped with no JSON-RPC
response, the same shape as the existing global limit. The registry holds weak
references and prunes dead keys on every decision, so it is bounded by live
connections rather than by every address ever seen. The ordinary and
high-difficulty listeners share one registry, so a miner using both ports
spends two slots from the same address budget.

The three budgets are plain per-session counters over one shared fixed window,
`PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS` (default 60). A budget is a
rate, never a session lifetime total: a whole window's allowance is admitted as
a burst and refills in full at the next window. Exceeding one answers the
request and then closes the connection, recording the reason on
`qbit_prism_stratum_connection_refusals_total`.

- `PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL` counts malformed **frames**,
  not bytes, and includes the malformed non-submit frames that were previously
  answered and forgotten at no accounting cost. One frame is one charge
  whatever it weighs.
- `PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL` counts every `mining.submit`
  whose job is not held by the session and whose ledger resume lookup misses.
  Each such submit costs exactly one lookup, so the budget bounds the ledger
  work one session can drive: a flood of unknown-job submits produces at most
  the budget plus one lookup per interval before the disconnect, whether the
  IDs repeat or not. There is deliberately no negative cache. A resume miss is
  not proof that the ID is bogus: besides honest staleness after a tip or
  payout-revision change, a resume returns a miss when its publication stamp
  changes mid-resume, when issuance authority loses a lease or tip race during
  revalidation, and while this frontend is momentarily behind on the parent or
  payout revision. Each of those clears on the miner's next share, so every
  submit must re-query, and each counts as a miss against the budget. That is
  also why the default stays 0: with the budget off, behavior is exactly as
  before, and an operator who enables it must size it above the worst honest
  miss rate rather than rely on a cache.
- `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL` counts `mining.authorize`
  attempts, charged before the address validation RPC, which is what bounds the
  `validateaddress` calls one connection can drive by cycling usernames.
  Re-authorizing to switch usernames stays supported: within the budget it
  behaves exactly as before.

## Recommended starting values

These are starting points for a frontend at the default
`PRISM_STRATUM_MAX_CONNECTIONS=384`, not tuned limits. Raise anything a real
deployment brushes against; every one of them refuses a legitimate miner if it
is set below an honest rate.

| Setting | Recommended start | Why |
| --- | --- | --- |
| `PRISM_STRATUM_SESSION_BUDGET_INTERVAL_SECONDS` | 60 | Long enough that a burst is averaged out, short enough that a spent budget recovers within a miner's patience. |
| `PRISM_STRATUM_MAX_MALFORMED_FRAMES_PER_INTERVAL` | 64 | An honest miner sends none. 64 tolerates a broken proxy or a firmware bug without ejecting the rig on the first bad frame. |
| `PRISM_STRATUM_MAX_UNKNOWN_JOBS_PER_INTERVAL` | 256 | Must sit well above an honest miner's in-flight submits per interval, because in the worst case every one of them misses: after a tip or payout-revision change, or through a run of transient resume races, all work a rig still holds arrives as unknown. At the default 15-second vardiff target a connection submits about 4 shares a minute, and a connection holds at most 64 retained jobs (`PRISM_STRATUM_SAME_TIP_JOB_RETENTION_PER_CONNECTION`); 256 covers several whole-retention flushes in one window. |
| `PRISM_STRATUM_MAX_AUTHORIZE_ATTEMPTS_PER_INTERVAL` | 32 | Normal sessions authorize once or twice. 32 leaves room for a client that re-authorizes on every reconnect attempt inside one connection. |
| `PRISM_STRATUM_MAX_CONNECTIONS_PER_IP` | 0 | See the runbook rule below. |

## Runbook rule for the per-source cap

**Leave `PRISM_STRATUM_MAX_CONNECTIONS_PER_IP` at 0 unless the deployment is
known to preserve miner source addresses.**

The cap keys on the observed last hop. This repository has no PROXY-protocol
support and ships no Stratum load balancer: it is operator-supplied
([#281](https://github.com/Qbit-Org/qbit-mining-bootstrap/issues/281),
`docs/prism-ha-reference-architecture.md`). Behind a terminating TCP proxy,
behind NAT egress, or behind a Docker userland proxy, many or all miners share
one key, and any cap below the global limit throttles the whole pool. The
failure mode is silent from the miner's side: an accepted connection closed
with no response. Before enabling it, confirm the topology delivers real client
addresses end to end, for example a pass-through L4 balancer or direct
exposure with `network_mode: host`.

When it is safe to enable, size it with reconnect-storm headroom of about twice
steady state. A per-source slot is released only when the session task ends, so
after a tip change or a balancer failover a farm reconnecting every rig at once
transiently needs roughly double its steady-state slots; a cap tuned to steady
state ejects legitimate reconnects. Note that a single NAT'd farm can present
hundreds of rigs from one address, so start from the observed maximum per
address and add the headroom, never from a guess.

Because a metric label may never carry a peer address, a refusal is counted as
`qbit_prism_stratum_connection_refusals_total{reason="ip_limit"}` and the
address appears only in the matching structured tracing event, which also
carries the configured limit and the listener name. Search the log when that
counter rises; a misconfigured cap is otherwise invisible per address by
design.

Load balancer policy, PROXY protocol and trusted-edge address handling are out
of scope here: document, do not implement. **This slice does not complete
#276's admission story beyond the criteria listed in that issue.**
