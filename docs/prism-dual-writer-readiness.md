# Dual-writer readiness and Stratum gating (3.1)

PRISM 3.1 runs two nodes, A and B, each with its own writable PostgreSQL
([configuration](prism-configuration.md#dual-writer-31)). Miners reach them
through a balancer that prefers A and keeps B as a hot backup, and that routes
by readiness: a node's death is a reroute, with no promotion and no fence.
This page is what a frontend tells that balancer, and how its Stratum
listeners follow it.

With `PRISM_DUAL_WRITER` off and `PRISM_READINESS_PORT` unset, the defaults,
none of this runs: `/healthz`, the Stratum listeners and every statement are
3.0's. The metric families below are declared and carry no samples.

## When a dual-writer frontend is ready

`/healthz` keeps every 3.0 readiness rule (prepared work on the observed tip
and the database payout revision, a fresh tip poll, the CTV fee floor, job
delivery). In dual mode `ok` and `ready` are also false unless both hold:

- **The own log is caught up.** The peer sync's startup lineage latch
  (decision D-8): false at start, and again on a detected local rollback,
  until the node has verified that it holds every row it originated that the
  peer holds, pulling back any it lacks. Losing the peer later never clears
  it, so a broken link never withdraws a serving node. A node that starts
  with the peer unreachable latches true unless its database shows evidence
  of a rollback (decision D-17): a new system identifier or WAL timeline
  since the last verification, or no verification recorded.
- **The writer is local.** The database this frontend writes to is this
  node's own writable primary: its never-copied `qbit_prism_node_identity`
  row (decision D-9, set by `qbit-prism-server node-identity set`) names
  `PRISM_NODE_INDEX`, it is not in recovery, and its sessions are not
  read-only.

`status` names the first that fails: `own-log-behind`, then
`writer-not-local`; otherwise `unavailable` as in 3.0.

`/healthz` gains a `dual_writer` object (CONTRACT.md §3):

```json
"dual_writer": {
  "node_index": 1,
  "carry_owner": false,
  "own_log_caught_up": true,
  "peer_sync": {"peer_reachable": true, "own_log_caught_up": true, "per_table": {}},
  "writer_path": "local"
}
```

`peer_sync` is the sync's status as it publishes it; `peer_reachable` and the
per-table lags are reported, never decided on. `writer_path` is the last probe
of the frontend's own pool:

| `writer_path` | Meaning | Effect |
| --- | --- | --- |
| `local` | this node's identity, writable | serves |
| `remote` | the identity row names the peer: a DSN left pointing at the other node | withdraws at once |
| `unidentified` | no identity row: the database was never personalised | withdraws at once |
| `read_only` | in recovery, or `default_transaction_read_only` on | withdraws at once |
| `unanswered` | the probe failed or took over 2 s | withdraws once it has lasted 4 s |
| `null` | no probe yet | not ready |

The probe is one statement, `pg_is_in_recovery()`,
`transaction_read_only` and the identity row, at most once a second however
many health reads ask; it never reads the peer.

## Admission

A frontend admits miners when its readiness says so, and keeps admitting
through an ordinary dip: every new tip and payout revision clears `ready`
until its replacement work is published. The `admission` object in
`/healthz`, present in dual mode or with the readiness endpoint on, reports
the decision:

```json
"admission": {"admitting": true, "state": "grace", "reason": null, "grace_seconds": 10}
```

| State | Admits | Meaning |
| --- | --- | --- |
| `starting` | no | never ready since the process started |
| `admitting` | yes | ready |
| `grace` | yes | readiness false for less than `PRISM_READINESS_GRACE_SECONDS` |
| `withdrawn` | no | admitted once, then withdrawn; `reason` says what blocks it now |

Withdrawal reasons: `own-log-behind` and `writer-not-local` at once, whatever
the grace; `not-ready` once readiness has stayed false for the whole grace
(default 10 s, 0 to 120, the operator readiness contract's ten seconds in
[the HA reference](prism-ha-reference-architecture.md#operator-tcp-load-balancer-readiness-contract)).
The health publisher decides at each publication; a decision older than the
health freshness budget, `max(15, 3 * PRISM_HEALTH_REFRESH_SECONDS)` seconds,
admits nothing, so a stalled publisher withdraws the frontend as `/healthz`
reports its snapshot stale.

## Stratum gating (dual mode)

A dual-writer frontend binds its Stratum addresses at startup, before the
coordinator starts, as 3.0 does: a restart that loses the bind race to a
listening predecessor still exits without writing cluster state. It listens
only while it admits miners:

- before its first admission, and after a withdrawal, the kernel refuses
  every connection with a reset: no TCP handshake completes, so no check,
  balancer or miner sees a frontend that cannot serve as up;
- withdrawing resets connections still queued unaccepted, so their miners
  reconnect elsewhere at once;
- sessions already accepted carry on; the balancer closes them when it marks
  the node down (`on-marked-down shutdown-sessions`).

A single writer's listeners are 3.0's: listening from startup, whatever
readiness says. Linux lets a second socket bind an address that a reserved one
holds while neither listens, so two frontends configured with one address
both start, and the second to be admitted fails to listen and exits.

## The readiness endpoint (decision D-7)

A readiness-only HTTP listener for the Hashbalancer's checks, meant for a
node's public address. It is off unless `PRISM_READINESS_PORT` is set.

| Setting | Default | Meaning |
| --- | --- | --- |
| `PRISM_READINESS_PORT` | `0` (off) | its TCP port; 9084 on the pair |
| `PRISM_READINESS_BIND` | `127.0.0.1` | its bind address; `0.0.0.0` inside a container |
| `PRISM_READINESS_TOKEN` or `PRISM_READINESS_TOKEN_FILE` | required with the port | the token, at least 16 printable ASCII characters without spaces; the file form reads a mounted file (a group-readable `0440` file is fine) |
| `PRISM_READINESS_GRACE_SECONDS` | `10` | the admission grace above, 0 to 120 |

`GET /readyz` (or `HEAD`) with the header `X-Qbit-Healthcheck-Token: <token>`
answers:

| Status | Body | When |
| --- | --- | --- |
| `200` | `ready` | the frontend admits miners and the decision is fresh |
| `503` | `not ready` | otherwise |
| `401` | `unauthorized` | a missing, repeated or wrong token, checked before anything else |
| `404` | `not found` | any other path or method, with the token |

Every answer is plain text with `Cache-Control: no-store` and
`Connection: close`, and says nothing else: no health payload, no metrics,
no operator route. Each connection carries one request and has a two-second
deadline, twice the balancer's check timeout. At most 256 connections are
served at once, and at most 8 from one source address (an IPv6 source counts by
its /64), so a client holding connections open cannot take the slots the
balancer's checks need; a connection past either cap is closed at once,
unanswered, which a checker reads as a failed probe. The operator listener
(`PRISM_AUDIT_PORT`) stays private.

On the pair, the endpoint is published on each node's public address under the
same condition as public Stratum, with the Hashbalancer's health token; both
nodes serve it before the Hashbalancer's PRISM routes switch to HTTP checks
(the deploy order of decision D-7).

## The container healthcheck (dual mode)

Routing reads readiness from `/readyz`. The container healthcheck,
`qbit-prism-server healthcheck`, is about liveness in dual mode, so a
converge that waits for a healthy container does not time out while a node
catches up on its own log after a restart, restore or rebuild. Given a
`/healthz` body that carries `dual_writer`, it passes while the frontend is
ready, while its own log is behind (the D-8 catch-up), or while its admission
is inside the grace. It fails on a fault:

- no answer within 3 s, as when the process has exited (a cluster halted
  before the start refuses the frontend's start);
- a stale snapshot or a stalled runtime (the body's `error`), or
  `job-delivery-stalled`;
- a `writer_path` other than `local`: the database unreachable, read-only,
  never personalised or the peer's;
- readiness lost for longer than the grace, or not yet gained after the
  catch-up. A cluster halted at runtime shows this way: its payout revision
  is no longer served, so the frontend withdraws once the grace has run out.

It reads `/healthz` on the operator listener. With `PRISM_AUDIT_PORT=0` in
dual mode it fails instead of probing Stratum, whose gated listeners refuse
connections while the frontend does not admit miners. A single writer's
healthcheck, and `self-check` in either mode, keep 3.0's readiness rule.

## The balancer

The Hashbalancer's `readiness` route policy (SwapLabsInc/qbit-tools,
`services/hashbalancer`), with A the primary and B the backup:
`GET /readyz` with the token on 9084 every 2 s (1 s once a check has
failed), DOWN after 3 failures and UP after 2 passes;
`observe layer4 error-limit 2 on-error mark-down`; `retries 3` with
`option redispatch 1`; `on-marked-down shutdown-sessions` and the primary's
`on-marked-up shutdown-backup-sessions`; `rate-limit sessions 25` on each
HAProxy task, which paces failover and failback waves. A withdrawn
frontend's refused connects mark it down after two failed connects, sooner
than its checks. A rebuild shorter than the grace keeps `/readyz` at `200`,
so an ordinary rebuild never ejects a frontend, whatever the check interval.

## Metrics

| Family | Labels | Meaning |
| --- | --- | --- |
| `qbit_prism_admission_admitting` | none | 1 while the frontend admits miners |
| `qbit_prism_admission_state` | `state` | one-hot admission state |
| `qbit_prism_admission_withdrawals_total` | `reason` | withdrawals since start |
| `qbit_prism_stratum_listener_accepting` | `listener=default,highdiff` | dual mode: whether a listener accepts |
| `qbit_prism_peer_sync_own_log_caught_up` | none | the D-8 latch, published by the peer sync |
| `qbit_prism_dual_writer_writer_path` | `path` | one-hot writer path |
| `qbit_prism_dual_writer_node_index` | none | `PRISM_NODE_INDEX` |
| `qbit_prism_dual_writer_carry_owner` | none | `PRISM_CARRY_OWNER` as configured |
| `qbit_prism_readiness_requests_total` | `result` | readiness endpoint answers |

The [metrics inventory](prism-native-metrics.md) has their full meanings. "The
non-owner serving miners" reads `qbit_prism_dual_writer_carry_owner == 0` with
`qbit_prism_connections > 0`.

## Tests

- Unit: the admission state machine, the signal's freshness, the endpoint's
  token, answers and connection caps, the reserved address, the writer-path
  classification,
  the metrics, and the healthcheck's liveness rule.
- `tests/healthcheck_cli.rs`: the healthcheck binary against canned
  `/healthz` answers: a dual-writer body catching up passes and one with a
  remote writer fails, a single writer keeps 3.0's rule, and dual mode
  without the operator listener refuses to probe Stratum.
- `tests/stratum_admission_gate.rs`: a gated listener refuses at the socket
  until admitted, refuses again after a withdrawal while an established
  session is still served, and closes when its decision goes stale.
- Gated, `tests/dual_writer_readiness.rs`: the latch and the identity at the
  coordinator, a database turning read-only and back, the peer lost after
  the latch, and a single writer's unchanged health.
- Gated, `tests/readiness_frontend.rs`: the server binary in dual mode, not
  yet admitted, refuses Stratum and answers `503` and `401`; a single writer
  listens from startup and answers `200`; five payout revision bumps under
  readiness polling never turn the endpoint from `200`, and a rebuild held
  open dips readiness into the grace, keeps `200` for the whole default
  grace, turns `503` only after it, and is readmitted once rebuilt. The
  healthcheck passes for the dual-mode frontend catching up and fails for one
  on the other node's database; a single writer's passes once ready and fails
  inside the grace, as in 3.0.
