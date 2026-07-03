# PRISM Pool High Availability And Redundancy Plan

Status: deferred. The team chose not to pursue PRISM HA for now (the
state-sync/failover complexity is not worth the risk at current scale) and is
instead hardening single-node reliability — fast recovery, failure isolation,
and backups — tracked as part of PRISM operational hardening. This document is retained as the design
reference if HA is revisited later. It is the production-topology companion to
the `docs/prism-ledger-ops.md` operations contract, answers how to run PRISM
redundantly without changing reward semantics, and maps the work to the
"queue-fed multi-frontend failover/HA" gap recorded as remaining for PRISM HA.

## The Question

Three options come up when operators think about running PRISM for real:

1. Run two independent PRISM pools for redundancy.
2. Run two Stratum hosts that talk to one Postgres instance.
3. Provision it the way leading pools (OCEAN/TIDES and similar) actually do.

Short answers:

- Option 1 is the wrong kind of redundancy. It splits hashrate, raises every
  miner's payout variance, fragments accrual, and violates the single-ledger
  contract. Do not do this.
- Option 2 is the right instinct, but it does not work as-is. The current
  coordinator couples the Stratum frontend and the ledger writer in one process,
  and the writer lease deliberately fences out a second writer. Making it work
  requires splitting the frontend from the writer with a durable queue between
  them.
- Option 3 is what this plan describes: one canonical ledger as the single
  consistency boundary, with every tier in front of it made redundant and
  stateless, and Postgres made HA underneath. This is how production pools run.

## Why Two Pools Is The Wrong Redundancy

Payout variance is a function of two things: total pool hashrate and the width
of the reward-smoothing window. PRISM mirrors OCEAN's TIDES window at 8x network
difficulty (see `docs/prism-ledger-ops.md` and `qbit_audit_share_window` in
`crates/qbit-prism/sql/001_share_ledger.sql`). Adding servers does not change
either input, so it does not change variance. Splitting into two pools changes
both inputs for the worse:

- Each pool now commands roughly half the hashrate, so each finds blocks about
  half as often. Per-pool block-interval variance goes up, not down. Miners feel
  this as longer, lumpier gaps between reward events.
- Sub-floor accrual fragments. A miner whose share is below the day-1 payout
  floor accrues a carry-forward balance per ledger
  (`qbit_payout_carry_forward`). Split across two ledgers, that miner may never
  cross the floor on either side, so on-chain payout is delayed or stranded.
- Operational surface doubles: two coordinators, two databases, two settlement
  key sets, two audit trails, two reorg reconcilers — with no shared safety.

There is also a hard contract reason. The accepted PRISM model is one ordered
log with one logical writer; active-active ledger insertion is explicitly out of
contract because it makes `share_seq` ordering ambiguous
(`docs/prism-ledger-ops.md`, "Writer Lease and Replay"). Two competing pools is
the most extreme form of active-active: two independent `share_seq` spaces that
can never be merged into one auditable split. Redundancy must be invisible to
the reward math, which means it must live in front of a single ledger, never by
forking the ledger.

## The One Rule

> Keep exactly one canonical ordered share ledger. Make everything in front of
> it redundant and stateless. Make the database under it highly available.

Every decision below follows from that rule. The ledger is the consistency
boundary; the writer lease that protects it lives in Postgres, so Postgres HA is
the foundation of writer HA.

## Target Production Topology

Five tiers, each independently redundant, with the ledger as the only
stateful-consistency point.

### Tier 0 — qbit full nodes (templates and submission)

Run at least two `qbitd` nodes. Frontends and the settlement path get
`getblocktemplate` from, and `submitblock` to, whichever node is healthy.
Template delivery is latency-sensitive; node redundancy protects job freshness
and block submission, not share accounting.

### Tier 1 — Stratum ingress edge (stateless, horizontally scalable)

Multiple frontend processes terminate miner TCP and run the cheap, per-message
Stratum work: `subscribe`/`authorize`/`notify`, BIP310 version rolling, vardiff,
and first-line share validation (worker identity, share shape, job id, target).
These are the same responsibilities the current coordinator already implements
in `lab/prism/prism_coordinator.py`; the change is that a frontend must not
write the ledger directly. On an accepted share it emits a normalized
accepted-share record to the durable queue (Tier 2) and returns the Stratum
result. Frontends sit behind TCP load balancing / DNS / anycast so a miner
reconnect lands on any healthy edge. Because they hold no authoritative state,
they can be added, drained, restarted, or geo-distributed freely.

### Tier 2 — durable share queue (at-least-once boundary)

This is the "durable queue or equivalent at-least-once delivery boundary" the
ledger ops contract already names but the lab coordinator does not yet
implement (today it calls `self.ledger.append(...)` in-process). Accepted shares
are buffered durably so neither a frontend restart nor a writer restart loses a
share. Delivery is idempotent because `share_id` is globally unique: replaying a
committed share fails without consuming a new sequence number
(`docs/prism-ledger-ops.md`, "Writer Lease and Replay"). The queue is what lets
the frontend tier and the writer tier fail and scale independently.

### Tier 3 — single logical ledger writer (active/standby, not active-active)

One writer drains the queue and inserts into `qbit_share_ledger`, holding the
Postgres writer lease keyed by `(writer_id, writer_epoch, writer_session_token)`
(`PsqlShareLedger` in `lab/prism/share_ledger.py`,
`qbit_ledger_writer_lease`). A standby writer with the same writer id and epoch
waits for lease expiry, then acquires a fresh session token and resumes at the
next database sequence value; stale writers are fenced before insert. The
settlement path — coinbase/manifest build, `submitblock`, maturity and reorg
reconciliation — runs behind the writer and reads the same canonical log. The
lease fencing primitives for this already exist and are proven by
`make test-prism-postgres-ledger`.

### Tier 4 — Postgres high availability

A single `prism-postgres` container (the current `compose.yaml` shape) is the
real single point of failure, because the lease and the ordered log both live in
the database. Production runs a primary plus at least one synchronous standby
with automatic failover, managed by Patroni or CloudNativePG. Use synchronous
replication so a database failover does not lose committed shares (RPO near
zero); the modest write-latency cost is acceptable because share insert is not
on the miner-facing hot path the way template delivery is. Database failover and
writer failover are the same event: when the primary moves, the writer lease
moves with it.

Net effect: all hashrate still converges on one ledger and one 8x TIDES window,
so block-finding rate and payout smoothing are identical to a single-box pool.
Redundancy becomes purely an availability property, invisible to reward math.

## How Leading Pools Do It (OCEAN/TIDES Comparison)

This topology is not novel; it is the standard production shape.

- TIDES ("Transparent Index of Distinct Extended Shares") is OCEAN's payout
  method: a rolling window of the last 8 blocks (8x network difficulty), every
  share tracked individually with order retained, paid pro-rata directly from
  the coinbase. PRISM implements the same window and the same coinbase-direct,
  per-share-auditable model, which is exactly why there can be only one ordered
  index.
- Production pools separate concerns into independently scalable tiers: edge
  Stratum gateways that terminate miners and forward shares, a backend that does
  validation/accounting/reward attribution, and a single authoritative
  share-accounting database that all gateways feed. Multi-region edge ingress
  with one shared backend "source of truth" for shares is the documented
  redundancy pattern.
- PostgreSQL HA via synchronous streaming replication with an automatic-failover
  manager (Patroni; CloudNativePG on Kubernetes) is the mainstream answer for
  the stateful tier.

PRISM already matches the hard part (one ordered ledger, coinbase-direct
payouts, independent audit). The HA work is wiring the standard
redundant-edge / single-store / HA-database tiers around it.

## Current Code: What Exists vs. What To Build

Exists today:

- Canonical ordered ledger and window/audit queries
  (`crates/qbit-prism/sql/001_share_ledger.sql`).
- Writer lease, session-token fencing, idempotent `share_id` replay, and
  resume-at-next-sequence after failover (`lab/prism/share_ledger.py`,
  proven by `make test-prism-postgres-ledger`).
- A coordinator that performs Stratum ingress and ledger writing in one process,
  plus maturity/reorg reconciliation and the audit API
  (`lab/prism/prism_coordinator.py`).
- Operator readiness probe (`make prism-self-check`) and a DB insert/query
  capacity harness (`make test-prism-postgres-throughput`).

To build for HA (the remaining HA gap):

1. Split the coordinator into a stateless Stratum frontend and a ledger writer
   (today they are one process calling `self.ledger.append` directly).
2. Add the durable at-least-once share queue between them (Tier 2).
3. Supervised standby-writer failover: the lease primitives exist, but there is
   no orchestration that promotes a warm standby and bounds the takeover window.
4. Postgres HA: primary + synchronous standby + automatic failover; the current
   compose ships a single Postgres container.
5. Stratum endpoint load balancing / DNS / anycast and frontend health/draining.
6. Live-swarm capacity proof: the throughput harness measures DB inserts only;
   the ledger ops contract notes it does not simulate miner swarms, reconnect
   storms, or stale-share bursts. Queue + DB need a load proof before launch.

## Phased Delivery

Each phase is shippable and strictly additive; none changes reward semantics.

### Phase A — warm standby + HA database (works with today's process model)

Run one active coordinator and a warm standby coordinator configured with the
same `PRISM_LEDGER_WRITER_ID` and `PRISM_LEDGER_WRITER_EPOCH`, both pointed at an
HA Postgres. If the active coordinator dies, the standby waits out the lease and
takes over with a fresh session token; the fencing in `share_ledger.py` already
makes this safe. This delivers process-failure and database-failure resilience
without the frontend/writer split.

- Limitation: only one coordinator serves miners at a time, so this is failover,
  not load scaling.
- In-flight shares during the failover gap can be lost until miners resubmit.
  That is acceptable for v1: Stratum miners reconnect and retry, and the 8x
  reward window self-heals over the next shares.
- Failover RTO is bounded by the remaining lease TTL. The Python ledger writer
  paths, including the block-state wrappers around the PL/pgSQL functions,
  refresh the lease to `PRISM_LEDGER_LEASE_TTL_SECONDS` (default 60s), while
  graceful coordinator shutdown releases the lease immediately. Operators can
  tune the TTL and standby acquisition poll cadence to trade RTO against
  false-promote risk.
- Acceptance: kill the active coordinator mid-mining on regtest; confirm the
  standby acquires the lease, resumes at the next `share_seq`, and that
  `qbit_carry_forward_integrity_report()` stays clean across the transition.

### Phase B — multi-ingress (frontend/writer split + durable queue)

Split ingress from writing and introduce the durable queue. Now multiple active
Stratum frontends run behind a TCP load balancer, all feeding one writer through
the queue.

- Delivers both load scaling and zero-loss ingress across frontend and writer
  restarts (the queue holds shares; `share_id` keeps replay idempotent).
- Acceptance: run N frontends + 1 writer on regtest; restart a frontend and the
  writer independently under load; confirm no accepted share is lost or
  duplicated in `qbit_share_ledger` and the audit window is unchanged.

### Phase C — full production HA

- Postgres on Patroni or CloudNativePG with synchronous replication and
  automatic failover.
- Automated writer promotion tied to the database leader, with alerting on lease
  age and writer identity.
- Geo-distributed frontends behind anycast/DNS; node redundancy at Tier 0.
- A live-swarm load proof for the queue and database, extending the existing
  throughput harness to cover reconnect storms and stale-share bursts.
- Acceptance: a documented failover drill (database primary loss and writer loss)
  with bounded RTO, zero committed-share loss, and a clean carry-forward
  integrity report afterward.

## Open Decisions

- Queue substrate: durable broker vs. a Postgres-backed intake table drained by
  the writer. A Postgres intake table keeps the dependency surface minimal and
  inherits Tier 4 HA, at the cost of more load on the same database.
- Lease TTL and standby poll cadence: the right RTO vs. false-promote trade-off
  for the target deployment.
- Whether Phase A failover-only HA is sufficient for first production launch,
  with Phase B/C as fast-follows, or whether multi-ingress is required on day
  one.
