# PRISM Public / Operator Endpoint Split

This document maps every endpoint the PRISM HTTP surface serves and classifies
each one as **public read surface**, **operator surface**, or
**payout-affecting**. It is the prerequisite for issue #145, which moves the
public read tier out of the coordinator process.

It exists so a reviewer can disagree with one classification without re-reading
the HTTP layer. Each row carries the evidence its classification rests on, and
the classification itself is declared in
[`lab/prism/endpoint_registry.py`](../lab/prism/endpoint_registry.py) and pinned
against real dispatch by `tests/test_prism_endpoint_registry.py`. Nothing here
changes request handling; this is the boundary written down.

## Result

**Exactly the 13 `/public/v1/*` routes are extracted. Everything else stays.**

That is a narrower answer than "move the read traffic", and the narrowing is the
substance of this document. Three findings drove it.

### 1. The boundary is already written down, and it excludes `/audit/*`

`PRISM.md` states that the coordinator "exposes a private audit/ops listener and
a dashboard-safe public API from the same process", lists `/healthz`,
`/metrics`, `/owed-balances` and every `/audit/*` route as private/internal,
instructs operators not to expose `/audit/*` to the internet, and concludes:
"operators can expose only `/public/v1` through a reverse proxy or dashboard
frontend."

`docs/public-dashboard-api/README.md` repeats it as a contract — "the public API
must not expose `/audit/*`, `/metrics`, `/healthz`, operator controls, raw
private sockets, credentials, or unrestricted internal manifests" — and
`tests/test_public_dashboard_api_contract.py` enforces it by asserting the
OpenAPI document contains no `/audit/` path.

The issue asks that published audit evidence be served from the artifact files
rather than through the coordinator. It already is, and through a public route:
`/public/v1/artifacts/{sha256}` resolves bodies from `body_uri` on disk when the
inline column is NULL, verified against the requested sha256, and
`/public/v1/blocks/{block_hash}/settlement-artifacts` links to it. **The
evidence content is public; the `/audit/*` routes are not.** They are internal
tooling over the same data: raw `sats` units, no pagination, full
broadcast-attempt histories, `parent_coinbase_tx_hex` and `manifest_set_json`
broadcast material, and in one case a caller-controlled unbounded window.

So the `PRISM_PUBLIC_ARTIFACT_CACHE_*` knobs move with the extracted service
because `/public/v1/artifacts/{sha256}` moves — which is exactly what the issue
predicted, and it lands without widening the public surface.

### 2. Two nominally public routes take the writer lock today

This is the finding that most changes the shape of the work.

`/public/v1/miners/{recipient_id}` — the most-polled route on the surface —
resolves an owed balance by calling `current_owed_balances()`, which returns
*every* recipient's balance under the writer lock and then filters in Python.
`/public/v1/blocks/{block_hash}/settlement-artifacts` falls through to
`audit_bundle()` under the writer lock for direct-coinbase blocks.

Both must be severed before the extraction is worth anything. Moving the process
without fixing these would move the GIL contention but keep the lock contention.

### 3. Some routes that look public are operator surfaces

The issue predicted the drift count and the health probes. The sweep found more:
`/owed` and `/owed-balances`, the `/payouts/{recipient_id}/status` alias, and
the entire `/audit/*` family. `/miners/{recipient_id}/status` is not even listed
in `PRISM.md`'s endpoint inventory and has no consumer outside tests — it is
legacy, superseded by `/public/v1/miners/{recipient_id}`.

## The three axes

### Audience

- **Public read** — the documented, versioned dashboard contract under
  `/public/v1`.
- **Operator** — process state, or the private audit/ops tooling.
- **Payout-affecting** — live payout obligations: the state the settlement
  machinery itself acts on. Out of scope for extraction by the terms of the
  issue.

### Ledger access

How the response reaches durable state. This decides what may move to a replica,
and it is the axis that matters for the money path.

| Tier | Mechanism | Consequence |
| --- | --- | --- |
| `WRITER_LOCK` | `_operation_gate(self._lock, "writer lock")` in `share_ledger.py` | Serializes against the lease-holding writer. Public volume here is share-ingest backpressure. |
| `READ_SLOT` | `_run_read_json` → `_operation_gate(self._read_semaphore, "read slot")`, bounded by `PRISM_POSTGRES_READ_CONCURRENCY` (default 4) | Primary Postgres, but not the writer lock. Replica-eligible as written. |
| `ARTIFACT_FILE` | The audit artifact directory (`PRISM_AUDIT_DIR`) | Already the durable public record. No database involved. |
| `PROCESS_STATE` | In-memory coordinator counters, gauges, snapshots | Cannot leave the process at all. |
| `QBIT_RPC` | `coordinator.rpc.call(...)` | A node read, not coordinator state. The extracted service gets its own client. |

The writer-lock reads reached from HTTP, verified against the `PsqlShareLedger`
in `lab/prism/share_ledger.py` (`def` line, then the line holding the gate):

| Ledger method | Definition | Gate | Reached by |
| --- | --- | --- | --- |
| `current_owed_balances` | `:4161` | `:4172` | `/owed`, `/owed-balances`, `/miners/{id}/status`, **`/public/v1/miners/{id}`** |
| `carry_forward_integrity_report` | `:4259` | `:4261` | `/audit/carry-forward-integrity` |
| `audit_share_window` | `:4327` | `:4353` | `/audit/share-window` |
| `audit_block_payouts` | `:4365` | `:4383` | `/audit/blocks/{h}/payouts`, `/audit/block/{h}` |
| `recipient_payout_history` | `:4389` | `:4418` | `/miners/{id}/status` |
| `audit_bundle` | `:4906` | `:4927` | `/audit/blocks/{h}/bundle`, **`/public/v1/blocks/{h}/settlement-artifacts`** |
| `audit_bundle_by_commitment` | `:4931` | `:4958` | `/audit/commitments/{leaf}/bundle` |

The SQL these run is read-only — the stored functions they call
(`qbit_current_owed_balances`, `qbit_prism_window`, `qbit_audit_block_fanouts`,
`qbit_fanout_status`, `qbit_audit_share_window`) are declared `STABLE` in
`crates/qbit-prism/sql/001_share_ledger.sql`. The cost is the in-process gate,
not a database lock, which is what makes these replica-eligible once the read is
routed off the writer. No HTTP route on any path performs an `INSERT`, `UPDATE`,
`DELETE` or advisory lock.

`public_api.py` also carries pre-read-model fallbacks that reach
`recipient_payout_history` and `ledger.all_shares()` when a `dashboard_*` method
is absent. On the production `PsqlShareLedger` every read model exists, so those
branches are unreachable; they matter only to the in-memory ledger used when
`PRISM_ALLOW_MEMORY_LEDGER=1`. The extracted service must keep that true, or the
fallbacks would reintroduce writer-lock reads on public routes.

## Operator surface — retained

| Path(s) | Ledger access | Why operator |
| --- | --- | --- |
| `/healthz` | `PROCESS_STATE` | Liveness of this instance: job-build progress, tip refresh, writer-lease state. Answered from a background-refreshed in-memory snapshot (`PRISM_HEALTH_REFRESH_SECONDS`, default 5s), overlaid with live progress on every request so a cached `ok=true` cannot mask a failed refresh. Wired to the compose healthcheck (`compose.yaml`, pinned by `tests/test_prism_compose_profile.py`) and to `make prism-self-check`. |
| `/readyz/mining` | `PROCESS_STATE` | Router-facing mining readiness of this instance (issue #186): whether a load balancer should send new miners here. A latched, hysteretic signal computed by the same background health refresher and served only as a copy of that cache. Distinct from `/healthz` and never public; see [Mining readiness is a separate contract](#mining-readiness-is-a-separate-contract). |
| `/metrics` | `PROCESS_STATE` | Prometheus exposition of this process's counters. Serves only the complete cached snapshot (`PRISM_METRICS_REFRESH_SECONDS`, default 5s). |
| `/audit/latest` | `ARTIFACT_FILE` | The operator's view of the live evidence envelope. Public consumers reach the same evidence content-addressed via `/public/v1/artifacts/{sha256}`. |
| `/audit/share-window` | `WRITER_LOCK` | Raw per-share rows for a caller-supplied anchor. The window is caller-controlled and unbounded. The public equivalent is the aggregated reward leaderboard. |
| `/audit/blocks/{block_hash}/payouts`, `/audit/block/{block_hash}` | `WRITER_LOCK` | Per-recipient payout rows in raw sats with `maturity_state`/`chain_state`. The short form is a legacy alias for the same handler. Public per-miner slice: `/public/v1/miners/{id}/payouts`. |
| `/audit/blocks/{block_hash}/bundle` | `WRITER_LOCK` + `ARTIFACT_FILE` | Full inline bundle plus `coinbase_tx_hex`. The bundle body itself is public, content-addressed. |
| `/audit/commitments/{commitment_leaf_hex}/bundle` | `WRITER_LOCK` + `ARTIFACT_FILE` | The same bundle by commitment leaf. No public commitment-leaf lookup exists. |
| `/audit/blocks/{block_hash}/ctv-fanout-manifest-set` | `READ_SLOT` | Full CTV recovery payload including `parent_coinbase_tx_hex` and `manifest_set_json` — broadcast material. |
| `/audit/blocks/{block_hash}/ctv-fanouts` | `READ_SLOT` | Raw artifact dicts with `fanout_tx_hex` and sats fields. |
| `/audit/fanouts/pending` | `READ_SLOT` | The broadcaster's work queue with full attempt histories. |
| `/audit/fanouts/{fanout_txid}/status` | `READ_SLOT` | Raw status including `submit_result` and attempt detail. |

## Payout-affecting — retained, out of scope

| Path(s) | Ledger access | Why |
| --- | --- | --- |
| `/audit/carry-forward-integrity`, `/audit/ledger-integrity` | `WRITER_LOCK` | The drift count (`current_drift_count`), mismatch list, and the `audit_head_sha256` operators are told to mirror after payout-affecting blocks. Reports whether the payout ledger disagrees with itself. Runs two statements under one held writer gate — the worst route on the surface to expose to uncontrolled volume. |
| `/owed`, `/owed-balances` | `WRITER_LOCK` | Every recipient's outstanding balance: the obligations the next settlement will pay. O(recipients) under the writer lock. There is no public all-recipients dump. |
| `/miners/{recipient_id}/status`, `/payouts/{recipient_id}/status` | `WRITER_LOCK` | The pre-dashboard payout-operations view in raw sats. Both aliases reach one handler. Superseded for public use by `/public/v1/miners/{recipient_id}`, which reports bits. |

## Public read surface — extracted

| Path | Ledger access | Extraction blocker |
| --- | --- | --- |
| `/public/v1/pool-summary` | `READ_SLOT` + `QBIT_RPC` | None. Needs its own qbit RPC client. |
| `/public/v1/blocks` | `READ_SLOT` | None. |
| `/public/v1/leaderboard` | `READ_SLOT` + `QBIT_RPC` | None. `window=reward` needs network difficulty from RPC. |
| `/public/v1/hashrate-series` | `READ_SLOT` | None. The widest ledger scan on the surface, and the strongest single argument for moving this traffic off the primary. |
| `/public/v1/mining-configuration` | none | Falls back to `getattr(coordinator, "port")` / `getattr(coordinator, "bind")` when the public URL env vars are unset. `PRISM_PUBLIC_STRATUM_URL` / `PRISM_STRATUM_PORT` must be supplied explicitly. |
| `/public/v1/miners/{recipient_id}` | `WRITER_LOCK` + `READ_SLOT` + `QBIT_RPC` | `owed_balance_for_recipient()` calls `current_owed_balances()`, a writer-lock read returning every recipient's balance, filtered in Python. Needs a per-recipient owed-balance read that does not fence. |
| `/public/v1/miners/{recipient_id}/earnings` | `READ_SLOT` | None. |
| `/public/v1/miners/{recipient_id}/payouts` | `READ_SLOT` | None. |
| `/public/v1/miners/{recipient_id}/workers` | `READ_SLOT` | None. |
| `/public/v1/blocks/{block_hash}/settlement-artifacts` | `WRITER_LOCK` + `READ_SLOT` + `ARTIFACT_FILE` | The CTV path is read-slot, but a direct-coinbase block falls through to `direct_coinbase_settlement_payload()` → `audit_bundle()` under the writer lock. Take that branch from the artifact file. |
| `/public/v1/fanouts/pending` | `READ_SLOT` | None. |
| `/public/v1/fanouts/{fanout_txid}` | `READ_SLOT` | None. |
| `/public/v1/artifacts/{sha256}` | `READ_SLOT` + `ARTIFACT_FILE` | None. Already resolves the body from `body_uri` on disk when the inline column is NULL. Carries the `PRISM_PUBLIC_ARTIFACT_CACHE_*` knobs. |

## What makes this an extraction rather than a rewrite

`public_api.py` reaches the coordinator through a small, complete surface:

- `coordinator.ledger` — 24 references, 10 direct methods plus 7 duck-typed
  `getattr` read models
- `coordinator.rpc` — 3 references: `getblockchaininfo`, `getblocktemplate`,
  `getnetworkinfo`
- `getattr(coordinator, "port")` and `getattr(coordinator, "bind")`, both with
  environment fallbacks already in place

There is no other coordinator state on the public path. The extracted service
supplies its own object with `.ledger` and `.rpc`, and the dashboard code is
unchanged.

## Completeness

The repository contains exactly one HTTP-serving surface outside tests: the
`AuditHttpFacade` listener in `lab/prism/audit_http.py`, fed by
`_CoordinatorAuditHttp` and `public_api.dispatch`. `make_audit_handler` in
`prism_coordinator.py` is a compatibility factory over the same facade, not a
second surface. The coordinator binds only the Stratum listener(s) and the audit
HTTP port. No Rust crate serves HTTP. Every route string in `tests/`, `test/`,
`docs/` and `doc/` falls inside the union of the routes classified here; the
only exception is a test-only `/control` route in the fake qbit RPC server.

## Staleness is a contract

The precedent is in `lab/prism/observability.py`: the cached `/metrics` endpoint
returns 503 rather than serving an unbounded-stale snapshot.

- `MINIMUM_HEALTH_STALE_SECONDS = 15.0`
- `stale_after = max(3 * refresh_seconds, MINIMUM_HEALTH_STALE_SECONDS)`
- `/healthz` returns 503 with `snapshot_age_seconds` once over budget
- `/metrics` returns `503 if stale else 200` and always emits
  `qbit_prism_metrics_snapshot_age_seconds`, `qbit_prism_metrics_snapshot_stale`
  and `qbit_prism_metrics_snapshot_available`

**Landed.** Each extracted endpoint states the staleness it tolerates, surfaces
the observed age to callers, and refuses rather than serving beyond its budget.
The budgets live in the `max_staleness_seconds` field of `Endpoint` in
`lab/prism/endpoint_registry.py`, derived rather than hand-picked:

```
budget = max(3 * (cache_ttl_seconds + underlying_cache_seconds),
             MINIMUM_PUBLIC_STALE_SECONDS)
```

with `MINIMUM_PUBLIC_STALE_SECONDS = 15`, matching the precedent's floor. The
derivation is documented in that module's docstring and pinned by
`tests/test_prism_endpoint_registry.py`, which asserts the budgets against the
formula and against `public_cache_policy` itself, so a changed cache default
cannot silently leave a budget behind.

Three sources of staleness compound and must all be counted:

1. The shared in-process response cache (`PublicResponseCache`), whose TTL is
   already per-route via `public_cache_policy`. Counted.
2. The pool reward-window aggregate cache in the ledger
   (`_pool_reward_window_aggregate`, `PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS`,
   default 30s), which sits underneath every miner page. Counted, and it is the
   only route where two caches sit in series: `/public/v1/miners/{recipient_id}`
   at 105s versus 15s for the other plain row reads.
3. Replica lag, once a replica exists. Contributes 0 today because no replica
   exists; when one lands it becomes another `underlying_cache_seconds` term
   rather than a new mechanism.

On the wire, every extracted response carries
`X-Prism-Staleness-Budget-Seconds` beside the existing `Age` header. Past the
budget the service returns 503 with `Cache-Control: no-store` and an ordinary
`prism.dashboard.error.v1` body naming both the budget and the observed age —
error code `upstream_unavailable`, already in the documented enum, rather than a
new error schema.

At the documented cache defaults the budgets are 15s for the plain row reads
(`/blocks`, `/leaderboard`, `/miners/{id}/earnings`, `/miners/{id}/payouts`,
`/blocks/{h}/settlement-artifacts`, `/fanouts/pending`, `/fanouts/{txid}`), 90s
for the pool-wide aggregates (`/pool-summary`, `/hashrate-series`,
`/miners/{id}/workers`), 105s for `/miners/{recipient_id}`, and 900s for
`/mining-configuration`.

`/public/v1/artifacts/{sha256}` is the exception, and says so explicitly through
the registry's `immutable_content` flag rather than by omitting a budget: content
is immutable and content-addressed, so a body that hashes to the requested
sha256 is correct at any age. It reports `X-Prism-Staleness-Budget-Seconds:
unbounded` and never refuses for staleness. The only observable staleness is the
404→200 transition for an artifact published moments ago, which no budget would
fix.

## Health is a contract

The coordinator's `/healthz` answers one question: is this instance failing to
deliver work to the miners it has admitted? It fails closed (HTTP 503, with
`initial-delivery-stalled` in `unhealthy_reasons`) for two conditions, both
measured against `PRISM_STRATUM_INITIAL_JOB_TIMEOUT_SECONDS`:

- **Genuine first-job starvation.** An authorized miner has waited the full
  deadline without any delivered work
  (`oldest_genuinely_pending_initial_job_age_seconds`).
- **Sustained semantic current-work loss.** Fewer than 95% of authorized miners
  hold work matching the current template fingerprint, payout generation, and
  their current connection, authorization, and difficulty generations
  (`semantic_current_work_ratio`) for longer than the deadline
  (`semantic_current_work_gap_age_seconds`), with no exact-tip work covering
  the gap either.

Exact-tip churn by itself stays diagnostic. When the observed tip advances
faster than fanout, `current_tip_job_coverage` drops and
`current_tip_coverage_gap_age_seconds` grows while miners still hold the latest
published work; the 2026-08-31 incident read 4 of 33 miners on the exact tip
with all 33 semantically current. Those fields keep reporting the churn, and
exact coverage still corroborates `overload` and the
`connection-capacity-saturated` and `stale-unknown-rejection-storm` reasons,
each of which needs an independent pressure signal. It no longer fails health
on its own (issue #216).

Router-facing mining readiness, whether a load balancer should send new miners
to this instance, is a distinct signal. It is not derivable from `/healthz`,
and it has its own route and contract below.

## Mining readiness is a separate contract

`GET /readyz/mining` (issue #186) answers a different question from `/healthz`:
not "is this process alive and delivering right now?" but "should a router keep
sending *new* miners here?". The two deliberately disagree at the edges.
`/healthz` is quick to fail and re-reads live progress on every request, which
is right for a liveness probe and wrong for a routing decision: every accepted
tip sweeps semantic coverage `0 -> partial -> 1 -> 0` for roughly the
tip-refresh cycle, and a router that followed that would move traffic on every
block. `/readyz/mining` is latched with hysteresis instead.

### Ownership

- **Policy** lives in `lab/prism/progress_health.py` as
  `MiningReadinessTracker`: a pure, scripted-clock state machine with two
  states (`ready`, `degraded`), two independent timers, and the closed reason
  vocabulary `MINING_READINESS_REASONS`. It keeps no history that grows with
  uptime: the two streak stamps and the last preview-timeout counter sample.
- **Sampling and the cache** live in `lab/prism/observability.py`, the same
  owner that caches `/healthz`. `refresh_health_snapshot` builds one
  `MiningReadinessSample` from the delivery snapshot and progress mapping it
  has already computed for that refresh, observes it, and publishes the
  returned immutable `MiningReadinessSnapshot` under the owner lock together
  with the base health snapshot.
- **Configuration** is loaded in `lab/prism/coordinator_config.py` into
  `LifecycleConfig` and handed to the observability owner by the coordinator
  adapter. `prism_coordinator.py` carries no readiness policy: it supplies the
  config, the preview-timeout counter read, and the HTTP wrapper.
- **The route** is classified in `lab/prism/endpoint_registry.py` as
  `OPERATOR` / `RETAIN` / `PROCESS_STATE` and dispatched by
  `lab/prism/audit_http.py`. `tests/test_prism_endpoint_registry.py` pins that
  it is absent from `extracted_paths()` and from every `/public/v1` literal.

### Cache and lock behaviour

The request path copies the published snapshot under the observability owner
lock, reads the monotonic clock, and returns. It never takes the coordinator
lock, the job-delivery or candidate-processing locks, and never queries the
ledger; `tests/test_prism_observability.py` drives the route against a
coordinator whose lock records any acquisition and whose ledger raises on any
attribute, and asserts a `200`.

Every input is sampled by a background refresher:

| Input | Sampled by | Source |
| --- | --- | --- |
| `semantic_current_work_ratio` | health refresher | the delivery snapshot already computed for `/healthz` (WP1's semantic gauge) |
| `pending_refresh`, `pending_refresh_age_seconds`, `refresh_pending_too_long`, `eligible_clients_requiring_refresh` | health refresher | the progress-health mapping already computed for `/healthz` |
| accepted-parent preview timeout counter | health refresher | one short hold of the coordinator lock, the same copy `/metrics` takes |
| oldest durable candidate age | **metrics refresher** | the Postgres pending-candidate aggregate is fenced behind the writer lock, which the health refresher must never wait on (a block landing holds it for up to the landing budget). The metrics renderer already pays for that read every cycle and hands the value to the observability owner (`record_oldest_durable_candidate_age`); the next health refresh reads the last one. The gauge's `-1` (unavailable) crosses over as `null`. |

Before the first complete health refresh there is no snapshot and the route
answers a fail-closed `503` with `reasons: ["warming_up"]`. Unlike `/healthz`,
there is no inline fallback when no refresher is running: readiness is never
sampled on a request thread. A failed refresh leaves the last snapshot in
place. The response carries `sample_age_seconds` and `sample_stale`
(`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15s)`, the `/healthz` budget) as
diagnostics; the latched state is served either way so a wedged refresher
cannot make the signal flap, and a consumer that wants to treat a stale sample
as not-ready has the facts to.

### Hysteresis

- **Entry condition**: semantic coverage below `0.95`, or the progress
  snapshot reporting `refresh_pending_too_long`. An ordinary in-budget pending
  refresh is not an entry condition.
- **Recovery condition**: semantic coverage at least `0.99`, no
  `refresh_pending_too_long`, no eligible client still requiring refresh, and
  no pending refresh at all.
- The entry condition must hold continuously for
  `PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS` (default `60`) before the
  state changes to `degraded`; the recovery condition must hold continuously
  for `PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS` (default `240`) before
  it returns to `ready`. Any contrary sample resets the timer in progress. The
  state changes exactly once per sustained episode and `state_age_seconds`
  counts from that change. Ready is the initial latched state at the first
  sample.
- The defaults are set by the two incidents: `60s` exceeds the 2026-08-21
  normal tip-refresh cycle (pending age peaked near `36.77s`), and `240s`
  exceeds the 213-second 2026-08-20 oscillation between the apparent healthy
  point at 20:10:27Z and stability at 20:14:00Z. Both traces are replayed on
  a scripted clock in `tests/test_prism_progress_health.py`, each with a
  counterfactual pin showing the shorter window flapping.
- Startup validates both knobs as finite, nonnegative numbers with the
  recovery window at least the entry dwell. The coverage thresholds and the
  `60s` old-candidate age are named constants, not knobs.
- The oldest durable candidate age (`>= 60s`, the documented warning alert on
  `qbit_prism_block_candidate_oldest_pending_seconds`) and the accepted-parent
  preview timeout rate annotate a degraded or recovering snapshot's `reasons`
  and are always reported as fields. Neither starts, extends, or blocks a
  transition.

### Metrics

`/metrics` renders `qbit_prism_mining_ready` (`0`/`1`) and the fixed-cardinality
`qbit_prism_mining_readiness_reason{reason=...}` gauge from the same cached
snapshot, so a scrape and the route always agree; `warming_up` reads `1` until
the first complete refresh. The families are part of the frozen render
reference in `tests/test_prism_metrics.py`.

### Distinction from the public tier

The extracted public read service serves its own `/healthz` and `/metrics`
describing *that* process. `/readyz/mining` names the coordinator instance
behind the router, is answered from the coordinator's process state, and is
listed as private/internal alongside `/healthz`. It must not be exposed
through the public tier or a public reverse proxy. The exact consumer contract
for qbit-tools is in [`router-integration-notes.md`](router-integration-notes.md);
the router implementation itself is out of scope here.

## Out of scope

Anything payout-affecting stays on the coordinator: the writer lease, share
acknowledgement, block landing, settlement, and the surfaces listed above. The
extracted service is read-only — it opens no write path to the ledger and holds
no lease. That is now structural rather than aspirational: it is constructed
without any `PRISM_LEDGER_WRITER_*` input, receives none of the signing or
block-submission configuration in compose, mounts the audit artifact volume
`:ro`, and refuses to start on the in-memory ledger (whose missing `dashboard_*`
read models would send `public_api`'s fallbacks back onto writer-lock reads).
`tests/test_prism_public_read_service.py` drives every extracted route against a
ledger that raises if any writer-lock read is reached.

The two blockers named above are removed. `dashboard_miner_owed_balance_bits`
and `dashboard_direct_coinbase_settlement` on `PsqlShareLedger` answer both
formerly-fenced public reads through `_run_read_json`, and `public_api` prefers
them through the same duck-typed pattern used by every other read model, keeping
the old paths as in-memory-ledger fallbacks.

Replica wiring is tracked separately. It needs `wal_level`, a replication slot,
and a standby service in the Postgres setup; today `prism-postgres` has a
dedicated WAL volume (`POSTGRES_INITDB_WALDIR`) but no replication
configuration. Until that lands, the extracted service reads the primary — a
separate process with its own connection pool, which already removes the GIL
contention, but not yet a separate database.
