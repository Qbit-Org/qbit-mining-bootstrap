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

Each extracted endpoint states the staleness it tolerates, surfaces the observed
age to callers, and refuses rather than serving beyond its budget. The
per-endpoint budgets are defined with the extraction.

Three sources of staleness compound and must all be counted:

1. The shared in-process response cache (`PublicResponseCache`), whose TTL is
   already per-route via `public_cache_policy`.
2. The pool reward-window aggregate cache in the ledger
   (`_pool_reward_window_aggregate`, `PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS`,
   default 30s), which sits underneath every miner page.
3. Replica lag, once a replica exists.

`/public/v1/artifacts/{sha256}` is the exception: content is immutable and
content-addressed, so staleness is harmless except at the 404→200 transition for
an artifact published within the budget.

## Out of scope

Anything payout-affecting stays on the coordinator: the writer lease, share
acknowledgement, block landing, settlement, and the surfaces listed above. The
extracted service is read-only — it opens no write path to the ledger and holds
no lease.

Replica wiring is tracked separately. It needs `wal_level`, a replication slot,
and a standby service in the Postgres setup; today `prism-postgres` has a
dedicated WAL volume (`POSTGRES_INITDB_WALDIR`) but no replication
configuration. Until that lands, the extracted service reads the primary — a
separate process with its own connection pool, which already removes the GIL
contention, but not yet a separate database.
