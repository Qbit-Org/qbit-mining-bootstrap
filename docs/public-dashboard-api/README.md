# PRISM Public Dashboard API Contract

This directory contains the public dashboard API contract shared by the PRISM
pool service and dashboard frontends.

The contract is source-of-truth for both sides:

- `../public-dashboard-api-v1.openapi.yaml` defines `/public/v1` endpoints.
- `fixtures/*.json` are mock responses the dashboard can render before a live
  backend exists.
- `tests/test_public_dashboard_api_contract.py` keeps the fixtures and public
  naming conventions from drifting.

## Architecture

The source-of-truth public dashboard API belongs with the pool software in this
repo. It is closest to the PRISM ledger, pool block records, payout state, and
qbit RPC data needed to compute trustworthy read models.

The `prism-dashboard` repo should own only the presentation layer:

- routes, charts, tables, styling, and responsive UI
- fixture-backed development before a live pool is available
- optional static/SSR serving, config loading, or reverse-proxy behavior

The dashboard app must not query Postgres, qbit RPC, private command sockets, or
internal audit endpoints directly. Its only stable data dependency should be the
sanitized `/public/v1` API described by this contract.

In deployment, `/public/v1` is served by its own process — the
`prism-public-api` service (`python3 -m lab.prism.public_read_service`), on
`PRISM_PUBLIC_API_PORT` (default `3342`). It is no longer served by the
`prism-coordinator` audit HTTP listener, which now answers only `/audit/*`,
`/healthz`, `/metrics`, `/owed*`, and the operator miner/payout status routes; a
`/public/v1` request to the coordinator returns its ordinary
`{"error": "unknown endpoint"}` 404.

The split exists because public read traffic scales with public interest rather
than with hashrate. Served in-process it shared the GIL that acknowledges shares
and lands blocks, and the primary Postgres connection the lease-holding writer
commits through. The extracted tier reads through bounded read slots only, never
acquires a writer lease, and depends on Postgres rather than on the coordinator,
so it keeps serving across coordinator restarts.

Operators can expose only that path from the pool service, or place a
dashboard/web proxy in front of it. The ownership boundary stays the same: pool
read models live here; dashboard rendering lives outside the pool process.
`prism-public-api` also serves its own `/healthz` and `/metrics` for that
process; those are operator surfaces and must not be exposed publicly.

## Caching

Successful `GET /public/v1` responses are safe to cache briefly. The service
emits conservative browser caching (`Cache-Control: public, max-age=0,
must-revalidate`) plus shared-cache headers for CDNs such as Vercel. Dynamic
dashboard read models default to a 5-second shared-cache TTL with 30 seconds of
`stale-while-revalidate`. The pool-wide aggregate read models —
`GET /public/v1/pool-summary`, `GET /public/v1/hashrate-series`, and
`GET /public/v1/miners/{recipient_id}/workers` — are expensive to recompute and
default to a 30-second shared-cache TTL instead.
`GET /public/v1/mining-configuration` defaults to 300 seconds, and
content-addressed artifact routes default to 86400 seconds with an immutable
shared-cache hint.

Operators can tune the defaults with:

- `PRISM_PUBLIC_CACHE_ENABLED`
- `PRISM_PUBLIC_CACHE_TTL_SECONDS`
- `PRISM_PUBLIC_CACHE_STALE_WHILE_REVALIDATE_SECONDS`
- `PRISM_PUBLIC_AGGREGATE_CACHE_TTL_SECONDS`
- `PRISM_PUBLIC_AGGREGATE_CACHE_STALE_WHILE_REVALIDATE_SECONDS`
- `PRISM_PUBLIC_CONFIG_CACHE_TTL_SECONDS`
- `PRISM_PUBLIC_CONFIG_CACHE_STALE_WHILE_REVALIDATE_SECONDS`
- `PRISM_PUBLIC_ARTIFACT_CACHE_TTL_SECONDS`
- `PRISM_PUBLIC_ARTIFACT_CACHE_STALE_WHILE_REVALIDATE_SECONDS`
- `PRISM_PUBLIC_CACHE_MAX_ENTRIES`
- `PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES`
- `PRISM_PUBLIC_CACHE_DEBUG_HEADERS`

The public read service also keeps a small in-process origin cache keyed by
normalized path and query string, and coalesces concurrent misses for the same
key. Error responses use `Cache-Control: no-store` and are not cached by that
origin cache.
Miner pages additionally share one briefly cached pool-wide reward-window
aggregate (`PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS`, default 30 seconds, 0
disables), so requests for different miners reuse a single recursive
reward-window scan instead of each re-running it.

## Staleness

Every `/public/v1` response states how old an answer that route is willing to
serve, and refuses rather than serve past it. A silently stale dashboard is
worse than an honest one.

- `X-Prism-Staleness-Budget-Seconds` — the budget for this route, in seconds.
  `/public/v1/artifacts/{sha256}` reports `unbounded`: its content is
  content-addressed and immutable, so a body that hashes to the requested
  sha256 is correct at any age and this route never refuses for staleness.
- `Age` — the observed age of the response actually served, as before.

When the observed age exceeds the budget, the service returns **503** with
`Cache-Control: no-store` and an ordinary `prism.dashboard.error.v1` body whose
message names both the budget and the observed age. Clients should treat this
as "the data behind this route is too old to answer with", not as a new error
schema — the error code is `upstream_unavailable`, already in the documented
enum.

Budgets are derived from the caches that sit under each route rather than
hand-picked:

```
budget = max(3 * (cache_ttl_seconds + underlying_cache_seconds), 15)
```

`cache_ttl_seconds` is the route's own shared-response TTL and
`underlying_cache_seconds` is any second cache stacked beneath it — today only
the pool reward-window aggregate (`PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS`,
default 30s) under `/public/v1/miners/{recipient_id}`. The factor of three and
the 15-second floor match the existing precedent for PRISM's cached `/metrics`
endpoint. At the documented defaults this yields:

| Route | Budget |
| --- | --- |
| `/public/v1/blocks` | 15s |
| `/public/v1/leaderboard` | 15s |
| `/public/v1/miners/{recipient_id}/earnings` | 15s |
| `/public/v1/miners/{recipient_id}/payouts` | 15s |
| `/public/v1/blocks/{block_hash}/settlement-artifacts` | 15s |
| `/public/v1/fanouts/pending` | 15s |
| `/public/v1/fanouts/{fanout_txid}` | 15s |
| `/public/v1/pool-summary` | 90s |
| `/public/v1/hashrate-series` | 90s |
| `/public/v1/miners/{recipient_id}/workers` | 90s |
| `/public/v1/miners/{recipient_id}` | 105s |
| `/public/v1/mining-configuration` | 900s |
| `/public/v1/artifacts/{sha256}` | unbounded |

The budgets are constants derived from the documented cache defaults — the
`max_staleness_seconds` values in `lab/prism/endpoint_registry.py` — and there
is no environment knob that raises a budget. An operator who raises one of the
cache TTL knobs that do exist (`PRISM_PUBLIC_CACHE_TTL_SECONDS`,
`PRISM_PUBLIC_AGGREGATE_CACHE_TTL_SECONDS`,
`PRISM_PUBLIC_CONFIG_CACHE_TTL_SECONDS`, or
`PRISM_PUBLIC_REWARD_WINDOW_CACHE_SECONDS`) above its route's budget will see
that route begin refusing with 503 rather than quietly serving older data.
Raising a TTL past its budget therefore also requires changing the registry
constant in the same change; otherwise leave the defaults alone. The
`PRISM_PUBLIC_ARTIFACT_CACHE_*` TTLs are exempt: the artifact route is
content-addressed and never refuses for staleness.

## Conventions

- Base path: `/public/v1`.
- Responses are JSON with a top-level `schema` tag.
- Timestamps are UTC ISO-8601 strings.
- Hashes are lowercase hex strings.
- Bits are JSON integers.
- Exact large numeric values are decimal strings. This includes share
  difficulty, network difficulty, window weights, percentages, and hashrates.
- Hashrate values use terahashes per second and are named `*_ths`.
- Pagination uses 1-based `page`, bounded `limit`, `total_count`, and
  `total_pages`.
- Optional fields are present as `null` when unavailable, so dashboard layout
  can remain stable.

## Reward Leaderboard

`GET /public/v1/leaderboard?window=reward` returns
`prism.dashboard.leaderboard.v2`, ranked by each recipient's counted work in the
live PRISM reward window. The window contains the newest eligible accepted
shares totaling `8 * network_difficulty`; if the oldest share crosses the
boundary, only the needed part of its difficulty is counted. The response
therefore exposes both requested and counted window weight, the observed share
count and wall-clock span, and whether enough work exists to complete the
window.

This is a work window, not a fixed time period. qbit's permissionless lane has a
75-second block target, so the nominal duration at 100% of that lane's hashrate
is `8 * 75 seconds = 600 seconds` (10 minutes). For a pool with fraction `p` of
the permissionless hashrate, its expected duration is `600 / p` seconds—also
eight times that pool's expected time to find a permissionless block. Actual
duration varies with share arrival, vardiff, and pool hashrate. It is unrelated
to the separate coinbase-maturity delay.

The live endpoint uses the snapshot time and current permissionless network
difficulty. A found block instead freezes eligibility at that block job's issue
time and uses the difficulty committed for that job, so the live view is a
prospective estimate rather than a reconstruction of a past payout. During
startup collection mode, a solved collection job pays its solver directly; the
collected ledger shares enter the next ready block's work window.

Live reward calculations require authoritative compact target bits from qbit's
block template or blockchain status. If neither source supplies valid bits, the
pool summary, miner detail, and reward leaderboard return `503` instead of
inventing a difficulty, reward split, or block-time estimate.

For reward responses, `search` and exact `recipient_id` filters are mutually
exclusive. Both are applied after the complete pool window has been grouped and
ranked, so returned ranks and pool totals stay global. `recipient_id` is rejected
when `window` is omitted or set to `3h`; those requests otherwise retain the
legacy `prism.dashboard.leaderboard.v1` response during rollout.

## Settlement Artifacts

PRISM settlement is not just a stats UI. When payouts route through CTV fanouts,
miners and third parties need enough public information to verify the payout and
broadcast the fanout transaction if the pool broadcaster is unavailable.

The public dashboard API therefore includes:

- `GET /public/v1/blocks/{block_hash}/settlement-artifacts`
- `GET /public/v1/fanouts/pending`
- `GET /public/v1/fanouts/{fanout_txid}`
- `GET /public/v1/artifacts/{sha256}`

These responses are dashboard-safe wrappers around public settlement artifacts.
Wrapper field names use public `*_bits` units. Exact canonical artifacts, such
as PRISM audit bundles and CTV manifest JSON, are linked by URL and SHA-256 so
they can be mirrored or downloaded without making dashboard clients depend on
internal audit routes.

`GET /public/v1/artifacts/{sha256}` is content-addressed: for audit bundles,
CTV fanout manifests, and manifest sets the response body is the exact canonical
byte sequence the artifact hash was computed over, so
`sha256(response body) == {sha256}` verifies the download with no
re-serialization step. Audit bundles written before canonical-byte persistence
was introduced retain the legacy reconstructed response until the verified
backfill publishes their canonical artifact.

On a read-replica deployment, the ledger row remains the visibility authority.
If the row has replayed but the canonical file is absent, the service uses the
legacy reconstructed response. If shared storage receives the file before the
replica replays its row, the artifact returns a non-cacheable `404` until replay
catches up; the file alone never exposes an uncommitted artifact. This immutable
route remains exempt from the ordinary freshness refusal. Replica readiness is
bounded by the existing WAL-receiver heartbeat contract, not by replay-lag
position.

Direct-coinbase blocks return the same settlement-artifacts wrapper with
`settlement_mode: direct_coinbase` and `fanouts: []`. A `404` means no public
settlement artifact index is known for that block, not an implied direct
coinbase settlement.

The public API must not expose `/audit/*`, `/metrics`, `/healthz`, operator
controls, raw private sockets, credentials, or unrestricted internal manifests.

## Miner Detail Tables

The miner summary endpoint is intentionally small enough for top cards. It may
embed short worker and payout previews, capped at five rows each. The Ocean-style
detail tables are separate paginated read models:

- `GET /public/v1/miners/{recipient_id}/earnings`
- `GET /public/v1/miners/{recipient_id}/payouts`
- `GET /public/v1/miners/{recipient_id}/workers`

This keeps long earnings, payout, and worker histories out of the summary
payload while still allowing the dashboard to render full stat pages.

## Mining Configuration

`GET /public/v1/mining-configuration` provides public pool fee, template policy,
and Stratum endpoint metadata for a dashboard configuration or "next block" tab.
When `PRISM_STRATUM_HIGHDIFF_PORT` enables the rental-scale high-diff listener,
the response includes a second `stratum_endpoints` entry. Set
`PRISM_PUBLIC_STRATUM_HIGHDIFF_URL` when the externally advertised URL differs
from the primary `PRISM_PUBLIC_STRATUM_URL` host/scheme plus the high-diff
listener port.

## Deferred Surfaces

Ocean exposes server-rendered template fragments and CSV/report download routes
for its own frontend. Those are not required public API surfaces for PRISM
dashboard v1. Template fragments are an Ocean implementation detail, and CSV
exports can be generated from the paginated JSON read models or added later as a
thin convenience layer without changing the core dashboard contract.
