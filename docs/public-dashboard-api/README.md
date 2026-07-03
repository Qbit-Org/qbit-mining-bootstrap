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

In deployment, the `prism-coordinator` audit HTTP listener serves `/public/v1`.
Operators can expose only that path from the pool service, or place a
dashboard/web proxy in front of it. The ownership boundary stays the same: pool
read models live here; dashboard rendering lives outside the pool process.

## Caching

Successful `GET /public/v1` responses are safe to cache briefly. The coordinator
emits conservative browser caching (`Cache-Control: public, max-age=0,
must-revalidate`) plus shared-cache headers for CDNs such as Vercel. Dynamic
dashboard read models default to a 5-second shared-cache TTL with 30 seconds of
`stale-while-revalidate`. `GET /public/v1/mining-configuration` defaults to 300
seconds, and content-addressed artifact routes default to 86400 seconds with an
immutable shared-cache hint.

Operators can tune the defaults with:

- `PRISM_PUBLIC_CACHE_ENABLED`
- `PRISM_PUBLIC_CACHE_TTL_SECONDS`
- `PRISM_PUBLIC_CACHE_STALE_WHILE_REVALIDATE_SECONDS`
- `PRISM_PUBLIC_CONFIG_CACHE_TTL_SECONDS`
- `PRISM_PUBLIC_CONFIG_CACHE_STALE_WHILE_REVALIDATE_SECONDS`
- `PRISM_PUBLIC_ARTIFACT_CACHE_TTL_SECONDS`
- `PRISM_PUBLIC_ARTIFACT_CACHE_STALE_WHILE_REVALIDATE_SECONDS`
- `PRISM_PUBLIC_CACHE_MAX_ENTRIES`
- `PRISM_PUBLIC_CACHE_MAX_RESPONSE_BYTES`
- `PRISM_PUBLIC_CACHE_DEBUG_HEADERS`

The coordinator also keeps a small in-process origin cache keyed by normalized
path and query string, and coalesces concurrent misses for the same key. Error
responses use `Cache-Control: no-store` and are not cached by that origin cache.

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

## Deferred Surfaces

Ocean exposes server-rendered template fragments and CSV/report download routes
for its own frontend. Those are not required public API surfaces for PRISM
dashboard v1. Template fragments are an Ocean implementation detail, and CSV
exports can be generated from the paginated JSON read models or added later as a
thin convenience layer without changing the core dashboard contract.
