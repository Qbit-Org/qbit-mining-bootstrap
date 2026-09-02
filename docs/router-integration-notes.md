# Router Integration Notes

These notes capture public operator guidance for placing a router or hash
aggregator in front of the qbit permissionless ckpool path.

## Compatibility Baseline

- Route permissionless qbit miners to ckpool's Stratum listener.
- Keep qbit payout usernames chain-correct; public qbit chains require P2MR
  payout addresses.
- Point ckpool at qbit RPC, not qbit P2P. Mainnet qbit RPC is `8352`; mainnet
  qbit P2P is `8355`.
- Preserve qbit's returned block version unless the miner negotiated version
  rolling inside the configured mask.

## Version Rolling

The bootstrap ckpool image starts with `CKPOOL_VERSION_MASK_MODE=dynamic`. At
startup it asks qbitd for `getblocktemplate` and uses
`versionrollingmask` when the connected node exposes it. Older qbitd builds
fall back to the configured `CKPOOL_VERSION_MASK`; the public sample env uses
`1fffe000` to match current qbitd permissionless templates.

Routers that negotiate BIP310 should only pass miner-controlled version bits
inside the mask granted by ckpool. If a miner requests a mask, the effective
mask is the intersection of the miner's requested mask and qbit's configured
mask. If qbitd returns `versionrollingmask=00000000`, routers should treat
version rolling as disabled for that upstream.

## Vardiff

ckpool caps vardiff at current network difficulty. Under qbit regtest, the
patched ckpool path floors network difficulty at `1/256`, so Stratum
difficulty is expected to stay at `0.00390625` even when a CPU miner solves
many local blocks. Treat that as a regtest artifact.

For signet or mainnet-like deployments, set `CKPOOL_MINDIFF`,
`CKPOOL_STARTDIFF`, and optionally `CKPOOL_MAXDIFF` based on expected worker
hashrate and desired share volume. Do not reuse regtest share-difficulty
defaults for production-facing routers without retuning. Bootstrap now fails
closed on non-regtest chains when the required min/start difficulty values are
missing.

## Username Handling

Use the qbit payout address as the leading username segment. If the router adds
worker identity, append it after a separator that the router can parse
consistently, for example:

```text
<qbit-payout-address>.<worker-id>
```

Validate malformed, empty, overlong, or control-character worker suffixes at
the router boundary before forwarding to ckpool.

## PRISM Coordinator Mining Readiness

A qbit-tools router deciding whether to send new miners to a PRISM coordinator
instance polls that instance's operator audit HTTP listener
(`PRISM_AUDIT_BIND` / `PRISM_AUDIT_PORT`) for `GET /readyz/mining`. This is the
signal issue #186 adds; it is distinct from `/healthz`, which is process
liveness and deliberately quick to fail, and it is never served by the public
read tier. The router implementation itself is out of scope for this document;
what follows is the contract it consumes.

### Status and body

| HTTP status | `ready` | `state` | Meaning |
| --- | --- | --- | --- |
| `200` | `true` | `ready` | Send new miners here. |
| `503` | `false` | `degraded` | Do not send new miners here. Also the fail-closed answer before the first complete background sample (`reasons: ["warming_up"]`). |

Route on the status code or on `ready`; they never disagree. The body is JSON
with `Cache-Control: no-store` and `schema: "qbit.prism.mining-readiness.v1"`:

| Field | Type | Meaning |
| --- | --- | --- |
| `ready` | boolean | The latched decision. |
| `state` | `"ready"` or `"degraded"` | The latched state. |
| `reasons` | array of strings | Fixed vocabulary, in vocabulary order: `warming_up`, `semantic_coverage_low`, `refresh_pending_too_long`, `refresh_pending`, `recovery_window_pending`, `durable_candidate_old`, `accepted_parent_preview_timeouts`. Diagnostic; never a routing input. |
| `state_age_seconds` | number | Monotonic seconds since the last state change (`0` while warming up). Grows between polls. |
| `transitions` | integer | State changes since process start. |
| `entry_streak_seconds` | number | How long the entry condition has held continuously while `ready` (`0` otherwise). |
| `recovery_streak_seconds` | number | How long the recovery condition has held continuously while `degraded` (`0` otherwise). |
| `semantic_current_work_ratio` | number or `null` | Fraction of authorized miners holding work for the current template fingerprint and payout generation at the last sample (`1.0` with no miners). `null` while warming up. |
| `refresh_pending` | boolean or `null` | A template/payout refresh is outstanding. |
| `refresh_pending_age_seconds` | number or `null` | Age of that outstanding refresh. |
| `refresh_pending_too_long` | boolean or `null` | The progress-health deadline (`PRISM_HEALTH_PENDING_REFRESH_MAX_AGE_SECONDS`) is exceeded. |
| `eligible_clients_requiring_refresh` | integer or `null` | Eligible miners still owed the current work. |
| `oldest_durable_candidate_age_seconds` | number or `null` | Age of the oldest durable pending block candidate as last read by the metrics refresher; `null` when unavailable. |
| `accepted_parent_preview_timeout_rate_per_second` | number | Rate of child job builds timing out waiting for an accepted-parent payout preview, from consecutive samples of the process counter; `0` on the first sample. |
| `entry_dwell_seconds`, `recovery_window_seconds` | number | The configured windows in effect. |
| `sample_age_seconds` | number or `null` | Age of the cached sample the answer was copied from. |
| `sample_stale` | boolean | `sample_age_seconds` exceeds `sample_stale_after_seconds` (`max(3 * PRISM_HEALTH_REFRESH_SECONDS, 15)`). The latched state is served regardless; a router may choose to treat a stale sample as not ready. |

### Reasons

While `ready`, `reasons` lists the entry conditions currently being dwelled on
(`semantic_coverage_low` at coverage below `0.95`,
`refresh_pending_too_long`), so an operator can see a dwell counting before it
lands. While `degraded`, it lists what currently blocks recovery
(`semantic_coverage_low` at coverage below `0.99`, `refresh_pending_too_long`,
or `refresh_pending` for an in-budget refresh or a miner still owed work), or
`recovery_window_pending` when nothing blocks it and the window is counting,
plus two annotations that never cause a transition: `durable_candidate_old`
(oldest durable candidate at least `60s`) and
`accepted_parent_preview_timeouts` (a nonzero timeout rate). `warming_up`
appears alone, only before the first sample.

### Hysteresis expectations

- The signal enters `degraded` only after the entry condition (semantic
  coverage below `0.95`, or `refresh_pending_too_long`) has held continuously
  for `PRISM_MINING_READINESS_ENTRY_DWELL_SECONDS` (default `60`).
- It returns to `ready` only after the recovery condition (coverage at least
  `0.99`, no `refresh_pending_too_long`, no miner requiring refresh, no
  pending refresh) has held continuously for
  `PRISM_MINING_READINESS_RECOVERY_WINDOW_SECONDS` (default `240`).
- Any contrary sample resets the timer in progress. Each sustained episode
  produces exactly one transition; `state_age_seconds` is monotonic between
  transitions.
- The normal accepted-tip cycle (coverage `0 -> partial -> 1 -> 0`, refresh
  resolving within roughly `37s`) never changes the state. The 2026-08-20
  incident degrades once and recovers `240s` after real stability, not at the
  first apparently healthy poll.
- Both windows are validated at startup as finite, nonnegative numbers with
  the recovery window at least the entry dwell; the coordinator refuses to
  start otherwise.

### Polling

Samples are taken by the coordinator's background health refresher every
`PRISM_HEALTH_REFRESH_SECONDS` (default `5`); polling faster than that returns
the same cached answer. Every answer is a copy of that cache: the request
never takes a coordinator lock or reads the ledger, so polling is cheap under
load. Expect `503` with `warming_up` from listener bind until the first
complete refresh, which on a large ledger can take minutes; treat it as
not-ready, not as an error. The same signal is exported as
`qbit_prism_mining_ready` (`0`/`1`) and
`qbit_prism_mining_readiness_reason{reason="..."}` on `/metrics`.

## Operational Checks

- Confirm `getblocktemplate '{"rules":["segwit"]}'` succeeds before admitting
  miners.
- Confirm submitted blocks are accepted by qbit through `submitblock`.
- Exercise reconnect storms and ensure miners receive a fresh notify after a
  qbit tip change.
- Alert on slow clean-job propagation from qbit tip changes to miner notify.
- Keep RPC credentials deployment-specific, and keep published qbit RPC ports
  loopback-only unless the deployment has explicit firewalling and auth.
